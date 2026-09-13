/* PHP embed SAPI module. Targets PHP 7.4-8.5.
 *
 * Deliberately not php_embed_init()/php_embed_shutdown(), which bundle SAPI,
 * module and one request together; a worker serves many, so module start-up
 * happens once and each script gets its own request startup/shutdown pair.
 *
 * Mutates named fields on libphp's own php_embed_module rather than building
 * a sapi_module_struct, the field names having been stable since 7.4. Only
 * the genuine signature differences are version-guarded. */

#include "proteus_php_mod.h"

#include <sapi/embed/php_embed.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <syslog.h>
#include <time.h>
#include <unistd.h>

/* log_message is always passed a real syslog(3) priority, never an arbitrary
 * int, so it maps cleanly onto the Rust side's level names. */
static const char *proteus_php_mod_level_name(int syslog_type_int) {
    switch (syslog_type_int) {
        case LOG_EMERG:
        case LOG_ALERT:
        case LOG_CRIT:
        case LOG_ERR: return "ERROR";
        case LOG_WARNING: return "WARN";
        case LOG_DEBUG: return "DEBUG";
        default: return "INFO"; /* LOG_NOTICE/LOG_INFO + anything unexpected */
    }
}

/* One JSON line on stderr, matching the Rust side's own log format so both
 * share one ingestion pipeline. */
static void proteus_php_mod_log_json(const char *type, const char *level, const char *message) {
    char buf[4096];
    size_t pos = 0;
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);

    /* Must match the Rust side's own timestamp format, which is not
     * configurable there. */
    struct tm tm_utc;
    gmtime_r(&ts.tv_sec, &tm_utc);
    char time_buf[32];
    strftime(time_buf, sizeof(time_buf), "%Y-%m-%dT%H:%M:%S", &tm_utc);

    int n = snprintf(buf, sizeof(buf),
                      "{\"timestamp\":\"%s.%06ldZ\",\"type\":\"%s\",\"level\":\"%s\",\"pid\":%d,\"message\":\"",
                      time_buf, ts.tv_nsec / 1000, type, level, (int) getpid());
    pos = (n > 0 && (size_t) n < sizeof(buf)) ? (size_t) n : sizeof(buf) - 1;

    for (const unsigned char *p = (const unsigned char *) message;
         *p != '\0' && pos < sizeof(buf) - 8;
         p++) {
        if (*p == '"' || *p == '\\') {
            buf[pos++] = '\\';
            buf[pos++] = (char) *p;
        } else if (*p == '\n') {
            buf[pos++] = '\\';
            buf[pos++] = 'n';
        } else if (*p == '\r') {
            buf[pos++] = '\\';
            buf[pos++] = 'r';
        } else if (*p == '\t') {
            buf[pos++] = '\\';
            buf[pos++] = 't';
        } else if (*p < 0x20) {
            pos += (size_t) snprintf(buf + pos, sizeof(buf) - pos, "\\u%04x", *p);
        } else {
            buf[pos++] = (char) *p;
        }
    }

    /* `<=` because the escape loop's bound leaves pos exactly at this
     * boundary; `<` would ship an unterminated JSON fragment. */
    if (pos <= sizeof(buf) - 3) {
        buf[pos++] = '"';
        buf[pos++] = '}';
        buf[pos++] = '\n';
    }
    ssize_t written = write(STDERR_FILENO, buf, pos);
    (void) written;
}

/* PHP's diagnostic channel. The signature is non-const before 8.0. */
#if PHP_VERSION_ID >= 80000
static void proteus_php_mod_log_message(const char *message, int syslog_type_int) {
#else
static void proteus_php_mod_log_message(char *message, int syslog_type_int) {
#endif
    proteus_php_mod_log_json("php", proteus_php_mod_level_name(syslog_type_int), message);
}

typedef struct {
    char   *buf;
    size_t  len;
    size_t  cap;
} proteus_php_mod_capture_t;

/* Headers only; the body streams straight through and is never buffered.
 * Not part of proteus_request_ctx below: buf/cap are kept across requests
 * to amortize allocation (see proteus_php_mod_capture_shrink), only len
 * is per-request. */
static proteus_php_mod_capture_t g_headers;

/* Everything one PHP request owns, reset in full at the top of every
 * execute_file() call. Still a single static, not a value threaded through
 * the SAPI hooks below: those signatures belong to libphp and carry no
 * user-data parameter. */
typedef struct {
    /* body */
    const char *body;
    size_t      body_len;
    size_t      body_pos;
    FILE       *body_file; /* set instead of body/body_len when spilled to disk */
    int         body_read_error; /* fread() on body_file hit ferror(), not EOF */

    /* request metadata */
    const char *const *extra_vars;
    size_t      extra_var_count;
    const char *cookie_header;

    /* response / fastcgi_finish_request() state */
    proteus_php_mod_chunk_fn chunk_cb;
    void       *chunk_cb_user_data;
    int         finished;   /* keeps END from firing twice */
    int         early_sent; /* fastcgi_finish_request() moved END early */
} proteus_request_ctx;

static proteus_request_ctx g_ctx;

/* On failure the buffer is left untouched, so a bail-out path cannot deref a
 * stale pointer and a retry stays safe. */
static int proteus_php_mod_grow(proteus_php_mod_capture_t *c, size_t extra) {
    if (c->len + extra + 1 <= c->cap) {
        return 0;
    }
    size_t new_cap = c->cap ? c->cap * 2 : 4096;
    while (new_cap < c->len + extra + 1) {
        new_cap *= 2;
    }
    char *new_buf = realloc(c->buf, new_cap);
    if (!new_buf) {
        return -1;
    }
    c->buf = new_buf;
    c->cap = new_cap;
    return 0;
}

/* `grow` never shrinks and execute_file only resets `len`, so without this a
 * single huge header set would stay resident for the worker's whole life.
 *
 * Fixed thresholds rather than a high-water heuristic: a set large enough to
 * cross the threshold is already past what any HTTP client will parse, so
 * retaining the buffer optimises for a case that does not recur. Realistic
 * responses stay below it and never reallocate at all. */
#define PROTEUS_PHP_MOD_HEADERS_SHRINK_ABOVE (64 * 1024)
#define PROTEUS_PHP_MOD_HEADERS_KEEP_CAP     (4 * 1024)

/* A failed shrink is a no-op: keeping an oversized buffer beats dropping a
 * valid one. */
static void proteus_php_mod_capture_shrink(proteus_php_mod_capture_t *c) {
    if (c->cap <= PROTEUS_PHP_MOD_HEADERS_SHRINK_ABOVE) {
        return;
    }
    char *shrunk = realloc(c->buf, PROTEUS_PHP_MOD_HEADERS_KEEP_CAP);
    if (shrunk == NULL) {
        return;
    }
    c->buf = shrunk;
    c->cap = PROTEUS_PHP_MOD_HEADERS_KEEP_CAP;
    /* `cap` must be exact: overstate it and `grow` skips the realloc it
     * owes, and the next append writes past the end of the buffer. */
    c->len = 0;
    c->buf[0] = '\0';
}

static void proteus_php_mod_capture_append(proteus_php_mod_capture_t *c, const char *str, size_t str_length) {
    if (proteus_php_mod_grow(c, str_length) != 0) {
        /* Drop the append rather than crash the worker over one header. */
        return;
    }
    memcpy(c->buf + c->len, str, str_length);
    c->len += str_length;
    c->buf[c->len] = '\0';
}

/* Core finalizes headers only by request end, not before an ordinary write,
 * so this forces the ordering. Idempotent. */
static size_t proteus_php_mod_ub_write(const char *str, size_t str_length) {
    if (!SG(headers_sent)) {
        sapi_send_headers();
    }
    if (!g_ctx.finished && g_ctx.chunk_cb) {
        g_ctx.chunk_cb(PROTEUS_PHP_MOD_CHUNK_BODY, 0, str, str_length, g_ctx.chunk_cb_user_data);
    }
    return str_length;
}

/* Newline-joined for the Rust side; header() has rejected embedded CR/LF
 * since PHP 5.1.2. */
static void proteus_php_mod_collect_header(void *data, void *arg) {
    sapi_header_struct *h = (sapi_header_struct *) data;
    proteus_php_mod_capture_t *out = (proteus_php_mod_capture_t *) arg;
    if (out->len > 0) {
        proteus_php_mod_capture_append(out, "\n", 1);
    }
    proteus_php_mod_capture_append(out, h->header, h->header_len);
}

/* Core calls this once with the full list, in place of the per-header hooks.
 * The return value tells core delivery is handled entirely here. */
static int proteus_php_mod_send_headers(sapi_headers_struct *sapi_headers) {
    g_headers.len = 0;
    zend_llist_apply_with_argument(&sapi_headers->headers, proteus_php_mod_collect_header, &g_headers);
    if (g_ctx.chunk_cb) {
        int status = sapi_headers->http_response_code;
        if (status == 0) {
            status = 200;
        }
        g_ctx.chunk_cb(PROTEUS_PHP_MOD_CHUNK_HEADERS, status, g_headers.buf, g_headers.len, g_ctx.chunk_cb_user_data);
    }
    return SAPI_HEADER_SENT_SUCCESSFULLY;
}

ZEND_BEGIN_ARG_INFO_EX(arginfo_proteus_php_mod_fastcgi_finish_request, 0, 0, 0)
ZEND_END_ARG_INFO()

/* Moves END early without backgrounding anything: the script keeps running
 * in this same call and its later output is dropped. The connection status
 * lets apps pair this with ignore_user_abort(true). */
PHP_FUNCTION(fastcgi_finish_request) {
    if (zend_parse_parameters_none() == FAILURE) {
        return;
    }

    if (g_ctx.chunk_cb == NULL || g_ctx.finished) {
        RETURN_FALSE;
    }

    php_output_end_all();
    if (!SG(headers_sent)) {
        sapi_send_headers();
    }

    g_ctx.early_sent = 1;
    g_ctx.finished = 1;
    g_ctx.chunk_cb(PROTEUS_PHP_MOD_CHUNK_END, 0, NULL, 0, g_ctx.chunk_cb_user_data);

    PG(connection_status) = PHP_CONNECTION_ABORTED;
    php_output_set_status(PHP_OUTPUT_DISABLED);

    RETURN_TRUE;
}

static const zend_function_entry proteus_php_mod_ext_functions[] = {
    PHP_FE(fastcgi_finish_request, arginfo_proteus_php_mod_fastcgi_finish_request)
    PHP_FE_END
};

/* Must be registered as a real module: a bare zend_register_functions() call
 * deterministically crashes the Optimizer's function_exists()
 * constant-folding pass. */
static zend_module_entry proteus_php_mod_module_entry = {
    STANDARD_MODULE_HEADER,
    "proteus_php_mod",
    proteus_php_mod_ext_functions,
    NULL, NULL, NULL, NULL, NULL,
    NULL,
    STANDARD_MODULE_PROPERTIES
};

/* php_embed leaves server_context NULL, which suppresses this hook and
 * $_POST parsing entirely unless execute_file sets a dummy non-NULL one. */
static char *proteus_php_mod_read_cookies(void) {
    return (char *) g_ctx.cookie_header;
}

/* The casts keep this portable across 7.4's non-const
 * php_register_variable() and 8's const one; every value here is a literal
 * or request-duration data. */
static void proteus_php_mod_register_variables(zval *track_vars_array) {
    /* Constant across every real SAPI, not request-derived. */
    php_register_variable("GATEWAY_INTERFACE", (char *) "CGI/1.1", track_vars_array);

    if (SG(request_info).request_method) {
        php_register_variable("REQUEST_METHOD", (char *) SG(request_info).request_method, track_vars_array);
    }
    if (SG(request_info).request_uri) {
        php_register_variable("REQUEST_URI", (char *) SG(request_info).request_uri, track_vars_array);
        /* Not derived from request_uri, which carries the query string that
         * PHP_SELF must never have; it arrives through extra_vars. */
    }
    if (SG(request_info).query_string) {
        php_register_variable("QUERY_STRING", (char *) SG(request_info).query_string, track_vars_array);
    }
    if (SG(request_info).content_type) {
        php_register_variable("CONTENT_TYPE", (char *) SG(request_info).content_type, track_vars_array);
    }
    if (SG(request_info).content_length > 0) {
        char len_buf[32];
        snprintf(len_buf, sizeof(len_buf), "%ld", (long) SG(request_info).content_length);
        php_register_variable("CONTENT_LENGTH", len_buf, track_vars_array);
    }

    for (size_t i = 0; i < g_ctx.extra_var_count; i++) {
        const char *entry = g_ctx.extra_vars[i];
        const char *eq = strchr(entry, '=');
        if (!eq) {
            continue;
        }
        size_t klen = (size_t) (eq - entry);
        if (klen >= 256) {
            continue;
        }
        char key[256];
        memcpy(key, entry, klen);
        key[klen] = '\0';
        php_register_variable(key, (char *) (eq + 1), track_vars_array);
    }
}

/* Tracks position across calls, reading from the spill file when there is
 * one. read_post()'s return of 0 is core's only "no more data" signal, so
 * fread() == 0 must not be treated as EOF without checking ferror() too. */
static size_t proteus_php_mod_read_post(char *buffer, size_t count_bytes) {
    if (g_ctx.body_file) {
        size_t n = fread(buffer, 1, count_bytes, g_ctx.body_file);
        if (n == 0 && ferror(g_ctx.body_file)) {
            /* Recorded, not acted on: core already treats 0 as "stop", and
             * the script may already be mid-execution. execute_file logs it. */
            g_ctx.body_read_error = 1;
        }
        return n;
    }
    size_t remaining = g_ctx.body_len - g_ctx.body_pos;
    size_t n = count_bytes < remaining ? count_bytes : remaining;
    if (n > 0) {
        memcpy(buffer, g_ctx.body + g_ctx.body_pos, n);
        g_ctx.body_pos += n;
    }
    return n;
}

/* The real table removal is a one-time pass during php_module_startup(),
 * already done by the time php.options are applied, so these directives must
 * be re-invoked explicitly. */
#if PHP_VERSION_ID < 80500
typedef int (*proteus_php_mod_disable_one_fn)(char *name, size_t name_length);

/* Returns 0 if every name in `list` was disabled, -1 if any was not. Still
 * processes the whole list either way, same as the NUL-drop policy below. */
static int proteus_php_mod_disable_list(const char *list, proteus_php_mod_disable_one_fn disable_one) {
    if (list == NULL || *list == '\0') {
        return 0;
    }
    char *base = strdup(list);
    if (base == NULL) {
        /* Skip the directive rather than deref NULL. */
        return -1;
    }
    int failed = 0;
    char *s = NULL;
    char *e = base;
    while (*e) {
        if (*e == ' ' || *e == ',') {
            if (s != NULL) {
                *e = '\0';
                if (disable_one(s, (size_t) (e - s)) != SUCCESS) {
                    failed = -1;
                }
                s = NULL;
            }
        } else if (s == NULL) {
            s = e;
        }
        e++;
    }
    if (s != NULL && disable_one(s, (size_t) (e - s)) != SUCCESS) {
        failed = -1;
    }
    free(base);
    return failed;
}
#endif

/* disable_classes and zend_disable_class() were removed in PHP 8.5. */
#if PHP_VERSION_ID < 80500
static int proteus_php_mod_disable_class_one(char *name, size_t name_length) {
    int rc = zend_disable_class(name, name_length);
    if (rc != SUCCESS) {
        char msg[320];
        snprintf(msg, sizeof(msg), "disable_classes: class '%.*s' was not found, not disabled", (int) name_length, name);
        proteus_php_mod_log_json("prototype", "ERROR", msg);
    }
    return rc;
}

static int proteus_php_mod_disable_classes(const char *list) {
    return proteus_php_mod_disable_list(list, proteus_php_mod_disable_class_one);
}
#else
static int proteus_php_mod_disable_classes(const char *list) {
    (void) list;
    return 0;
}
#endif

#if PHP_VERSION_ID >= 80000
/* zend_disable_functions() gives no per-name failure signal; silently
 * skipping an unknown name is its own intended behaviour, not a failure. */
static int proteus_php_mod_disable_functions(const char *list) {
    if (list != NULL && *list != '\0') {
        zend_disable_functions(list);
    }
    return 0;
}
#else
static int proteus_php_mod_disable_function_one(char *name, size_t name_length) {
    int rc = zend_disable_function(name, name_length);
    if (rc != SUCCESS) {
        char msg[320];
        snprintf(msg, sizeof(msg), "disable_functions: function '%.*s' was not found, not disabled", (int) name_length, name);
        proteus_php_mod_log_json("prototype", "ERROR", msg);
    }
    return rc;
}

static int proteus_php_mod_disable_functions(const char *list) {
    return proteus_php_mod_disable_list(list, proteus_php_mod_disable_function_one);
}
#endif

/* ZEND_INI_SYSTEM mutates the directive's own `modifiable` flag as a side
 * effect, so a later ini_set() is rejected regardless of its original
 * modifiability. force_change because this is SAPI setup, not an ini_set()
 * emulation.
 *
 * Returns 0 if every entry applied cleanly, -1 if anything failed - a
 * partially-applied, possibly security-relevant configuration must not look
 * like success to the caller. */
static int proteus_php_mod_apply_ini(const char *const *entries, size_t count, int modify_type) {
    int failed = 0;
    for (size_t i = 0; i < count; i++) {
        const char *entry = entries[i];
        const char *eq = strchr(entry, '=');
        if (eq == NULL) {
            continue;
        }
        size_t klen = (size_t) (eq - entry);
        const char *val = eq + 1;

        zend_string *name = zend_string_init(entry, klen, 0);
        zend_string *value = zend_string_init(val, strlen(val), 0);
        /* Copied internally, so these stay ours to free. */
        if (zend_alter_ini_entry_ex(name, value, modify_type, PHP_INI_STAGE_ACTIVATE, 1) != SUCCESS) {
            char msg[320];
            snprintf(msg, sizeof(msg), "failed to set php option '%.*s'", (int) klen, entry);
            proteus_php_mod_log_json("prototype", "ERROR", msg);
            failed = -1;
        }

        if (klen == strlen("disable_functions") && strncmp(entry, "disable_functions", klen) == 0) {
            if (proteus_php_mod_disable_functions(val) != 0) {
                failed = -1;
            }
        } else if (klen == strlen("disable_classes") && strncmp(entry, "disable_classes", klen) == 0) {
            if (proteus_php_mod_disable_classes(val) != 0) {
                failed = -1;
            }
        }

        zend_string_release(name);
        zend_string_release(value);
    }
    return failed;
}

int proteus_php_mod_init(
    const char *const *admin_entries, size_t admin_count,
    const char *const *user_entries, size_t user_count
) {
#if defined(SIGPIPE) && defined(SIG_IGN)
    /* A script using sockets should not die to SIGPIPE. */
    signal(SIGPIPE, SIG_IGN);
#endif

    php_embed_module.ub_write = proteus_php_mod_ub_write;
    php_embed_module.log_message = proteus_php_mod_log_message;
    php_embed_module.register_server_variables = proteus_php_mod_register_variables;
    php_embed_module.read_post = proteus_php_mod_read_post;
    php_embed_module.read_cookies = proteus_php_mod_read_cookies;
    php_embed_module.send_headers = proteus_php_mod_send_headers;

    /* OPcache's accel_find_sapi() hardcodes an allowlist that "embed" is not
     * on, so masquerade as one that is. */
    php_embed_module.name = "cli-server";

    sapi_startup(&php_embed_module);

    /* MINIT only; request startup is per-call. */
    if (php_embed_module.startup(&php_embed_module) == FAILURE) {
        return -1;
    }

    /* The supported way to add a module after php_module_startup(). */
    if (zend_startup_module(&proteus_php_mod_module_entry) == FAILURE) {
        return -1;
    }

    /* Core already chdir()s into the script's directory and restores cwd
     * itself, even on zend_bailout. */

    /* User first, then admin, so admin wins on a key collision. */
    int user_ok = proteus_php_mod_apply_ini(user_entries, user_count, ZEND_INI_USER);
    int admin_ok = proteus_php_mod_apply_ini(admin_entries, admin_count, ZEND_INI_SYSTEM);
    if (user_ok != 0 || admin_ok != 0) {
        return -1;
    }

    return 0;
}

/* -1 means request startup itself failed, not that the script errored; the
 * caller must then synthesize its own error response. */
int proteus_php_mod_execute_file(
    const char *path, const proteus_php_mod_request_t *req,
    proteus_php_mod_chunk_fn chunk_cb, void *chunk_cb_user_data,
    int *out_early_sent
) {
    g_headers.len = 0;
    if (g_headers.cap == 0) {
        if (proteus_php_mod_grow(&g_headers, 1) != 0) {
            return -1; /* OOM before request startup even begins */
        }
        g_headers.buf[0] = '\0';
    }

    /* Whole-struct zero, so a field this function forgets to set below
     * defaults to zero/NULL rather than carrying over from the last request. */
    g_ctx = (proteus_request_ctx){0};

    g_ctx.extra_vars = req->extra_vars;
    g_ctx.extra_var_count = req->extra_var_count;
    g_ctx.body = req->body;
    g_ctx.body_len = req->body_len;
    /* Exactly one of the inline body and the body fd is set. The fd came
     * from master over SCM_RIGHTS and names an unlinked file, so there is no
     * path to race and nothing to check about ownership. */
    if (req->body_fd >= 0) {
        /* dup, because fclose() below closes whatever fdopen took, and the
         * fd itself belongs to the caller. */
        int fd = dup(req->body_fd);
        if (fd < 0 || !(g_ctx.body_file = fdopen(fd, "rb"))) {
            if (fd >= 0) {
                close(fd);
            }
            proteus_php_mod_log_json("worker", "ERROR",
                "could not open the request body fd, failing the request");
            proteus_php_mod_capture_shrink(&g_headers);
            *out_early_sent = 0;
            return -1;
        }
    }
    g_ctx.cookie_header = req->cookie_header;
    g_ctx.chunk_cb = chunk_cb;
    g_ctx.chunk_cb_user_data = chunk_cb_user_data;

    SG(request_info).request_method = req->method;
    SG(request_info).request_uri = (char *) req->uri;
    SG(request_info).query_string = (char *) req->query_string;
    SG(request_info).content_type = req->content_type;
    /* Must be what read_post will actually deliver, or PHP's $_POST parser
     * waits on CONTENT_LENGTH bytes that never come. A rejected spill file
     * no longer reaches this point at all, so this is always accurate. */
    SG(request_info).content_length = (zend_long) g_ctx.body_len;

    /* Unlocks sapi_activate()'s cookie and $_POST parsing, which php_embed
     * otherwise skips. Never dereferenced. */
    SG(server_context) = (void *) 1;

    /* Populates $_SERVER['PHP_AUTH_*']. */
    php_handle_auth_data(req->authorization);

    if (php_request_startup() == FAILURE) {
        *out_early_sent = 0;
        if (g_ctx.body_file) {
            fclose(g_ctx.body_file);
            g_ctx.body_file = NULL;
        }
        proteus_php_mod_capture_shrink(&g_headers);
        return -1;
    }

    /* After php_request_startup(), which unconditionally resets this to
     * HTTP/1.0. The version decides whether a bare header("Location: ...")
     * on POST/PUT/DELETE becomes 303 or 302. */
    SG(request_info).proto_num = 1001;

    /* sapi_activate() does not reset http_response_code, which is zeroed
     * only at process start. Without this, a script that sets no status
     * inherits the previous request's code. */
    SG(sapi_headers).http_response_code = 200;

    zend_file_handle file_handle;
    zend_stream_init_filename(&file_handle, path);

    zend_first_try {
        php_execute_script(&file_handle);
    } zend_end_try();

    /* Declared on 7.4 and 8.0 too, but calling it there risks a double-free. */
#if PHP_VERSION_ID >= 80100
    zend_destroy_file_handle(&file_handle);
#endif

    /* Not forcing 500 on an uncaught exception, matching every other SAPI. */

    php_request_shutdown((void *) 0);

    /* A zero-output script reaches HEADERS only here, via
     * php_request_shutdown()'s own sapi_send_headers(). END may already have
     * fired early. */
    if (!g_ctx.finished) {
        g_ctx.finished = 1;
        if (g_ctx.chunk_cb) {
            g_ctx.chunk_cb(PROTEUS_PHP_MOD_CHUNK_END, 0, NULL, 0, g_ctx.chunk_cb_user_data);
        }
    }

    if (g_ctx.body_file) {
        fclose(g_ctx.body_file);
        g_ctx.body_file = NULL;
    }
    if (g_ctx.body_read_error) {
        /* Too late for an HTTP failure: the script already ran. */
        proteus_php_mod_log_json("worker", "ERROR", "request body read failed mid-request");
    }
    /* Here rather than at the next call, or an idle worker holds the
     * oversized buffer for exactly as long as it is least worth holding. */
    proteus_php_mod_capture_shrink(&g_headers);

    *out_early_sent = g_ctx.early_sent;
    return 0;
}

void proteus_php_mod_shutdown(void) {
    php_module_shutdown();
    sapi_shutdown();
    free(g_headers.buf);
    g_headers.buf = NULL;
    g_headers.cap = g_headers.len = 0;
}
