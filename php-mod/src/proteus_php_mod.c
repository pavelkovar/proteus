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

/* PHP's diagnostic channel: one JSON line on stderr, never the response
 * body. The signature is non-const before 8.0. */
#if PHP_VERSION_ID >= 80000
static void proteus_php_mod_log_message(const char *message, int syslog_type_int) {
#else
static void proteus_php_mod_log_message(char *message, int syslog_type_int) {
#endif
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
                      "{\"timestamp\":\"%s.%06ldZ\",\"type\":\"php\",\"level\":\"%s\",\"pid\":%d,\"message\":\"",
                      time_buf, ts.tv_nsec / 1000,
                      proteus_php_mod_level_name(syslog_type_int), (int) getpid());
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

typedef struct {
    char   *buf;
    size_t  len;
    size_t  cap;
} proteus_php_mod_capture_t;

/* Headers only; the body streams straight through and is never buffered. */
static proteus_php_mod_capture_t g_headers;

/* Reset per execute_file() call; one request per thread. g_finished keeps
 * END from firing twice and stops output being forwarded after an early
 * finish. */
static proteus_php_mod_chunk_fn g_chunk_cb;
static void *g_chunk_cb_user_data;
static int g_finished;
static int g_early_sent;

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
    if (!g_finished && g_chunk_cb) {
        g_chunk_cb(PROTEUS_PHP_MOD_CHUNK_BODY, 0, str, str_length, g_chunk_cb_user_data);
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
    if (g_chunk_cb) {
        int status = sapi_headers->http_response_code;
        if (status == 0) {
            status = 200;
        }
        g_chunk_cb(PROTEUS_PHP_MOD_CHUNK_HEADERS, status, g_headers.buf, g_headers.len, g_chunk_cb_user_data);
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

    if (g_chunk_cb == NULL || g_finished) {
        RETURN_FALSE;
    }

    php_output_end_all();
    if (!SG(headers_sent)) {
        sapi_send_headers();
    }

    g_early_sent = 1;
    g_finished = 1;
    g_chunk_cb(PROTEUS_PHP_MOD_CHUNK_END, 0, NULL, 0, g_chunk_cb_user_data);

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

/* One request per thread, so globals suffice. */
static const char *const *g_extra_vars;
static size_t g_extra_var_count;
static const char *g_body;
static size_t g_body_len;
static size_t g_body_pos;
/* Set instead of the inline body when master spilled it to disk. Read
 * incrementally, and closed on every exit path. */
static FILE *g_body_file;
static const char *g_cookie_header;

/* php_embed leaves server_context NULL, which suppresses this hook and
 * $_POST parsing entirely unless execute_file sets a dummy non-NULL one. */
static char *proteus_php_mod_read_cookies(void) {
    return (char *) g_cookie_header;
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

    for (size_t i = 0; i < g_extra_var_count; i++) {
        const char *entry = g_extra_vars[i];
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
 * one. */
static size_t proteus_php_mod_read_post(char *buffer, size_t count_bytes) {
    if (g_body_file) {
        size_t n = fread(buffer, 1, count_bytes, g_body_file);
        return n;
    }
    size_t remaining = g_body_len - g_body_pos;
    size_t n = count_bytes < remaining ? count_bytes : remaining;
    if (n > 0) {
        memcpy(buffer, g_body + g_body_pos, n);
        g_body_pos += n;
    }
    return n;
}

/* The real table removal is a one-time pass during php_module_startup(),
 * already done by the time php.options are applied, so these directives must
 * be re-invoked explicitly. */
#if PHP_VERSION_ID < 80500
typedef void (*proteus_php_mod_disable_one_fn)(char *name, size_t name_length);

static void proteus_php_mod_disable_list(const char *list, proteus_php_mod_disable_one_fn disable_one) {
    if (list == NULL || *list == '\0') {
        return;
    }
    char *base = strdup(list);
    if (base == NULL) {
        /* Skip the directive rather than deref NULL. */
        return;
    }
    char *s = NULL;
    char *e = base;
    while (*e) {
        if (*e == ' ' || *e == ',') {
            if (s != NULL) {
                *e = '\0';
                disable_one(s, (size_t) (e - s));
                s = NULL;
            }
        } else if (s == NULL) {
            s = e;
        }
        e++;
    }
    if (s != NULL) {
        disable_one(s, (size_t) (e - s));
    }
    free(base);
}
#endif

/* disable_classes and zend_disable_class() were removed in PHP 8.5. */
#if PHP_VERSION_ID < 80500
static void proteus_php_mod_disable_class_one(char *name, size_t name_length) {
    (void) zend_disable_class(name, name_length);
}

static void proteus_php_mod_disable_classes(const char *list) {
    proteus_php_mod_disable_list(list, proteus_php_mod_disable_class_one);
}
#else
static void proteus_php_mod_disable_classes(const char *list) {
    (void) list;
}
#endif

#if PHP_VERSION_ID >= 80000
static void proteus_php_mod_disable_functions(const char *list) {
    if (list != NULL && *list != '\0') {
        zend_disable_functions(list);
    }
}
#else
static void proteus_php_mod_disable_function_one(char *name, size_t name_length) {
    (void) zend_disable_function(name, name_length);
}

static void proteus_php_mod_disable_functions(const char *list) {
    proteus_php_mod_disable_list(list, proteus_php_mod_disable_function_one);
}
#endif

/* ZEND_INI_SYSTEM mutates the directive's own `modifiable` flag as a side
 * effect, so a later ini_set() is rejected regardless of its original
 * modifiability. force_change because this is SAPI setup, not an ini_set()
 * emulation. */
static void proteus_php_mod_apply_ini(const char *const *entries, size_t count, int modify_type) {
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
        zend_alter_ini_entry_ex(name, value, modify_type, PHP_INI_STAGE_ACTIVATE, 1);

        if (klen == strlen("disable_functions") && strncmp(entry, "disable_functions", klen) == 0) {
            proteus_php_mod_disable_functions(val);
        } else if (klen == strlen("disable_classes") && strncmp(entry, "disable_classes", klen) == 0) {
            proteus_php_mod_disable_classes(val);
        }

        zend_string_release(name);
        zend_string_release(value);
    }
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
    proteus_php_mod_apply_ini(user_entries, user_count, ZEND_INI_USER);
    proteus_php_mod_apply_ini(admin_entries, admin_count, ZEND_INI_SYSTEM);

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

    g_extra_vars = req->extra_vars;
    g_extra_var_count = req->extra_var_count;
    g_body = req->body;
    g_body_len = req->body_len;
    g_body_pos = 0;
    /* Exactly one of the inline body and the spill path is set. A failed
     * open or ownership check degrades to an empty body. */
    g_body_file = NULL;
    if (req->body_file_path) {
        g_body_file = fopen(req->body_file_path, "rb");
        if (g_body_file) {
            /* The spill path is predictable and the directory shared, so a
             * sibling worker of the same uid could race a symlink in ahead of
             * this open. fstat() on the fd actually obtained is bound to the
             * resolved inode and cannot be swapped afterwards. */
            struct stat st;
            if (fstat(fileno(g_body_file), &st) != 0 || !S_ISREG(st.st_mode) || st.st_uid != geteuid()) {
                fprintf(stderr, "[proteus] spilled request body file %s failed ownership/type check, refusing it\n", req->body_file_path);
                fclose(g_body_file);
                g_body_file = NULL;
            }
        }
        if (!g_body_file) {
            fprintf(stderr, "[proteus] fopen(%s) failed for spilled request body\n", req->body_file_path);
            g_body_len = 0;
        }
    }
    g_cookie_header = req->cookie_header;
    g_chunk_cb = chunk_cb;
    g_chunk_cb_user_data = chunk_cb_user_data;
    g_finished = 0;
    g_early_sent = 0;

    SG(request_info).request_method = req->method;
    SG(request_info).request_uri = (char *) req->uri;
    SG(request_info).query_string = (char *) req->query_string;
    SG(request_info).content_type = req->content_type;
    /* Must be what read_post will actually deliver: the two diverge when the
     * ownership check rejected a spilled file, and PHP's $_POST parser trusts
     * CONTENT_LENGTH and would wait for data that never comes. */
    SG(request_info).content_length = (zend_long) g_body_len;

    /* Unlocks sapi_activate()'s cookie and $_POST parsing, which php_embed
     * otherwise skips. Never dereferenced. */
    SG(server_context) = (void *) 1;

    /* Populates $_SERVER['PHP_AUTH_*']. */
    php_handle_auth_data(req->authorization);

    if (php_request_startup() == FAILURE) {
        *out_early_sent = 0;
        if (g_body_file) {
            fclose(g_body_file);
            g_body_file = NULL;
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
    if (!g_finished) {
        g_finished = 1;
        if (g_chunk_cb) {
            g_chunk_cb(PROTEUS_PHP_MOD_CHUNK_END, 0, NULL, 0, g_chunk_cb_user_data);
        }
    }

    if (g_body_file) {
        fclose(g_body_file);
        g_body_file = NULL;
    }
    /* Here rather than at the next call, or an idle worker holds the
     * oversized buffer for exactly as long as it is least worth holding. */
    proteus_php_mod_capture_shrink(&g_headers);

    *out_early_sent = g_early_sent;
    return 0;
}

void proteus_php_mod_shutdown(void) {
    php_module_shutdown();
    sapi_shutdown();
    free(g_headers.buf);
    g_headers.buf = NULL;
    g_headers.cap = g_headers.len = 0;
}
