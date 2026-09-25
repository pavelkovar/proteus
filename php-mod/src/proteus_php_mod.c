/* PHP embed SAPI module, targeting PHP 7.4-8.6. */

#include "proteus_php_mod.h"

#include <sapi/embed/php_embed.h>
#include <ctype.h>
#include <fcntl.h>
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

#if PHP_VERSION_ID >= 80000
static void proteus_php_mod_log_message(const char *message, int syslog_type_int) {
#else
static void proteus_php_mod_log_message(char *message, int syslog_type_int) {
#endif
    proteus_php_mod_log_json("php", proteus_php_mod_level_name(syslog_type_int), message);
}

/* Everything one PHP request owns, reset in full at the top of every
 * execute_file() call. A static, not a threaded value: the SAPI hooks
 * below belong to libphp and carry no user-data parameter. */
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

/* Core finalizes headers only by request end, not before an ordinary write,
 * so this forces the ordering. Idempotent. */
static size_t proteus_php_mod_ub_write(const char *str, size_t str_length) {
    if (!SG(headers_sent)) {
        sapi_send_headers();
    }
    if (!g_ctx.finished && g_ctx.chunk_cb
        && g_ctx.chunk_cb(PROTEUS_PHP_MOD_CHUNK_BODY, 0, str, str_length,
                          g_ctx.chunk_cb_user_data) != 0) {
        /* Reporting rather than deciding: this honours ignore_user_abort,
         * and does not return when PHP chooses to bail. */
        php_handle_aborted_connection();
        return 0;
    }
    return str_length;
}

/* Covers typical header sets without a heap allocation. */
#define PROTEUS_PHP_MOD_HEADERS_STACK (8 * 1024)

/* Core calls this once with the full list, in place of the per-header hooks,
 * and the headers go to the Rust side newline-joined (header() has rejected
 * embedded CR/LF since PHP 5.1.2). Outside the Zend heap on purpose: headers
 * must still go out after a memory_limit fatal. */
static int proteus_php_mod_send_headers(sapi_headers_struct *sapi_headers) {
    zend_llist *list = &sapi_headers->headers;
    zend_llist_position it;
    size_t total = 0;
    for (sapi_header_struct *h = zend_llist_get_first_ex(list, &it); h != NULL;
         h = zend_llist_get_next_ex(list, &it)) {
        total += h->header_len + 1;
    }

    char stack_buf[PROTEUS_PHP_MOD_HEADERS_STACK];
    char *buf = total <= sizeof(stack_buf) ? stack_buf : malloc(total);
    size_t len = 0;
    if (buf != NULL) {
        for (sapi_header_struct *h = zend_llist_get_first_ex(list, &it); h != NULL;
             h = zend_llist_get_next_ex(list, &it)) {
            if (len > 0) {
                buf[len++] = '\n';
            }
            memcpy(buf + len, h->header, h->header_len);
            len += h->header_len;
        }
    }

    if (g_ctx.chunk_cb) {
        int status = sapi_headers->http_response_code;
        if (status == 0) {
            status = 200;
        }
        g_ctx.chunk_cb(PROTEUS_PHP_MOD_CHUNK_HEADERS, status, buf, len, g_ctx.chunk_cb_user_data);
    }
    if (buf != stack_buf) {
        free(buf);
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

/* php.options as `php -d` INI text for sapi_module.ini_entries, so PHP parses
 * them after php.ini and before extensions read directives (OPcache sizes its
 * memory then). Lives for the process: sapi_module points into it. */
static char *g_ini_entries;

static int proteus_php_mod_ini_entry_ok(const char *entry) {
    const char *eq = strchr(entry, '=');
    if (eq == NULL || eq == entry || strpbrk(entry, "\r\n") != NULL) {
        char msg[320];
        snprintf(msg, sizeof(msg), "invalid php option '%.200s': expected one key=value line", entry);
        proteus_php_mod_log_json("prototype", "ERROR", msg);
        return -1;
    }
    return 0;
}

/* Quotes when `php -d` does (value not starting alphanumeric or with a quote):
 * `E_ALL & ~E_DEPRECATED` is evaluated, `.:/usr/share/php` kept whole.
 * `out` needs strlen(entry) + 3. */
static size_t proteus_php_mod_render_ini_entry(char *out, const char *entry) {
    const char *eq = strchr(entry, '=');
    const char *val = eq + 1;
    size_t key_len = (size_t) (val - entry); /* includes the '=' */
    size_t val_len = strlen(val);
    int quote = val_len > 0 && !isalnum((unsigned char) *val) && *val != '"' && *val != '\'';
    size_t pos = 0;
    memcpy(out + pos, entry, key_len);
    pos += key_len;
    if (quote) {
        out[pos++] = '"';
    }
    memcpy(out + pos, val, val_len);
    pos += val_len;
    if (quote) {
        out[pos++] = '"';
    }
    out[pos++] = '\n';
    return pos;
}

/* Admin last, so it wins a key collision. */
static int proteus_php_mod_build_ini_entries(
    const char *const *admin_entries, size_t admin_count,
    const char *const *user_entries, size_t user_count
) {
    size_t cap = 1;
    for (size_t i = 0; i < user_count; i++) {
        if (proteus_php_mod_ini_entry_ok(user_entries[i]) != 0) {
            return -1;
        }
        cap += strlen(user_entries[i]) + 3;
    }
    for (size_t i = 0; i < admin_count; i++) {
        if (proteus_php_mod_ini_entry_ok(admin_entries[i]) != 0) {
            return -1;
        }
        cap += strlen(admin_entries[i]) + 3;
    }
    if (cap == 1) {
        return 0; /* nothing to hand over */
    }
    char *buf = malloc(cap);
    if (buf == NULL) {
        return -1;
    }
    size_t pos = 0;
    for (size_t i = 0; i < user_count; i++) {
        pos += proteus_php_mod_render_ini_entry(buf + pos, user_entries[i]);
    }
    for (size_t i = 0; i < admin_count; i++) {
        pos += proteus_php_mod_render_ini_entry(buf + pos, admin_entries[i]);
    }
    buf[pos] = '\0';
    g_ini_entries = buf;
    return 0;
}

/* Instructions, not directives: they have no ini entry to look up. */
static int proteus_php_mod_is_ini_instruction(const char *key, size_t key_len) {
    return (key_len == strlen("extension") && strncmp(key, "extension", key_len) == 0)
        || (key_len == strlen("zend_extension") && strncmp(key, "zend_extension", key_len) == 0);
}

/* After startup: rejects unknown keys (a typo must not pass as a setting) and
 * locks admin ones against ini_set(). Returns -1 after reporting every unknown. */
static int proteus_php_mod_check_ini(const char *const *entries, size_t count, int lock) {
    int failed = 0;
    for (size_t i = 0; i < count; i++) {
        const char *entry = entries[i];
        size_t key_len = (size_t) (strchr(entry, '=') - entry);
        if (proteus_php_mod_is_ini_instruction(entry, key_len)) {
            continue;
        }
        zend_ini_entry *ini_entry = zend_hash_str_find_ptr(EG(ini_directives), entry, key_len);
        if (ini_entry == NULL) {
            char msg[320];
            snprintf(msg, sizeof(msg), "unknown php option '%.*s'", (int) key_len, entry);
            proteus_php_mod_log_json("prototype", "ERROR", msg);
            failed = -1;
            continue;
        }
        if (lock) {
            ini_entry->modifiable = ZEND_INI_SYSTEM;
        }
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
    php_embed_module.flush = NULL;
    php_embed_module.php_ini_ignore_cwd = 1;

    /* OPcache's accel_find_sapi() hardcodes an allowlist that "embed" is not
     * on, so masquerade as one that is. */
    php_embed_module.name = "cli-server";
    php_embed_module.pretty_name = "proteus";

    if (proteus_php_mod_build_ini_entries(admin_entries, admin_count, user_entries, user_count) != 0) {
        return -1;
    }

    sapi_startup(&php_embed_module);

    /* sapi_startup() clears ini_entries; php_module_startup() parses them. */
    php_embed_module.ini_entries = g_ini_entries;

#if PHP_VERSION_ID < 80200
    if (php_module_startup(&php_embed_module, &proteus_php_mod_module_entry, 1) == FAILURE) {
#else
    if (php_module_startup(&php_embed_module, &proteus_php_mod_module_entry) == FAILURE) {
#endif
        return -1;
    }

    /* Core already chdir()s into the script's directory and restores cwd
     * itself, even on zend_bailout. */

    int user_ok = proteus_php_mod_check_ini(user_entries, user_count, 0);
    int admin_ok = proteus_php_mod_check_ini(admin_entries, admin_count, 1);
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
        /* dup: fclose() below closes it and the fd is the caller's. Cloexec, so
         * a process the script exec()s does not inherit the request body. */
        int fd = fcntl(req->body_fd, F_DUPFD_CLOEXEC, 0);
        if (fd < 0 || !(g_ctx.body_file = fdopen(fd, "rb"))) {
            if (fd >= 0) {
                close(fd);
            }
            proteus_php_mod_log_json("worker", "ERROR",
                "could not open the request body fd, failing the request");
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
    *out_early_sent = g_ctx.early_sent;
    return 0;
}
