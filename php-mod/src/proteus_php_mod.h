/* Public C ABI of the PHP module. No PHP structs ever cross this boundary. */
#ifndef PROTEUS_PHP_MOD_H
#define PROTEUS_PHP_MOD_H

#include <stddef.h>

typedef struct {
    const char *method;
    const char *uri;           /* path+query, e.g. "/foo?a=b" */
    const char *query_string;  /* just "a=b", may be empty */
    const char *content_type;  /* may be NULL */
    const char *const *extra_vars; /* "KEY=VALUE" strings for $_SERVER */
    size_t extra_var_count;
    const char *body;           /* inline body bytes - NULL if body_fd is set instead */
    size_t body_len;
    int body_fd;                /* spilled body, an fd master passed over SCM_RIGHTS;
                                  * -1 when the body is inline. Read incrementally,
                                  * like the inline case. */
    const char *cookie_header;  /* raw Cookie header value, may be NULL - populates $_COOKIE */
    const char *authorization;  /* raw Authorization header value, may be NULL - populates PHP_AUTH_* */
} proteus_php_mod_request_t;

/* "KEY=VALUE" strings. Admin entries lock against a later ini_set(); user
 * entries set a default only. */
int proteus_php_mod_init(
    const char *const *admin_entries, size_t admin_count,
    const char *const *user_entries, size_t user_count
);

/* Streams through this, fired in order per request: one HEADERS, any BODY,
 * exactly one END - early if the script called fastcgi_finish_request(),
 * which then keeps running. `data` is borrowed for the call's duration. */
typedef enum {
    PROTEUS_PHP_MOD_CHUNK_HEADERS = 1,
    PROTEUS_PHP_MOD_CHUNK_BODY = 2,
    PROTEUS_PHP_MOD_CHUNK_END = 3,
} proteus_php_mod_chunk_kind;

/* Returns non-zero once the client has stopped listening, which the body write
 * reports to PHP as a failed write. */
typedef int (*proteus_php_mod_chunk_fn)(
    proteus_php_mod_chunk_kind kind,
    int status,
    const char *data, size_t data_len,
    void *user_data
);

/* fastcgi_finish_request() only moves END earlier; this call stays fully
 * synchronous either way. */
int proteus_php_mod_execute_file(
    const char *path, const proteus_php_mod_request_t *req,
    proteus_php_mod_chunk_fn chunk_cb, void *chunk_cb_user_data,
    int *out_early_sent
);

#endif
