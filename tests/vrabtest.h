#ifndef _VRABTEST_H_
#define _VRABTEST_H_

#include <darray.h>

struct redisContext;
struct redisAsyncContext;
struct abtest_group;

typedef struct conn_context {
    struct redisContext *ctx;
    struct redisAsyncContext *actx;    
} conn_context;

typedef struct abtest_server {
    sds host;
    int port;

    darray *conn_contexts;  /* connection context */

    void *data;
} abtest_server;

typedef unsigned int (*backend_server_idx_t)(struct abtest_group*, char *, size_t);
typedef abtest_server *(*backend_server_t)(struct abtest_group*, char *, size_t);

typedef struct abtest_group {
    int type;
    
    darray abtest_servers;    /* type: abtest_server */

    backend_server_idx_t    get_backend_server_idx;
    backend_server_t        get_backend_server;
} abtest_group;

extern int expire_enabled;
extern long long test_interval;
extern long long last_test_begin_time;

darray *abtest_groups_create(char *groups_string);
void abtest_groups_destroy(darray *abgs);

#endif
