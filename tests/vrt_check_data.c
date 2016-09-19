#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <fcntl.h>
#include <errno.h>
#include <signal.h>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/utsname.h>

#include <hiredis.h>
#include <async.h>
#include <adapters/ae.h>

#include <dhashkit.h>
#include <dlist.h>
#include <dmtqueue.h>
#include <dlog.h>

#include <vrt_util.h>
#include <vrt_public.h>
#include <vrabtest.h>
#include <vrt_produce_data.h>
#include <vrt_dispatch_data.h>
#include <vrt_backend.h>
#include <vrt_check_data.h>

#define CHECK_DATA_FLAG_NONE        (1<<0)
#define CHECK_DATA_FLAG_MASTER      (1<<1)
#define CHECK_DATA_FLAG_SLAVE       (1<<2)

#define CHECK_UNIT_STATE_NULL           0
#define CHECK_UNIT_STATE_GET_EXPIRE     1
#define CHECK_UNIT_STATE_GET_TYPE       2
#define CHECK_UNIT_STATE_GET_VALUE      3

typedef struct check_data_thread {
    int id;
    pthread_t thread_id;
    
    aeEventLoop *el;
    int hz;
    int cronloops;  /* Number of times the cron function run */

    darray *abgs;   /* Type is abtest_group */
    int scan_group_idx; /* The group idx to scan keys */
    darray *scan_servers;   /* The servers in the scan group, type is abtest_server */
    int scan_finished_count;
    long long cursor;   /* scan cursor */
    dlist *check_units;

    long long check_begin_time; /* Unit is second */
    long long scan_keys_count;
} check_data_thread;

typedef struct check_unit {
    check_data_thread *cdt;

    dlistNode *lnode;
    
    sds key;

    int key_persist;
    long long min_ttl, max_ttl_gap;
    
    int key_type;
    int state;

    darray servers; /* Servers used to send the check messages, type is pointer of abtest_server */
    darray replys;  /* Used to cache the replys from the servers, type is pointer of redisReply */

    unsigned int servers_count;
    unsigned int replys_count;
    unsigned int not_exist_count;
} check_unit;

typedef struct data_checker {
    pthread_t thread_id;
    
    aeEventLoop *el;
    int hz;
    int cronloops;          /* Number of times the cron function run */

    sds test_target_groups;

    int flags;
    sds checker;
    conn_context *master;   /* If this is a slave */

    long long check_begin_time; /* Unit is second */
} data_checker;

static data_checker dc;

/* Last begin time to check the data.
 * Unit is second */
static long long last_check_begin_time;

static darray *cdts = NULL;

static check_unit *check_unit_create(void)
{
    check_unit *cunit;

    cunit = malloc(sizeof(*cunit));
    if (cunit == NULL) {
        return NULL;
    }

    cunit->cdt = NULL;

    cunit->lnode = NULL;
    
    cunit->key = NULL;
    cunit->key_persist = 0;
    cunit->min_ttl = 0;
    cunit->max_ttl_gap = 0;
    cunit->key_type = -1;
    cunit->state = CHECK_UNIT_STATE_NULL;
    darray_init(&cunit->servers, 2, sizeof(abtest_server*));
    darray_init(&cunit->replys, 2, sizeof(redisReply*));

    cunit->servers_count = 0;
    cunit->replys_count = 0;
    cunit->not_exist_count = 0;
    
    return cunit;
}

static void check_unit_destroy(check_unit *cunit)
{
    if (cunit->cdt != NULL && cunit->lnode != NULL) {
        dlistDelNode(cunit->cdt->check_units,cunit->lnode);
        cunit->lnode = NULL;
    }

    if (cunit->key != NULL) {
        sdsfree(cunit->key);
        cunit->key = NULL;
    }

    while (darray_n(&cunit->servers) > 0) {
        darray_pop(&cunit->servers);
    }
    darray_deinit(&cunit->servers);

    while (darray_n(&cunit->replys) > 0) {
        redisReply **reply = darray_pop(&cunit->replys);
        freeReplyObject(*reply);
    }
    darray_deinit(&cunit->replys);

    free(cunit);
}

static int check_conn_context_init(conn_context *cc, char *host, int port)
{
    cc->ctx = NULL;
    cc->actx = NULL;

    cc->actx = redisAsyncConnect(host, port);
    if (cc->actx == NULL) {
        return VRT_ERROR;
    }
    
    return VRT_OK;
}

static void check_conn_context_deinit(conn_context *cc)
{
    if (cc->ctx) {
        redisFree(cc->ctx);
        cc->ctx == NULL;
    }

    if (cc->actx) {
        cc->actx->ev.cleanup = NULL;
        redisAsyncFree(cc->actx);
        cc->actx == NULL;
    }
}

static void connect_callback(const redisAsyncContext *c, int status) {
    check_data_thread *cdt = c->data;
    if (status != REDIS_OK) {
        test_log_out("Error: %s\n", c->errstr);
        //aeStop(loop);
        return;
    }

    //test_log_out("Connected...\n");
}

static void disconnect_callback(const redisAsyncContext *c, int status) {
    check_data_thread *cdt = c->data;
    if (status != REDIS_OK) {
        test_log_out("Error: %s\n", c->errstr);
        //aeStop(loop);
        return;
    }

    //test_log_out("Disconnected...\n");
    //aeStop(loop);
}

static int sort_replys_if_needed(check_unit *cunit)
{
    int step = 0, idx_cmp = 0;

    if (cunit->key_type == REDIS_SET) {
        step = 1;
    } else if (cunit->key_type == REDIS_HASH) {
        step = 2;
    }

    if (step > 0) {
        int i;
        redisReply **reply;
        for (i = 0; i < darray_n(&cunit->replys); i ++) {
            reply = darray_get(&cunit->replys, i);
            if ((*reply)->type != REDIS_REPLY_ARRAY)
                continue;
            sort_array_by_step((*reply)->element, (*reply)->elements, 
                step, idx_cmp, reply_string_binary_compare);
        }
    }
    
    return VRT_OK;
}

/* 1: All replys are same
 * 0: replys are different */
static int check_replys_if_same(check_unit *cunit)
{
    unsigned int j;
    redisReply **replyb, **reply;

    sort_replys_if_needed(cunit);

    replyb = darray_get(&cunit->replys,0);
    
    for (j = 1; j < cunit->replys_count ; j ++) {
        reply = darray_get(&cunit->replys,j);
        if (check_two_replys_if_same(*replyb, *reply)) {
            return 0;
        }
    }
    
    return 1;
}

#define TTL_MISTAKE_CAN_BE_ACCEPT   3
static void check_data_callback(redisAsyncContext *c, void *r, void *privdata) {
    redisReply *reply = r, *reply_sub, *reply_elem;
    redisReply *reply_clone, **elem;
    check_unit *cunit = privdata;
    check_data_thread *cdt = cunit->cdt;
    conn_context *cc;
    long long value;
    char *errmsg;
    int j;
    
    if (reply == NULL) return;

    if (cunit->state == CHECK_UNIT_STATE_GET_EXPIRE) {
        if (reply->type != REDIS_REPLY_INTEGER) {
            errmsg = "ttl command reply type is not integer";
            goto error;
        }

        reply_clone = steal_hiredis_redisreply(reply);
        elem = darray_push(&cunit->replys);
        *elem = reply_clone;
        cunit->replys_count ++;
        
        if (cunit->replys_count >= cunit->servers_count) {
            char *argv[2];
            size_t argvlen[2];
            long long min, max;
            int persist;

            elem = darray_get(&cunit->replys, 0);
            reply_elem = *elem;
            if (reply_elem->integer == -1) {
                persist = 1;
            } else if (reply_elem->integer == -2) {
                cunit->not_exist_count ++;
                min = max = 0;
            } else if (reply_elem->integer < -2) {
                errmsg = "ttl command reply integer is less than -2";
                goto error;
            } else {
                min = max =  reply_elem->integer;
            }
            
            for (j = 1; j < darray_n(&cunit->replys); j ++) {
                elem = darray_get(&cunit->replys, j);
                reply_elem = *elem;
                if (persist && reply_elem->integer != -1) {
                    errmsg = "key in some server is persist, but others are not";
                    goto error;
                }
                
                if (reply_elem->integer == -1) {
                    if (persist != 1) {
                        errmsg = "key in some server is persist, but others are not";
                        goto error;
                    }
                } else if (reply_elem->integer == -2) {
                    cunit->not_exist_count ++;
                    if (min > 0) min = 0;
                } else if (reply_elem->integer < -2) {
                    errmsg = "ttl command reply integer is less than -2";
                    goto error;
                } else {
                    if (reply_elem->integer < min) min = reply_elem->integer;
                    if (reply_elem->integer > max) max = reply_elem->integer;
                }
            }

            if (cunit->not_exist_count >= cunit->servers_count) {
                /* The key in all the target group is expired */
                goto done;
            }
            
            if (persist) {
                cunit->key_persist = 1;
            } else {
                cunit->min_ttl = min;
                cunit->max_ttl_gap = max-min;
                if (cunit->max_ttl_gap > TTL_MISTAKE_CAN_BE_ACCEPT) {
                    errmsg = "ttl mistake is too big between groups";
                    goto error;
                }
            }
            
            /* Check step 2: get the key type */
            argv[0] = "type";
            argvlen[0] = 4;
            argv[1] = cunit->key;
            argvlen[1] = sdslen(cunit->key);
            for (j = 0; j < darray_n(&cunit->servers); j ++) {
                abtest_server **abs = darray_get(&cunit->servers,j);
                conn_context *cc = darray_get((*abs)->conn_contexts, 0);
                
                redisAsyncCommandArgv(cc->actx, check_data_callback, 
                    cunit, 2, argv, argvlen);
            }
            
            cunit->state = CHECK_UNIT_STATE_GET_TYPE;
            goto next_step;
        }

        return;
    }

    if (cunit->state == CHECK_UNIT_STATE_GET_TYPE) {        
        if (reply->type != REDIS_REPLY_STATUS) {
            errmsg = "type command reply type is not status";
            goto error;
        }

        if (!strcmp(reply->str, "none")) {
            /* This key doesn't exit, may be expired or evicted */
            cunit->not_exist_count ++;
        } else {
            reply_clone = steal_hiredis_redisreply(reply);
            elem = darray_push(&cunit->replys);
            *elem = reply_clone;
            cunit->replys_count ++;
        }

        if (cunit->not_exist_count >= cunit->servers_count) {
            /* The key in all the target group is expired */
            goto done;
        } else if (cunit->replys_count >= (cunit->servers_count-cunit->not_exist_count)) {
            int argc;
            char **argv;
            size_t *argvlen;

            if (cunit->not_exist_count > 0 && cunit->key_persist) {
                errmsg = "key is persist, but not exist in some servers";
                goto error;
            }
            
            if (check_replys_if_same(cunit) != 1) {
                errmsg = "type command replys are not same";
                goto error;
            }

            elem = darray_get(&cunit->replys,0);
            if (!strcmp((*elem)->str,"string")) {
                cunit->key_type = REDIS_STRING;
                
                argc = 2;
                argv = malloc(argc*sizeof(char *));
                argvlen = malloc(argc*sizeof(size_t));

                argv[0] = "get";
                argvlen[0] = 3;
                argv[1] = cunit->key;
                argvlen[1] = sdslen(cunit->key);
            } else if (!strcmp((*elem)->str,"list")) {
                cunit->key_type = REDIS_LIST;

                argc = 4;
                argv = malloc(argc*sizeof(char *));
                argvlen = malloc(argc*sizeof(size_t));

                argv[0] = "lrange";
                argvlen[0] = 6;
                argv[1] = cunit->key;
                argvlen[1] = sdslen(cunit->key);
                argv[2] = "0";
                argvlen[2] = 1;
                argv[3] = "-1";
                argvlen[3] = 2;
            } else if (!strcmp((*elem)->str,"set")) {
                cunit->key_type = REDIS_SET;

                argc = 2;
                argv = malloc(argc*sizeof(char *));
                argvlen = malloc(argc*sizeof(size_t));

                argv[0] = "smembers";
                argvlen[0] = 8;
                argv[1] = cunit->key;
                argvlen[1] = sdslen(cunit->key);
            } else if (!strcmp((*elem)->str,"zset")) {
                cunit->key_type = REDIS_ZSET;

                argc = 4;
                argv = malloc(argc*sizeof(char *));
                argvlen = malloc(argc*sizeof(size_t));

                argv[0] = "zrange";
                argvlen[0] = 6;
                argv[1] = cunit->key;
                argvlen[1] = sdslen(cunit->key);
                argv[2] = "0";
                argvlen[2] = 1;
                argv[3] = "-1";
                argvlen[3] = 2;
            } else if (!strcmp((*elem)->str,"hash")) {
                cunit->key_type = REDIS_HASH;

                argc = 2;
                argv = malloc(argc*sizeof(char *));
                argvlen = malloc(argc*sizeof(size_t));

                argv[0] = "hgetall";
                argvlen[0] = 7;
                argv[1] = cunit->key;
                argvlen[1] = sdslen(cunit->key);
            } else {
                errmsg = "not supported key type";
                goto error;
            }

            /* Check step 3: get the value */
            for (j = 0; j < darray_n(&cunit->servers); j ++) {
                abtest_server **abs = darray_get(&cunit->servers,j);
                conn_context *cc = darray_get((*abs)->conn_contexts, 0);
                
                redisAsyncCommandArgv(cc->actx, check_data_callback, 
                    cunit, argc, argv, argvlen);
            }
            free(argv);
            free(argvlen);
            
            cunit->state = CHECK_UNIT_STATE_GET_VALUE;
            goto next_step;
        }

        return;
    }

    if (cunit->state == CHECK_UNIT_STATE_GET_VALUE) {
        int not_exist = 0;
        if (cunit->key_type == REDIS_STRING) {
            if (reply->type == REDIS_REPLY_NIL) {
                not_exist = 1;
            } else if (reply->type != REDIS_REPLY_STRING) {
                errmsg = "get command reply type is not string";
                goto error;
            }
        } else if (cunit->key_type == REDIS_LIST) {
            if (reply->type != REDIS_REPLY_ARRAY) {
                errmsg = "lrange command reply type is not array";
                goto error;
            }
            if (reply->elements == 0) {
                not_exist = 1;
            }
        } else if (cunit->key_type == REDIS_SET) {
            if (reply->type != REDIS_REPLY_ARRAY) {
                errmsg = "smembers command reply type is not array";
                goto error;
            }
            if (reply->elements == 0) {
                not_exist = 1;
            }
        } else if (cunit->key_type == REDIS_ZSET) {
            if (reply->type != REDIS_REPLY_ARRAY) {
                errmsg = "zrange command reply type is not array";
                goto error;
            }
            if (reply->elements == 0) {
                not_exist = 1;
            }
        } else if (cunit->key_type == REDIS_HASH) {
            if (reply->type != REDIS_REPLY_ARRAY) {
                errmsg = "hgetall command reply type is not array";
                goto error;
            }
            if (reply->elements == 0) {
                not_exist = 1;
            }
        } else {
            errmsg = "not supported key type";
            goto error;
        }

        if (not_exist) {
            cunit->not_exist_count ++;
        } else {
            reply_clone = steal_hiredis_redisreply(reply);
            elem = darray_push(&cunit->replys);
            *elem = reply_clone;
            cunit->replys_count ++;
        }

        if (cunit->not_exist_count >= cunit->servers_count) {
            /* The key in all the target group is expired */
            goto done;
        } else if (cunit->replys_count >= (cunit->servers_count-cunit->not_exist_count)) {
            if (cunit->not_exist_count > 0 && cunit->key_persist) {
                errmsg = "key is persist, but not exist in some servers";
                goto error;
            }
        
            if (check_replys_if_same(cunit) != 1) {
                errmsg = "values for reply are not same";
                goto error;
            }
            
            goto done;
        }

        return;
    }

done:

    check_unit_destroy(cunit);
    
    return;
    
next_step:

    cunit->replys_count = 0;
    cunit->not_exist_count = 0;
    while (darray_n(&cunit->replys) > 0) {
        elem = darray_pop(&cunit->replys);
        freeReplyObject(*elem);
    }
    
    return;
    
error:

    log_hexdump(LOG_ERR,cunit->key,sdslen(cunit->key),
        "%s, scan group id: %d, key(len:%zu, type: %s): ", 
        errmsg, 
        cdt->scan_group_idx, 
        sdslen(cunit->key),get_key_type_string(cunit->key_type));
    
    check_unit_destroy(cunit);
}

static int start_check_data(char *key, size_t keylen, check_data_thread *cdt)
{
    check_unit *cu = check_unit_create();
    int j;

    cu->cdt = cdt;
    cu->key = sdsnewlen(key,keylen);
    dlistPush(cdt->check_units,cu);
    cu->lnode = dlistLast(cdt->check_units);

    for (j = 0; j < darray_n(cdt->abgs); j ++) {
        abtest_group *abg = darray_get(cdt->abgs, j);
        abtest_server *abs = abg->get_backend_server(abg,key,keylen);
        abtest_server **elem = darray_push(&cu->servers);
        conn_context *cc = darray_get(abs->conn_contexts, 0);
        char *argv[2];
        size_t argvlen[2];
        
        *elem = abs;
        cu->servers_count ++;

        /* Check step 1: get the expire */
        argv[0] = "ttl";
        argvlen[0] = 3;
        argv[1] = key;
        argvlen[1] = keylen;
        redisAsyncCommandArgv(cc->actx, check_data_callback, 
            cu, 2, argv, argvlen);
    }
    cu->state = CHECK_UNIT_STATE_GET_EXPIRE;

    return VRT_OK;
}

static void scan_for_check_callback(redisAsyncContext *c, void *r, void *privdata) {
    redisReply *reply = r, *reply_sub, *reply_elem;
    abtest_server *abs = privdata;
    check_data_thread *cdt = abs->data;
    conn_context *cc;
    long long value;
    size_t k;
    
    if (reply == NULL) return;


    if (reply->type != REDIS_REPLY_ARRAY) {
        return;
    }

    if (reply->elements != 2) {
        return;
    }

    reply_sub = reply->element[0];
    if (reply_sub->type != REDIS_REPLY_STRING || 
        string2ll(reply_sub->str,reply_sub->len,&value) != 1) {
        return;
    }

    cdt->cursor = value;

    reply_sub = reply->element[1];
    if (reply_sub->type != REDIS_REPLY_ARRAY) {
        return;
    }

    for (k = 0; k < reply_sub->elements; k ++) {
        reply_elem = reply_sub->element[k];
        if (reply_elem->type != REDIS_REPLY_STRING) {
            return;
        }

        start_check_data(reply_elem->str,reply_elem->len,cdt);
    }

    cdt->scan_keys_count += reply_sub->elements;

    if (cdt->cursor == 0) {
        cdt->scan_finished_count ++;
    }
}

static int check_data_threads_finished_count = 0;
static void one_check_data_thread_finished(void)
{
    update_state_add(check_data_threads_finished_count,1);
}

static int all_check_data_threads_finished(void)
{
    int finished_count;
    update_state_get(check_data_threads_finished_count,&finished_count);

    if (finished_count >= darray_n(cdts)) {
        return 1;
    }

    return 0;
}

static int check_data_thread_cron(aeEventLoop *eventLoop, long long id, void *clientData)
{
    check_data_thread *cdt = clientData;

    ASSERT(eventLoop == cdt->el);

    if (cdt->scan_finished_count >= darray_n(cdt->scan_servers)) {
        if (dlistLength(cdt->check_units) == 0) {
            aeStop(cdt->el);
            one_check_data_thread_finished();
            log_debug(LOG_NOTICE, "One check thread finished,scaned %lld keys",
                cdt->scan_keys_count);
            return 1;
        }
    } else if (dlistLength(cdt->check_units) < 3000) {
        abtest_group *abg;
        abtest_server **abs;
        int *idx;
        conn_context *cc;
        
        abg = darray_get(cdt->abgs, cdt->scan_group_idx);
        abs = darray_get(cdt->scan_servers, cdt->scan_finished_count);
        cc = darray_get((*abs)->conn_contexts, 0);

        redisAsyncCommand(cc->actx, scan_for_check_callback, 
            *abs, "scan %lld count 1000", cdt->cursor);
    }

    cdt->cronloops ++;
    return 1000/cdt->hz;
}

static int check_data_thread_init(check_data_thread *cdt, char *test_target_groups)
{
    int i, j, k;

    cdt->id = 0;
    cdt->thread_id = 0;
    cdt->el = NULL;
    cdt->hz = 200;
    cdt->cronloops = 0;
    
    cdt->abgs = NULL;
    cdt->scan_group_idx = 0;
    cdt->scan_servers = NULL;
    cdt->scan_finished_count = 0;
    cdt->cursor = 0;
    cdt->check_units = NULL;

    cdt->check_begin_time = 0;
    cdt->scan_keys_count = 0;

    cdt->el = aeCreateEventLoop(200);
    if (cdt->el == NULL) {
        return VRT_ERROR;
    }

    cdt->scan_servers = darray_create(1,sizeof(abtest_server*));
    
    cdt->abgs = abtest_groups_create(test_target_groups);
    if (cdt->abgs == NULL) {
        return VRT_ERROR;
    }

    /* Init connection context for each server */
    for (i = 0; i < darray_n(cdt->abgs); i ++) {
        abtest_group *abg = darray_get(cdt->abgs, i);
        for (j = 0; j < darray_n(&abg->abtest_servers); j ++) {
            abtest_server *abs = darray_get(&abg->abtest_servers, j);
            abs->conn_contexts = darray_create(1, sizeof(conn_context));
            for (k = 0; k < 1; k ++) {
                conn_context *cc = darray_push(abs->conn_contexts);
                if (check_conn_context_init(cc,abs->host,abs->port) != VRT_OK) {
                    return VRT_ERROR;
                }
                cc->actx->data = cdt;
                redisAeAttach(cdt->el, cc->actx);
                redisAsyncSetConnectCallback(cc->actx,connect_callback);
                redisAsyncSetDisconnectCallback(cc->actx,disconnect_callback);
            }
        }
    }

    if (aeCreateTimeEvent(cdt->el, 1, check_data_thread_cron, cdt, NULL) == AE_ERR) {
        return VRT_ERROR;
    }

    cdt->check_units = dlistCreate();
    
    return VRT_OK;
}

static void check_data_thread_deinit(check_data_thread *cdt)
{
    if (cdt->el) {
        aeDeleteEventLoop(cdt->el);
        cdt->el = NULL;
    }

    if (cdt->scan_servers) {
        while (darray_n(cdt->scan_servers) > 0) {
            darray_pop(cdt->scan_servers);
        }
        darray_destroy(cdt->scan_servers);
        cdt->scan_servers = NULL;
    }

    if (cdt->abgs) {
        int i, j, k;
        /* Deinit connection context for each server */
        for (i = 0; i < darray_n(cdt->abgs); i ++) {
            abtest_group *abg = darray_get(cdt->abgs, i);
            for (j = 0; j < darray_n(&abg->abtest_servers); j ++) {
                abtest_server *abs = darray_get(&abg->abtest_servers, j);
                while (darray_n(abs->conn_contexts) > 0) {
                    conn_context *cc = darray_pop(abs->conn_contexts);
                    check_conn_context_deinit(cc);
                }
            }
        }
        
        abtest_groups_destroy(cdt->abgs);
        cdt->abgs = NULL;
    }

    if (cdt->check_units) {
        while (dlistLength(cdt->check_units) > 0) {
            check_unit *cu = dlistPop(cdt->check_units);
            check_unit_destroy(cu);
        }
        dlistRelease(cdt->check_units);
        cdt->check_units = NULL;
    }
}

static int checking_data;
static int checking_data_or_not(void)
{
    int checking;

    update_state_get(checking_data,&checking);

    if (checking) return 1;
    else return 0;
}

static int check_data_threads_count = 8;
static void destroy_check_data_threads(void);
/* return value :
 * -1: error
 * 0 : ok
 * 1 : not need */
static int create_check_data_threads(void)
{
    darray *abgs = NULL;
    abtest_group *abg;
    int groups_count;
    int threads_count_per_group;
    int check_thread_id = 0;
    int i, j, k;
    
    if (cdts != NULL) {
        destroy_check_data_threads();
    }

    cdts = darray_create(2,sizeof(check_data_thread));
    if (cdts == NULL) {
        return -1;
    }

    abgs = abtest_groups_create(dc.test_target_groups);
    if (abgs == NULL) {
        return -1;
    }

    groups_count = darray_n(abgs);
    if (groups_count == 1) {
        abtest_groups_destroy(abgs);
        return 1;
    }

    threads_count_per_group = check_data_threads_count/groups_count;
    if (threads_count_per_group <= 0) {
        threads_count_per_group = 1;
    }
    
    for (i = 0; i < groups_count; i ++) {
        int servers_count, threads_count;
        int servers_count_per_thread;
        int server_idx = 0;

        threads_count = threads_count_per_group;
        abg = darray_get(abgs, i);
        servers_count = darray_n(&abg->abtest_servers);
        servers_count_per_thread = servers_count/threads_count;
        if (servers_count_per_thread == 0) {
            servers_count_per_thread = 1;
            threads_count = servers_count;
        }
        for (j = 0; j < threads_count; j ++) {
            abtest_server *abs;
            
            check_data_thread *cdt = darray_push(cdts);
            check_data_thread_init(cdt,dc.test_target_groups);
            cdt->id = check_thread_id++;
            cdt->scan_group_idx = i;

            abg = darray_get(cdt->abgs, cdt->id);
            
            for (k = 0; k < servers_count_per_thread; k ++) {
                abtest_server **elem = darray_push(cdt->scan_servers);
                abs = darray_get(&abg->abtest_servers, server_idx++);
                abs->data = cdt;
                *elem = abs;
            }

            if (j == threads_count-1) {
                while (server_idx < servers_count) {
                    abtest_server **elem = darray_push(cdt->scan_servers);
                    abs = darray_get(&abg->abtest_servers, server_idx++);
                    abs->data = cdt;
                    *elem = abs;
                }
            }
        }
    }
    
    abtest_groups_destroy(abgs);
    
    return 0;
}

static void destroy_check_data_threads(void)
{
    if (cdts != NULL) {
        while (darray_n(cdts) > 0) {
            check_data_thread *cdt = darray_pop(cdts);
            check_data_thread_deinit(cdt);
        }
        darray_destroy(cdts);
        cdts = NULL;
    }
}

static void *check_data_thread_run(void *args)
{
    check_data_thread *cdt = args;
    
    srand(vrt_usec_now()^(int)pthread_self());

    aeMain(cdt->el);
    
    return NULL;
}

static int start_check_data_threads(void)
{
    int j;
    check_data_thread *cdt;

    if (cdts == NULL) return VRT_ERROR;

    for (j = 0; j < darray_n(cdts); j ++) {
        pthread_attr_t attr;
        
        cdt = darray_get(cdts, j);
        pthread_attr_init(&attr);
        pthread_create(&cdt->thread_id, 
            &attr, check_data_thread_run, cdt);  
    }
    
    return VRT_OK;
}

static int begin_check_data(void)
{
    create_check_data_threads();
    start_check_data_threads();
    
    update_state_set(checking_data,1);

    return VRT_OK;
}

static void end_check_data(void)
{
    update_state_set(check_data_threads_finished_count,0);
    update_state_set(checking_data,0);
}

static int data_checker_cron(aeEventLoop *eventLoop, long long id, void *clientData)
{
    ASSERT(eventLoop == dc.el);

    if (!test_if_need_pause() && vrt_sec_now()-last_test_begin_time > test_interval) {
        test_need_to_pause();
        log_notice("Start pause the test...");
    }

    if (!checking_data_or_not() && test_if_need_pause() && 
        all_threads_paused()) {
        
        log_notice("Finished pause the test. Tested %lld commands, %lld error reply(%.2f%%).", 
            get_total_tested_commands_count_per_cycle(),
            get_total_reply_err_count_per_cycle(),
            (float)get_total_reply_err_count_per_cycle()/(float)get_total_tested_commands_count_per_cycle()*100);
        reset_total_count_per_cycle();
        sleep(1);
        last_check_begin_time = vrt_sec_now();
        begin_check_data();
        log_notice("Start checking the data...");
    }

    if (checking_data_or_not() && all_check_data_threads_finished()) {
        end_check_data();
        log_notice("Finished checking the data\n");
        test_can_continue();
        last_test_begin_time = vrt_sec_now();
    }

    dc.cronloops ++;
    return 1000/dc.hz;
}

int vrt_data_checker_init(char *checker, char *test_target_groups)
{
    int ret;
    
    dc.thread_id = 0;
    dc.el = NULL;
    dc.hz = 10;
    dc.cronloops = 0;
    dc.test_target_groups = NULL;
    dc.flags = CHECK_DATA_FLAG_NONE;
    dc.checker = NULL;
    dc.master = NULL;
    dc.check_begin_time = 0;
    
    dc.el = aeCreateEventLoop(10);
    if (dc.el == NULL) {
        return VRT_ERROR;
    }

    if (aeCreateTimeEvent(dc.el, 1, data_checker_cron, NULL, NULL) == AE_ERR) {
        return VRT_ERROR;
    }

    dc.test_target_groups = sdsnew(test_target_groups);

    dc.checker = sdsnew(checker);

    if (!strcasecmp(checker,"myself")) {
        dc.flags |= CHECK_DATA_FLAG_MASTER;
    } else {
        sds host;
        int port;
        dc.flags |= CHECK_DATA_FLAG_SLAVE;
        host = get_host_port_from_address_string(checker, &port);
        if (host == NULL) {
            return VRT_ERROR;
        }
        dc.master = malloc(sizeof(conn_context));
        ret = check_conn_context_init(dc.master, host, port);
        sdsfree(host);
        if (ret != VRT_OK) {
            return VRT_ERROR;
        }
    }

    return VRT_OK;
}

void vrt_data_checker_deinit(void)
{
    if (dc.el) {
        aeDeleteEventLoop(dc.el);
        dc.el = NULL;
    }

    if (dc.test_target_groups) {
        sdsfree(dc.test_target_groups);
        dc.test_target_groups = NULL;
    }

    if (dc.checker) {
        sdsfree(dc.checker);
        dc.checker = NULL;
    }

    if (dc.master) {
        check_conn_context_deinit(dc.master);
        free(dc.master);
        dc.master = NULL;
    }

    destroy_check_data_threads();
}

static void *vrt_data_checker_run(void *args)
{
    srand(vrt_usec_now()^(int)pthread_self());

    aeMain(dc.el);
    
    return NULL;
}

int vrt_start_data_checker(void)
{
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_create(&dc.thread_id, 
        &attr, vrt_data_checker_run, NULL);   
    return VRT_OK;
}

int vrt_wait_data_checker(void)
{
	pthread_join(dc.thread_id, NULL);
   
    return VRT_OK;
}

static int test_need_pause = 0;

int test_if_need_pause(void)
{
    int need_pause;

    update_state_get(test_need_pause,&need_pause);

    if (need_pause) return 1;
    else return 0;
}

void test_can_continue(void)
{
     update_state_set(test_need_pause,0);
     update_state_set(produce_threads_pause_finished_count,0);
     update_state_set(dispatch_threads_pause_finished_count,0);
     update_state_set(backend_threads_pause_finished_count,0);
}

void test_need_to_pause(void)
{
    update_state_set(test_need_pause,1);
}

void one_produce_thread_paused(void)
{
    update_state_add(produce_threads_pause_finished_count,1);
}

void one_dispatch_thread_paused(void)
{
    update_state_add(dispatch_threads_pause_finished_count,1);
}

void one_backend_thread_paused(void)
{
    update_state_add(backend_threads_pause_finished_count,1);
}

int all_produce_threads_paused(void)
{
    int paused_threads;

    update_state_get(produce_threads_pause_finished_count,&paused_threads);
    if (paused_threads < produce_data_threads_count) {
        return 0;
    }

    return 1;
}

int all_dispatch_threads_paused(void)
{
    int paused_threads;

    update_state_get(dispatch_threads_pause_finished_count,&paused_threads);
    if (paused_threads < dispatch_data_threads_count) {
        return 0;
    }

    return 1;
}

int all_backend_threads_paused(void)
{
    int paused_threads;

    update_state_get(backend_threads_pause_finished_count,&paused_threads);
    if (paused_threads < backend_threads_count) {
        return 0;
    }

    return 1;
}

int all_threads_paused(void)
{
    int paused_threads;

    update_state_get(produce_threads_pause_finished_count,&paused_threads);
    if (paused_threads < produce_data_threads_count) {
        return 0;
    }

    update_state_get(dispatch_threads_pause_finished_count,&paused_threads);
    if (paused_threads < dispatch_data_threads_count) {
        return 0;
    }

    update_state_get(backend_threads_pause_finished_count,&paused_threads);
    if (paused_threads < backend_threads_count) {
        return 0;
    }

    return 1;
}
