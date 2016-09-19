#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <fcntl.h>
#include <errno.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/utsname.h>

#include <hiredis.h>
#include <async.h>
#include <adapters/ae.h>

#include <dhashkit.h>
#include <dlist.h>
#include <dmtqueue.h>

#include <vrt_util.h>
#include <vrt_public.h>
#include <vrabtest.h>
#include <vrt_produce_data.h>
#include <vrt_dispatch_data.h>
#include <vrt_backend.h>

typedef struct task_data {
    long long maxmemory;
    long long used_memory;
    long long total_system_memory;
    
    int deleting;   /* backend thread is deleting keys */
    long long cursor;   /* scan cursor */
} task_data;

int backend_threads_count;
static darray *backend_threads = NULL;

int backend_threads_pause_finished_count;

static int task_data_create(void)
{
    task_data *td;

    td = malloc(sizeof(*td));

    td->maxmemory = 0;
    td->used_memory = 0;
    td->total_system_memory = 0;
    td->deleting = 0;
    td->cursor = 0;

    return td;
}

static void task_data_destroy(task_data *td)
{
    free(td);
}

static int backend_conn_context_init(conn_context *cc, char *host, int port)
{
    cc->ctx = NULL;
    cc->actx = NULL;

    cc->actx = redisAsyncConnect(host, port);
    if (cc->actx == NULL) {
        return VRT_ERROR;
    }
    
    return VRT_OK;
}

static void backend_conn_context_deinit(conn_context *cc)
{
    if (cc->ctx) {
        redisFree(cc->ctx);
        cc->ctx == NULL;
    }

    if (cc->actx) {
        redisAsyncFree(cc->actx);
        cc->actx == NULL;
    }
}

static void connect_callback(const redisAsyncContext *c, int status) {
    backend_thread *bt = c->data;
    if (status != REDIS_OK) {
        test_log_out("Error: %s\n", c->errstr);
        //aeStop(loop);
        return;
    }

    //test_log_out("Connected...\n");
}

static void disconnect_callback(const redisAsyncContext *c, int status) {
    backend_thread *bt = c->data;
    if (status != REDIS_OK) {
        test_log_out("Error: %s\n", c->errstr);
        //aeStop(loop);
        return;
    }

    //test_log_out("Disconnected...\n");
    //aeStop(loop);
}

static void scan_for_delete_callback(redisAsyncContext *c, void *r, void *privdata) {
    redisReply *reply = r, *reply_sub, *reply_elem;
    abtest_server *abs = privdata;
    task_data *td = abs->data;
    conn_context *cc;
    long long value;
    size_t k;
    
    if (reply == NULL) return;

    if (!td->deleting) {
        return;
    }

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

    td->cursor = value;

    reply_sub = reply->element[1];
    if (reply_sub->type != REDIS_REPLY_ARRAY) {
        return;
    }

    for (k = 0; k < reply_sub->elements; k ++) {
        reply_elem = reply_sub->element[k];
        if (reply_elem->type != REDIS_REPLY_STRING) {
            return;
        }

        data_unit *du = data_unit_get();
        du->dp = delete_data_producer;
        du->argc = 2;
        du->argv = malloc(du->argc*sizeof(sds));
        du->argv[0] = sdsnew(delete_data_producer->name);
        du->argv[1] = sdsnewlen(reply_elem->str,reply_elem->len);
        data_dispatch(du);
    }

    cc = darray_get(abs->conn_contexts, 0);
    redisAsyncCommand(cc->actx, scan_for_delete_callback, 
        abs, "scan %lld count 1000", td->cursor);
}

static void update_memory_callback(redisAsyncContext *c, void *r, void *privdata) {
    redisReply *reply = r;
    abtest_server *abs = privdata;
    task_data *td = abs->data;
    
    if (reply == NULL) return;

    td->used_memory = get_longlong_from_info_reply(reply, "used_memory");

    if (td->maxmemory == 0) {
        td->total_system_memory = get_longlong_from_info_reply(reply, "total_system_memory");
    }
}

static void update_maxmemory_callback(redisAsyncContext *c, void *r, void *privdata) {
    redisReply *reply = r;
    abtest_server *abs = privdata;
    task_data *td = abs->data;
    redisReply *reply_sub;
    long long value;
    
    if (reply == NULL) return;

    if (reply->type != REDIS_REPLY_ARRAY) {
        return;
    }

    if (reply->elements != 2) {
        return;
    }

    reply_sub = reply->element[0];
    if (reply_sub->type != REDIS_REPLY_STRING || 
        strcmp(reply_sub->str, "maxmemory")) {
        return;
    }

    reply_sub = reply->element[1];
    if (reply_sub->type != REDIS_REPLY_STRING || 
        string2ll(reply_sub->str,reply_sub->len,&value) != 1) {
        return;
    }

    td->maxmemory = value;
}

static void update_memory_info(darray *abgs)
{
    long long i, j;
    
    for (i = 0; i < darray_n(abgs); i ++) {
        abtest_group *abg = darray_get(abgs, i);
        for (j = 0; j < darray_n(&abg->abtest_servers); j ++) {
            abtest_server *abs = darray_get(&abg->abtest_servers, j);
            conn_context *cc = darray_get(abs->conn_contexts, 0);
            
            redisAsyncCommand(cc->actx, update_memory_callback, abs, "info memory");
            redisAsyncCommand(cc->actx, update_maxmemory_callback, abs, "config get maxmemory");
        }
    }
}

static void check_memory_enough(backend_thread *bt)
{
    long long i, j;
    darray *abgs = bt->abgs;
    
    for (i = 0; i < darray_n(abgs); i ++) {
        abtest_group *abg = darray_get(abgs, i);
        for (j = 0; j < darray_n(&abg->abtest_servers); j ++) {
            abtest_server *abs = darray_get(&abg->abtest_servers, j);
            task_data *td = abs->data;
            long long max_memory_allowed = 0;
            
            if (td->used_memory) {
                if (td->maxmemory) {
                    max_memory_allowed = td->maxmemory;
                } else if (td->total_system_memory) {
                    max_memory_allowed = td->total_system_memory;
                }

                if (max_memory_allowed) { 
                    if (td->used_memory*100/max_memory_allowed > 80) {
                        if (!td->deleting) {
                            conn_context *cc = darray_get(abs->conn_contexts, 0);
                            redisAsyncCommand(cc->actx, scan_for_delete_callback, 
                                abs, "scan %lld count 1000", td->cursor);
                            td->deleting = 1;
                            bt->deleting ++;
                        }
                    } else if (td->deleting) {
                        td->deleting = 0;
                        bt->deleting --;
                    }
                }
            }
        }
    }
}

static int backend_thread_cron(aeEventLoop *eventLoop, long long id, void *clientData)
{
    backend_thread *bt = clientData;
    
    ASSERT(eventLoop == bt->el);

    /* At the begin of this loop */
    if (bt->pause) {
        if (!test_if_need_pause()) {
            bt->pause = 0;
        } else {
            bt->cronloops ++;
            return 1000;
        }
    }

    update_memory_info(bt->abgs);
    check_memory_enough(bt);

    /* At the end of this loop */
    if (!bt->pause && test_if_need_pause() && !bt->deleting) {
        bt->pause = 1;
        one_backend_thread_paused();
    }
    
    bt->cronloops ++;
    return 1000/bt->hz;
}

static int backend_thread_init(backend_thread *bt, char *test_target_groups)
{
    int i, j, k;

    bt->id = 0;
    bt->thread_id = 0;
    bt->el = NULL;
    bt->hz = 10;
    bt->cronloops = 0;
    bt->deleting = 0;
    bt->pause = 0;
    
    bt->el = aeCreateEventLoop(1);
    if (bt->el == NULL) {
        return VRT_ERROR;
    }

    bt->abgs = abtest_groups_create(test_target_groups);
    if (bt->abgs == NULL) {
        return VRT_ERROR;
    }

    /* Init connection context for each server */
    for (i = 0; i < darray_n(bt->abgs); i ++) {
        abtest_group *abg = darray_get(bt->abgs, i);
        for (j = 0; j < darray_n(&abg->abtest_servers); j ++) {
            abtest_server *abs = darray_get(&abg->abtest_servers, j);
            
            abs->conn_contexts = darray_create(1, sizeof(conn_context));
            for (k = 0; k < 1; k ++) {
                conn_context *cc = darray_push(abs->conn_contexts);
                if (backend_conn_context_init(cc,abs->host,abs->port) != VRT_OK) {
                    return VRT_ERROR;
                }
                cc->actx->data = bt;
                redisAeAttach(bt->el, cc->actx);
                redisAsyncSetConnectCallback(cc->actx,connect_callback);
                redisAsyncSetDisconnectCallback(cc->actx,disconnect_callback);
            }

            abs->data = task_data_create();
        }
    }

    if (aeCreateTimeEvent(bt->el, 1, backend_thread_cron, bt, NULL) == AE_ERR) {
        return VRT_ERROR;
    }
    
    return VRT_OK;
}

static void backend_thread_deinit(backend_thread *bt)
{
    if (bt->el) {
        aeDeleteEventLoop(bt->el);
        bt->el = NULL;
    }

    if (bt->abgs) {
        int i, j, k;
        /* Deinit connection context for each server */
        for (i = 0; i < darray_n(bt->abgs); i ++) {
            abtest_group *abg = darray_get(bt->abgs, i);
            for (j = 0; j < darray_n(&abg->abtest_servers); j ++) {
                abtest_server *abs = darray_get(&abg->abtest_servers, j);
                while (darray_n(abs->conn_contexts) > 0) {
                    conn_context *cc = darray_pop(abs->conn_contexts);
                    backend_conn_context_deinit(cc);
                }

                if (abs->data) {
                    task_data_destroy(abs->data);
                    abs->data;
                }
            }
        }
        
        abtest_groups_destroy(bt->abgs);
        bt->abgs = NULL;
    }
}

int vrt_backend_init(int threads_count, char *test_target_groups)
{
    int j;
    
    backend_threads_count = threads_count;
    backend_threads = darray_create(threads_count, sizeof(backend_thread));
    if (backend_threads == NULL) {
        return VRT_ERROR;
    }

    for (j = 0; j < threads_count; j ++) {
        backend_thread *bt = darray_push(backend_threads);
        if (backend_thread_init(bt, test_target_groups) != VRT_OK) {
            return VRT_ERROR;
        }
        bt->id = j;
    }
    
    return VRT_OK;
}

void vrt_backend_deinit(void)
{
    if (backend_threads) {
        while (darray_n(backend_threads) > 0) {
            backend_thread *bt = darray_pop(backend_threads);
            backend_thread_deinit(bt);
        }
        darray_destroy(backend_threads);
        backend_threads = NULL;
    }
}

static void *vrt_backend_thread_run(void *args)
{
    backend_thread *bt = args;
    srand(vrt_usec_now()^(int)pthread_self());

    aeMain(bt->el);
    
    return NULL;
}

int vrt_start_backend(void)
{
    unsigned int i;
    for (i = 0; i < darray_n(backend_threads); i ++) {
        pthread_attr_t attr;
        backend_thread *bt;
        pthread_attr_init(&attr);
        bt = darray_get(backend_threads, i);
        pthread_create(&bt->thread_id, 
            &attr, vrt_backend_thread_run, bt);
    }
    
    return VRT_OK;
}

int vrt_wait_backend(void)
{
    unsigned int i;
    /* wait for the produce threads finish */
	for(i = 0; i < darray_n(backend_threads); i ++){
		backend_thread *bt = darray_get(backend_threads, i);
		pthread_join(bt->thread_id, NULL);
	}
    
    return VRT_OK;
}
