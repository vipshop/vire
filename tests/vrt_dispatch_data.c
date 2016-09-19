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
#include <dlog.h>

#include <vrt_util.h>
#include <vrt_public.h>
#include <vrabtest.h>
#include <vrt_dispatch_data.h>
#include <vrt_produce_data.h>

int dispatch_data_threads_count;
static darray *dispatch_data_threads = NULL;

int dispatch_threads_pause_finished_count;

static long long total_tested_commands_count_per_cycle = 0;
static long long total_reply_err_count_per_cycle = 0;
static long long total_tested_commands_count = 0;
static long long total_reply_err_count = 0;

long long get_total_tested_commands_count_per_cycle(void)
{
    long long count;
    update_state_get(total_tested_commands_count_per_cycle,&count);
    return count;
}

long long get_total_reply_err_count_per_cycle(void)
{
    long long count;
    update_state_get(total_reply_err_count_per_cycle,&count);
    return count;
}

void reset_total_count_per_cycle(void)
{
    update_state_set(total_tested_commands_count_per_cycle,0);
    update_state_set(total_reply_err_count_per_cycle,0);
}

typedef struct reply_unit {
    int total_count;
    int received_count;
    redisReply **replys;
    data_unit *du;
} reply_unit;

static void show_replys_inconsistency_msg(data_unit *du, redisReply *reply1, redisReply *reply2)
{
    int *keyindex, numkeys;
    sds key = NULL;

    keyindex = get_keys_from_data_producer(du->dp, du->argv, du->argc, &numkeys);
    if (numkeys > 0) {
        key = du->argv[keyindex[0]];
    }
    free(keyindex);

    if (key) {
        log_hexdump(LOG_ERR,key,sdslen(key),
            "%s command replys are inconsistency, "
            "reply type: %d %d, "
            "reply status: %s %s, "
            "reply error: %s %s, "
            "reply integer: %lld %lld, "
            "reply array len: %zu %zu, "
            "key(len:%zu): ", 
            du->dp->name, 
            reply1->type, reply2->type, 
            reply1->type==REDIS_REPLY_STATUS?reply1->str:"NULL",
            reply2->type==REDIS_REPLY_STATUS?reply2->str:"NULL",
            reply1->type==REDIS_REPLY_ERROR?reply1->str:"NULL",
            reply2->type==REDIS_REPLY_ERROR?reply2->str:"NULL",
            reply1->type==REDIS_REPLY_INTEGER?reply1->integer:0,
            reply2->type==REDIS_REPLY_INTEGER?reply2->integer:0,
            reply1->type==REDIS_REPLY_ARRAY?reply1->elements:0,
            reply2->type==REDIS_REPLY_ARRAY?reply2->elements:0,
            sdslen(key));
    } else {
        log_error("%s command replys are inconsistency, "
            "reply type: %d %d, "
            "reply status: %s %s, "
            "reply error: %s %s, "
            "reply integer: %lld %lld, "
            "reply array len: %zu %zu",
            du->dp->name,
            reply1->type, reply2->type, 
            reply1->type==REDIS_REPLY_STATUS?reply1->str:"NULL",
            reply2->type==REDIS_REPLY_STATUS?reply2->str:"NULL",
            reply1->type==REDIS_REPLY_ERROR?reply1->str:"NULL",
            reply2->type==REDIS_REPLY_ERROR?reply2->str:"NULL",
            reply1->type==REDIS_REPLY_INTEGER?reply1->integer:0,
            reply2->type==REDIS_REPLY_INTEGER?reply2->integer:0,
            reply1->type==REDIS_REPLY_ARRAY?reply1->elements:0,
            reply2->type==REDIS_REPLY_ARRAY?reply2->elements:0);
    }

}

static int sort_replys_if_needed(reply_unit *ru)
{
    data_unit *du = ru->du;
    data_producer *dp = du->dp;
    int step = 0, idx_cmp = 0;

    if (dp->cmd_type&TEST_CMD_TYPE_SET) {
        if (!strcmp(dp->name,"smembers") || 
            !strcmp(dp->name,"sunion") || 
            !strcmp(dp->name,"sdiff") || 
            !strcmp(dp->name,"sinter")) {
            step = 1;
        }
    } else if (dp->cmd_type&TEST_CMD_TYPE_HASH) {
        if (!strcmp(dp->name,"hkeys") || 
            !strcmp(dp->name,"hvals")) {
            step = 1;
        } else if (!strcmp(dp->name,"hgetall")) {
            step = 2;
            idx_cmp = 0;
        }
    }

    if (step > 0) {
        int i;
        redisReply *reply;
        for (i = 0; i < ru->received_count; i ++) {
            reply = ru->replys[i];
            if (reply->type != REDIS_REPLY_ARRAY)
                continue;
            sort_array_by_step(reply->element, reply->elements, 
                step, idx_cmp, reply_string_binary_compare);
        }
    }
    
    return VRT_OK;
}

static int check_replys_if_same(reply_unit *ru)
{
    int j;
    redisReply **replys = ru->replys;
    redisReply *replyb, *reply;

    sort_replys_if_needed(ru);

    replyb = replys[0];
    
    for (j = 1; j < ru->total_count ; j ++) {
        reply = replys[j];
        if (check_two_replys_if_same(replyb, reply)) {
            show_replys_inconsistency_msg(ru->du, replyb, reply);
            return 0;
        }
    }
    
    return 1;
}

struct callback_data {
    dispatch_data_thread *ddt;
    reply_unit *ru;
    int idx;
};

static void reply_callback(redisAsyncContext *c, void *r, void *privdata) {
    int ret;
    redisReply *reply;
    struct callback_data *cbd = privdata;
    dispatch_data_thread *ddt = cbd->ddt;
    reply_unit *ru = cbd->ru;
    
    if (r == NULL) {
        reply = NULL;
    } else {
        /* Beacause reply will be freed by hiredis in async way. */
        reply = steal_hiredis_redisreply(r);
    }

    ru->replys[cbd->idx] = reply;
    ru->received_count ++;
    free(cbd);

    if (ru->received_count >= ru->total_count) {
        int j;
        
        ret = check_replys_if_same(ru);
        if (ret == 1 && reply != NULL) {
            data_unit *du = ru->du;
            data_producer *dp = du->dp;
            if (reply->type == REDIS_REPLY_ERROR) {
                ddt->reply_type_err_count_per_cycle++;
            }

            /* Cache this key if needed. */
            if (dp->need_cache_key_proc != NULL) {
                produce_scheme *ps = du->data;
                if (dp->need_cache_key_proc(reply)) {
                    key_cache_array *kcp = kcp_get_from_ps(ps, dp);
                    sds key = get_one_key_from_data_unit(du);
                    key_cache_array_input(kcp,key,sdslen(key));
                }
            }
        }

        /* release the reply_unit */
        for (j = 0; j < ru->total_count; j ++) {
            freeReplyObject(ru->replys[j]);
            ru->replys[j] = NULL;
        }
        free(ru->replys);
        data_unit_put(ru->du);
        free(ru);
        
        ddt->count_wait_for_reply --;
        ASSERT(ddt->count_wait_for_reply >= 0);
        
        ddt->reply_total_count_per_cycle++;
    }
}

static int dispatch_thread_send_data(dispatch_data_thread *ddt)
{
    int count_per_time = 1000;
    data_unit *du;

    while ((du = dmtqueue_pop(ddt->datas)) != NULL) {
        redisAsyncContext *actx;
        int j;
        
        size_t *argvlen = malloc(du->argc*sizeof(size_t));
        reply_unit *ru = malloc(sizeof(reply_unit));
        ru->du = du;
        ru->total_count = darray_n(ddt->abgs);
        ru->received_count = 0;
        ru->replys = malloc(ru->total_count*sizeof(redisReply *));
        for (j = 0; j < du->argc; j ++) {
            argvlen[j] = sdslen(du->argv[j]);
        }
        for (j = 0; j < darray_n(ddt->abgs); j ++) {
            struct callback_data *cbd;
            int *keyindex, numkeys;
            abtest_server *abs;

            cbd = malloc(sizeof(struct callback_data));
            cbd->ddt = ddt;
            cbd->ru = ru;
            cbd->idx = j;
            abtest_group *abg = darray_get(ddt->abgs, j);

            keyindex = get_keys_from_data_producer(du->dp, du->argv, du->argc, &numkeys);
            if (numkeys == 0) {
                unsigned int idx;
                idx = (unsigned int)rand()%darray_n(&abg->abtest_servers);
                abs = darray_get(&abg->abtest_servers,idx);
            } else {
                sds key = du->argv[keyindex[0]];
                abs = abg->get_backend_server(abg,key,sdslen(key));
            }
            free(keyindex);
            
            conn_context *cc = darray_get(abs->conn_contexts, 
                du->hashvalue%darray_n(abs->conn_contexts));
            actx = cc->actx;
            redisAsyncCommandArgv(actx, reply_callback, cbd, du->argc, du->argv, argvlen);
        }
        free(argvlen);

        ddt->count_wait_for_reply ++;
        
        if (count_per_time-- <= 0) break;
    }

    return VRT_OK;
}

static int dispatch_data_thread_cron(aeEventLoop *eventLoop, long long id, void *clientData)
{
    dispatch_data_thread *ddt = clientData;

    ASSERT(eventLoop == ddt->el);

    /* At the begin of this loop */
    if (ddt->pause) {
        if (!test_if_need_pause()) {
            ddt->pause = 0;
        } else {
            ddt->cronloops ++;
            return 1000;
        }
    }

    if (ddt->count_wait_for_reply < 4000 && 
        !dmtqueue_empty(ddt->datas)) {
        dispatch_thread_send_data(ddt);
    }

    /* At the end of this loop */
    if (test_if_need_pause() && 
        all_produce_threads_paused() && 
        all_backend_threads_paused() && 
        dmtqueue_empty(ddt->datas) &&
        dlistLength(ddt->rdatas) == 0) {
        
        ddt->pause = 1;

        /* Update the dispatch state */
        update_state_add(total_tested_commands_count_per_cycle,
            ddt->reply_total_count_per_cycle);
        update_state_add(total_tested_commands_count,
            ddt->reply_total_count_per_cycle);
        ddt->reply_total_count_per_cycle = 0;
        update_state_add(total_reply_err_count_per_cycle,
            ddt->reply_type_err_count_per_cycle);
        update_state_add(total_reply_err_count,
            ddt->reply_type_err_count_per_cycle);
        ddt->reply_type_err_count_per_cycle = 0;

        one_dispatch_thread_paused();
    }

    ddt->cronloops ++;
    return 1000/ddt->hz;
}

static void connect_callback(const redisAsyncContext *c, int status) {
    dispatch_data_thread *ddt = c->data;
    if (status != REDIS_OK) {
        log_error("Error: %s\n", c->errstr);
        //aeStop(loop);
        return;
    }

    //test_log_out("Connected...\n");
}

static void disconnect_callback(const redisAsyncContext *c, int status) {
    dispatch_data_thread *ddt = c->data;
    if (status != REDIS_OK) {
        log_error("Error: %s\n", c->errstr);
        //aeStop(loop);
        return;
    }

    //test_log_out("Disconnected...\n");
    //aeStop(loop);
}

static int dispatch_conn_context_init(conn_context *cc, char *host, int port)
{
    cc->ctx = NULL;
    cc->actx = NULL;

    cc->actx = redisAsyncConnect(host, port);
    if (cc->actx == NULL) {
        return VRT_ERROR;
    }
    
    return VRT_OK;
}

static void dispatch_conn_context_deinit(conn_context *cc)
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

static int dispatch_data_thread_init(dispatch_data_thread *ddt, char *test_target_groups, int connections)
{
    int i, j, k;

    ddt->id = 0;
    ddt->thread_id = 0;
    ddt->el = NULL;
    ddt->hz = 10;
    ddt->cronloops = 0;
    ddt->datas = NULL;
    ddt->rdatas = NULL;
    ddt->abgs = NULL;
    ddt->pause = 0;
    ddt->count_wait_for_reply = 0;
    ddt->reply_total_count_per_cycle = 0;
    ddt->reply_type_err_count_per_cycle = 0;

    ddt->el = aeCreateEventLoop(200);
    if (ddt->el == NULL) {
        return VRT_ERROR;
    }

    ddt->datas = dmtqueue_create();
    if (ddt->datas == NULL) {
        return VRT_ERROR;
    }

    if (dmtqueue_init_with_lockqueue(ddt->datas, NULL) != 0) {
        return VRT_ERROR;
    }

    ddt->rdatas = dlistCreate();
    if (ddt->rdatas == NULL) {
        return VRT_ERROR;
    }
    
    ddt->abgs = abtest_groups_create(test_target_groups);
    if (ddt->abgs == NULL) {
        return VRT_ERROR;
    }

    /* Init connection context for each server */
    for (i = 0; i < darray_n(ddt->abgs); i ++) {
        abtest_group *abg = darray_get(ddt->abgs, i);
        for (j = 0; j < darray_n(&abg->abtest_servers); j ++) {
            abtest_server *abs = darray_get(&abg->abtest_servers, j);
            abs->conn_contexts = darray_create(connections, sizeof(conn_context));
            for (k = 0; k < connections; k ++) {
                conn_context *cc = darray_push(abs->conn_contexts);
                if (dispatch_conn_context_init(cc,abs->host,abs->port) != VRT_OK) {
                    return VRT_ERROR;
                }
                cc->actx->data = ddt;
                redisAeAttach(ddt->el, cc->actx);
                redisAsyncSetConnectCallback(cc->actx,connect_callback);
                redisAsyncSetDisconnectCallback(cc->actx,disconnect_callback);
            }
        }
    }

    if (aeCreateTimeEvent(ddt->el, 1, dispatch_data_thread_cron, ddt, NULL) == AE_ERR) {
        return VRT_ERROR;
    }
    
    return VRT_OK;
}

static void dispatch_data_thread_deinit(dispatch_data_thread *ddt)
{
    if (ddt->el) {
        aeDeleteEventLoop(ddt->el);
        ddt->el = NULL;
    }

    if (ddt->datas) {
        dmtqueue_destroy(ddt->datas);
        ddt->datas = NULL;
    }

    if (ddt->abgs) {
        int i, j, k;
        /* Deinit connection context for each server */
        for (i = 0; i < darray_n(ddt->abgs); i ++) {
            abtest_group *abg = darray_get(ddt->abgs, i);
            for (j = 0; j < darray_n(&abg->abtest_servers); j ++) {
                abtest_server *abs = darray_get(&abg->abtest_servers, j);
                while (darray_n(abs->conn_contexts) > 0) {
                    conn_context *cc = darray_pop(abs->conn_contexts);
                    dispatch_conn_context_deinit(cc);
                }
            }
        }
        
        abtest_groups_destroy(ddt->abgs);
        ddt->abgs = NULL;
    }
}

int vrt_dispatch_data_init(int threads_count, char *test_target_groups, int connections)
{
    int j;
    
    dispatch_data_threads_count = threads_count;
    dispatch_data_threads = darray_create(threads_count, sizeof(dispatch_data_thread));
    if (dispatch_data_threads == NULL) {
        return VRT_ERROR;
    }

    for (j = 0; j < threads_count; j ++) {
        dispatch_data_thread *ddt = darray_push(dispatch_data_threads);
        if (dispatch_data_thread_init(ddt, test_target_groups, connections) != VRT_OK) {
            return VRT_ERROR;
        }
        ddt->id = j;
    }
    
    return VRT_OK;
}

void vrt_dispatch_data_deinit(void)
{
    if (dispatch_data_threads) {
        while (darray_n(dispatch_data_threads) > 0) {
            dispatch_data_thread *ddt = darray_pop(dispatch_data_threads);
            dispatch_data_thread_deinit(ddt);
        }
        darray_destroy(dispatch_data_threads);
        dispatch_data_threads = NULL;
    }
}

static void *vrt_dispatch_data_thread_run(void *args)
{
    dispatch_data_thread *ddt = args;
    srand(vrt_usec_now()^(int)pthread_self());

    aeMain(ddt->el);
    
    return NULL;
}

int vrt_start_dispatch_data(void)
{
    unsigned int i;
    for (i = 0; i < darray_n(dispatch_data_threads); i ++) {
        pthread_attr_t attr;
        dispatch_data_thread *ddt;
        pthread_attr_init(&attr);
        ddt = darray_get(dispatch_data_threads, i);
        pthread_create(&ddt->thread_id, 
            &attr, vrt_dispatch_data_thread_run, ddt);
    }
    
    return VRT_OK;
}

int vrt_wait_dispatch_data(void)
{
    unsigned int i;
    /* wait for the produce threads finish */
	for(i = 0; i < darray_n(dispatch_data_threads); i ++){
		dispatch_data_thread *ddt = darray_get(dispatch_data_threads, i);
		pthread_join(ddt->thread_id, NULL);
	}
    
    return VRT_OK;
}

/* return value 
  * 1: need sleep a while because of there are too many data cached
  * 0: can normally continue
  * -1: error occur */
int data_dispatch(data_unit *du)
{
    int thread_idx;
    dispatch_data_thread *ddt;
    long long length;
    int *keyindex, numkeys;

    keyindex = get_keys_from_data_producer(du->dp, du->argv, du->argc, &numkeys);

    if (numkeys == 0) {
        du->hashvalue = (unsigned int)rand();
    } else {
        sds key = du->argv[keyindex[0]];
        du->hashvalue = (unsigned int)hash_crc32a(key, sdslen(key));
    }
    free(keyindex);
    
    thread_idx = du->hashvalue%dispatch_data_threads_count;
    ddt = darray_get(dispatch_data_threads, thread_idx);
    length = dmtqueue_push(ddt->datas, du);
    if (length <= 0) {
        test_log_error("Data unit push to dispatch thread %d failed", ddt->id);
        return -1;
    } else if (length > 2000) {
        return 1;
    }

    return 0;
}
