#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <fcntl.h>
#include <errno.h>
#include <signal.h>
#include <assert.h>
#include <sys/stat.h>
#include <sys/utsname.h>

#include <hiredis.h>
#include <darray.h>

#include <vrt_util.h>
#include <vrt_public.h>
#include <vrabtest.h>
#include <vrt_produce_data.h>

typedef struct produce_scheme {
    long long ckeys_write_idx;
    long long max_ckeys_count;    /* Max keys count that can be cached in the ckeys array. */
    sds *ckeys;    /* Cached keys that may exist in the target redis/vire servers. */

    int hit_ratio;   /* Hit ratio for the read commands. [0%,100%] */
    int hit_ratio_idx;   /* [0,hit_ratio_array_len-1] */
    int hit_ratio_array_len; /* 100 usually */
    int *hit_ratio_array;    /* Stored 0 or 1 for every element, 1 means used key in the cached keys array. */
} produce_scheme;

typedef struct produce_thread {
    int id;
    pthread_t thread_id;

    produce_scheme *ps;

    int pause;
    long long looptimes;
} produce_thread;

data_producer *delete_data_producer = NULL;

static unsigned int key_length_min;
static unsigned int key_length_max;
static unsigned int key_length_range_gap;
static unsigned int field_length_max;
static unsigned int string_length_max;

static int cmd_type;

static darray needed_cmd_type_producer;  /* type:  data_producer*/
static unsigned int needed_cmd_type_producer_count;

int produce_data_threads_count;
static darray produce_threads;

int produce_threads_pause_finished_count;

static sds get_random_cached_key(produce_scheme *ps)
{
    int idx;
    sds key;

    key = ps->ckeys[0];
    if (sdslen(key) == 0) {
        return NULL;
    }

    key = ps->ckeys[ps->max_ckeys_count-1];
    if (sdslen(key) == 0) {
        idx = rand()%ps->ckeys_write_idx;
    } else {
        idx = rand()%ps->max_ckeys_count;
    }
    
    return ps->ckeys[idx];
}

static unsigned int get_random_num(void)
{
    return (unsigned int)rand();
}

static char get_random_char(void)
{
    return (char)rand()%250 + 5;
}

static sds get_random_key(void)
{
    unsigned int i, len;
    sds str = sdsempty();
    
    len = key_length_range_gap==0?key_length_min:
        (get_random_num()%key_length_range_gap+key_length_min);
    if (len == 0) len ++;
    str = sdsMakeRoomFor(str,(size_t)len);
    sdsIncrLen(str, (int)len);

    for (i = 0; i < len; i ++) {
        str[i] = (char)get_random_char();
    }

    return str;
}

static sds get_random_string(void)
{
    unsigned int i, len;
    sds str = sdsempty();
    
    len = get_random_num()%string_length_max;
    str = sdsMakeRoomFor(str,(size_t)len);
    sdsIncrLen(str, (int)len);

    for (i = 0; i < len; i ++) {
        str[i] = get_random_char();
    }

    return str;
}

static sds get_random_key_with_hit_ratio(produce_scheme *ps)
{
    sds key;
    if (ps->hit_ratio_array[ps->hit_ratio_idx++] == 0 || 
        ps->ckeys[ps->ckeys_write_idx] == NULL) {
        key = get_random_key();
    } else {
        key = get_random_cached_key(ps);
        if (key == NULL) key = get_random_key();
        else key = sdsdup(key);
    }
    if (ps->hit_ratio_idx >= ps->hit_ratio_array_len) {
        ps->hit_ratio_idx = 0;
    }
    return key;
}

static data_unit *get_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;

    du = data_unit_get();
    du->dp = dp;
    du->argc = 2;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps);
    
    return du;
}

static data_unit *set_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;

    du = data_unit_get();
    du->dp = dp;
    du->argc = 3;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key();
    du->argv[2] = get_random_string();
    
    return du;
}

static data_unit *setnx_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;

    du = data_unit_get();
    du->dp = dp;
    du->argc = 3;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps);
    du->argv[2] = get_random_string();
    
    return du;
}

static data_unit *setex_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;

    du = data_unit_get();
    du->dp = dp;
    du->argc = 4;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key();
    du->argv[2] = sdsfromlonglong(rand()%10000);
    du->argv[3] = get_random_string();
    
    return du;
}

static data_unit *psetex_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;

    du = data_unit_get();
    du->dp = dp;
    du->argc = 4;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key();
    du->argv[2] = sdsfromlonglong(rand()%10000);
    du->argv[3] = get_random_string();
    
    return du;
}

static data_unit *del_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;

    du = data_unit_get();
    du->dp = dp;
    du->argc = 2;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key();
    
    return du;
}

static data_unit *expire_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;

    du = data_unit_get();
    du->dp = dp;
    du->argc = 3;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps);
    du->argv[2] = sdsfromlonglong(rand()%10000);
    
    return du;
}

static data_unit *expireat_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;

    du = data_unit_get();
    du->dp = dp;
    du->argc = 3;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps);
    du->argv[2] = sdsfromlonglong(vrt_msec_now()/1000LL+rand()%10000);
    
    return du;
}

static data_unit *exists_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;

    du = data_unit_get();
    du->dp = dp;
    du->argc = 2;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps);
    
    return du;
}

static data_unit *ttl_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;

    du = data_unit_get();
    du->dp = dp;
    du->argc = 2;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps);
    
    return du;
}

static data_unit *pttl_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;

    du = data_unit_get();
    du->dp = dp;
    du->argc = 2;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps);
    
    return du;
}

static data_unit *incr_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;

    du = data_unit_get();
    du->dp = dp;
    du->argc = 2;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key();
    
    return du;
}

static int producers_count;
data_producer redis_data_producer_table[] = {
    /* Key */
    {"del",del_cmd_producer,-2,"w",0,NULL,1,-1,1,TEST_CMD_TYPE_KEY},
    {"exists",exists_cmd_producer,-2,"rF",0,NULL,1,-1,1,TEST_CMD_TYPE_KEY},
    {"ttl",ttl_cmd_producer,2,"rF",0,NULL,1,1,1,TEST_CMD_TYPE_EXPIRE},
    {"pttl",pttl_cmd_producer,2,"rF",0,NULL,1,1,1,TEST_CMD_TYPE_EXPIRE},
    {"expire",expire_cmd_producer,3,"wF",0,NULL,1,1,1,TEST_CMD_TYPE_EXPIRE},
    {"expireat",expireat_cmd_producer,3,"wF",0,NULL,1,1,1,TEST_CMD_TYPE_EXPIRE},
    /* String */
    {"get",get_cmd_producer,2,"rF",0,NULL,1,1,1,TEST_CMD_TYPE_STRING},
    {"set",set_cmd_producer,-3,"wmA",0,NULL,1,1,1,TEST_CMD_TYPE_STRING},
    {"setnx",setnx_cmd_producer,3,"wmF",0,NULL,1,1,1,TEST_CMD_TYPE_STRING},
    {"setex",setex_cmd_producer,4,"wmA",0,NULL,1,1,1,TEST_CMD_TYPE_EXPIRE},
    {"psetex",psetex_cmd_producer,4,"wmA",0,NULL,1,1,1,TEST_CMD_TYPE_EXPIRE},
    {"incr",incr_cmd_producer,2,"wmF",0,NULL,1,1,1,TEST_CMD_TYPE_STRING}
};

data_unit *data_unit_get(void)
{
    data_unit *du = malloc(sizeof(data_unit));
    du->dp = NULL;
    du->argc = 0;
    du->argv = NULL;
    du->hashvalue = 0;
    return du;
}

void data_unit_put(data_unit *du)
{
    int idx;
    
    for (idx = 0; idx < du->argc; idx ++) {
        if (du->argv[idx])
            sdsfree(du->argv[idx]);
    }
    free(du->argv);
    free(du);
}

static produce_scheme *produce_scheme_create(long long max_cached_keys, int hit_ratio)
{
    produce_scheme *ps;
    int count, idx;
    int ratio;
    
    ps = malloc(sizeof(*ps));
    if (ps == NULL) return NULL;

    ps->max_ckeys_count = max_cached_keys;
    ps->ckeys_write_idx = 0;
    ps->ckeys = malloc(ps->max_ckeys_count*sizeof(sds));
    for (idx = 0; idx < ps->max_ckeys_count; idx ++) {
        ps->ckeys[idx] = sdsempty();
        sdsMakeRoomFor(ps->ckeys[idx],key_length_max);
    }

    /* Generate the hit ratio. */
    ps->hit_ratio_array_len = 100;
    ps->hit_ratio = hit_ratio;
    ps->hit_ratio_idx = 0;
    ps->hit_ratio_array = malloc(ps->hit_ratio_array_len*sizeof(int));
    ratio = ps->hit_ratio_array_len/ps->hit_ratio;
    if (ratio > 1) {
        count = ps->hit_ratio;
        for (idx = 0; idx < ps->hit_ratio_array_len; idx ++) {
            ps->hit_ratio_array[idx] = 0;
        }
    } else {
        count = ps->hit_ratio_array_len - ps->hit_ratio;
        for (idx = 0; idx < ps->hit_ratio_array_len; idx ++) {
            ps->hit_ratio_array[idx] = 1;
        }
    }
    while (count > 0) {
        idx = rand()%ps->hit_ratio_array_len;
        if (ratio > 1) {
            if (ps->hit_ratio_array[idx] == 0) {
                count --;
                ps->hit_ratio_array[idx] = 1;
            }
        } else {
            if (ps->hit_ratio_array[idx] == 1) {
                count --;
                ps->hit_ratio_array[idx] = 0;
            }
        }
    }

    return ps;
}

static void produce_scheme_destroy(produce_scheme *ps)
{
    int j;
    if (ps->ckeys) {
        for (j = 0; j < ps->max_ckeys_count; j ++) {
            if (ps->ckeys[j]) sdsfree(ps->ckeys[j]);
        }
        free(ps->ckeys);
    }
    
    free(ps->hit_ratio_array);

    free(ps);
}

static int vrt_produce_threads_init(unsigned int produce_threads_count, 
    long long cached_keys, int hit_ratio)
{
    unsigned int idx;
    darray_init(&produce_threads, produce_threads_count, sizeof(produce_thread));
    produce_data_threads_count = produce_threads_count;
    for (idx = 0; idx < produce_threads_count; idx ++) {
        produce_thread *pt = darray_push(&produce_threads);
        pt->id = idx;
        pt->thread_id = 0;
        pt->ps = produce_scheme_create(cached_keys, hit_ratio);
        pt->pause = 0;
        pt->looptimes = 0;
    }
    
    return VRT_OK;
}

static void vrt_produce_threads_deinit(void)
{
    produce_thread *pt;
    while (darray_n(&produce_threads) > 0) {
        pt = darray_pop(&produce_threads);
        if (pt->ps) {
            produce_scheme_destroy(pt->ps);
            pt->ps = NULL;
        }
    }
    darray_deinit(&produce_threads);
}

static void *vrt_produce_thread_run(void *args)
{
    int ret;
    produce_thread *pt = args;
    unsigned int idx, j;
    data_producer **dp;
    data_unit *du;

    srand(vrt_usec_now()^(int)pthread_self());

    while (1) {
        /* At begin of this loop */
        if (pt->pause) {
            usleep(1000000);    /* sleep 1 second */
            if (!test_if_need_pause()) {
                pt->pause = 0;
            } else {
                continue;
            }
        } else if (pt->looptimes%10000 == 0) {
            if (test_if_need_pause()) {
                pt->pause = 1;
                one_produce_thread_paused();
                continue;
            }
        }
        
        idx = rand()%needed_cmd_type_producer_count;
        dp = darray_get(&needed_cmd_type_producer,idx);
        du = (*dp)->proc(*dp,pt->ps);

        /* Cache this key if needed. */
        if ((*dp)->flags&PRO_ADD) {
            produce_scheme *ps = pt->ps;
            int numkeys;
            int *keyindex;
            sds key;

            keyindex = get_keys_from_data_producer((*dp),du->argv,du->argc,&numkeys);
            if (numkeys <= 0) {
                assert(0);
                return NULL;
            }

            key = du->argv[keyindex[0]];
            free(keyindex);
            sdscpylen(ps->ckeys[ps->ckeys_write_idx],key,sdslen(key));
            ps->ckeys_write_idx ++;
            if (ps->ckeys_write_idx >= ps->max_ckeys_count)
                ps->ckeys_write_idx = 0;
        }

        /* Dispatch the test data */
        ret = data_dispatch(du);
        if (ret == -1) {
            data_unit_put(du);
        } else if (ret == 1) {
            usleep(10000);
        }

        pt->looptimes ++;
    }
    
    return NULL;
}

int vrt_produce_data_init(int key_length_range_min,int key_length_range_max, 
    int string_max_length,int fields_max_count,
    int produce_cmd_types,unsigned int produce_threads_count,long long cached_keys,
    int hit_ratio)
{
    int j;
    
    key_length_min = key_length_range_min;
    key_length_max = key_length_range_max;
    if (key_length_max < key_length_min) return VRT_ERROR;
    key_length_range_gap = key_length_max-key_length_min;
    field_length_max = fields_max_count;
    string_length_max = string_max_length;
    cmd_type = produce_cmd_types;
    darray_init(&needed_cmd_type_producer, 100, sizeof(data_producer*));

    producers_count = sizeof(redis_data_producer_table)/sizeof(data_producer);
    for (j = 0; j < producers_count; j++) {
        data_producer *dp = redis_data_producer_table+j;
        char *f = dp->sflags;

        while(*f != '\0') {
            switch(*f) {
            case 'w': dp->flags |= PRO_WRITE; break;
            case 'r': dp->flags |= PRO_READONLY; break;
            case 'm': dp->flags |= PRO_DENYOOM; break;
            case 'a': dp->flags |= PRO_ADMIN; break;
            case 'p': dp->flags |= PRO_PUBSUB; break;
            case 's': dp->flags |= PRO_NOSCRIPT; break;
            case 'R': dp->flags |= PRO_RANDOM; break;
            case 'S': dp->flags |= PRO_SORT_FOR_SCRIPT; break;
            case 'l': dp->flags |= PRO_LOADING; break;
            case 't': dp->flags |= PRO_STALE; break;
            case 'M': dp->flags |= PRO_SKIP_MONITOR; break;
            case 'k': dp->flags |= PRO_ASKING; break;
            case 'F': dp->flags |= PRO_FAST; break;
            case 'A': dp->flags |= PRO_ADD; break;
            default: return VRT_ERROR;
            }
            f++;
        }

        if (dp->cmd_type&cmd_type) {
            data_producer **dp_elem = darray_push(
                &needed_cmd_type_producer);
            *dp_elem = dp;
            needed_cmd_type_producer_count ++;
        }
        if (dp->cmd_type&TEST_CMD_TYPE_EXPIRE && expire_enabled) {
            data_producer **dp_elem = darray_push(
                &needed_cmd_type_producer);
            *dp_elem = dp;
            needed_cmd_type_producer_count ++;
        }

        if (!strcmp(dp->name,"del")) {
            delete_data_producer = dp;
        }
    }

    if (delete_data_producer == NULL) {
        return VRT_ERROR;
    }

    if (needed_cmd_type_producer_count == 0) {
        return VRT_ERROR;
    }

    vrt_produce_threads_init(produce_threads_count, cached_keys, hit_ratio);
    
    return VRT_OK;
}

void vrt_produce_data_deinit(void)
{
    vrt_produce_threads_deinit();

    needed_cmd_type_producer.nelem = 0;
    darray_deinit(&needed_cmd_type_producer);
}

int vrt_start_produce_data(void)
{
    unsigned int i;
    for (i = 0; i < darray_n(&produce_threads); i ++) {
        pthread_attr_t attr;
        produce_thread *pt;
        pthread_attr_init(&attr);
        pt = darray_get(&produce_threads, i);
        pthread_create(&pt->thread_id, 
            &attr, vrt_produce_thread_run, pt);
    }
    
    last_test_begin_time = vrt_sec_now();
    return VRT_OK;
}

int vrt_wait_produce_data(void)
{
    unsigned int i;
    /* wait for the produce threads finish */
	for(i = 0; i < darray_n(&produce_threads); i ++){
		produce_thread *pt = darray_get(&produce_threads, i);
		pthread_join(pt->thread_id, NULL);
	}
    
    return VRT_OK;
}

/* -----------------------------------------------------------------------------
 * API to get key arguments from data producers
 * ---------------------------------------------------------------------------*/

/* The base case is to use the keys position as given in the data producer table
 * (firstkey, lastkey, step). */
static int *get_keys_using_data_producer_table(data_producer *dp,sds *argv, int argc, int *numkeys) {
    int j, i = 0, last, *keys;

    if (dp->firstkey == 0) {
        *numkeys = 0;
        return NULL;
    }
    last = dp->lastkey;
    if (last < 0) last = argc+last;
    keys = malloc(sizeof(int)*((last - dp->firstkey)+1));
    for (j = dp->firstkey; j <= last; j += dp->keystep) {
        keys[i++] = j;
    }
    *numkeys = i;
    return keys;
}

/* Return all the arguments that are keys in the command passed via argc / argv.
 *
 * The command returns the positions of all the key arguments inside the array,
 * so the actual return value is an heap allocated array of integers. The
 * length of the array is returned by reference into *numkeys.
 *
 * 'cmd' must be point to the corresponding entry into the redisCommand
 * table, according to the command name in argv[0].
 *
 * This function uses the command table if a command-specific helper function
 * is not required, otherwise it calls the command-specific function. */
int *get_keys_from_data_producer(data_producer *dp, sds *argv, int argc, int *numkeys) {
    if (dp->getkeys_proc) {
        return dp->getkeys_proc(dp,argv,argc,numkeys);
    } else {
        return get_keys_using_data_producer_table(dp,argv,argc,numkeys);
    }
}
