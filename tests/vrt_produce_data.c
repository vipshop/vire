#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <fcntl.h>
#include <errno.h>
#include <signal.h>
#include <assert.h>
#include <math.h>
#include <sys/stat.h>
#include <sys/utsname.h>

#include <dspecialconfig.h>

#include <hiredis.h>
#include <darray.h>
#include <dutil.h>
#include <dlog.h>

#include <vrt_util.h>
#include <vrt_public.h>
#include <vrabtest.h>
#include <vrt_produce_data.h>

#define PRODUCE_KEY_CACHE_POOL_COUNT 5

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

static int key_cache_pools_count = 0;

static darray needed_cmd_type_producer;  /* type:  data_producer*/
static unsigned int needed_cmd_type_producer_count;

int produce_data_threads_count;
static darray produce_threads;

int produce_threads_pause_finished_count;

static int non_empty_kcps_count = 0;
unsigned int non_empty_kcps_idx[PRODUCE_KEY_CACHE_POOL_COUNT] = {-1};

static sds get_random_cached_key(produce_scheme *ps, data_producer *dp)
{
    key_cache_array *kcp = kcp_get_from_ps(ps,dp);
    return key_cache_array_random(kcp);
}

static int get_random_int(void)
{
    if (rand()%2 == 1) {
        return 0 - (int)rand();
    } else {
        return (int)rand();
    }
}

static unsigned int get_random_unsigned_int(void)
{
    return (unsigned int)rand();
}

static char get_random_char(void)
{
    return (char)rand()%250 + 5;
    //return (char)(rand()%25 + 97);
}

static sds get_random_key(void)
{
    unsigned int i, len;
    sds str = sdsempty();
    
    len = key_length_range_gap==0?key_length_min:
        (get_random_unsigned_int()%key_length_range_gap+key_length_min);
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
    
    len = get_random_unsigned_int()%string_length_max;
    str = sdsMakeRoomFor(str,(size_t)len);
    sdsIncrLen(str, (int)len);

    for (i = 0; i < len; i ++) {
        str[i] = get_random_char();
    }

    return str;
}

static sds get_random_float_str(void)
{
    unsigned int decimal_len;
    sds str;

    if (rand()%2 == 1) {
        str = sdsnew("-");
    } else {
        str = sdsempty();
    }

    if (rand()%2 == 1) {
        str = sdscatfmt(str,"%u.%u",
            get_random_unsigned_int(),
            get_random_unsigned_int());
    } else {
        str = sdscatfmt(str,"%u",
            get_random_unsigned_int());
    }

    return str;
}

#define ZSET_RANGE_MIN_MAX_TYPE_RANK    0
#define ZSET_RANGE_MIN_MAX_TYPE_SCORE   1
#define ZSET_RANGE_MIN_MAX_TYPE_LEX     2
static sds *get_random_zset_range_min_max_str(int range_type)
{
    sds *range; /* range[0] is the min, range[1] is the max */
    unsigned int probability = rand()%100;

    range = malloc(2*sizeof(sds));
    if (range_type == ZSET_RANGE_MIN_MAX_TYPE_RANK) {
        unsigned int min = get_random_unsigned_int();
        unsigned int max = get_random_unsigned_int();

        if (probability >= 95 && min <= max || 
            probability < 95 && min > max) {
            range[0] = sdsfromlonglong((long long)max);
            range[1] = sdsfromlonglong((long long)min);
        } else {
            range[0] = sdsfromlonglong((long long)min);
            range[1] = sdsfromlonglong((long long)max);
        }
    } else if (range_type == ZSET_RANGE_MIN_MAX_TYPE_SCORE) {
        sds min_str = get_random_float_str();
        sds max_str = get_random_float_str();
        float min, max;
        char *eptr;
        sds swap;
        unsigned int min_probability = rand()%3;
        unsigned int max_probability = rand()%3;

        min = strtod(min_str,&eptr);
        if (eptr[0] != '\0' || isnan(min)) {
            sdsfree(min_str);
            sdsfree(max_str);
            free(range);
            return NULL;
        }
        max = strtod(max_str,&eptr);
        if (eptr[0] != '\0' || isnan(max)) {
            sdsfree(min_str);
            sdsfree(max_str);
            free(range);
            return NULL;
        }
        if (probability >= 95 && min <= max || 
            probability < 95 && min > max) {
            swap = min_str;
            min_str = max_str;
            max_str = swap;
        }
        
        if (min_probability == 0) {
            range[0] = sdsnew("-inf");
        } else if (min_probability == 1) {
            range[0] = sdsnew("(");
            range[0] = sdscatfmt(range[0],"%S",min_str);
            sdsfree(min_str);
        } else {
            range[0] = min_str;
        }
        if (max_probability == 0) {
            range[1] = sdsnew("+inf");
        } else if (max_probability == 1) {
            range[1] = sdsnew("(");
            range[1] = sdscatfmt(range[1],"%S",max_str);
            sdsfree(max_str);
        } else {
            range[1] = max_str;
        }
    } else if (range_type == ZSET_RANGE_MIN_MAX_TYPE_LEX) {
        sds min_str = get_random_string();
        sds max_str = get_random_string();
        sds swap;
        unsigned int min_probability = rand()%3;
        unsigned int max_probability = rand()%3;

        if (probability >= 95 && sdscmp(min_str,max_str) < 0 || 
            probability < 95 && sdscmp(min_str,max_str) > 0) {
            swap = min_str;
            min_str = max_str;
            max_str = swap;
        }
        
        if (min_probability == 0) {
            range[0] = sdsnew("-");
        } else if (min_probability == 1) {
            range[0] = sdsnew("(");
            range[0] = sdscatfmt(range[0],"%S",min_str);
            sdsfree(min_str);
        } else {
            range[0] = sdsnew("[");
            range[0] = sdscatfmt(range[0],"%S",min_str);
            sdsfree(min_str);
        }
        if (max_probability == 0) {
            range[1] = sdsnew("+");
        } else if (max_probability == 1) {
            range[1] = sdsnew("(");
            range[1] = sdscatfmt(range[1],"%S",max_str);
            sdsfree(max_str);
        } else {
            range[1] = sdsnew("[");
            range[1] = sdscatfmt(range[1],"%S",max_str);
            sdsfree(max_str);
        }
    } else {
        free(range);
        range = NULL; 
    }

    return range;
}

static unsigned int get_random_field_len(void)
{
    return get_random_unsigned_int()%field_length_max + 1;
}

static sds get_random_key_with_hit_ratio(produce_scheme *ps, data_producer *dp)
{
    sds key;
    if (ps->hit_ratio_array[ps->hit_ratio_idx++] == 0) {
        key = get_random_key();
    } else {
        key = get_random_cached_key(ps,dp);
        if (key == NULL) key = get_random_key();
    }
    if (ps->hit_ratio_idx >= ps->hit_ratio_array_len) {
        ps->hit_ratio_idx = 0;
    }
    return key;
}

/************** Need cache key implement ************/
static int nck_when_noerror(redisReply *reply)
{
    if (reply == NULL) return 0;

    if (reply->type != REDIS_REPLY_ERROR) {
        return 1;
    }

    return 0;
}

static int nck_when_ok(redisReply *reply)
{
    if (reply == NULL) return 0;

    if (reply->type == REDIS_REPLY_STATUS && 
        !strcmp(reply->str, "OK")) {
        return 1;
    }

    return 0;
}

static int nck_when_str(redisReply *reply)
{
    if (reply == NULL) return 0;

    if (reply->type == REDIS_REPLY_STRING) {
        return 1;
    }

    return 0;
}

static int nck_when_unsigned_integer(redisReply *reply)
{
    if (reply == NULL) return 0;

    if (reply->type == REDIS_REPLY_INTEGER && 
        reply->integer >= 0) {
        return 1;
    }

    return 0;
}

static int nck_when_nonzero_unsigned_integer(redisReply *reply)
{
    if (reply == NULL) return 0;

    if (reply->type == REDIS_REPLY_INTEGER && 
        reply->integer > 0) {
        return 1;
    }

    return 0;
}

static int nck_when_zero_or_one(redisReply *reply)
{
    if (reply == NULL) return 0;

    if (reply->type == REDIS_REPLY_INTEGER && 
        (reply->integer == 0 || reply->integer == 1)) {
        return 1;
    }

    return 0;
}

static int nck_when_one(redisReply *reply)
{
    if (reply == NULL) return 0;

    if (reply->type == REDIS_REPLY_INTEGER && 
        reply->integer == 1) {
        return 1;
    }

    return 0;
}

/************** Need cache key implement end ************/

static data_unit *get_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;

    du = data_unit_get();
    du->dp = dp;
    du->argc = 2;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
    
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
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
    du->argv[2] = get_random_string();
    
    return du;
}

/* Need cache key? */
static int setnx_cmd_nck(redisReply *reply)
{
    if (reply == NULL) return 0;

    if (reply->type == REDIS_REPLY_INTEGER && 
        reply->integer == 1) {
        return 1;
    }

    return 0;
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
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
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
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
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
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
    
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
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
    
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
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
    
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

static data_unit *decr_cmd_producer(data_producer *dp, produce_scheme *ps)
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

static data_unit *incrby_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;

    du = data_unit_get();
    du->dp = dp;
    du->argc = 3;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key();
    du->argv[2] = sdsfromlonglong(rand()%10000);
    
    return du;
}

static data_unit *decrby_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;

    du = data_unit_get();
    du->dp = dp;
    du->argc = 3;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key();
    du->argv[2] = sdsfromlonglong(rand()%10000);
    
    return du;
}

static data_unit *append_cmd_producer(data_producer *dp, produce_scheme *ps)
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

/* Need cache key? */
static int append_cmd_nck(redisReply *reply)
{
    if (reply == NULL) return 0;

    if (reply->type == REDIS_REPLY_INTEGER) {
        return 1;
    }

    return 0;
}

static data_unit *strlen_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;

    du = data_unit_get();
    du->dp = dp;
    du->argc = 2;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
    
    return du;
}

static data_unit *getset_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;

    du = data_unit_get();
    du->dp = dp;
    du->argc = 3;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
    du->argv[2] = get_random_string();
    
    return du;
}

static data_unit *incrbyfloat_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;

    du = data_unit_get();
    du->dp = dp;
    du->argc = 3;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
    du->argv[2] = get_random_float_str();
    
    return du;
}

static data_unit *setbit_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;

    du = data_unit_get();
    du->dp = dp;
    du->argc = 4;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
    du->argv[2] = sdsfromlonglong(get_random_unsigned_int()%30000);
    if (rand()%2) {
        du->argv[3] = sdsnew("1");
    } else {
        du->argv[3] = sdsnew("0");
    }
    
    return du;
}

static data_unit *getbit_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;

    du = data_unit_get();
    du->dp = dp;
    du->argc = 3;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
    du->argv[2] = sdsfromlonglong(get_random_unsigned_int()%30000);
    
    return du;
}

static data_unit *setrange_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;

    du = data_unit_get();
    du->dp = dp;
    du->argc = 4;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
    du->argv[2] = sdsfromlonglong(get_random_unsigned_int()%30000);
    du->argv[3] = get_random_string();
    
    return du;
}

static data_unit *getrange_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;

    du = data_unit_get();
    du->dp = dp;
    du->argc = 4;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
    du->argv[2] = sdsfromlonglong(get_random_int()%30000);
    du->argv[3] = sdsfromlonglong(get_random_int()%30000);
    
    return du;
}

static data_unit *bitcount_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;
    int with_range = 0;

    if (rand()%2)
        with_range = 1;

    du = data_unit_get();
    du->dp = dp;
    du->argc = with_range?4:2;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
    if (with_range) {
        du->argv[2] = sdsfromlonglong(get_random_int()%30000);
        du->argv[3] = sdsfromlonglong(get_random_int()%30000);
    }
    return du;
}

static data_unit *bitpos_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;
    int with_range = 0; /* 0: no range; 1: just have start; 2: have start and end. */
    unsigned int probability = rand()%3;
    
    if (probability == 0)
        with_range = 0;
    else if (probability == 1)
        with_range = 1;
    else if (probability == 2)
        with_range = 2;

    du = data_unit_get();
    du->dp = dp;
    du->argc = with_range==0?3:(with_range==1?4:5);
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
    if (rand()%2)
        du->argv[2] = sdsnew("0");
    else
        du->argv[2] = sdsnew("1");
    if (with_range > 0)
        du->argv[3] = sdsfromlonglong(get_random_int()%30000);
    if (with_range == 2)
        du->argv[4] = sdsfromlonglong(get_random_int()%30000);

    return du;
}

static data_unit *mget_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;

    du = data_unit_get();
    du->dp = dp;
    du->argc = 2;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
    
    return du;
}

static data_unit *mset_cmd_producer(data_producer *dp, produce_scheme *ps)
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

static data_unit *hset_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;

    du = data_unit_get();
    du->dp = dp;
    du->argc = 4;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key();
    du->argv[2] = get_random_string();
    du->argv[3] = get_random_string();
    
    return du;
}

static data_unit *hget_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;

    du = data_unit_get();
    du->dp = dp;
    du->argc = 3;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
    du->argv[2] = get_random_string();
    
    return du;
}

static data_unit *hlen_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;

    du = data_unit_get();
    du->dp = dp;
    du->argc = 2;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
    
    return du;
}

static data_unit *hdel_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;
    unsigned int j, field_length;

    field_length = get_random_field_len();

    du = data_unit_get();
    du->dp = dp;
    du->argc = 2 + field_length;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
    for (j = 0; j < field_length; j ++) {
        du->argv[2+j] = get_random_string();
    }
    
    return du;
}

static data_unit *hexists_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;

    du = data_unit_get();
    du->dp = dp;
    du->argc = 3;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
    du->argv[2] = get_random_string();
    
    return du;
}

static data_unit *hkeys_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;

    du = data_unit_get();
    du->dp = dp;
    du->argc = 2;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
    
    return du;
}

static data_unit *hvals_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;

    du = data_unit_get();
    du->dp = dp;
    du->argc = 2;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
    
    return du;
}

static data_unit *hgetall_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;

    du = data_unit_get();
    du->dp = dp;
    du->argc = 2;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
    
    return du;
}

static data_unit *hincrby_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;

    du = data_unit_get();
    du->dp = dp;
    du->argc = 4;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
    du->argv[2] = get_random_string();
    du->argv[3] = sdsfromlonglong(get_random_int());
    
    return du;
}

static data_unit *hincrbyfloat_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;

    du = data_unit_get();
    du->dp = dp;
    du->argc = 4;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
    du->argv[2] = get_random_string();
    du->argv[3] = get_random_float_str();
    
    return du;
}

static data_unit *hmget_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;
    unsigned int j, field_length;

    field_length = get_random_field_len();

    du = data_unit_get();
    du->dp = dp;
    du->argc = 2+field_length;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
    for (j = 0; j < field_length; j ++) {
        du->argv[2+j] = get_random_string();
    }
    
    return du;
}

static data_unit *hmset_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;
    unsigned int j, field_length;

    field_length = get_random_field_len();

    du = data_unit_get();
    du->dp = dp;
    du->argc = 2+field_length*2;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key();
    for (j = 2; j < 2+field_length*2; j += 2) {
        du->argv[j] = get_random_string();
        du->argv[j+1] = get_random_string();
    }
    
    return du;
}

static data_unit *hsetnx_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;

    du = data_unit_get();
    du->dp = dp;
    du->argc = 4;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key();
    du->argv[2] = get_random_string();
    du->argv[3] = get_random_string();
    
    return du;
}

static data_unit *hstrlen_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;

    du = data_unit_get();
    du->dp = dp;
    du->argc = 3;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
    du->argv[2] = get_random_string();
    
    return du;
}

static data_unit *rpush_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;
    unsigned int j, field_length;

    field_length = get_random_field_len();

    du = data_unit_get();
    du->dp = dp;
    du->argc = 2+field_length;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key();
    for (j = 0; j < field_length; j ++) {
        du->argv[2+j] = get_random_string();
    }
    
    return du;
}

/* Need cache key? */
static int rpush_cmd_nck(redisReply *reply)
{
    if (reply == NULL) return 0;

    if (reply->type == REDIS_REPLY_INTEGER) {
        return 1;
    }

    return 0;
}

static data_unit *lpush_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;
    unsigned int j, field_length;

    field_length = get_random_field_len();

    du = data_unit_get();
    du->dp = dp;
    du->argc = 2+field_length;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key();

    for (j = 0; j < field_length; j ++) {
        du->argv[2+j] = get_random_string();
    }
    
    return du;
}

static data_unit *lrange_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;

    du = data_unit_get();
    du->dp = dp;
    du->argc = 4;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
    du->argv[2] = sdsfromlonglong((long long)get_random_int()%(field_length_max+1));
    du->argv[3] = sdsfromlonglong((long long)get_random_int()%(field_length_max+1));
    
    return du;
}

static data_unit *rpop_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;

    du = data_unit_get();
    du->dp = dp;
    du->argc = 2;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
    
    return du;
}

static data_unit *lpop_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;

    du = data_unit_get();
    du->dp = dp;
    du->argc = 2;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
    
    return du;
}

static data_unit *llen_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;

    du = data_unit_get();
    du->dp = dp;
    du->argc = 2;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
    
    return du;
}

static data_unit *lrem_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;

    du = data_unit_get();
    du->dp = dp;
    du->argc = 4;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
    du->argv[2] = sdsfromlonglong((long long)get_random_int()%(field_length_max+1));
    du->argv[3] = get_random_string();
    
    return du;
}

static data_unit *ltrim_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;

    du = data_unit_get();
    du->dp = dp;
    du->argc = 4;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
    du->argv[2] = sdsfromlonglong((long long)get_random_int()%(field_length_max+1));
    du->argv[3] = sdsfromlonglong((long long)get_random_int()%(field_length_max+1));
    
    return du;
}

static data_unit *lindex_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;

    du = data_unit_get();
    du->dp = dp;
    du->argc = 3;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
    du->argv[2] = sdsfromlonglong((long long)get_random_int()%(field_length_max+1));
    
    return du;
}

static data_unit *lset_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;

    du = data_unit_get();
    du->dp = dp;
    du->argc = 4;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
    du->argv[2] = sdsfromlonglong((long long)get_random_int()%(field_length_max+1));
    du->argv[3] = get_random_string();
    
    return du;
}

static data_unit *sadd_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;
    unsigned int j, field_length;

    field_length = get_random_field_len();

    du = data_unit_get();
    du->dp = dp;
    du->argc = 2 + field_length;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key();
    for (j = 0; j < field_length; j ++) {
        du->argv[2+j] = get_random_string();
    }
    
    return du;
}

static data_unit *smembers_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;

    du = data_unit_get();
    du->dp = dp;
    du->argc = 2;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
    
    return du;
}

static data_unit *scard_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;

    du = data_unit_get();
    du->dp = dp;
    du->argc = 2;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
    
    return du;
}

static data_unit *srem_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;
    unsigned int j, field_length;

    field_length = get_random_field_len();

    du = data_unit_get();
    du->dp = dp;
    du->argc = 2+field_length;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
    for (j = 0; j < field_length; j ++) {
        du->argv[2+j] = get_random_string();
    }
    
    return du;
}

static data_unit *sismember_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;

    du = data_unit_get();
    du->dp = dp;
    du->argc = 3;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
    du->argv[2] = get_random_string();
    
    return du;
}

static data_unit *sunion_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;

    du = data_unit_get();
    du->dp = dp;
    du->argc = 2;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
    
    return du;
}

static data_unit *sdiff_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;

    du = data_unit_get();
    du->dp = dp;
    du->argc = 2;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
    
    return du;
}

static data_unit *sinter_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;

    du = data_unit_get();
    du->dp = dp;
    du->argc = 2;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
    
    return du;
}

/* Need cache key? */
static int lpush_cmd_nck(redisReply *reply)
{
    if (reply == NULL) return 0;

    if (reply->type == REDIS_REPLY_INTEGER) {
        return 1;
    }

    return 0;
}

static data_unit *zadd_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;
    unsigned int j, field_length;

    field_length = get_random_field_len();

    du = data_unit_get();
    du->dp = dp;
    du->argc = 2+field_length*2;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key();

    for (j = 2; j < 2+field_length*2; j += 2) {
        du->argv[j] = get_random_float_str();
        du->argv[j+1] = get_random_string();
    }
    
    return du;
}

/* Need cache key? */
static int zadd_cmd_nck(redisReply *reply)
{
    if (reply == NULL) return 0;

    if (reply->type == REDIS_REPLY_INTEGER) {
        return 1;
    }

    return 0;
}

static data_unit *zincrby_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;
    unsigned int j, field_length;

    du = data_unit_get();
    du->dp = dp;
    du->argc = 4;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
    du->argv[2] = get_random_float_str();;
    du->argv[3] = get_random_string();
    
    return du;
}

/* Need cache key? */
static int zincrby_cmd_nck(redisReply *reply)
{
    if (reply == NULL) return 0;

    if (reply->type == REDIS_REPLY_INTEGER) {
        return 1;
    }

    return 0;
}

static data_unit *zrange_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;
    unsigned int j, field_length;
    int withscores;

    if (rand()%2 == 1) {
        withscores = 1;
    } else {
        withscores = 0;
    }

    du = data_unit_get();
    du->dp = dp;
    du->argc = withscores?5:4;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
    du->argv[2] = sdsfromlonglong(0);
    du->argv[3] = sdsfromlonglong(get_random_int()%10000);
    if (withscores) du->argv[4] = sdsnew("withscores");
    
    return du;
}

static data_unit *zrevrange_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;
    unsigned int j, field_length;
    int withscores;

    if (rand()%2 == 1) {
        withscores = 1;
    } else {
        withscores = 0;
    }

    du = data_unit_get();
    du->dp = dp;
    du->argc = withscores?5:4;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
    du->argv[2] = sdsfromlonglong(0);
    du->argv[3] = sdsfromlonglong(get_random_int()%10000);
    if (withscores) du->argv[4] = sdsnew("withscores");
    
    return du;
}

static data_unit *zrem_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;
    unsigned int j, field_length;

    field_length = get_random_field_len();

    du = data_unit_get();
    du->dp = dp;
    du->argc = 2+field_length;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);

    for (j = 2; j < 2+field_length; j ++) {
        du->argv[j] = get_random_string();
    }
    
    return du;
}

static data_unit *zcard_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;
    
    du = data_unit_get();
    du->dp = dp;
    du->argc = 2;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
    
    return du;
}

static data_unit *zcount_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;
    sds *range = get_random_zset_range_min_max_str(ZSET_RANGE_MIN_MAX_TYPE_SCORE);
    
    du = data_unit_get();
    du->dp = dp;
    du->argc = 4;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
    du->argv[2] = range[0];
    du->argv[3] = range[1];

    free(range);
    return du;
}

static data_unit *zrangebyscore_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;
    unsigned int idx = 0, arg_count = 0;
    int withscores,limit;
    sds *range = get_random_zset_range_min_max_str(ZSET_RANGE_MIN_MAX_TYPE_SCORE);

    arg_count = 4;
    if (rand()%2 == 1) {
        withscores = 1;
        arg_count ++;
    } else {
        withscores = 0;
    }
    if (rand()%2 == 1) {
        limit = 1;
        arg_count += 3;
    } else {
        limit = 0;
    }

    du = data_unit_get();
    du->dp = dp;
    du->argc = arg_count;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[idx++] = sdsnew(dp->name);
    du->argv[idx++] = get_random_key_with_hit_ratio(ps,dp);
    du->argv[idx++] = range[0];
    du->argv[idx++] = range[1];
    if (withscores) du->argv[idx++] = sdsnew("withscores");
    if (limit) {
        du->argv[idx++] = sdsnew("limit");
        du->argv[idx++] = sdsfromlonglong(get_random_unsigned_int());
        du->argv[idx++] = sdsfromlonglong(get_random_unsigned_int());
    }

    ASSERT(arg_count == idx);

    free(range);
    return du;
}

static data_unit *zrevrangebyscore_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;
    unsigned int idx = 0, arg_count = 0;
    int withscores,limit;
    sds *range = get_random_zset_range_min_max_str(ZSET_RANGE_MIN_MAX_TYPE_SCORE);

    arg_count = 4;
    if (rand()%2 == 1) {
        withscores = 1;
        arg_count ++;
    } else {
        withscores = 0;
    }
    if (rand()%2 == 1) {
        limit = 1;
        arg_count += 3;
    } else {
        limit = 0;
    }

    du = data_unit_get();
    du->dp = dp;
    du->argc = arg_count;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[idx++] = sdsnew(dp->name);
    du->argv[idx++] = get_random_key_with_hit_ratio(ps,dp);
    du->argv[idx++] = range[0];
    du->argv[idx++] = range[1];
    if (withscores) du->argv[idx++] = sdsnew("withscores");
    if (limit) {
        du->argv[idx++] = sdsnew("limit");
        du->argv[idx++] = sdsfromlonglong(get_random_unsigned_int());
        du->argv[idx++] = sdsfromlonglong(get_random_unsigned_int());
    }

    ASSERT(arg_count == idx);

    free(range);
    return du;
}

static data_unit *zrank_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;
    
    du = data_unit_get();
    du->dp = dp;
    du->argc = 3;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
    du->argv[2] = get_random_string();
        
    return du;
}

static data_unit *zrevrank_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;
    
    du = data_unit_get();
    du->dp = dp;
    du->argc = 3;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
    du->argv[2] = get_random_string();
        
    return du;
}

static data_unit *zscore_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;
    
    du = data_unit_get();
    du->dp = dp;
    du->argc = 3;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
    du->argv[2] = get_random_string();
        
    return du;
}

static data_unit *zremrangebyscore_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;
    sds *range = get_random_zset_range_min_max_str(ZSET_RANGE_MIN_MAX_TYPE_SCORE);
    
    du = data_unit_get();
    du->dp = dp;
    du->argc = 4;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
    du->argv[2] = range[0];
    du->argv[3] = range[1];

    free(range);
    return du;
}

static data_unit *zremrangebyrank_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;
    sds *range = get_random_zset_range_min_max_str(ZSET_RANGE_MIN_MAX_TYPE_RANK);
    
    du = data_unit_get();
    du->dp = dp;
    du->argc = 4;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
    du->argv[2] = range[0];
    du->argv[3] = range[1];

    free(range);
    return du;
}

static data_unit *zremrangebylex_cmd_producer(data_producer *dp, produce_scheme *ps)
{
    data_unit *du;
    sds *range = get_random_zset_range_min_max_str(ZSET_RANGE_MIN_MAX_TYPE_LEX);
    
    du = data_unit_get();
    du->dp = dp;
    du->argc = 4;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key_with_hit_ratio(ps,dp);
    du->argv[2] = range[0];
    du->argv[3] = range[1];

    free(range);
    return du;
}

static int producers_count;
data_producer redis_data_producer_table[] = {
    /* Key */
    {"del",del_cmd_producer,-2,"w",0,NULL,1,-1,1,TEST_CMD_TYPE_KEY,NULL},
    {"exists",exists_cmd_producer,-2,"rF",0,NULL,1,-1,1,TEST_CMD_TYPE_KEY,NULL},
    {"ttl",ttl_cmd_producer,2,"rF",0,NULL,1,1,1,TEST_CMD_TYPE_EXPIRE,NULL},
    {"pttl",pttl_cmd_producer,2,"rF",0,NULL,1,1,1,TEST_CMD_TYPE_EXPIRE,NULL},
    {"expire",expire_cmd_producer,3,"wF",0,NULL,1,1,1,TEST_CMD_TYPE_EXPIRE,NULL},
    {"expireat",expireat_cmd_producer,3,"wF",0,NULL,1,1,1,TEST_CMD_TYPE_EXPIRE,NULL},
    /* String */
    {"get",get_cmd_producer,2,"rF",0,NULL,1,1,1,TEST_CMD_TYPE_STRING,NULL},
    {"set",set_cmd_producer,-3,"wmA",0,NULL,1,1,1,TEST_CMD_TYPE_STRING,nck_when_ok},
    {"setnx",setnx_cmd_producer,3,"wmFA",0,NULL,1,1,1,TEST_CMD_TYPE_STRING,setnx_cmd_nck},
    {"setex",setex_cmd_producer,4,"wmA",0,NULL,1,1,1,TEST_CMD_TYPE_EXPIRE,nck_when_ok},
    {"psetex",psetex_cmd_producer,4,"wmA",0,NULL,1,1,1,TEST_CMD_TYPE_EXPIRE,nck_when_ok},
    {"incr",incr_cmd_producer,2,"wmF",0,NULL,1,1,1,TEST_CMD_TYPE_STRING,NULL},
    {"decr",decr_cmd_producer,2,"wmF",0,NULL,1,1,1,TEST_CMD_TYPE_STRING,NULL},
    {"incrby",incrby_cmd_producer,3,"wmF",0,NULL,1,1,1,TEST_CMD_TYPE_STRING,NULL},
    {"decrby",decrby_cmd_producer,3,"wmF",0,NULL,1,1,1,TEST_CMD_TYPE_STRING,NULL},
    {"append",append_cmd_producer,3,"wmA",0,NULL,1,1,1,TEST_CMD_TYPE_STRING,append_cmd_nck},
    {"strlen",strlen_cmd_producer,2,"rF",0,NULL,1,1,1,TEST_CMD_TYPE_STRING,NULL},
    {"getset",getset_cmd_producer,3,"wmA",0,NULL,1,1,1,TEST_CMD_TYPE_STRING,nck_when_noerror},
    {"incrbyfloat",incrbyfloat_cmd_producer,3,"wmFA",0,NULL,1,1,1,TEST_CMD_TYPE_STRING,nck_when_str},
    {"setbit",setbit_cmd_producer,4,"wmA",0,NULL,1,1,1,TEST_CMD_TYPE_STRING,nck_when_zero_or_one},
    {"getbit",getbit_cmd_producer,3,"rF",0,NULL,1,1,1,TEST_CMD_TYPE_STRING,NULL},
    {"setrange",setrange_cmd_producer,4,"wmA",0,NULL,1,1,1,TEST_CMD_TYPE_STRING,nck_when_nonzero_unsigned_integer},
    {"getrange",getrange_cmd_producer,4,"r",0,NULL,1,1,1,TEST_CMD_TYPE_STRING,NULL},
    {"bitcount",bitcount_cmd_producer,-2,"r",0,NULL,1,1,1,TEST_CMD_TYPE_STRING,NULL},
    {"bitpos",bitpos_cmd_producer,-3,"r",0,NULL,1,1,1,TEST_CMD_TYPE_STRING,NULL},
    {"mget",mget_cmd_producer,-2,"r",0,NULL,1,-1,1,TEST_CMD_TYPE_STRING,NULL},
    {"mset",mset_cmd_producer,-3,"wmA",0,NULL,1,-1,2,TEST_CMD_TYPE_STRING,nck_when_ok},
    /* Hash */
    {"hset",hset_cmd_producer,4,"wmFA",0,NULL,1,1,1,TEST_CMD_TYPE_HASH,nck_when_one},
    {"hget",hget_cmd_producer,3,"rF",0,NULL,1,1,1,TEST_CMD_TYPE_HASH,NULL},
    {"hlen",hlen_cmd_producer,2,"rF",0,NULL,1,1,1,TEST_CMD_TYPE_HASH,NULL},
    {"hdel",hdel_cmd_producer,-3,"wF",0,NULL,1,1,1,TEST_CMD_TYPE_HASH,NULL},
    {"hexists",hexists_cmd_producer,3,"rF",0,NULL,1,1,1,TEST_CMD_TYPE_HASH,NULL},
    {"hkeys",hkeys_cmd_producer,2,"rS",0,NULL,1,1,1,TEST_CMD_TYPE_HASH,NULL},
    {"hvals",hvals_cmd_producer,2,"rS",0,NULL,1,1,1,TEST_CMD_TYPE_HASH,NULL},
    {"hgetall",hgetall_cmd_producer,2,"r",0,NULL,1,1,1,TEST_CMD_TYPE_HASH,NULL},
    {"hincrby",hincrby_cmd_producer,4,"wmF",0,NULL,1,1,1,TEST_CMD_TYPE_HASH,NULL},
    {"hincrbyfloat",hincrbyfloat_cmd_producer,4,"wmF",0,NULL,1,1,1,TEST_CMD_TYPE_HASH,NULL},
    {"hmget",hmget_cmd_producer,-3,"r",0,NULL,1,1,1,TEST_CMD_TYPE_HASH,NULL},
    {"hmset",hmset_cmd_producer,-4,"wmA",0,NULL,1,1,1,TEST_CMD_TYPE_HASH,nck_when_ok},
    {"hsetnx",hsetnx_cmd_producer,4,"wmFA",0,NULL,1,1,1,TEST_CMD_TYPE_HASH,nck_when_one},
    {"hstrlen",hstrlen_cmd_producer,3,"rF",0,NULL,1,1,1,TEST_CMD_TYPE_HASH,NULL},
    /* List */
    {"rpush",rpush_cmd_producer,-3,"wmFA",0,NULL,1,1,1,TEST_CMD_TYPE_LIST,rpush_cmd_nck},
    {"lpush",lpush_cmd_producer,-3,"wmFA",0,NULL,1,1,1,TEST_CMD_TYPE_LIST,lpush_cmd_nck},
    {"lrange",lrange_cmd_producer,4,"r",0,NULL,1,1,1,TEST_CMD_TYPE_LIST,NULL},
    {"rpop",rpop_cmd_producer,2,"wF",0,NULL,1,1,1,TEST_CMD_TYPE_LIST,NULL},
    {"lpop",lpop_cmd_producer,2,"wF",0,NULL,1,1,1,TEST_CMD_TYPE_LIST,NULL},
    {"llen",llen_cmd_producer,2,"rF",0,NULL,1,1,1,TEST_CMD_TYPE_LIST,NULL},
    {"lrem",lrem_cmd_producer,4,"w",0,NULL,1,1,1,TEST_CMD_TYPE_LIST,NULL},
    {"ltrim",ltrim_cmd_producer,4,"w",0,NULL,1,1,1,TEST_CMD_TYPE_LIST,NULL},
    {"lindex",lindex_cmd_producer,3,"r",0,NULL,1,1,1,TEST_CMD_TYPE_LIST,NULL},
    {"lset",lset_cmd_producer,4,"wm",0,NULL,1,1,1,TEST_CMD_TYPE_LIST,NULL},
    /* Set */
    {"sadd",sadd_cmd_producer,-3,"wmFA",0,NULL,1,1,1,TEST_CMD_TYPE_SET,nck_when_unsigned_integer},
    {"smembers",smembers_cmd_producer,2,"rS",0,NULL,1,1,1,TEST_CMD_TYPE_SET,NULL},
    {"scard",scard_cmd_producer,2,"rF",0,NULL,1,1,1,TEST_CMD_TYPE_SET,NULL},
    {"srem",srem_cmd_producer,-3,"wF",0,NULL,1,1,1,TEST_CMD_TYPE_SET,NULL},
    {"sismember",sismember_cmd_producer,3,"rF",0,NULL,1,1,1,TEST_CMD_TYPE_SET,NULL},
    {"sunion",sunion_cmd_producer,-2,"rS",0,NULL,1,-1,1,TEST_CMD_TYPE_SET,NULL},
    {"sdiff",sdiff_cmd_producer,-2,"rS",0,NULL,1,-1,1,TEST_CMD_TYPE_SET,NULL},
    {"sinter",sinter_cmd_producer,-2,"rS",0,NULL,1,-1,1,TEST_CMD_TYPE_SET,NULL},
    /* SortedSet */
    {"zadd",zadd_cmd_producer,-4,"wmFA",0,NULL,1,1,1,TEST_CMD_TYPE_ZSET,zadd_cmd_nck},
    {"zincrby",zincrby_cmd_producer,4,"wmFA",0,NULL,1,1,1,TEST_CMD_TYPE_ZSET,zincrby_cmd_nck},
    {"zrange",zrange_cmd_producer,-4,"r",0,NULL,1,1,1,TEST_CMD_TYPE_ZSET,NULL},
    {"zrevrange",zrevrange_cmd_producer,-4,"r",0,NULL,1,1,1,TEST_CMD_TYPE_ZSET,NULL},
    {"zrem",zrem_cmd_producer,-3,"wF",0,NULL,1,1,1,TEST_CMD_TYPE_ZSET,NULL},
    {"zcard",zcard_cmd_producer,2,"rF",0,NULL,1,1,1,TEST_CMD_TYPE_ZSET,NULL},
    {"zcount",zcount_cmd_producer,4,"rF",0,NULL,1,1,1,TEST_CMD_TYPE_ZSET,NULL},
    {"zrangebyscore",zrangebyscore_cmd_producer,-4,"r",0,NULL,1,1,1,TEST_CMD_TYPE_ZSET,NULL},
    {"zrevrangebyscore",zrevrangebyscore_cmd_producer,-4,"r",0,NULL,1,1,1,TEST_CMD_TYPE_ZSET,NULL},
    {"zrank",zrank_cmd_producer,3,"rF",0,NULL,1,1,1,TEST_CMD_TYPE_ZSET,NULL},
    {"zrevrank",zrevrank_cmd_producer,3,"rF",0,NULL,1,1,1,TEST_CMD_TYPE_ZSET,NULL},
    {"zscore",zscore_cmd_producer,3,"rF",0,NULL,1,1,1,TEST_CMD_TYPE_ZSET,NULL},
    {"zremrangebyscore",zremrangebyscore_cmd_producer,4,"w",0,NULL,1,1,1,TEST_CMD_TYPE_ZSET,NULL},
    {"zremrangebyrank",zremrangebyrank_cmd_producer,4,"w",0,NULL,1,1,1,TEST_CMD_TYPE_ZSET,NULL}
};

data_unit *data_unit_get(void)
{
    data_unit *du = malloc(sizeof(data_unit));
    du->dp = NULL;
    du->argc = 0;
    du->argv = NULL;
    du->hashvalue = 0;
    du->data = NULL;
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
    ps->kcps = NULL;
    ps->hit_ratio_array = NULL;

    ps->kcps = darray_create(PRODUCE_KEY_CACHE_POOL_COUNT,sizeof(key_cache_array *));
    for (idx = 0; idx < PRODUCE_KEY_CACHE_POOL_COUNT; idx ++) {
        key_cache_array **kcp = darray_push(ps->kcps);
        *kcp = key_cache_array_create(max_cached_keys/PRODUCE_KEY_CACHE_POOL_COUNT);
        if (*kcp == NULL) {
            return NULL;
        }
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
    if (ps->kcps) {
        for (j = 0; j < PRODUCE_KEY_CACHE_POOL_COUNT; j ++) {
            key_cache_array **kcp = darray_pop(ps->kcps);
            if (*kcp)key_cache_array_destroy(*kcp);
        }
        darray_destroy(ps->kcps);
    }
    
    free(ps->hit_ratio_array);

    free(ps);
}

static unsigned int get_kcp_idx(int type)
{
    unsigned int idx;
    
    switch(type)
    {
    case TEST_CMD_TYPE_STRING:
        idx = 0;
        break;
        
    case TEST_CMD_TYPE_LIST:
        idx = 1;
        break;

    case TEST_CMD_TYPE_SET:
        idx = 2;
        break;

    case TEST_CMD_TYPE_ZSET:
        idx = 3;
        break;

    case TEST_CMD_TYPE_HASH:
        idx = 4;
        break;

    default:
        idx = -1;
        break;
    }

    return idx;
}

static void set_non_empty_kcps_idx(void)
{
    if (cmd_type&TEST_CMD_TYPE_STRING) {
        non_empty_kcps_idx[non_empty_kcps_count++] = 
            get_kcp_idx(TEST_CMD_TYPE_STRING);
    }
    if (cmd_type&TEST_CMD_TYPE_LIST) {
        non_empty_kcps_idx[non_empty_kcps_count++] = 
            get_kcp_idx(TEST_CMD_TYPE_LIST);
    }
    if (cmd_type&TEST_CMD_TYPE_SET) {
        non_empty_kcps_idx[non_empty_kcps_count++] = 
            get_kcp_idx(TEST_CMD_TYPE_SET);
    }
    if (cmd_type&TEST_CMD_TYPE_ZSET) {
        non_empty_kcps_idx[non_empty_kcps_count++] = 
            get_kcp_idx(TEST_CMD_TYPE_ZSET);
    }
    if (cmd_type&TEST_CMD_TYPE_HASH) {
        non_empty_kcps_idx[non_empty_kcps_count++] = 
            get_kcp_idx(TEST_CMD_TYPE_HASH);
    }
}

/* Get a key cache pool from the produce scheme */
key_cache_array *kcp_get_from_ps(produce_scheme *ps, data_producer *dp)
{
    unsigned int idx;
    key_cache_array **kcp;
    
    if (ps == NULL || ps->kcps == NULL || dp == NULL) return NULL;

    if (dp->cmd_type == TEST_CMD_TYPE_KEY) {
        if (non_empty_kcps_count==0) {
            idx = -1;
        } else {
            idx = rand()%non_empty_kcps_count;
            idx = non_empty_kcps_idx[idx];
            ASSERT(idx >= 0);
        }  
    } else {
        idx = get_kcp_idx(dp->cmd_type);
    }
    
    if (idx >= PRODUCE_KEY_CACHE_POOL_COUNT || idx < 0) {
        return NULL;
    }

    kcp = darray_get(ps->kcps, idx);

    return *kcp;
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

        du->data = pt->ps;

        /* Dispatch the test data */
        ret = data_dispatch(du);
        if (ret == -1) {
            data_unit_put(du);
        } else if (ret == 1) {
            usleep(100000);
        }

        pt->looptimes ++;
    }
    
    return NULL;
}

static int add_to_needed_cmd_type_producer(data_producer *dp)
{
    data_producer **dp_elem = darray_push(&needed_cmd_type_producer);

    *dp_elem = dp;
    needed_cmd_type_producer_count ++;
    
    return VRT_OK;
}

int vrt_produce_data_init(int key_length_range_min,int key_length_range_max, 
    int string_max_length,int fields_max_count,
    int produce_cmd_types,darray *produce_cmd_blacklist,darray *produce_cmd_whitelist,
    unsigned int produce_threads_count,long long cached_keys,
    int hit_ratio)
{
    int j, k;
    
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

        if (delete_data_producer == NULL && 
            !strcmp(dp->name,"del")) {
            delete_data_producer = dp;
        }

        if (produce_cmd_whitelist != NULL) {
            for (k = 0; k < darray_n(produce_cmd_whitelist); k ++) {
                sds *cmdname = darray_get(produce_cmd_whitelist, k);
                if (!strcasecmp(dp->name,*cmdname)) {
                    add_to_needed_cmd_type_producer(dp);
                    break;
                }
            }
            continue;
        }

        /* Check if this is in the blacklist */
        if (produce_cmd_blacklist != NULL) {
            int is_in_blacklist = 0;
            for (k = 0; k < darray_n(produce_cmd_blacklist); k ++) {
                sds *cmdname = darray_get(produce_cmd_blacklist, k);
                if (!strcasecmp(dp->name,*cmdname)) {
                    is_in_blacklist = 1;
                    break;
                }
            }
            
            if (is_in_blacklist) {
                continue;
            }
        }

        /* Add the needed command producer */
        if (dp->cmd_type&cmd_type) {
            add_to_needed_cmd_type_producer(dp);
        }
        if (dp->cmd_type&TEST_CMD_TYPE_EXPIRE && expire_enabled) {
            add_to_needed_cmd_type_producer(dp);
        }
    }

    set_non_empty_kcps_idx();

    if (darray_n(&needed_cmd_type_producer) == 0) {
        log_error("No command need to test");
        return VRT_ERROR;
    }

    if (delete_data_producer == NULL) {
        return VRT_ERROR;
    }
    
    if (needed_cmd_type_producer_count == 0) {
        return VRT_ERROR;
    }

    for (j = 0; j < needed_cmd_type_producer_count; j ++) {
        data_producer **dp_elem = darray_get(&needed_cmd_type_producer,j);
        log_debug(LOG_INFO, "needed test command[%d]: %s", j, (*dp_elem)->name);
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

sds get_one_key_from_data_unit(data_unit *du)
{
    int numkeys;
    int *keyindex;
    sds key;

    keyindex = get_keys_from_data_producer(du->dp,du->argv,du->argc,&numkeys);
    if (numkeys <= 0) {
        NOT_REACHED();
        return NULL;
    }

    key = du->argv[keyindex[0]];
    free(keyindex);

    return key;
}

void print_producer_command(data_unit *du)
{
    int j;
    sds cmd = sdsempty();
    
    for (j = 0; j < du->argc; j ++) {
        cmd = sdscatsds(cmd,du->argv[j]);
        cmd = sdscat(cmd," ");
    }
    cmd = sdscat(cmd,"\n");
    log_write_len(cmd,sdslen(cmd));
    sdsfree(cmd);
}
