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

#include <vrt_util.h>
#include <vrt_public.h>
#include <vrt_simple.h>

#define ERRMSG_MAX_LEN LOG_MAX_LEN-100
static char errmsg[ERRMSG_MAX_LEN];

static int simple_test_cmd_get_set(vire_instance *vi)
{
    char *key = "test_cmd_get_set-key";
    char *value = "test_cmd_get_set-value";
    char *MESSAGE = "GET/SET simple test";
    redisReply * reply = NULL;
    
    reply = redisCommand(vi->ctx, "set %s %s", key, value);
    if (reply == NULL || reply->type != REDIS_REPLY_STATUS || 
        reply->len != 2 || strcmp(reply->str,"OK")) {
        goto error;
    }
    freeReplyObject(reply);

    reply = redisCommand(vi->ctx, "get %s", key);
    if (reply == NULL || reply->type != REDIS_REPLY_STRING || 
        reply->len != strlen(value) || strcmp(reply->str,value)) {
        goto error;
    }
    freeReplyObject(reply);
    reply = NULL;

    show_test_result(VRT_TEST_OK,MESSAGE,errmsg);

    return 1;

error:

    if (reply) freeReplyObject(reply);

    show_test_result(VRT_TEST_ERR,MESSAGE,errmsg);
    errmsg[0] = '\0';

    return 0;
}

static int simple_test_cmd_setnx(vire_instance *vi)
{
    char *key = "test_cmd_setnx-key";
    char *value = "test_cmd_setnx-value";
    char *MESSAGE = "SETNX simple test";
    redisReply * reply = NULL;

    reply = redisCommand(vi->ctx, "del %s", key);
    if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
        goto error;
    }
    freeReplyObject(reply);
    
    reply = redisCommand(vi->ctx, "setnx %s %s", key, value);
    if (reply == NULL || reply->type != REDIS_REPLY_INTEGER || 
        reply->integer != 1) {
        goto error;
    }
    freeReplyObject(reply);

    reply = redisCommand(vi->ctx, "get %s", key);
    if (reply == NULL || reply->type != REDIS_REPLY_STRING || 
        reply->len != strlen(value) || strcmp(reply->str,value)) {
        goto error;
    }
    freeReplyObject(reply);

    reply = redisCommand(vi->ctx, "setnx %s %s", key, value);
    if (reply == NULL || reply->type != REDIS_REPLY_INTEGER || 
        reply->integer != 0) {
        goto error;
    }
    freeReplyObject(reply);

    show_test_result(VRT_TEST_OK,MESSAGE,errmsg);

    return 1;

error:

    if (reply) freeReplyObject(reply);

    show_test_result(VRT_TEST_ERR,MESSAGE,errmsg);
    errmsg[0] = '\0';
    
    return 0;
}

static int simple_test_cmd_setex(vire_instance *vi)
{
    char *key = "test_cmd_setex-key";
    char *value = "test_cmd_setex-value";
    long long seconds = 100;
    char *MESSAGE = "SETEX simple test";
    redisReply * reply = NULL;
    
    reply = redisCommand(vi->ctx, "setex %s %lld %s", key, seconds, value);
    if (reply == NULL || reply->type != REDIS_REPLY_STATUS || 
        reply->len != 2 || strcmp(reply->str,"OK")) {
        goto error;
    }
    freeReplyObject(reply);

    reply = redisCommand(vi->ctx, "get %s", key);
    if (reply == NULL || reply->type != REDIS_REPLY_STRING || 
        reply->len != strlen(value) || strcmp(reply->str,value)) {
        goto error;
    }
    freeReplyObject(reply);

    reply = redisCommand(vi->ctx, "ttl %s", key);
    if (reply == NULL || reply->type != REDIS_REPLY_INTEGER || 
        reply->integer > seconds ||  reply->integer < seconds - 2) {
        goto error;
    }
    freeReplyObject(reply);

    show_test_result(VRT_TEST_OK,MESSAGE,errmsg);

    return 1;

error:

    if (reply) freeReplyObject(reply);

    show_test_result(VRT_TEST_ERR,MESSAGE,errmsg);
    errmsg[0] = '\0';

    return 0;
}

static int simple_test_cmd_psetex(vire_instance *vi)
{
    char *key = "test_cmd_psetex-key";
    char *value = "test_cmd_psetex-value";
    long long milliseconds = 100000;
    char *MESSAGE = "PSETEX simple test";
    redisReply * reply = NULL;
    
    reply = redisCommand(vi->ctx, "psetex %s %lld %s", key, milliseconds, value);
    if (reply == NULL || reply->type != REDIS_REPLY_STATUS || 
        reply->len != 2 || strcmp(reply->str,"OK")) {
        goto error;
    }
    freeReplyObject(reply);

    reply = redisCommand(vi->ctx, "get %s", key);
    if (reply == NULL || reply->type != REDIS_REPLY_STRING || 
        reply->len != strlen(value) || strcmp(reply->str,value)) {
        goto error;
    }
    freeReplyObject(reply);

    reply = redisCommand(vi->ctx, "pttl %s", key);
    if (reply == NULL || reply->type != REDIS_REPLY_INTEGER || 
        reply->integer > milliseconds ||  reply->integer < milliseconds - 2000) {
        goto error;
    }
    freeReplyObject(reply);

    show_test_result(VRT_TEST_OK,MESSAGE,errmsg);

    return 1;

error:

    if (reply) freeReplyObject(reply);

    show_test_result(VRT_TEST_ERR,MESSAGE,errmsg);
    errmsg[0] = '\0';

    return 0;
}

static int simple_test_cmd_incr(vire_instance *vi)
{
    char *key = "test_cmd_incr-key";
    long long n = 0, incr_times = 100;
    char *MESSAGE = "INCR simple test";
    redisReply * reply = NULL;

    reply = redisCommand(vi->ctx, "del %s", key);
    if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
        goto error;
    }
    freeReplyObject(reply);

    while (n < incr_times) {
        reply = redisCommand(vi->ctx, "incr %s", key);
        if (reply == NULL || reply->type != REDIS_REPLY_INTEGER || 
            reply->integer != n+1) {
            vrt_scnprintf(errmsg, LOG_MAX_LEN, "incr %lld times error", n+1);
            goto error;
        }
        freeReplyObject(reply);
        
        n ++;
    }

    reply = redisCommand(vi->ctx, "get %s", key);
    if (reply == NULL || reply->type != REDIS_REPLY_STRING) {
        goto error;
    } else {
        long long value;
        if (!string2ll(reply->str,reply->len,&value) || value != incr_times) {
            vrt_scnprintf(errmsg, LOG_MAX_LEN, "incr to %lld error, %s in fact", 
                incr_times, reply->str);
            goto error;
        }
    }
    freeReplyObject(reply);

    reply = redisCommand(vi->ctx, "set %s %s", key, "a");
    if (reply == NULL || reply->type != REDIS_REPLY_STATUS || 
        reply->len != 2 || strcmp(reply->str,"OK")) {
        goto error;
    }
    freeReplyObject(reply);

    reply = redisCommand(vi->ctx, "incr %s", key);
    if (reply == NULL || reply->type != REDIS_REPLY_ERROR) {
        goto error;
    }
    freeReplyObject(reply);

    show_test_result(VRT_TEST_OK,MESSAGE,errmsg);

    return 1;

error:

    if (reply) freeReplyObject(reply);

    show_test_result(VRT_TEST_ERR,MESSAGE,errmsg);
    errmsg[0] = '\0';

    return 0;
}

static int simple_test_cmd_decr(vire_instance *vi)
{
    char *key = "test_cmd_decr-key";
    long long n = 0, decr_times = 100;
    char *MESSAGE = "DECR simple test";
    redisReply * reply = NULL;

    reply = redisCommand(vi->ctx, "del %s", key);
    if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
        goto error;
    }
    freeReplyObject(reply);

    while (n < decr_times) {
        reply = redisCommand(vi->ctx, "decr %s", key);
        if (reply == NULL || reply->type != REDIS_REPLY_INTEGER || 
            reply->integer + n != -1) {
            vrt_scnprintf(errmsg, LOG_MAX_LEN, "incr %lld times error", n+1);
            goto error;
        }
        freeReplyObject(reply);
        
        n ++;
    }

    reply = redisCommand(vi->ctx, "get %s", key);
    if (reply == NULL || reply->type != REDIS_REPLY_STRING) {
        goto error;
    } else {
        long long value;
        if (!string2ll(reply->str,reply->len,&value) || value + decr_times != 0) {
            vrt_scnprintf(errmsg, LOG_MAX_LEN, "decr to -%lld error, %s in fact", 
                decr_times, reply->str);
            goto error;
        }
    }
    freeReplyObject(reply);

    reply = redisCommand(vi->ctx, "set %s %s", key, "a");
    if (reply == NULL || reply->type != REDIS_REPLY_STATUS || 
        reply->len != 2 || strcmp(reply->str,"OK")) {
        goto error;
    }
    freeReplyObject(reply);

    reply = redisCommand(vi->ctx, "incr %s", key);
    if (reply == NULL || reply->type != REDIS_REPLY_ERROR) {
        goto error;
    }
    freeReplyObject(reply);

    show_test_result(VRT_TEST_OK,MESSAGE,errmsg);

    return 1;

error:

    if (reply) freeReplyObject(reply);

    show_test_result(VRT_TEST_ERR,MESSAGE,errmsg);
    errmsg[0] = '\0';

    return 0;
}

static int simple_test_cmd_incrby(vire_instance *vi)
{
    char *key = "test_cmd_incrby-key";
    long long n = 0, incrby_times = 100, incrby_step = 3;
    char *MESSAGE = "INCRBY simple test";
    redisReply * reply = NULL;

    reply = redisCommand(vi->ctx, "del %s", key);
    if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
        goto error;
    }
    freeReplyObject(reply);

    while (n < incrby_times) {
        reply = redisCommand(vi->ctx, "incrby %s %lld", key, incrby_step);
        if (reply == NULL || reply->type != REDIS_REPLY_INTEGER || 
            reply->integer != (n+1)*incrby_step) {
            vrt_scnprintf(errmsg, LOG_MAX_LEN, "incrby %lld %lld times error", 
                incrby_step, n+1);
            goto error;
        }
        freeReplyObject(reply);
        
        n ++;
    }

    reply = redisCommand(vi->ctx, "get %s", key);
    if (reply == NULL || reply->type != REDIS_REPLY_STRING) {
        goto error;
    } else {
        long long value;
        if (!string2ll(reply->str,reply->len,&value) || 
            value != incrby_times*incrby_step) {
            vrt_scnprintf(errmsg, LOG_MAX_LEN, "incrby to %lld error, %s in fact", 
                incrby_times*incrby_step, reply->str);
            goto error;
        }
    }
    freeReplyObject(reply);

    reply = redisCommand(vi->ctx, "set %s %s", key, "a");
    if (reply == NULL || reply->type != REDIS_REPLY_STATUS || 
        reply->len != 2 || strcmp(reply->str,"OK")) {
        goto error;
    }
    freeReplyObject(reply);

    reply = redisCommand(vi->ctx, "incrby %s %lld", key, incrby_step);
    if (reply == NULL || reply->type != REDIS_REPLY_ERROR) {
        goto error;
    }
    freeReplyObject(reply);

    show_test_result(VRT_TEST_OK,MESSAGE,errmsg);

    return 1;

error:

    if (reply) freeReplyObject(reply);

    show_test_result(VRT_TEST_ERR,MESSAGE,errmsg);
    errmsg[0] = '\0';

    return 0;
}

static int simple_test_cmd_decrby(vire_instance *vi)
{
    char *key = "test_cmd_decrby-key";
    long long n = 0, decrby_times = 100, decrby_step = 3;
    char *MESSAGE = "DECRBY simple test";
    redisReply * reply = NULL;

    reply = redisCommand(vi->ctx, "del %s", key);
    if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
        goto error;
    }
    freeReplyObject(reply);

    while (n < decrby_times) {
        reply = redisCommand(vi->ctx, "decrby %s %lld", key, decrby_step);
        if (reply == NULL || reply->type != REDIS_REPLY_INTEGER || 
            reply->integer + (n+1)*decrby_step != 0) {
            vrt_scnprintf(errmsg, LOG_MAX_LEN, "decrby %lld %lld times error", 
                decrby_step, n+1);
            goto error;
        }
        freeReplyObject(reply);
        
        n ++;
    }

    reply = redisCommand(vi->ctx, "get %s", key);
    if (reply == NULL || reply->type != REDIS_REPLY_STRING) {
        goto error;
    } else {
        long long value;
        if (!string2ll(reply->str,reply->len,&value) || 
            value + decrby_times*decrby_step != 0) {
            vrt_scnprintf(errmsg, LOG_MAX_LEN, "decrby to -%lld error, %s in fact", 
                decrby_times*decrby_step, reply->str);
            goto error;
        }
    }
    freeReplyObject(reply);

    reply = redisCommand(vi->ctx, "set %s %s", key, "a");
    if (reply == NULL || reply->type != REDIS_REPLY_STATUS || 
        reply->len != 2 || strcmp(reply->str,"OK")) {
        goto error;
    }
    freeReplyObject(reply);

    reply = redisCommand(vi->ctx, "decrby %s %lld", key, decrby_step);
    if (reply == NULL || reply->type != REDIS_REPLY_ERROR) {
        goto error;
    }
    freeReplyObject(reply);

    show_test_result(VRT_TEST_OK,MESSAGE,errmsg);

    return 1;

error:

    if (reply) freeReplyObject(reply);

    show_test_result(VRT_TEST_ERR,MESSAGE,errmsg);
    errmsg[0] = '\0';

    return 0;
}

static int simple_test_cmd_append(vire_instance *vi)
{
    char *key = "test_cmd_append-key";
    char *final_value = "pqwpioqjqwoiuqiorueljsakhdflkqueuquewqwei[oqfiqpq-0ewrq0hdalkjz.zhjaidhfioahd";
    char *start = final_value, *pos = start, *end = final_value+strlen(final_value);
    int step = 3, len;
    char buf[20];
    char *MESSAGE = "APPEND simple test";
    redisReply * reply = NULL;

    reply = redisCommand(vi->ctx, "del %s", key);
    if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
        goto error;
    }
    freeReplyObject(reply);

    while (pos < end) {        
        len = (end-pos >= step) ? step : (end-pos);
        memcpy(buf,pos,len);
        buf[len] = '\0';
        reply = redisCommand(vi->ctx, "append %s %s", key, buf);
        if (reply == NULL || reply->type != REDIS_REPLY_INTEGER) {
            goto error;
        } else if (reply->integer != pos-start+len) {
            vrt_scnprintf(errmsg, LOG_MAX_LEN, "append %s %s error", 
                key, buf);
            goto error;
        }
        freeReplyObject(reply);
        
        pos += len;
    }

    reply = redisCommand(vi->ctx, "get %s", key);
    if (reply == NULL || reply->type != REDIS_REPLY_STRING || 
        reply->len != strlen(final_value) || strcmp(reply->str,final_value)) {
        goto error;
    }
    freeReplyObject(reply);

    show_test_result(VRT_TEST_OK,MESSAGE,errmsg);

    return 1;

error:

    if (reply) freeReplyObject(reply);

    show_test_result(VRT_TEST_ERR,MESSAGE,errmsg);
    errmsg[0] = '\0';

    return 0;
}

static int simple_test_cmd_strlen(vire_instance *vi)
{
    char *key = "test_cmd_strlen-key";
    char *value = "test_cmd_strlen-value";
    char *MESSAGE = "STRLEN simple test";
    redisReply * reply = NULL;
    
    reply = redisCommand(vi->ctx, "set %s %s", key, value);
    if (reply == NULL || reply->type != REDIS_REPLY_STATUS || 
        reply->len != 2 || strcmp(reply->str,"OK")) {
        goto error;
    }
    freeReplyObject(reply);

    reply = redisCommand(vi->ctx, "strlen %s", key);
    if (reply == NULL || reply->type != REDIS_REPLY_INTEGER || 
        reply->integer != strlen(value)) {
        goto error;
    }
    freeReplyObject(reply);
    reply = NULL;

    show_test_result(VRT_TEST_OK,MESSAGE,errmsg);

    return 1;

error:

    if (reply) freeReplyObject(reply);

    show_test_result(VRT_TEST_ERR,MESSAGE,errmsg);
    errmsg[0] = '\0';

    return 0;
}

static int simple_test_cmd_getset(vire_instance *vi)
{
    char *key = "test_cmd_getset-key";
    char *oldvalue = "test_cmd_getset-oldvalue";
    char *newvalue = "test_cmd_getset-newvalue";
    char *MESSAGE = "GETSET simple test";
    redisReply * reply = NULL;
    
    reply = redisCommand(vi->ctx, "set %s %s", key, oldvalue);
    if (reply == NULL || reply->type != REDIS_REPLY_STATUS || 
        reply->len != 2 || strcmp(reply->str,"OK")) {
        goto error;
    }
    freeReplyObject(reply);

    reply = redisCommand(vi->ctx, "getset %s %s", key, newvalue);
    if (reply == NULL || reply->type != REDIS_REPLY_STRING || 
        reply->len != strlen(oldvalue) || strcmp(reply->str,oldvalue)) {
        goto error;
    }
    freeReplyObject(reply);

    reply = redisCommand(vi->ctx, "get %s", key);
    if (reply == NULL || reply->type != REDIS_REPLY_STRING || 
        reply->len != strlen(newvalue) || strcmp(reply->str,newvalue)) {
        goto error;
    }
    freeReplyObject(reply);

    show_test_result(VRT_TEST_OK,MESSAGE,errmsg);

    return 1;

error:

    if (reply) freeReplyObject(reply);

    show_test_result(VRT_TEST_ERR,MESSAGE,errmsg);
    errmsg[0] = '\0';

    return 0;
}

static int simple_test_cmd_incrbyfloat(vire_instance *vi)
{
    char *key = "test_cmd_incrbyfloat-key";
    char *final_value = "314.00000000000000022";
    long long n = 0, incrby_times = 100;
    float incrbyfloat_step = 3.14;
    char *MESSAGE = "INCRBYFLOAT simple test";
    redisReply * reply = NULL;

    reply = redisCommand(vi->ctx, "del %s", key);
    if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
        goto error;
    }
    freeReplyObject(reply);

    while (n < incrby_times) {
        reply = redisCommand(vi->ctx, "incrbyfloat %s %f", key, incrbyfloat_step);
        if (reply == NULL || reply->type != REDIS_REPLY_STRING) {
            vrt_scnprintf(errmsg, LOG_MAX_LEN, "incrbyfloat %f %lld times error", 
                incrbyfloat_step, n+1);
            goto error;
        }
        freeReplyObject(reply);
        
        n ++;
    }

    reply = redisCommand(vi->ctx, "get %s", key);
    if (reply == NULL || reply->type != REDIS_REPLY_STRING || 
        strcmp(reply->str,final_value)) {
        vrt_scnprintf(errmsg, LOG_MAX_LEN, "incrbyfloat to %s error", final_value);
        goto error;
    }
    freeReplyObject(reply);

    reply = redisCommand(vi->ctx, "set %s %s", key, "a");
    if (reply == NULL || reply->type != REDIS_REPLY_STATUS || 
        reply->len != 2 || strcmp(reply->str,"OK")) {
        goto error;
    }
    freeReplyObject(reply);

    reply = redisCommand(vi->ctx, "incrbyfloat %s %f", key, incrbyfloat_step);
    if (reply == NULL || reply->type != REDIS_REPLY_ERROR) {
        goto error;
    }
    freeReplyObject(reply);

    show_test_result(VRT_TEST_OK,MESSAGE,errmsg);

    return 1;

error:

    if (reply) freeReplyObject(reply);

    show_test_result(VRT_TEST_ERR,MESSAGE,errmsg);
    errmsg[0] = '\0';

    return 0;
}

static int simple_test_cmd_getbit_setbit_bitcount(vire_instance *vi)
{
    char *key = "test_cmd_getbit_setbit_bitcount-key";
    char *MESSAGE = "GETBIT/SETBIT/BITCOUNT simple test";
    int begin = 11, step = 3, times = 79, n;
    redisReply * reply = NULL;

    n = 0;
    while(n < times) {
        reply = redisCommand(vi->ctx, "setbit %s %d 1", key, begin+n*step);
        if (reply == NULL || reply->type != REDIS_REPLY_INTEGER || 
            reply->integer != 0) {
            goto error;
        }
        freeReplyObject(reply);

        n ++;
    }

    n = 0;
    while(n < times) {
        reply = redisCommand(vi->ctx, "getbit %s %d", key, begin+n*step);
        if (reply == NULL || reply->type != REDIS_REPLY_INTEGER || 
            reply->integer != 1) {
            goto error;
        }
        freeReplyObject(reply);

        n ++;
    }

    reply = redisCommand(vi->ctx, "bitcount %s", key);
    if (reply == NULL || reply->type != REDIS_REPLY_INTEGER || 
        reply->integer != times) {
        goto error;
    }
    freeReplyObject(reply);

    show_test_result(VRT_TEST_OK,MESSAGE,errmsg);

    return 1;

error:

    if (reply) freeReplyObject(reply);

    show_test_result(VRT_TEST_ERR,MESSAGE,errmsg);
    errmsg[0] = '\0';

    return 0;
}

static int simple_test_cmd_getrange_setrange(vire_instance *vi)
{
    char *key = "test_cmd_getrange_setrange-key";
    char *MESSAGE = "GETRANGE/SETRANGE simple test";
    char *range_value = "o090pl[]m,187h";
    int begin = 11, step = 53, times = 79, n;
    redisReply * reply = NULL;

    n = 0;
    while(n < times) {
        reply = redisCommand(vi->ctx, "setrange %s %d %s", 
            key, begin+n*step, range_value);
        if (reply == NULL || reply->type != REDIS_REPLY_INTEGER || 
            reply->integer != begin+n*step+strlen(range_value)) {
            vrt_scnprintf(errmsg, LOG_MAX_LEN, "setrange %s %d %s error", 
                key, begin+n*step, range_value);
            goto error;
        }
        freeReplyObject(reply);

        n ++;
    }

    n = 0;
    while(n < times) {
        reply = redisCommand(vi->ctx, "getrange %s %d %d", key, 
            begin+n*step, begin+n*step+strlen(range_value)-1);
        if (reply == NULL || reply->type != REDIS_REPLY_STRING || 
            reply->len != strlen(range_value) || strcmp(reply->str, range_value)) {
            vrt_scnprintf(errmsg, LOG_MAX_LEN, "getrange %s %d %d error", 
                key, begin+n*step, begin+n*step+strlen(range_value)-1);
            goto error;
        }
        freeReplyObject(reply);

        n ++;
    }

    show_test_result(VRT_TEST_OK,MESSAGE,errmsg);

    return 1;

error:

    if (reply) freeReplyObject(reply);

    show_test_result(VRT_TEST_ERR,MESSAGE,errmsg);
    errmsg[0] = '\0';

    return 0;
}

void simple_test(void)
{
    vire_instance *vi;
    int ok_count = 0;
    
    vi = start_one_vire_instance();
    if (vi == NULL) {
        test_log_error("Run vire instance failed");
        return;
    }

    errmsg[0] = '\0';

    /* String */
    ok_count+=simple_test_cmd_get_set(vi);
    ok_count+=simple_test_cmd_setnx(vi);
    ok_count+=simple_test_cmd_setex(vi);
    ok_count+=simple_test_cmd_psetex(vi);
    ok_count+=simple_test_cmd_incr(vi);
    ok_count+=simple_test_cmd_decr(vi);
    ok_count+=simple_test_cmd_incrby(vi);
    ok_count+=simple_test_cmd_decrby(vi);
    ok_count+=simple_test_cmd_append(vi);
    ok_count+=simple_test_cmd_strlen(vi);
    ok_count+=simple_test_cmd_getset(vi);
    ok_count+=simple_test_cmd_incrbyfloat(vi);
    ok_count+=simple_test_cmd_getbit_setbit_bitcount(vi);
    ok_count+=simple_test_cmd_getrange_setrange(vi);
    
    vire_instance_destroy(vi);
}
