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

static int simple_test_cmd_bitpos(vire_instance *vi)
{
    char *key = "test_cmd_bitpos-key";
    char *MESSAGE = "BITPOS simple test";
    int pos = 11;
    redisReply * reply = NULL;

    reply = redisCommand(vi->ctx, "del %s", key);
    if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
        goto error;
    }
    freeReplyObject(reply);

    reply = redisCommand(vi->ctx, "bitpos %s 1", key);
    if (reply == NULL || reply->type != REDIS_REPLY_INTEGER || 
        reply->integer != -1) {
        vrt_scnprintf(errmsg, LOG_MAX_LEN, "bitpos %s 1 first time error", 
            key);
        goto error;
    }
    freeReplyObject(reply);

    reply = redisCommand(vi->ctx, "setbit %s 1 0", key);
    if (reply == NULL || reply->type != REDIS_REPLY_INTEGER || 
        reply->integer != 0) {
        vrt_scnprintf(errmsg, LOG_MAX_LEN, "setbit %s 1 0 error", 
            key);
        goto error;
    }
    freeReplyObject(reply);

    reply = redisCommand(vi->ctx, "bitpos %s 1", key);
    if (reply == NULL || reply->type != REDIS_REPLY_INTEGER || 
        reply->integer != -1) {
        vrt_scnprintf(errmsg, LOG_MAX_LEN, "bitpos %s 1 second time error", 
            key);
        goto error;
    }
    freeReplyObject(reply);

    reply = redisCommand(vi->ctx, "setbit %s %d 1", key, pos);
    if (reply == NULL || reply->type != REDIS_REPLY_INTEGER || 
        reply->integer != 0) {
        vrt_scnprintf(errmsg, LOG_MAX_LEN, "setbit %s %d 1 error", 
            key, pos);
        goto error;
    }
    freeReplyObject(reply);

    reply = redisCommand(vi->ctx, "bitpos %s 1", key);
    if (reply == NULL || reply->type != REDIS_REPLY_INTEGER || 
        reply->integer != pos) {
        vrt_scnprintf(errmsg, LOG_MAX_LEN, "bitpos %s 1 third time error", 
            key);
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

#define MGET_MSET_KEYS_COUNT 333
static int simple_test_cmd_mget_mset(vire_instance *vi)
{
    char *key = "test_cmd_mget_mset-key";
    char *value = "test_cmd_mget_mset-value";
    char *MESSAGE = "MGET/MSET simple test";
    char keys[MGET_MSET_KEYS_COUNT][30];
    char values[MGET_MSET_KEYS_COUNT][30];
    char *argv[1+2*MGET_MSET_KEYS_COUNT];
    size_t argvlen[1+2*MGET_MSET_KEYS_COUNT];
    int j, idx;
    redisReply *reply = NULL;

    for (j = 0; j < MGET_MSET_KEYS_COUNT; j ++) {
        vrt_scnprintf(keys[j], 30,"%s%d", key, j);
        vrt_scnprintf(values[j], 30,"%s%d", value, j);
    }
    
    argv[0] = "mset";
    argvlen[0] = strlen(argv[0]);
    idx = 1;
    for (j = 0; j < MGET_MSET_KEYS_COUNT; j ++) {
        argv[idx] = keys[j];
        argvlen[idx++] = strlen(keys[j]);
        argv[idx] = values[j];
        argvlen[idx++] = strlen(values[j]);
    }
    
    reply = redisCommandArgv(vi->ctx, 1+2*MGET_MSET_KEYS_COUNT, argv, argvlen);
    if (reply == NULL || reply->type != REDIS_REPLY_STATUS || 
        reply->len != 2 || strcmp(reply->str,"OK")) {
        vrt_scnprintf(errmsg, LOG_MAX_LEN, "mset %d keys error", 
            MGET_MSET_KEYS_COUNT);
        goto error;
    }
    freeReplyObject(reply);

    argv[0] = "mget";
    argvlen[0] = strlen(argv[0]);
    for (j = 1; j < 1+MGET_MSET_KEYS_COUNT; j ++) {
        argv[j] = keys[j-1];
        argvlen[j] = strlen(argv[j]);
    }

    reply = redisCommandArgv(vi->ctx, 1+MGET_MSET_KEYS_COUNT, argv, argvlen);
    if (reply == NULL || reply->type != REDIS_REPLY_ARRAY || 
        reply->elements != MGET_MSET_KEYS_COUNT) {
        vrt_scnprintf(errmsg, LOG_MAX_LEN, "mget %d keys error", 
            MGET_MSET_KEYS_COUNT);
        goto error;
    }
    for (j = 0; j < MGET_MSET_KEYS_COUNT; j ++) {
        redisReply *reply_sub = reply->element[j];
        if (reply_sub == NULL ||
            reply_sub->type != REDIS_REPLY_STRING || 
            reply_sub->len != strlen(values[j]) || 
            strcmp(reply_sub->str, values[j]))
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

#define TEST_HASH_ENCODED_ZIPLIST    0
#define TEST_HASH_ENCODED_HT         1
#define TEST_HASH_ENCODED_CAUSED_BY_FILED    0
#define TEST_HASH_ENCODED_CAUSED_BY_VALUE    1
#define TEST_HASH_ENCODED_CAUSED_BY_ALL      2
#define TEST_HASH_ENCODED_ZIPLIST_FIELD_COUNT    56
#define TEST_HASH_ENCODED_HT_FIELD_COUNT         678
#define TEST_HASH_ENCODED_ZIPLIST_VALUE_LEN      21
#define TEST_HASH_ENCODED_HT_VALUE_LEN           111

struct test_hash_member {
    char *field;
    char *value;
};

static int test_hash_member_length(struct test_hash_member **thms)
{
    int j = 0;
    while (thms[j]) {
        j ++;
    }
    return j;
}

static void test_hash_members_destroy(struct test_hash_member **thms)
{
    int j = 0;
    while (thms[j]) {
        free(thms[j]->field);
        free(thms[j]->value);
        free(thms[j]);
        j ++;
    }
    free(thms);
}

static struct test_hash_member **simple_test_hash_init(vire_instance *vi, char *key, int hash_encode, int encode_cause)
{
    char *field = "test_hash-field";
    char *value = "test_hash-value";
    int field_count, value_len;
    int j,n;
    struct test_hash_member **thms = NULL;
    redisReply *reply = NULL;

    reply = redisCommand(vi->ctx, "del %s", key);
    if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
        goto error;
    }
    freeReplyObject(reply);

    if (hash_encode == TEST_HASH_ENCODED_ZIPLIST) {
        field_count = TEST_HASH_ENCODED_ZIPLIST_FIELD_COUNT;
        value_len = TEST_HASH_ENCODED_ZIPLIST_VALUE_LEN;
    } else if (encode_cause == TEST_HASH_ENCODED_CAUSED_BY_FILED) {
        field_count = TEST_HASH_ENCODED_HT_FIELD_COUNT;
        value_len = TEST_HASH_ENCODED_ZIPLIST_VALUE_LEN;
    } else if (encode_cause == TEST_HASH_ENCODED_CAUSED_BY_VALUE) {
        field_count = TEST_HASH_ENCODED_ZIPLIST_FIELD_COUNT;
        value_len = TEST_HASH_ENCODED_HT_VALUE_LEN;
    } else if (encode_cause == TEST_HASH_ENCODED_CAUSED_BY_ALL) {
        field_count = TEST_HASH_ENCODED_HT_FIELD_COUNT;
        value_len = TEST_HASH_ENCODED_HT_VALUE_LEN;
    }
    
    thms = malloc((field_count+1)*sizeof(struct test_hash_member*));
    for (j = 0; j < field_count; j ++) {
        thms[j] = malloc(sizeof(struct test_hash_member));
        thms[j]->field = malloc(30*sizeof(char));
        thms[j]->value = malloc((value_len+1)*sizeof(char));
        vrt_scnprintf(thms[j]->field, 30, "%s%d", field, j);
        n = vrt_scnprintf(thms[j]->value, value_len, "%s%d", value, j);
        if (n < value_len) {
            memset(thms[j]->value,'x',value_len-n);
            thms[j]->value[value_len] = '\0';
        }
    }
    thms[field_count] = NULL;

    for (j = 0; j < field_count; j ++) { 
        reply = redisCommand(vi->ctx, "hset %s %s %s", 
            key, thms[j]->field, thms[j]->value);
        if (reply == NULL || reply->type != REDIS_REPLY_INTEGER || 
            reply->integer != 1) {
            goto error;
        }
        freeReplyObject(reply);
    }

    reply = redisCommand(vi->ctx, "hlen %s", key);
    if (reply == NULL || reply->type != REDIS_REPLY_INTEGER || 
        reply->integer != field_count) {
        goto error;
    }
    freeReplyObject(reply);

    reply = redisCommand(vi->ctx, "object encoding %s", key);
    if (reply == NULL || reply->type != REDIS_REPLY_STRING) {
        goto error;
    } else {
        if (hash_encode == TEST_HASH_ENCODED_ZIPLIST) {
            if(reply->len != 7 || strcmp(reply->str, "ziplist")) {
                goto error;
            }
        } else {
            if(reply->len != 9 || strcmp(reply->str, "hashtable")) {
                goto error;
            }
        }
    }
    freeReplyObject(reply);
    
    return thms;

error:

    if (thms) {
        test_hash_members_destroy(thms);
        thms = NULL;
    }

    if (reply) freeReplyObject(reply);

    return NULL;
}

static int simple_test_hash_encode(vire_instance *vi)
{
    char *key = "test_hash_encode";
    char *MESSAGE = "HASH ENCODE simple test";
    struct test_hash_member **thms;
    
    thms = simple_test_hash_init(vi,key,TEST_HASH_ENCODED_ZIPLIST,TEST_HASH_ENCODED_CAUSED_BY_FILED);
    if (thms == NULL) {
        goto error;
    }
    test_hash_members_destroy(thms);
    thms = simple_test_hash_init(vi,key,TEST_HASH_ENCODED_ZIPLIST,TEST_HASH_ENCODED_CAUSED_BY_VALUE);
    if (thms == NULL) {
        goto error;
    }
    test_hash_members_destroy(thms);
    thms = simple_test_hash_init(vi,key,TEST_HASH_ENCODED_HT,TEST_HASH_ENCODED_CAUSED_BY_FILED);
    if (thms == NULL) {
        goto error;
    }
    test_hash_members_destroy(thms);
    thms = simple_test_hash_init(vi,key,TEST_HASH_ENCODED_HT,TEST_HASH_ENCODED_CAUSED_BY_VALUE);
    if (thms == NULL) {
        goto error;
    }
    test_hash_members_destroy(thms);
    thms = simple_test_hash_init(vi,key,TEST_HASH_ENCODED_HT,TEST_HASH_ENCODED_CAUSED_BY_ALL);
    if (thms == NULL) {
        goto error;
    }
    test_hash_members_destroy(thms);
    
    show_test_result(VRT_TEST_OK,MESSAGE,errmsg);

    return 1;

error:

    show_test_result(VRT_TEST_ERR,MESSAGE,errmsg);
    errmsg[0] = '\0';

    return 0;
}

static int simple_test_cmd_hget_hset(vire_instance *vi)
{
    char *key = "test_cmd_hget_hset-key";
    char *field = "test_cmd_hget_hset-field";
    char *value = "test_cmd_hget_hset-value";
    char *MESSAGE = "HGET/HSET simple test";
    redisReply * reply = NULL;
    struct test_hash_member **thms = NULL;
    int idx;

    reply = redisCommand(vi->ctx, "del %s", key);
    if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
        goto error;
    }
    freeReplyObject(reply);
    
    reply = redisCommand(vi->ctx, "hset %s %s %s", key, field, value);
    if (reply == NULL || reply->type != REDIS_REPLY_INTEGER || 
        reply->integer != 1) {
        goto error;
    }
    freeReplyObject(reply);

    reply = redisCommand(vi->ctx, "hget %s %s", key, field);
    if (reply == NULL || reply->type != REDIS_REPLY_STRING || 
        reply->len != strlen(value) || strcmp(reply->str,value)) {
        goto error;
    }
    freeReplyObject(reply);

    thms = simple_test_hash_init(vi,key,TEST_HASH_ENCODED_HT,TEST_HASH_ENCODED_CAUSED_BY_FILED);
    if (thms == NULL) {
        goto error;
    }
    reply = redisCommand(vi->ctx, "hset %s %s %s", key, field, value);
    if (reply == NULL || reply->type != REDIS_REPLY_INTEGER || 
        reply->integer != 1) {
        goto error;
    }
    freeReplyObject(reply);
    reply = redisCommand(vi->ctx, "hget %s %s", key, field);
    if (reply == NULL || reply->type != REDIS_REPLY_STRING || 
        reply->len != strlen(value) || strcmp(reply->str,value)) {
        goto error;
    }
    freeReplyObject(reply);
    idx = test_hash_member_length(thms)/2;
    reply = redisCommand(vi->ctx, "hget %s %s", key, thms[idx]->field);
    if (reply == NULL || reply->type != REDIS_REPLY_STRING || 
        reply->len != strlen(thms[idx]->value) || strcmp(reply->str,thms[idx]->value)) {
        goto error;
    }
    freeReplyObject(reply);
    test_hash_members_destroy(thms);

    show_test_result(VRT_TEST_OK,MESSAGE,errmsg);

    return 1;

error:

    if (reply) freeReplyObject(reply);
    if (thms) test_hash_members_destroy(thms);

    show_test_result(VRT_TEST_ERR,MESSAGE,errmsg);
    errmsg[0] = '\0';

    return 0;
}

static int simple_test_cmd_hlen(vire_instance *vi)
{
    char *key = "test_cmd_hlen-key";
    char *field = "test_cmd_hlen-field";
    char *value = "test_cmd_hlen-value";
    char *MESSAGE = "HLEN simple test";
    redisReply * reply = NULL;
    int hash_len, j;

    hash_len = 51;
    reply = redisCommand(vi->ctx, "del %s", key);
    if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
        goto error;
    }
    freeReplyObject(reply);
    for (j = 0; j < hash_len; j ++) {
        reply = redisCommand(vi->ctx, "hset %s %s%d %s", key, field, j, value);
        if (reply == NULL || reply->type != REDIS_REPLY_INTEGER || 
            reply->integer != 1) {
            goto error;
        }
        freeReplyObject(reply);
    }
    reply = redisCommand(vi->ctx, "hlen %s", key);
    if (reply == NULL || reply->type != REDIS_REPLY_INTEGER || 
        reply->integer != hash_len) {
        goto error;
    }
    freeReplyObject(reply);

    hash_len = 5111;
    reply = redisCommand(vi->ctx, "del %s", key);
    if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
        goto error;
    }
    freeReplyObject(reply);
    for (j = 0; j < hash_len; j ++) {
        reply = redisCommand(vi->ctx, "hset %s %s%d %s", key, field, j, value);
        if (reply == NULL || reply->type != REDIS_REPLY_INTEGER || 
            reply->integer != 1) {
            goto error;
        }
        freeReplyObject(reply);
    }
    reply = redisCommand(vi->ctx, "hlen %s", key);
    if (reply == NULL || reply->type != REDIS_REPLY_INTEGER || 
        reply->integer != hash_len) {
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

static int simple_test_cmd_hdel(vire_instance *vi)
{
    char *key = "test_cmd_hdel-key";
    char *field = "test_cmd_hdel-field";
    char *value = "test_cmd_hdel-value";
    char *MESSAGE = "HDEL simple test";
    redisReply * reply = NULL;
    struct test_hash_member **thms = NULL;
    int idx;

    reply = redisCommand(vi->ctx, "del %s", key);
    if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
        goto error;
    }
    freeReplyObject(reply);
    
    reply = redisCommand(vi->ctx, "hset %s %s%d %s", key, field, 1, value);
    if (reply == NULL || reply->type != REDIS_REPLY_INTEGER || 
        reply->integer != 1) {
        goto error;
    }
    freeReplyObject(reply);
    reply = redisCommand(vi->ctx, "hset %s %s%d %s", key, field, 2, value);
    if (reply == NULL || reply->type != REDIS_REPLY_INTEGER || 
        reply->integer != 1) {
        goto error;
    }
    freeReplyObject(reply);

    reply = redisCommand(vi->ctx, "hget %s %s%d", key, field, 1);
    if (reply == NULL || reply->type != REDIS_REPLY_STRING || 
        reply->len != strlen(value) || strcmp(reply->str, value)) {
        goto error;
    }
    freeReplyObject(reply);
    reply = redisCommand(vi->ctx, "hdel %s %s%d", key, field, 1);
    if (reply == NULL || reply->type != REDIS_REPLY_INTEGER || 
        reply->integer != 1) {
        goto error;
    }
    freeReplyObject(reply);
    reply = redisCommand(vi->ctx, "hget %s %s%d", key, field, 1);
    if (reply == NULL || reply->type != REDIS_REPLY_NIL) {
        goto error;
    }
    freeReplyObject(reply);
    
    reply = redisCommand(vi->ctx, "hdel %s %s%d", key, field, 2);
    if (reply == NULL || reply->type != REDIS_REPLY_INTEGER || 
        reply->integer != 1) {
        goto error;
    }
    freeReplyObject(reply);
    reply = redisCommand(vi->ctx, "exists %s", key);
    if (reply == NULL || reply->type != REDIS_REPLY_INTEGER || 
        reply->integer != 0) {
        goto error;
    }
    freeReplyObject(reply);
    
    thms = simple_test_hash_init(vi,key,TEST_HASH_ENCODED_HT,TEST_HASH_ENCODED_CAUSED_BY_FILED);
    if (thms == NULL) {
        goto error;
    }
    idx = test_hash_member_length(thms)/2;
    reply = redisCommand(vi->ctx, "hdel %s %s", key, thms[idx]->field);
    if (reply == NULL || reply->type != REDIS_REPLY_INTEGER || 
        reply->integer != 1) {
        goto error;
    }
    freeReplyObject(reply);    
    reply = redisCommand(vi->ctx, "hget %s %s", key, thms[idx]->field);
    if (reply == NULL || reply->type != REDIS_REPLY_NIL) {
        goto error;
    }
    freeReplyObject(reply);
    test_hash_members_destroy(thms);

    show_test_result(VRT_TEST_OK,MESSAGE,errmsg);

    return 1;

error:

    if (reply) freeReplyObject(reply);
    if (thms) test_hash_members_destroy(thms);

    show_test_result(VRT_TEST_ERR,MESSAGE,errmsg);
    errmsg[0] = '\0';

    return 0;
}

static int simple_test_cmd_pfadd_pfcount(vire_instance *vi)
{
    char *key = "test_cmd_pfadd_pfcount-key";
    char *value = "test_cmd_pfadd_pfcount-value";
    char *MESSAGE = "PFADD/PFCOUNT simple test";
    redisReply * reply = NULL;
    int n = 0, count = 20329, repeat;

    while (repeat < 2) {
        int expect_count;
        reply = redisCommand(vi->ctx, "pfadd %s %s%d", key, value, n++);
        if (reply == NULL || reply->type != REDIS_REPLY_INTEGER) {
            goto error;
        }
        freeReplyObject(reply);
        if (n >= count) {
            repeat++;
            n = 0;
        }

        if (repeat == 0) {
            expect_count = n;
        } else {
            expect_count = count;
        }
        
        reply = redisCommand(vi->ctx, "pfcount %s", key);
        if (reply == NULL || reply->type != REDIS_REPLY_INTEGER) {
            goto error;
        }
        if (reply->integer != (long long)expect_count) {
            float mistake = ((float)expect_count-(float)reply->integer)/(float)expect_count;
            if (mistake < -0.02 || mistake > 0.02) {
                vrt_scnprintf(errmsg, LOG_MAX_LEN, "pfadd %d different elements is not approximated pfcount returned %lld, mistake %f", 
                    expect_count, reply->integer, mistake);
                goto error;
            }
        }
        freeReplyObject(reply);
    }

    show_test_result(VRT_TEST_OK,MESSAGE,errmsg);

    return 1;

error:

    if (reply) freeReplyObject(reply);

    show_test_result(VRT_TEST_ERR,MESSAGE,errmsg);
    errmsg[0] = '\0';

    return 0;
}

int simple_test(void)
{
    vire_instance *vi;
    int ok_count = 0, all_count = 0;
    
    vi = start_one_vire_instance();
    if (vi == NULL) {
        test_log_error("Run vire instance failed");
        return;
    }

    errmsg[0] = '\0';

    /* String */
    ok_count+=simple_test_cmd_get_set(vi); all_count++;
    ok_count+=simple_test_cmd_setnx(vi); all_count++;
    ok_count+=simple_test_cmd_setex(vi); all_count++;
    ok_count+=simple_test_cmd_psetex(vi); all_count++;
    ok_count+=simple_test_cmd_incr(vi); all_count++;
    ok_count+=simple_test_cmd_decr(vi); all_count++;
    ok_count+=simple_test_cmd_incrby(vi); all_count++;
    ok_count+=simple_test_cmd_decrby(vi); all_count++;
    ok_count+=simple_test_cmd_append(vi); all_count++;
    ok_count+=simple_test_cmd_strlen(vi); all_count++;
    ok_count+=simple_test_cmd_getset(vi); all_count++;
    ok_count+=simple_test_cmd_incrbyfloat(vi); all_count++;
    ok_count+=simple_test_cmd_getbit_setbit_bitcount(vi); all_count++;
    ok_count+=simple_test_cmd_getrange_setrange(vi); all_count++;
    ok_count+=simple_test_cmd_bitpos(vi); all_count++;
    ok_count+=simple_test_cmd_mget_mset(vi); all_count++;
    /* Hash */
    ok_count+=simple_test_hash_encode(vi); all_count++;
    ok_count+=simple_test_cmd_hget_hset(vi); all_count++;
    ok_count+=simple_test_cmd_hlen(vi); all_count++;
    ok_count+=simple_test_cmd_hdel(vi); all_count++;
    /* HyperLogLog */
    ok_count+=simple_test_cmd_pfadd_pfcount(vi); all_count++;
    
    vire_instance_destroy(vi);

    return ok_count==all_count?1:0;
}
