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
    long long n = 0, incr_times = 100;
    char *MESSAGE = "DECR simple test";
    redisReply * reply = NULL;

    reply = redisCommand(vi->ctx, "del %s", key);
    if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
        goto error;
    }
    freeReplyObject(reply);

    while (n < incr_times) {
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
        if (!string2ll(reply->str,reply->len,&value) || value + incr_times != 0) {
            vrt_scnprintf(errmsg, LOG_MAX_LEN, "decr to -%lld error, %s in fact", 
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

    ok_count+=simple_test_cmd_get_set(vi);
    ok_count+=simple_test_cmd_setnx(vi);
    ok_count+=simple_test_cmd_setex(vi);
    ok_count+=simple_test_cmd_psetex(vi);
    ok_count+=simple_test_cmd_incr(vi);
    ok_count+=simple_test_cmd_decr(vi);
    
    vire_instance_destroy(vi);
}
