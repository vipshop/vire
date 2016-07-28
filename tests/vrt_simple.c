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

static int simple_test_cmd_get_set(vire_instance *vi)
{
    char *key = "test_cmd_get_set-key";
    char *value = "test_cmd_get_set-value";
    char *MESSAGE = "GET/SET simple test";
    redisReply * reply = NULL;
    
    reply = redisCommand(vi->ctx, "set %s %s", key, value);
    if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
        goto error;
    }
    freeReplyObject(reply);

    reply = redisCommand(vi->ctx, "get %s", key);
    if (reply == NULL || reply->type != REDIS_REPLY_STRING) {
        goto error;
    } else if (reply->len != strlen(value)) {
        goto error;
    } else if (strcmp(reply->str,value)) {
        goto error;
    }
    freeReplyObject(reply);
    reply = NULL;

    show_test_result(VRT_TEST_OK,MESSAGE);

    return VRT_OK;

error:

    if (reply) freeReplyObject(reply);

    show_test_result(VRT_TEST_ERR,MESSAGE);

    return VRT_ERROR;
}

void simple_test(void)
{
    vire_instance *vi;
    
    vi = start_one_vire_instance();
    if (vi == NULL) {
        test_log_error("Run vire instance failed");
        return;
    }

    simple_test_cmd_get_set(vi);
    
    
    vire_instance_destroy(vi);
}
