#ifndef _VRT_PUBLIC_H_
#define _VRT_PUBLIC_H_

#ifdef HAVE_CONFIG_H
# include <config.h>
#endif

#include <vr_specialconfig.h>

#include <unistd.h>

#include <hiredis.h>

#define VRT_TEST_OK     0
#define VRT_TEST_ERR    1

#define TEST_CMD_TYPE_STRING    (1<<0)
#define TEST_CMD_TYPE_LIST      (1<<1)
#define TEST_CMD_TYPE_SET       (1<<2)
#define TEST_CMD_TYPE_ZSET      (1<<3)
#define TEST_CMD_TYPE_HASH      (1<<4)
#define TEST_CMD_TYPE_SERVER    (1<<5)
#define TEST_CMD_TYPE_KEY       (1<<6)

typedef struct vire_instance {
    sds host;
    int port;
    
    sds dir;
    sds conf_file;
    sds pid_file;
    sds log_file;

    int running;
    int pid;
    redisContext *ctx;
} vire_instance;

void set_execute_file(char *file);

vire_instance *vire_instance_create(int port);
void vire_instance_destroy(vire_instance *vi);

int vire_server_run(vire_instance *vi);
void vire_server_stop(vire_instance *vi);

int create_work_dir(void);
int destroy_work_dir(void);

vire_instance *start_one_vire_instance(void);

void show_test_result(int result,char *test_content,char *errmsg);

#endif
