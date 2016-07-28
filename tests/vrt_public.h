#ifndef _VRT_PUBLIC_H_
#define _VRT_PUBLIC_H_

#ifdef HAVE_CONFIG_H
# include <config.h>
#endif

#include <vr_specialconfig.h>

#include <unistd.h>

#include <hiredis.h>

#define VRT_OK        0
#define VRT_ERROR    -1

#define VRT_TEST_OK     0
#define VRT_TEST_ERR    1

#define VRT_UINT8_MAXLEN     (3 + 1)
#define VRT_UINT16_MAXLEN    (5 + 1)
#define VRT_UINT32_MAXLEN    (10 + 1)
#define VRT_UINT64_MAXLEN    (20 + 1)
#define VRT_UINTMAX_MAXLEN   VRT_UINT64_MAXLEN

#define VRT_MAXHOSTNAMELEN   256

#define LF                  (uint8_t) 10
#define CR                  (uint8_t) 13
#define CRLF                "\x0d\x0a"
#define CRLF_LEN            (sizeof("\x0d\x0a") - 1)

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

void show_test_result(int result,char *test_message);

#endif
