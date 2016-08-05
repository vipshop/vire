#ifndef _VRT_DISPATCH_DATA_H_
#define _VRT_DISPATCH_DATA_H_

#include <darray.h>

struct abtest_group;
struct dlist;
struct dmtlist;
struct data_unit;
struct aeEventLoop;

typedef struct dispatch_conn_context {
    struct redisContext *ctx;
    struct redisAsyncContext *actx;    
} dispatch_conn_context;

typedef struct dispatch_data_thread {
    int id;
    pthread_t thread_id;
    
    struct aeEventLoop *el;
    int hz;
    int cronloops;          /* Number of times the cron function run */

    struct dmtlist *datas;  /* Value is data_unit, used receive data 
                                        from produce data thread, and send to the abtest groups. */
    struct dlist *rdatas;   /* Value is reply_unit, used to cache data 
                                        that has not received from abtest groups completely */

    darray *abgs; /* type is abtest_group */
} dispatch_data_thread;

int vrt_dispatch_data_init(int threads_count, char *test_target_groups, int connections);
void vrt_dispatch_data_deinit(void);

int vrt_start_dispatch_data(void);
int vrt_wait_dispatch_data(void);

int data_dispatch(struct data_unit *du);

#endif
