#ifndef _VRT_DISPATCH_DATA_H_
#define _VRT_DISPATCH_DATA_H_

#include <darray.h>

struct abtest_group;
struct dlist;
struct dmtqueue;
struct data_unit;
struct aeEventLoop;

typedef struct dispatch_data_thread {
    int id;
    pthread_t thread_id;
    
    struct aeEventLoop *el;
    int hz;
    int cronloops;          /* Number of times the cron function run */

    struct dmtqueue *datas;  /* Value is data_unit, used receive data 
                                        from produce data thread, and send to the abtest groups. */
    struct dlist *rdatas;   /* Value is reply_unit, used to cache data 
                                        that has not received from abtest groups completely */

    darray *abgs; /* type is abtest_group */

    int pause;

    int count_wait_for_reply;

    long long reply_total_count_per_cycle;
    long long reply_type_err_count_per_cycle;
} dispatch_data_thread;

extern int dispatch_data_threads_count;

extern int dispatch_threads_pause_finished_count;

long long get_total_tested_commands_count_per_cycle(void);
long long get_total_reply_err_count_per_cycle(void);
void reset_total_count_per_cycle(void);

int vrt_dispatch_data_init(int threads_count, char *test_target_groups, int connections);
void vrt_dispatch_data_deinit(void);

int vrt_start_dispatch_data(void);
int vrt_wait_dispatch_data(void);

int data_dispatch(struct data_unit *du);

#endif
