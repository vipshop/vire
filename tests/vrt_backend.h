#ifndef _VRT_BACKEND_H_
#define _VRT_BACKEND_H_

#include <darray.h>

struct abtest_group;
struct dlist;
struct dmtlist;
struct data_unit;
struct aeEventLoop;

typedef struct backend_thread {
    int id;
    pthread_t thread_id;
    
    struct aeEventLoop *el;
    int hz;
    int cronloops;          /* Number of times the cron function run */

    darray *abgs; /* type is abtest_group */

    int deleting;
    int pause;
} backend_thread;

extern int backend_threads_count;

extern int backend_threads_pause_finished_count;

int vrt_backend_init(int threads_count, char *test_target_groups);
void vrt_backend_deinit(void);

int vrt_start_backend(void);
int vrt_wait_backend(void);

#endif
