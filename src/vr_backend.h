#ifndef _VR_BACKEND_H_
#define _VR_BACKEND_H_

typedef struct vr_backend {

    int id;
    vr_eventloop vel;

    /* Some global state in order to continue the work incrementally 
       * across calls for activeExpireCycle() to expire some keys. */
    unsigned int current_db;    /* Last DB tested. */
    int timelimit_exit;         /* Time limit hit in previous call? */
    long long last_fast_cycle;  /* When last fast cycle ran. */

    /* We use global counters so if we stop the computation at a given
       * DB we'll be able to start from the successive in the next
       * cron loop iteration for databasesCron() to resize and reshash db. */
    unsigned int resize_db;
    unsigned int rehash_db;
}vr_backend;

extern struct darray backends;

int backends_init(uint32_t backend_count);
int backends_run(void);
int backends_wait(void);
void backends_deinit(void);

#endif
