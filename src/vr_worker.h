#ifndef _VR_WORKER_H_
#define _VR_WORKER_H_

typedef struct vr_worker {

    vr_eventloop vel;
    
    int socketpairs[2];         /*0: belong to listen thread, 1: belong to myself*/
    
    list *csul;
    pthread_mutex_t csullock;

    /* This function has some global state in order to continue the work
     * incrementally across calls. */
    unsigned int current_db;    /* Last DB tested. */
    int timelimit_exit;         /* Time limit hit in previous call? */
    long long last_fast_cycle;  /* When last fast cycle ran. */
}vr_worker;

extern struct array workers;

int workers_init(uint32_t worker_count);
int workers_run(void);
int workers_wait(void);
void workers_deinit(void);

void dispatch_conn_new(vr_listen *vlisten, int sd);

void worker_before_sleep(struct aeEventLoop *eventLoop, void *private_data);
int worker_cron(struct aeEventLoop *eventLoop, long long id, void *clientData);

#endif
