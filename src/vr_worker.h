#ifndef _VR_WORKER_H_
#define _VR_WORKER_H_

typedef struct vr_worker {

    int id;
    vr_eventloop vel;
    
    int socketpairs[2];         /*0: belong to master thread, 1: belong to myself*/
    
    dlist *csul;    /* Connect swap unit list */
    pthread_mutex_t csullock;   /* swap unit list locker */

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
}vr_worker;

struct connswapunit {
    int num;
    void *data;
    struct connswapunit *next;
};

extern struct darray workers;

int workers_init(uint32_t worker_count);
int workers_run(void);
int workers_wait(void);
void workers_deinit(void);

struct connswapunit *csui_new(void);
void csui_free(struct connswapunit *item);

void csul_push(vr_worker *worker, struct connswapunit *su);
struct connswapunit *csul_pop(vr_worker *worker);

int worker_get_next_idx(int curidx);

void dispatch_conn_new(vr_listen *vlisten, int sd);

void worker_before_sleep(struct aeEventLoop *eventLoop, void *private_data);
int worker_cron(struct aeEventLoop *eventLoop, long long id, void *clientData);

#endif
