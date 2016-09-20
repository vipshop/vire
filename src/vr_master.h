#ifndef _VR_MASTER_H_
#define _VR_MASTER_H_

typedef struct vr_master {

    vr_eventloop vel;
    
    struct darray listens;   /* type: vr_listen */

    dlist *cbsul;    /* Connect back swap unit list */
    pthread_mutex_t cbsullock;   /* swap unit list locker */
}vr_master;

extern vr_master master;

int master_init(vr_conf *conf);
void master_deinit(void);

void dispatch_conn_exist(struct client *c, int tid);

int master_run(void);

#endif
