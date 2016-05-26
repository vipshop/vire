#ifndef _VR_MASTER_H_
#define _VR_MASTER_H_

typedef struct vr_master {

    vr_eventloop vel;
    
    vr_listen *listen;
    
}vr_master;

extern vr_master master;

int master_init(vr_conf *conf);
void master_deinit(void);

int master_run(void);

#endif
