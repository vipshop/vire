#ifndef _VR_THREAD_H_
#define _VR_THREAD_H_

typedef void *(*vr_thread_func_t)(void *data);

typedef struct vr_thread {
    int id;
    pthread_t thread_id;

    vr_thread_func_t fun_run;
    void *data;
}vr_thread;

int vr_thread_init(vr_thread *thread);
void vr_thread_deinit(vr_thread *thread);
int vr_thread_start(vr_thread *thread);

#endif
