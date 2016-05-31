#include <vr_core.h>

int
vr_thread_init(vr_thread *thread)
{    
    if (thread == NULL) {
        return VR_ERROR;
    }

    thread->id = 0;
    thread->thread_id = 0;
    thread->fun_run = NULL;
    thread->data = NULL;

    return VR_OK;
}

void
vr_thread_deinit(vr_thread *thread)
{
    if (thread == NULL) {
        return;
    }

    thread->id = 0;
    thread->thread_id = 0;
    thread->fun_run = NULL;
    thread->data = NULL;
}

static void *vr_thread_run(void *data)
{
    vr_thread *thread = data;
    srand(vr_usec_now()^(int)pthread_self());
    
    thread->fun_run(thread->data);
}

int vr_thread_start(vr_thread *thread)
{
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    
    if (thread == NULL || thread->fun_run == NULL) {
        return VR_ERROR;
    }

    pthread_create(&thread->thread_id, 
        &attr, vr_thread_run, thread);

    return VR_OK;
}
