#include <vr_core.h>

vr_master master;

static int setup_master(void);
static void *master_thread_run(void *args);

int
master_init(vr_conf *conf)
{
    rstatus_t status;
    
    vr_eventloop_init(&master.vel);

    master.vel.thread.fun_run = master_thread_run;

    master.listen = vr_listen_create(conf->listen);
    if (master.listen == NULL) {
        log_error("create listen failed");
        return VR_ERROR;
    }

    status = vr_listen_begin(master.listen);
    if (status != VR_OK) {
        log_error("begin listen to %s failed", master.listen->name);
        return VR_ERROR;
    }

    setup_master();

    return VR_OK;
}

void
master_deinit(void)
{
    vr_eventloop_deinit(&master.vel);

    if (master.listen != NULL) {
        vr_listen_destroy(master.listen);
        master.listen = NULL;
    }
}

static void
client_accept(aeEventLoop *el, int fd, void *privdata, int mask) {
    int sd;
    vr_listen *vlisten = privdata;

    while((sd = vr_listen_accept(vlisten)) > 0) {
        dispatch_conn_new(vlisten, sd);
    }
}

static int
setup_master(void)
{
    rstatus_t status;
    
    status = aeCreateFileEvent(master.vel.el, master.listen->sd, AE_READABLE, 
        client_accept, master.listen);
    if (status == AE_ERR) {
        log_error("Unrecoverable error creating master ipfd file event.");
        return VR_ERROR;
    }
    
    return VR_OK;
}

static void *
master_thread_run(void *args)
{    
    /* vire master run */
    aeMain(master.vel.el);

    return NULL;
}

int
master_run(void)
{
    vr_thread_start(&master.vel.thread);
    return VR_OK;
}
