#include <vr_core.h>

vr_master master;

static int setup_master(void);
static void *master_thread_run(void *args);

int
master_init(vr_conf *conf)
{
    rstatus_t status;
    uint32_t j;
    sds *host, listen_str;
    vr_listen **vlisten;
    
    vr_eventloop_init(&master.vel);

    master.vel.thread.fun_run = master_thread_run;

    array_init(&master.listens,array_n(&cserver->binds),sizeof(vr_listen*));

    for (j = 0; j < array_n(&cserver->binds); j ++) {
        host = array_get(&cserver->binds,j);
        listen_str = sdsdup(*host);
        listen_str = sdscatfmt(listen_str, ":%i", cserver->port);
        vlisten = array_push(&master.listens);
        *vlisten = vr_listen_create(listen_str);
        if (*vlisten == NULL) {
            array_pop(&master.listens);
            log_error("Create listen %s failed", listen_str);
            sdsfree(listen_str);
            return VR_ERROR;
        }
        sdsfree(listen_str);
    }

    for (j = 0; j < array_n(&master.listens); j ++) {
        vlisten = array_get(&master.listens, j);
        status = vr_listen_begin(*vlisten);
        if (status != VR_OK) {
            log_error("Begin listen to %s failed", (*vlisten)->name);
            return VR_ERROR;
        }
    }

    setup_master();

    return VR_OK;
}

void
master_deinit(void)
{
    vr_listen **vlisten;
    
    vr_eventloop_deinit(&master.vel);

    while (array_n(&master.listens) > 0) {
        vlisten = array_pop(&master.listens);
        vr_listen_destroy(*vlisten);
    }
    array_deinit(&master.listens);
    
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
    uint32_t j;
    vr_listen **vlisten;

    for (j = 0; j < array_n(&master.listens); j ++) {
        vlisten = array_get(&master.listens,j);
        status = aeCreateFileEvent(master.vel.el, (*vlisten)->sd, AE_READABLE, 
            client_accept, *vlisten);
        if (status == AE_ERR) {
            log_error("Unrecoverable error creating master ipfd file event.");
            return VR_ERROR;
        }
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
