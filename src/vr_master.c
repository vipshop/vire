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
    int threads_num;
    int filelimit;

    master.cbsul = NULL;
    pthread_mutex_init(&master.cbsullock, NULL);

    conf_server_get(CONFIG_SOPN_THREADS,&threads_num);
    filelimit = threads_num*2+CONFIG_MIN_RESERVED_FDS;
    vr_eventloop_init(&master.vel,filelimit);
    master.vel.thread.fun_run = master_thread_run;

    darray_init(&master.listens,darray_n(&cserver->binds),sizeof(vr_listen*));

    for (j = 0; j < darray_n(&cserver->binds); j ++) {
        host = darray_get(&cserver->binds,j);
        listen_str = sdsdup(*host);
        listen_str = sdscatfmt(listen_str, ":%i", cserver->port);
        vlisten = darray_push(&master.listens);
        *vlisten = vr_listen_create(listen_str);
        if (*vlisten == NULL) {
            darray_pop(&master.listens);
            log_error("Create listen %s failed", listen_str);
            sdsfree(listen_str);
            return VR_ERROR;
        }
        sdsfree(listen_str);
    }

    for (j = 0; j < darray_n(&master.listens); j ++) {
        vlisten = darray_get(&master.listens, j);
        status = vr_listen_begin(*vlisten);
        if (status != VR_OK) {
            log_error("Begin listen to %s failed", (*vlisten)->name);
            return VR_ERROR;
        }
    }

    master.cbsul = dlistCreate();
    if (master.cbsul == NULL) {
        log_error("Create list failed: out of memory");
        return VR_ENOMEM;
    }

    setup_master();

    return VR_OK;
}

void
master_deinit(void)
{
    vr_listen **vlisten;
    
    vr_eventloop_deinit(&master.vel);

    while (darray_n(&master.listens) > 0) {
        vlisten = darray_pop(&master.listens);
        vr_listen_destroy(*vlisten);
    }
    darray_deinit(&master.listens);
    
}

static void
client_accept(aeEventLoop *el, int fd, void *privdata, int mask) {
    int sd;
    vr_listen *vlisten = privdata;

    while((sd = vr_listen_accept(vlisten)) > 0) {
        dispatch_conn_new(vlisten, sd);
    }
}

static void
cbsul_push(struct connswapunit *su)
{
    pthread_mutex_lock(&master.cbsullock);
    dlistPush(master.cbsul, su);
    pthread_mutex_unlock(&master.cbsullock);
}

static struct connswapunit *
cbsul_pop(void)
{
    struct connswapunit *su = NULL;

    pthread_mutex_lock(&master.cbsullock);
    su = dlistPop(master.cbsul);
    pthread_mutex_unlock(&master.cbsullock);
    
    return su;
}

void
dispatch_conn_exist(client *c, int tid)
{
    struct connswapunit *su = csui_new();
    char buf[1];
    vr_worker *worker;

    if (su == NULL) {
        freeClient(c);
        /* given that malloc failed this may also fail, but let's try */
        log_error("Failed to allocate memory for connection swap object\n");
        return ;
    }

    su->num = tid;
    su->data = c;
    
    unlinkClientFromEventloop(c);

    cbsul_push(su);

    worker = darray_get(&workers, (uint32_t)c->curidx);

    /* Back to master */
    buf[0] = 'b';
    if (vr_write(worker->socketpairs[1], buf, 1) != 1) {
        log_error("Notice the worker failed.");
    }
}

static void
thread_event_process(aeEventLoop *el, int fd, void *privdata, int mask) {

    rstatus_t status;
    vr_worker *worker = privdata;
    char buf[1];
    int idx;
    client *c;
    struct connswapunit *su;

    ASSERT(el == master.vel.el);
    ASSERT(fd == worker->socketpairs[0]);

    if (vr_read(fd, buf, 1) != 1) {
        log_warn("Can't read for worker(id:%d) socketpairs[1](%d)", 
            worker->vel.thread.id, fd);
        buf[0] = 'b';
    }
    
    switch (buf[0]) {
    case 'b':
        su = cbsul_pop();
        if (su == NULL) {
            log_warn("Pop from connection back swap list is null");
            return;
        }
        
        idx = su->num;
        su->num = worker->id;
        worker = darray_get(&workers, (uint32_t)idx);
        csul_push(worker, su);

        /* Jump to the target worker. */
        buf[0] = 'j';
        if (vr_write(worker->socketpairs[0], buf, 1) != 1) {
            log_error("Notice the worker failed.");
        }
        break;
    default:
        log_error("read error char '%c' for worker(id:%d) socketpairs[0](%d)", 
            buf[0], worker->vel.thread.id, worker->socketpairs[1]);
        break;
    }
}

static int
setup_master(void)
{
    rstatus_t status;
    uint32_t j;
    vr_listen **vlisten;
    vr_worker *worker;

    for (j = 0; j < darray_n(&workers); j ++) {
        worker = darray_get(&workers, j);
        status = aeCreateFileEvent(master.vel.el, worker->socketpairs[0], 
            AE_READABLE, thread_event_process, worker);
        if (status == AE_ERR) {
            log_error("Unrecoverable error creating master ipfd file event.");
            return VR_ERROR;
        }
    }

    for (j = 0; j < darray_n(&master.listens); j ++) {
        vlisten = darray_get(&master.listens,j);
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
