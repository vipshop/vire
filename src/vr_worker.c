#include <vr_core.h>

/* Which thread we assigned a connection to most recently. */
static int last_worker_thread = -1;
static int num_worker_threads;
static struct array workers;

static void *worker_thread_run(void *args);

struct connswapunit {
    int sd;
    vr_listen *vlisten;
    struct connswapunit *next;
};

#define SU_PER_ALLOC 64

/* Free list of swapunit structs */
static struct connswapunit *csui_freelist;
static pthread_mutex_t csui_freelist_lock;

/*
 * Returns a fresh connection connswapunit queue item.
 */
static struct connswapunit *
csui_new(void) {
    struct connswapunit *item = NULL;
    pthread_mutex_lock(&csui_freelist_lock);
    if (csui_freelist) {
        item = csui_freelist;
        csui_freelist = item->next;
    }
    pthread_mutex_unlock(&csui_freelist_lock);

    if (NULL == item) {
        int i;

        /* Allocate a bunch of items at once to reduce fragmentation */
        item = vr_alloc(sizeof(struct connswapunit) * SU_PER_ALLOC);
        if (NULL == item) {
            return NULL;
        }

        /*
         * Link together all the new items except the first one
         * (which we'll return to the caller) for placement on
         * the freelist.
         */
        for (i = 2; i < SU_PER_ALLOC; i++)
            item[i - 1].next = &item[i];

        pthread_mutex_lock(&csui_freelist_lock);
        item[SU_PER_ALLOC - 1].next = csui_freelist;
        csui_freelist = &item[1];
        pthread_mutex_unlock(&csui_freelist_lock);
    }

    return item;
}

/*
 * Frees a connection connswapunit queue item (adds it to the freelist.)
 */
static void 
csui_free(struct connswapunit *item) {
    pthread_mutex_lock(&csui_freelist_lock);
    item->next = csui_freelist;
    csui_freelist = item;
    pthread_mutex_unlock(&csui_freelist_lock);
}

static void
csul_push(vr_worker *worker, struct connswapunit *su)
{
    pthread_mutex_lock(&worker->csullock);
    listPush(worker->csul, su);
    pthread_mutex_unlock(&worker->csullock);
}

static struct connswapunit *
csul_pop(vr_worker *worker)
{
    struct connswapunit *su = NULL;

    pthread_mutex_lock(&worker->csullock);
    su = listPop(worker->csul);
    pthread_mutex_unlock(&worker->csullock);
    
    return su;
}

int
vr_worker_init(vr_worker *worker)
{
    rstatus_t status;
    
    if (worker == NULL) {
        return VR_ERROR;
    }

    vr_eventloop_init(&worker->vel);
    worker->socketpairs[0] = -1;
    worker->socketpairs[1] = -1;
    worker->csul = NULL;
    pthread_mutex_init(&worker->csullock, NULL);
    worker->current_db = 0;
    worker->timelimit_exit = 0;
    worker->last_fast_cycle = 0;

    worker->vel.thread.fun_run = worker_thread_run;
    worker->vel.thread.data = worker;

    status = socketpair(AF_LOCAL, SOCK_STREAM, 0, worker->socketpairs);
    if (status < 0) {
        log_error("create socketpairs failed: %s", strerror(errno));
        return VR_ERROR;
    }
    status = vr_set_nonblocking(worker->socketpairs[0]);
    if (status < 0) {
        log_error("set socketpairs[0] %d nonblocking failed: %s", 
            worker->socketpairs[0], strerror(errno));
        close(worker->socketpairs[0]);
        worker->socketpairs[0] = -1;
        close(worker->socketpairs[1]);
        worker->socketpairs[1] = -1;
        return VR_ERROR;
    }
    status = vr_set_nonblocking(worker->socketpairs[1]);
    if (status < 0) {
        log_error("set socketpairs[1] %d nonblocking failed: %s", 
            worker->socketpairs[1], strerror(errno));
        close(worker->socketpairs[0]);
        worker->socketpairs[0] = -1;
        close(worker->socketpairs[1]);
        worker->socketpairs[1] = -1;
        return VR_ERROR;
    }

    worker->csul = listCreate();
    if (worker->csul == NULL) {
        log_error("create list failed: out of memory");
        return VR_ENOMEM;
    }
    
    return VR_OK;
}

void
vr_worker_deinit(vr_worker *worker)
{
    if (worker == NULL) {
        return;
    }

    vr_eventloop_deinit(&worker->vel);

    if (worker->socketpairs[0] > 0){
        close(worker->socketpairs[0]);
        worker->socketpairs[0] = -1;
    }
    if (worker->socketpairs[1] > 0){
        close(worker->socketpairs[1]);
        worker->socketpairs[1] = -1;
    }

    if (worker->csul != NULL) {
        listRelease(worker->csul);
        worker->csul = NULL;
    }
}

void
dispatch_conn_new(vr_listen *vlisten, int sd)
{
    struct connswapunit *su = csui_new();
    char buf[1];
    vr_worker *worker;

    if (su == NULL) {
        close(sd);
        /* given that malloc failed this may also fail, but let's try */
        log_error("Failed to allocate memory for connection object\n");
        return ;
    }
    
    int tid = (last_worker_thread + 1) % num_worker_threads;
    worker = array_get(&workers, (uint32_t)tid);

    last_worker_thread = tid;

    su->sd = sd;
    su->vlisten = vlisten;

    csul_push(worker, su);

    buf[0] = 'c';
    if (vr_write(worker->socketpairs[0], buf, 1) != 1) {
        log_error("Notice the worker failed.");
    }
}

static void
thread_event_process(aeEventLoop *el, int fd, void *privdata, int mask) {

    rstatus_t status;
    vr_worker *worker = privdata;
    char buf[1];
    int sd;
    vr_listen *vlisten;
    struct conn *conn;
    struct connswapunit *csu;
    client *c;

    ASSERT(el == worker->vel.el);
    ASSERT(fd == worker->socketpairs[1]);

    if (vr_read(fd, buf, 1) != 1) {
        log_debug(LOG_DEBUG, "can't read for worker(id:%d) socketpairs[1](%d)", 
            worker->vel.thread.id, fd);
        buf[0] = 'c';
    }
    
    switch (buf[0]) {
    case 'c':
        csu = csul_pop(worker);
        if (csu == NULL) {
            return;
        }
        sd = csu->sd;
        vlisten = csu->vlisten;
        csui_free(csu);
        conn = conn_get(worker->vel.cb);
        if (conn == NULL) {
            log_error("get conn for c %d failed: %s", 
                sd, strerror(errno));
            status = close(sd);
            if (status < 0) {
                log_error("close c %d failed, ignored: %s", sd, strerror(errno));
            }
            return;
        }
        conn->sd = sd;
    
        status = vr_set_nonblocking(conn->sd);
        if (status < 0) {
            log_error("set nonblock on c %d failed: %s", 
                conn->sd, strerror(errno));
            conn_put(conn);
            return;
        }
    
        if (vlisten->info.family == AF_INET || vlisten->info.family == AF_INET6) {
            status = vr_set_tcpnodelay(conn->sd);
            if (status < 0) {
                log_warn("set tcpnodelay on c %d failed, ignored: %s",
                    conn->sd, strerror(errno));
            }
        }

        c = createClient(&worker->vel, conn);
        if (c == NULL) {
            log_error("create client failed");
            conn_put(conn);
            return;
        }
        
        status = aeCreateFileEvent(worker->vel.el, conn->sd, AE_READABLE, 
            readQueryFromClient, c);
        if (status == AE_ERR) {
            log_error("Unrecoverable error creating worker ipfd file event.");
            return;
        }
    
        log_debug(LOG_DEBUG, "accepted c %d from '%s'", 
            conn->sd, vr_unresolve_peer_desc(conn->sd));
        break;
    default:
        log_error("read error char '%c' for worker(id:%d) socketpairs[1](%d)", 
            buf[0], worker->vel.thread.id, worker->socketpairs[1]);
        break;
    }
}

static int
setup_worker(vr_worker *worker)
{
    rstatus_t status;
    
    status = aeCreateFileEvent(worker->vel.el, worker->socketpairs[1], AE_READABLE, 
        thread_event_process, worker);
    if (status == AE_ERR) {
        log_error("Unrecoverable error creating worker ipfd file event.");
        return VR_ERROR;
    }

    aeSetBeforeSleepProc(worker->vel.el, worker_before_sleep, worker);

    /* Create the serverCron() time event, that's our main way to process
     * background operations. */
    if(aeCreateTimeEvent(worker->vel.el, 1, worker_cron, worker, NULL) == AE_ERR) {
        serverPanic("Can't create the serverCron time event.");
        return VR_ERROR;
    }
    
    return VR_OK;
}

static void *
worker_thread_run(void *args)
{
    vr_worker *worker = args;
    
    /* vire worker run */
    aeMain(worker->vel.el);

    return NULL;
}

int
workers_init(uint32_t worker_count)
{
    rstatus_t status;
    uint32_t i;
    vr_worker *worker;
    
    csui_freelist = NULL;
    pthread_mutex_init(&csui_freelist_lock, NULL);

    array_init(&workers, worker_count, sizeof(vr_worker));

    for (i = 0; i < worker_count; i ++) {
        worker = array_push(&workers);
        vr_worker_init(worker);

        status = setup_worker(worker);
        if (status != VR_OK) {
            exit(1);
        }
    }
    
    num_worker_threads = (int)array_n(&workers);

    return VR_OK;
}

int
workers_run(void)
{
    uint32_t i, thread_count;
    vr_worker *worker;

    thread_count = (uint32_t)num_worker_threads;

    for (i = 0; i < thread_count; i ++) {
        worker = array_get(&workers, i);
        vr_thread_start(&worker->vel.thread);
    }

    return VR_OK;
}

int
workers_wait(void)
{
    uint32_t i, thread_count;
    vr_worker *worker;

    thread_count = (uint32_t)num_worker_threads;

    for (i = 0; i < thread_count; i ++) {
        worker = array_get(&workers, i);
        pthread_join(worker->vel.thread.thread_id, NULL);
    }

    return VR_OK;
}

void
workers_deinit(void)
{
    vr_worker *worker;

    while(array_n(&workers)) {
        worker = array_pop(&workers);
		vr_worker_deinit(worker);
    }
}

/* This function gets called every time Redis is entering the
 * main loop of the event driven library, that is, before to sleep
 * for ready file descriptors. */
void
worker_before_sleep(struct aeEventLoop *eventLoop, void *private_data) {
    vr_worker *worker = private_data;

    UNUSED(eventLoop);
    UNUSED(private_data);

    ASSERT(eventLoop == worker->vel.el);

    /* Handle writes with pending output buffers. */
    handleClientsWithPendingWrites(&worker->vel);

    activeExpireCycle(worker, ACTIVE_EXPIRE_CYCLE_FAST);
}

int
worker_cron(struct aeEventLoop *eventLoop, long long id, void *clientData) {
    vr_worker *worker = clientData;

    UNUSED(eventLoop);
    UNUSED(id);
    UNUSED(clientData);

    ASSERT(eventLoop == worker->vel.el);

    run_with_period(100, worker->vel.cronloops) {
        
    }

    /* Close clients that need to be closed asynchronous */
    freeClientsInAsyncFreeQueue(&worker->vel);

    activeExpireCycle(worker, ACTIVE_EXPIRE_CYCLE_SLOW);

    worker->vel.cronloops ++;
    return 1000/server.hz;
}
