#include <sys/uio.h>

#include <vr_core.h>

static uint64_t ntotal_conn;       /* total # connections counter from start */
static uint32_t ncurr_conn;        /* current # connections */
static uint32_t ncurr_cconn;       /* current # client connections */

static void conn_free(struct conn *conn);

static struct conn *
_conn_get(conn_base *cb)
{
    struct conn *conn;

    if (cb != NULL && listLength(cb->free_connq) > 0) {
        conn = listPop(cb->free_connq);
    } else {
        conn = vr_alloc(sizeof(*conn));
        if (conn == NULL) {
            return NULL;
        }
        conn->cb = cb;

        conn->inqueue = NULL;
        conn->outqueue = NULL;
    }

    conn->owner = NULL;

    conn->sd = -1;

    conn->send_bytes = 0;
    conn->recv_bytes = 0;

    conn->err = 0;
    conn->recv_active = 0;
    conn->recv_ready = 0;
    conn->send_active = 0;
    conn->send_ready = 0;

    conn->connecting = 0;
    conn->connected = 0;
    conn->eof = 0;
    conn->done = 0;

    if (conn->inqueue == NULL) {
        conn->inqueue = listCreate();
        if (conn->inqueue == NULL) {
            conn_free(conn);
            return NULL;
        }
    }

    if (conn->outqueue == NULL) {
        conn->outqueue = listCreate();
        if (conn->outqueue == NULL) {
            conn_free(conn);
            return NULL;
        }
    }
    
    if (cb != NULL) {
        cb->ntotal_conn++;
        cb->ncurr_conn++;
    }
    
    return conn;
}

struct conn *
conn_get(conn_base *cb)
{
    struct conn *conn;

    conn = _conn_get(cb);
    if (conn == NULL) {
        return NULL;
    }

    log_debug(LOG_VVERB, "get conn %p client %d", conn, conn->sd);

    return conn;
}

static void
conn_free(struct conn *conn)
{
    log_debug(LOG_VVERB, "free conn %p", conn);

    if (conn == NULL) {
        return;
    }

    if (conn->sd > 0) {
        close(conn->sd);
        conn->sd = -1;
    }

    if (conn->inqueue) {
        sds buf;
        while (buf = listPop(conn->inqueue)) {
            sdsfree(buf);
        }
        listRelease(conn->inqueue);
        conn->inqueue = NULL;
    }

    if (conn->outqueue) {
        sds buf;
        while (buf = listPop(conn->outqueue)) {
            sdsfree(buf);
        }
        listRelease(conn->outqueue);
        conn->outqueue = NULL;
    }
    
    vr_free(conn);
}

void
conn_put(struct conn *conn)
{
    conn_base *cb = conn->cb;
    
    ASSERT(conn->owner == NULL);

    log_debug(LOG_VVERB, "put conn %p", conn);

    if (conn->sd > 0) {
        close(conn->sd);
        conn->sd = -1;
    }

    if (cb == NULL) {
        conn_free(conn);
        return;
    }

    if (conn->inqueue) {
        sds buf;
        while (buf = listPop(conn->inqueue)) {
            sdsfree(buf);
        }
    }

    if (conn->outqueue) {
        sds buf;
        while (buf = listPop(conn->outqueue)) {
            sdsfree(buf);
        }
    }

    listPush(cb->free_connq, conn);
    cb->ncurr_cconn--;
    cb->ncurr_conn--;
}

int
conn_init(conn_base *cb)
{
    log_debug(LOG_DEBUG, "conn size %d", sizeof(struct conn));

    cb->free_connq = NULL;
    cb->ntotal_conn = 0;
    cb->ncurr_cconn = 0;
    cb->ncurr_cconn = 0;

    cb->free_connq = listCreate();
    if (cb->free_connq == NULL) {
        return VR_ENOMEM;
    }

    return VR_OK;
}

void
conn_deinit(conn_base *cb)
{
    struct conn *conn;

    if (cb->free_connq) {
        while (conn = listPop(cb->free_connq)) {
            conn_free(conn);
        }
        ASSERT(listLength(cb->free_connq) == 0);
        listRelease(cb->free_connq);
    }
}

ssize_t
conn_recv(struct conn *conn, void *buf, size_t size)
{
    ssize_t n;

    ASSERT(buf != NULL);
    ASSERT(size > 0);
    ASSERT(conn->recv_ready);

    for (;;) {
        n = vr_read(conn->sd, buf, size);

        log_debug(LOG_VERB, "recv on sd %d %zd of %zu", conn->sd, n, size);

        if (n > 0) {
            if (n < (ssize_t) size) {
                conn->recv_ready = 0;
            }
            conn->recv_bytes += (size_t)n;
            return n;
        }

        if (n == 0) {
            conn->recv_ready = 0;
            conn->eof = 1;
            log_debug(LOG_INFO, "recv on sd %d eof rb %zu sb %zu", conn->sd,
                      conn->recv_bytes, conn->send_bytes);
            return n;
        }

        if (errno == EINTR) {
            log_debug(LOG_VERB, "recv on sd %d not ready - eintr", conn->sd);
            continue;
        } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
            conn->recv_ready = 0;
            log_debug(LOG_VERB, "recv on sd %d not ready - eagain", conn->sd);
            return VR_EAGAIN;
        } else {
            conn->recv_ready = 0;
            conn->err = errno;
            log_error("recv on sd %d failed: %s", conn->sd, strerror(errno));
            return VR_ERROR;
        }
    }

    NOT_REACHED();

    return VR_ERROR;
}

ssize_t
conn_send(struct conn *conn, void *buf, size_t nsend)
{
    ssize_t n;

    ASSERT(nsend != 0);
    ASSERT(conn->send_ready);

    for (;;) {
        n = vr_write(conn->sd, buf, nsend);

        log_debug(LOG_VERB, "send on sd %d %zd of %zu",
                  conn->sd, n, nsend);

        if (n > 0) {
            if (n < (ssize_t) nsend) {
                conn->send_ready = 0;
            }
            conn->send_bytes += (size_t)n;
            return n;
        }

        if (n == 0) {
            log_warn("send on sd %d returned zero", conn->sd);
            conn->send_ready = 0;
            return 0;
        }

        if (errno == EINTR) {
            log_debug(LOG_VERB, "send on sd %d not ready - eintr", conn->sd);
            continue;
        } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
            conn->send_ready = 0;
            log_debug(LOG_VERB, "send on sd %d not ready - eagain", conn->sd);
            return VR_EAGAIN;
        } else {
            conn->send_ready = 0;
            conn->err = errno;
            log_error("send on sd %d failed: %s", conn->sd, strerror(errno));
            return VR_ERROR;
        }
    }

    NOT_REACHED();

    return VR_ERROR;
}

ssize_t
conn_sendv(struct conn *conn, struct array *sendv, size_t nsend)
{
    ssize_t n;

    ASSERT(array_n(sendv) > 0);
    ASSERT(nsend != 0);
    ASSERT(conn->send_ready);

    for (;;) {
        n = vr_writev(conn->sd, sendv->elem, sendv->nelem);

        log_debug(LOG_VERB, "sendv on sd %d %zd of %zu in %"PRIu32" buffers",
                  conn->sd, n, nsend, sendv->nelem);

        if (n > 0) {
            if (n < (ssize_t) nsend) {
                conn->send_ready = 0;
            }
            conn->send_bytes += (size_t)n;
            return n;
        }

        if (n == 0) {
            log_warn("sendv on sd %d returned zero", conn->sd);
            conn->send_ready = 0;
            return 0;
        }

        if (errno == EINTR) {
            log_debug(LOG_VERB, "sendv on sd %d not ready - eintr", conn->sd);
            continue;
        } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
            conn->send_ready = 0;
            log_debug(LOG_VERB, "sendv on sd %d not ready - eagain", conn->sd);
            return VR_EAGAIN;
        } else {
            conn->send_ready = 0;
            conn->err = errno;
            log_error("sendv on sd %d failed: %s", conn->sd, strerror(errno));
            return VR_ERROR;
        }
    }

    NOT_REACHED();

    return VR_ERROR;
}

uint32_t
conn_ncurr_conn()
{
    return ncurr_conn;
}

uint64_t
conn_ntotal_conn()
{
    return ntotal_conn;
}

uint32_t
conn_ncurr_cconn()
{
    return ncurr_cconn;
}
