#ifndef _VR_CONNECTION_H_
#define _VR_CONNECTION_H_

typedef struct conn_base {
    dlist *free_connq;           /* free conn q */
    uint64_t ntotal_conn;       /* total # connections counter from start */
    uint32_t ncurr_conn;        /* current # connections */
    uint32_t ncurr_cconn;       /* current # client connections */
}conn_base;

struct conn {
    void                *owner;          /* connection owner */
    
    conn_base           *cb;             /* connect base */

    int                 sd;              /* socket descriptor */

    size_t              recv_bytes;      /* received (read) bytes */
    size_t              send_bytes;      /* sent (written) bytes */

    err_t               err;             /* connection errno */
    unsigned            recv_active:1;   /* recv active? */
    unsigned            recv_ready:1;    /* recv ready? */
    unsigned            send_active:1;   /* send active? */
    unsigned            send_ready:1;    /* send ready? */

    unsigned            connecting:1;    /* connecting? */
    unsigned            connected:1;     /* connected? */
    unsigned            eof:1;           /* eof? aka passive close? */
    unsigned            done:1;          /* done? aka close? */

    dlist                *inqueue;        /* incoming request queue */
    dlist                *outqueue;       /* outputing response queue */
};

struct conn *conn_get(conn_base *cb);
void conn_put(struct conn *conn);

int conn_init(conn_base *cb);
void conn_deinit(conn_base *cb);

ssize_t conn_recv(struct conn *conn, void *buf, size_t size);
ssize_t conn_send(struct conn *conn, void *buf, size_t nsend);
ssize_t conn_sendv(struct conn *conn, struct darray *sendv, size_t nsend);

#endif
