#ifndef _VR_EVENTLOOP_H_
#define _VR_EVENTLOOP_H_

typedef struct vr_eventloop {
    vr_thread thread;

    aeEventLoop *el;
    int hz;                     /* cron() calls frequency in hertz */
    int cronloops;              /* Number of times the cron function run */
    
    /* time cache */
    time_t unixtime;            /* Unix time sampled every cron cycle. */
    long long mstime;           /* Like 'unixtime' but with milliseconds resolution. */

    unsigned lruclock:LRU_BITS; /* Clock for LRU eviction */

    conn_base *cb;

    uint64_t next_client_id;    /* Next client unique ID. Incremental. */
    struct client *current_client;     /* Current client, only used on crash report */
    dlist *clients;              /* List of active clients */
    dlist *clients_pending_write;/* There is to write or install handler. */
    dlist *clients_to_close;     /* Clients to close asynchronously */

    int clients_paused;         /* True if clients are currently paused */
    long long clients_pause_end_time; /* Time when we undo clients_paused */

    vr_stats *stats;            /* stats for this thread */
    size_t resident_set_size;   /* RSS sampled in workerCron(). */

    long long dirty;            /* Changes to DB from the last save */

    /* Blocked clients */
    unsigned int bpop_blocked_clients; /* Number of clients blocked by lists */
    dlist *unblocked_clients;        /* list of clients to unblock before next loop */

    /* Synchronous replication. */
    dlist *clients_waiting_acks;     /* Clients waiting in WAIT command. */

    /* Pubsub */
    dict *pubsub_channels;  /* Map channels to list of subscribed clients */
    dlist *pubsub_patterns;  /* A list of pubsub_patterns */
    int notify_keyspace_events; /* Events to propagate via Pub/Sub. This is an
                                   xor of NOTIFY_... flags. */

    conf_cache cc; /* Cache the hot config option to improve vire speed. */

    struct darray *cstable; /* type: commandStats */
}vr_eventloop;

int vr_eventloop_init(vr_eventloop *vel, int filelimit);
void vr_eventloop_deinit(vr_eventloop *vel);

#endif
