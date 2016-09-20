#ifndef _VR_REPLICATION_H_
#define _VR_REPLICATION_H_

/* Slave replication state. Used in server.repl_state for slaves to remember
 * what to do next. */
#define REPL_STATE_NONE 0 /* No active replication */
#define REPL_STATE_CONNECT 1 /* Must connect to master */
#define REPL_STATE_CONNECTING 2 /* Connecting to master */
/* --- Handshake states, must be ordered --- */
#define REPL_STATE_RECEIVE_PONG 3 /* Wait for PING reply */
#define REPL_STATE_SEND_AUTH 4 /* Send AUTH to master */
#define REPL_STATE_RECEIVE_AUTH 5 /* Wait for AUTH reply */
#define REPL_STATE_SEND_PORT 6 /* Send REPLCONF listening-port */
#define REPL_STATE_RECEIVE_PORT 7 /* Wait for REPLCONF reply */
#define REPL_STATE_SEND_CAPA 8 /* Send REPLCONF capa */
#define REPL_STATE_RECEIVE_CAPA 9 /* Wait for REPLCONF reply */
#define REPL_STATE_SEND_PSYNC 10 /* Send PSYNC */
#define REPL_STATE_RECEIVE_PSYNC 11 /* Wait for PSYNC reply */
/* --- End of handshake states --- */
#define REPL_STATE_TRANSFER 12 /* Receiving .rdb from master */
#define REPL_STATE_CONNECTED 13 /* Connected to master */

/* State of slaves from the POV of the master. Used in client->replstate.
 * In SEND_BULK and ONLINE state the slave receives new updates
 * in its output queue. In the WAIT_BGSAVE states instead the server is waiting
 * to start the next background saving in order to send updates to it. */
#define SLAVE_STATE_WAIT_BGSAVE_START 6 /* We need to produce a new RDB file. */
#define SLAVE_STATE_WAIT_BGSAVE_END 7 /* Waiting RDB file creation to finish. */
#define SLAVE_STATE_SEND_BULK 8 /* Sending RDB file to slave. */
#define SLAVE_STATE_ONLINE 9 /* RDB file transmitted, sending just updates. */

/* Slave capabilities. */
#define SLAVE_CAPA_NONE 0
#define SLAVE_CAPA_EOF (1<<0)   /* Can parse the RDB EOF streaming format. */

/* Synchronous read timeout - slave side */
#define CONFIG_REPL_SYNCIO_TIMEOUT 5

#define REPLICATION_ROLE_MASTER 0
#define REPLICATION_ROLE_SLAVE  1

#define CONFIG_DEFAULT_REPL_BACKLOG_SIZE (1024*1024)    /* 1mb */
#define CONFIG_DEFAULT_REPL_BACKLOG_TIME_LIMIT (60*60)  /* 1 hour */
#define CONFIG_REPL_BACKLOG_MIN_SIZE (1024*16)          /* 16k */

struct vr_replication {
    vr_eventloop vel;

    int role;               /* Master/slave? */

    /* Replication (master) */
    dlist *slaves;           /* List of slaves */
    int slaveseldb;                 /* Last SELECTed DB in replication output */
    long long master_repl_offset;   /* Global replication offset */
    int repl_ping_slave_period;     /* Master pings the slave every N seconds */
    char *repl_backlog;             /* Replication backlog for partial syncs */
    long long repl_backlog_size;    /* Backlog circular buffer size */
    long long repl_backlog_histlen; /* Backlog actual data length */
    long long repl_backlog_idx;     /* Backlog circular buffer current offset */
    long long repl_backlog_off;     /* Replication offset of first byte in the
                                       backlog buffer. */
    time_t repl_backlog_time_limit; /* Time without slaves after the backlog
                                       gets released. */
    time_t repl_no_slaves_since;    /* We have no slaves since that time.
                                       Only valid if server.slaves len is 0. */
    int repl_min_slaves_to_write;   /* Min number of slaves to write. */
    int repl_min_slaves_max_lag;    /* Max lag of <count> slaves to write. */
    int repl_good_slaves_count;     /* Number of slaves with lag <= max_lag. */
    int repl_diskless_sync;         /* Send RDB to slaves sockets directly. */
    int repl_diskless_sync_delay;   /* Delay to start a diskless repl BGSAVE. */

    /* Replication (slave) */
    char *masterauth;               /* AUTH with this password with master */
    char *masterhost;               /* Hostname of master */
    int masterport;                 /* Port of master */
    int repl_timeout;               /* Timeout after N seconds of master idle */
    client *master;     /* Client that is master for this slave */
    client *cached_master; /* Cached master to be reused for PSYNC. */
    int repl_syncio_timeout; /* Timeout for synchronous I/O calls */
    int repl_state;          /* Replication status if the instance is a slave */
    off_t repl_transfer_size; /* Size of RDB to read from master during sync. */
    off_t repl_transfer_read; /* Amount of RDB read from master during sync. */
    off_t repl_transfer_last_fsync_off; /* Offset when we fsync-ed last time. */
    int repl_transfer_s;     /* Slave -> Master SYNC socket */
    int repl_transfer_fd;    /* Slave -> Master SYNC temp file descriptor */
    char *repl_transfer_tmpfile; /* Slave-> master SYNC temp file name */
    time_t repl_transfer_lastio; /* Unix time of the latest read, for timeout */
    int repl_serve_stale_data; /* Serve stale data when link is down? */
    int repl_slave_ro;          /* Slave is read only? */
    time_t repl_down_since; /* Unix time at which link with master went down */
    int repl_disable_tcp_nodelay;   /* Disable TCP_NODELAY after SYNC? */
    int slave_priority;             /* Reported in INFO and used by Sentinel. */
    char repl_master_runid[CONFIG_RUN_ID_SIZE+1];  /* Master run id for PSYNC. */
    long long repl_master_initial_offset;         /* Master PSYNC offset. */
};

extern struct vr_replication repl;

int vr_replication_init(void);
void vr_replication_deinit(void);

void unblockClientWaitingReplicas(client *c);
void refreshGoodSlavesCount(void);
void replicationHandleMasterDisconnection(void);
void replicationCacheMaster(client *c);
char *replicationGetSlaveName(client *c);
void replconfCommand(client *c);
void putSlaveOnline(client *slave);
void replicationSendAck(void);
void replicationFeedMonitors(client *c, dlist *monitors, int dictid, robj **argv, int argc);
void replicationFeedSlaves(dlist *slaves, int dictid, robj **argv, int argc);
void feedReplicationBacklogWithObject(robj *o);
void feedReplicationBacklog(void *ptr, size_t len);

#endif
