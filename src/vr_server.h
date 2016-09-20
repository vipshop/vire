#ifndef _VR_SERVER_H_
#define _VR_SERVER_H_

#define CONFIG_MIN_RESERVED_FDS 32 /* For extra operations of
                                            * listening sockets, log files and so forth*/

#define CONFIG_AUTHPASS_MAX_LEN 512
#define PROTO_SHARED_SELECT_CMDS 10
#define CRON_DBS_PER_CALL 16

#define ACTIVE_EXPIRE_CYCLE_LOOKUPS_PER_LOOP 20 /* Loopkups per loop. */
#define ACTIVE_EXPIRE_CYCLE_FAST_DURATION 1000 /* Microseconds */
#define ACTIVE_EXPIRE_CYCLE_SLOW_TIME_PERC 25 /* CPU max % for keys collection */
#define ACTIVE_EXPIRE_CYCLE_SLOW 0
#define ACTIVE_EXPIRE_CYCLE_FAST 1

#define SCAN_TYPE_KEY   0
#define SCAN_TYPE_HASH  1
#define SCAN_TYPE_SET   2
#define SCAN_TYPE_ZSET  3

/* Redis maxmemory strategies */
#define CONFIG_DEFAULT_MAXMEMORY_POLICY MAXMEMORY_NO_EVICTION

/* Zip structure related defaults */
#define OBJ_HASH_MAX_ZIPLIST_ENTRIES 512
#define OBJ_HASH_MAX_ZIPLIST_VALUE 64
#define OBJ_SET_MAX_INTSET_ENTRIES 512
#define OBJ_ZSET_MAX_ZIPLIST_ENTRIES 128
#define OBJ_ZSET_MAX_ZIPLIST_VALUE 64

/* List defaults */
#define OBJ_LIST_MAX_ZIPLIST_SIZE -2
#define OBJ_LIST_COMPRESS_DEPTH 0

/* HyperLogLog defines */
#define CONFIG_DEFAULT_HLL_SPARSE_MAX_BYTES 3000

/* List related stuff */
#define LIST_HEAD 0
#define LIST_TAIL 1

#define ZSKIPLIST_MAXLEVEL 32 /* Should be enough for 2^32 elements */
#define ZSKIPLIST_P 0.25      /* Skiplist P = 1/4 */

/* Units */
#define UNIT_SECONDS 0
#define UNIT_MILLISECONDS 1

/* Hash table parameters */
#define HASHTABLE_MIN_FILL        10      /* Minimal hash table fill 10% */

/* Using the following macro you can run code inside serverCron() with the
 * specified period, specified in milliseconds.
 * The actual resolution depends on server.hz. */
#define run_with_period(_ms_, cronloops) if ((_ms_ <= 1000/server.hz) || !(cronloops%((_ms_)/(1000/server.hz))))

/* Macro used to obtain the current LRU clock.
 * If the current resolution is lower than the frequency we refresh the
 * LRU clock (as it should be in production servers) we return the
 * precomputed value, otherwise we need to resort to a function call. */
#define LRU_CLOCK() ((1000/server.hz <= LRU_CLOCK_RESOLUTION) ? server.lruclock : getLRUClock())

/* The following structure represents a node in the server.ready_keys list,
 * where we accumulate all the keys that had clients blocked with a blocking
 * operation such as B[LR]POP, but received new data in the context of the
 * last executed command.
 *
 * After the execution of every command or script, we run this list to check
 * if as a result we should serve data to clients blocked, unblocking them.
 * Note that server.ready_keys will not have duplicates as there dictionary
 * also called ready_keys in every structure representing a Redis database,
 * where we make sure to remember if a given key was already added in the
 * server.ready_keys list. */
typedef struct readyList {
    redisDb *db;
    robj *key;
} readyList;

struct vr_server {
    aeEventLoop *el;
    dlist *clients;
    
    /* General */
    pid_t pid;                  /* Main process pid. */
    char *executable;           /* Absolute executable file path. */
    char *configfile;           /* Absolute config file path, or NULL */
    int hz;                     /* serverCron() calls frequency in hertz */

    struct darray dbs;           /* database array, type: redisDB */
    int dbnum;                  /* Total number of DBs */
    int dblnum;                 /* Logical number of configured DBs */
    int dbinum;                 /* Number of internal DBs for per logical DB */
    
    dict *commands;             /* Command table */
    dict *orig_commands;        /* Command table before command renaming. */
    
    unsigned lruclock:LRU_BITS; /* Clock for LRU eviction */
    int activerehashing;        /* Incremental rehash in serverCron() */

    char *pidfile;              /* PID file path */
    int arch_bits;              /* 32 or 64 depending on sizeof(long) */
    char runid[CONFIG_RUN_ID_SIZE+1];  /* ID always different at every exec. */
    
    /* Networking */
    int port;                   /* TCP listening port */
    int tcp_backlog;            /* TCP listen() backlog */

    int tcpkeepalive;               /* Set SO_KEEPALIVE if non-zero. */
    size_t client_max_querybuf_len; /* Limit for client query buffer length */

    /* Zip structure config, see redis.conf for more information  */
    size_t hash_max_ziplist_entries;
    size_t hash_max_ziplist_value;
    size_t set_max_intset_entries;
    size_t zset_max_ziplist_entries;
    size_t zset_max_ziplist_value;
    size_t hll_sparse_max_bytes;
    /* List parameters */
    int list_max_ziplist_size;
    int list_compress_depth;
    
    clientBufferLimitsConfig client_obuf_limits[CLIENT_TYPE_OBUF_COUNT];

    dlist *monitors;    /* List of slaves and MONITORs */

    time_t starttime;       /* Server start time */

    /* time cache */
    time_t unixtime;        /* Unix time sampled every cron cycle. */
    long long mstime;       /* Like 'unixtime' but with milliseconds resolution. */

    char *unixsocket;           /* UNIX socket path */

    /* RDB / AOF loading information */
    int loading;                /* We are loading data from disk if true */
    off_t loading_total_bytes;
    off_t loading_loaded_bytes;
    time_t loading_start_time;
    off_t loading_process_events_interval_bytes;

    /* AOF persistence */
    int aof_state;                  /* AOF_(ON|OFF|WAIT_REWRITE) */
    int aof_fsync;                  /* Kind of fsync() policy */
    char *aof_filename;             /* Name of the AOF file */
    int aof_no_fsync_on_rewrite;    /* Don't fsync if a rewrite is in prog. */
    int aof_rewrite_perc;           /* Rewrite AOF if % growth is > M and... */
    off_t aof_rewrite_min_size;     /* the AOF file is at least N bytes. */
    off_t aof_rewrite_base_size;    /* AOF size on latest startup or rewrite. */
    off_t aof_current_size;         /* AOF current size. */
    int aof_rewrite_scheduled;      /* Rewrite once BGSAVE terminates. */
    pid_t aof_child_pid;            /* PID if rewriting process */
    dlist *aof_rewrite_buf_blocks;   /* Hold changes during an AOF rewrite. */
    sds aof_buf;      /* AOF buffer, written before entering the event loop */
    int aof_fd;       /* File descriptor of currently selected AOF file */
    int aof_selected_db; /* Currently selected DB in AOF */
    time_t aof_flush_postponed_start; /* UNIX time of postponed AOF flush */
    time_t aof_last_fsync;            /* UNIX time of last fsync() */
    time_t aof_rewrite_time_last;   /* Time used by last AOF rewrite run. */
    time_t aof_rewrite_time_start;  /* Current AOF rewrite start time. */
    int aof_lastbgrewrite_status;   /* VR_OK or VR_ERROR */
    unsigned long aof_delayed_fsync;  /* delayed AOF fsync() counter */
    int aof_rewrite_incremental_fsync;/* fsync incrementally while rewriting? */
    int aof_last_write_status;      /* VR_OK or VR_ERROR */
    int aof_last_write_errno;       /* Valid if aof_last_write_status is ERR */
    int aof_load_truncated;         /* Don't stop on unexpected AOF EOF. */
    /* AOF pipes used to communicate between parent and child during rewrite. */
    int aof_pipe_write_data_to_child;
    int aof_pipe_read_data_from_parent;
    int aof_pipe_write_ack_to_parent;
    int aof_pipe_read_ack_from_child;
    int aof_pipe_write_ack_to_child;
    int aof_pipe_read_ack_from_parent;
    int aof_stop_sending_diff;     /* If true stop sending accumulated diffs
                                      to child process. */
    sds aof_child_diff;             /* AOF diff accumulator child side. */

    /* RDB persistence */
    long long dirty;                /* Changes to DB from the last save */
    long long dirty_before_bgsave;  /* Used to restore dirty on failed BGSAVE */
    pid_t rdb_child_pid;            /* PID of RDB saving child */
    struct saveparam *saveparams;   /* Save points array for RDB */
    int saveparamslen;              /* Number of saving points */
    char *rdb_filename;             /* Name of RDB file */
    int rdb_compression;            /* Use compression in RDB? */
    int rdb_checksum;               /* Use RDB checksum? */
    time_t lastsave;                /* Unix time of last successful save */
    time_t lastbgsave_try;          /* Unix time of last attempted bgsave */
    time_t rdb_save_time_last;      /* Time used by last RDB save run. */
    time_t rdb_save_time_start;     /* Current RDB save start time. */
    int rdb_child_type;             /* Type of save by active child. */
    int lastbgsave_status;          /* VR_OK or VR_ERROR */
    int stop_writes_on_bgsave_err;  /* Don't allow writes if can't BGSAVE */
    int rdb_pipe_write_result_to_parent; /* RDB pipes used to return the state */
    int rdb_pipe_read_result_from_child; /* of each slave in diskless SYNC. */

    /* Scripting */
    //lua_State *lua; /* The Lua interpreter. We use just one for all clients */
    struct client *lua_client;   /* The "fake client" to query Redis from Lua */
    struct client *lua_caller;   /* The client running EVAL right now, or NULL */
    dict *lua_scripts;         /* A dictionary of SHA1 -> Lua scripts */
    mstime_t lua_time_limit;  /* Script timeout in milliseconds */
    mstime_t lua_time_start;  /* Start time of script, milliseconds time */
    int lua_write_dirty;  /* True if a write command was called during the
                             execution of the current script. */
    int lua_random_dirty; /* True if a random command was called during the
                             execution of the current script. */
    int lua_replicate_commands; /* True if we are doing single commands repl. */
    int lua_multi_emitted;/* True if we already proagated MULTI. */
    int lua_repl;         /* Script replication flags for redis.set_repl(). */
    int lua_timedout;     /* True if we reached the time limit for script
                             execution. */
    int lua_kill;         /* Kill the script if true. */
    int lua_always_replicate_commands; /* Default replication type. */

    /* Blocked clients */
    dlist *ready_keys;        /* List of readyList structures for BLPOP & co */

    /* Propagation of commands in AOF / replication */
    redisOpArray also_propagate;    /* Additional command to propagate. */

    /* Pubsub */
    dict *pubsub_channels;  /* Map channels to list of subscribed clients */
    dlist *pubsub_patterns;  /* A list of pubsub_patterns */
    int notify_keyspace_events; /* Events to propagate via Pub/Sub. This is an
                                   xor of NOTIFY_... flags. */

    /* Fast pointers to often looked up command */
    struct redisCommand *delCommand, *multiCommand, *lpushCommand, *lpopCommand,
                        *rpopCommand, *sremCommand, *execCommand;

    /* System hardware info */
    size_t system_memory_size;  /* Total memory in system as reported by OS */
};

/* ZSETs use a specialized version of Skiplists */
typedef struct zskiplistNode {
    robj *obj;
    double score;
    struct zskiplistNode *backward;
    struct zskiplistLevel {
        struct zskiplistNode *forward;
        unsigned int span;
    } level[];
} zskiplistNode;

typedef struct zskiplist {
    struct zskiplistNode *header, *tail;
    unsigned long length;
    int level;
} zskiplist;

typedef struct zset {
    dict *dict;
    zskiplist *zsl;
} zset;

/* Structure to hold list iteration abstraction. */
typedef struct {
    robj *subject;
    unsigned char encoding;
    unsigned char direction; /* Iteration direction */
    quicklistIter *iter;
} listTypeIterator;

/* Structure for an entry while iterating over a list. */
typedef struct {
    listTypeIterator *li;
    quicklistEntry entry; /* Entry in quicklist */
} listTypeEntry;

/* Structure to hold set iteration abstraction. */
typedef struct {
    robj *subject;
    int encoding;
    int ii; /* intset iterator */
    dictIterator *di;
} setTypeIterator;

/* Structure to hold hash iteration abstraction. Note that iteration over
 * hashes involves both fields and values. Because it is possible that
 * not both are required, store pointers in the iterator to avoid
 * unnecessary memory allocation for fields/values. */
typedef struct {
    robj *subject;
    int encoding;

    unsigned char *fptr, *vptr;

    dictIterator *di;
    dictEntry *de;
} hashTypeIterator;

struct sharedObjectsStruct {
    robj *crlf, *ok, *err, *emptybulk, *czero, *cone, *cnegone, *pong, *space,
    *colon, *nullbulk, *nullmultibulk, *queued,
    *emptymultibulk, *wrongtypeerr, *nokeyerr, *syntaxerr, *sameobjecterr,
    *outofrangeerr, *noscripterr, *loadingerr, *slowscripterr, *bgsaveerr,
    *masterdownerr, *roslaveerr, *execaborterr, *noautherr, *noadminerr, *noreplicaserr,
    *busykeyerr, *oomerr, *plus, *messagebulk, *pmessagebulk, *subscribebulk,
    *unsubscribebulk, *psubscribebulk, *punsubscribebulk, *del, *rpop, *lpop,
    *lpush, *emptyscan, *minstring, *maxstring,
    *select[PROTO_SHARED_SELECT_CMDS],
    *integers[OBJ_SHARED_INTEGERS],
    *mbulkhdr[OBJ_SHARED_BULKHDR_LEN], /* "*<value>\r\n" */
    *bulkhdr[OBJ_SHARED_BULKHDR_LEN],  /* "$<value>\r\n" */
    *outofcomplexitylimit,
    *sentinel;  /* NULL pointer */
};


extern struct vr_server server;
extern struct sharedObjectsStruct shared;;
extern dictType hashDictType;
extern dictType setDictType;
extern dictType zsetDictType;

#define serverPanic(_e) _log(__FILE__, __LINE__, LOG_EMERG, 1, "assert faild: %s", #_e)
#define serverAssertWithInfo(_c,_o,_e) ((_e)?(void)0 : (_log(__FILE__, __LINE__, LOG_EMERG, 1, "assert faild: %s", #_e)))

unsigned int dictStrHash(const void *key);
unsigned int dictStrCaseHash(const void *key);
unsigned int dictSdsHash(const void *key);
unsigned int dictSdsCaseHash(const void *key);
int dictStrKeyCompare(void *privdata, const void *key1, const void *key2);
int dictStrKeyCaseCompare(void *privdata, const void *key1, const void *key2);
int dictSdsKeyCompare(void *privdata, const void *key1, const void *key2);
int dictSdsKeyCaseCompare(void *privdata, const void *key1, const void *key2);
void *dictSdsKeyDupFromStr(void *privdata, const void *key);
void dictSdsDestructor(void *privdata, void *val);
void dictObjectDestructor(void *privdata, void *val);
int dictEncObjKeyCompare(void *privdata, const void *key1, const void *key2);
unsigned int dictEncObjHash(const void *key);
unsigned int dictObjHash(const void *key);
int dictObjKeyCompare(void *privdata, const void *key1, const void *key2);
void dictListDestructor(void *privdata, void *val);

int init_server(struct instance *nci);

unsigned int getLRUClock(void);

int freeMemoryIfNeeded(vr_eventloop *vel);
void pingCommand(struct client *c);
int time_independent_strcmp(char *a, char *b);
void authCommand(struct client *c) ;
void adminCommand(struct client *c) ;

int htNeedsResize(dict *dict);

sds genVireInfoString(vr_eventloop *vel, char *section);
void infoCommand(client *c);
void echoCommand(client *c);
void timeCommand(client *c);

int adjustOpenFilesLimit(int maxclients);

#endif
