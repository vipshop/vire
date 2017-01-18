#ifndef _VR_DB_H_
#define _VR_DB_H_

struct bigkeyDumpHelper;

#define DB_FLAGS_DUMPING (1<<0)
#define DB_FLAGS_DUMP_FIRST_STEP (1<<1)

/* To improve the quality of the LRU approximation we take a set of keys
 * that are good candidate for eviction across freeMemoryIfNeeded() calls.
 *
 * Entries inside the eviciton pool are taken ordered by idle time, putting
 * greater idle times to the right (ascending order).
 *
 * Empty entries have the key pointer set to NULL. */
#define MAXMEMORY_EVICTION_POOL_SIZE 16
struct evictionPoolEntry {
    unsigned long long idle;    /* Object idle time. */
    sds key;                    /* Key name. */
};

/* Vire database representation. There are multiple databases identified
 * by integers from 0 (the default database) up to the max configured
 * database. The database number is the 'id' field in the structure. */
typedef struct redisDb {
    dict *dict;                 /* The keyspace for this DB */
    dict *expires;              /* Timeout of keys with a timeout set */
    dict *blocking_keys;        /* Keys with clients waiting for data (BLPOP) */
    dict *ready_keys;           /* Blocked keys that received a PUSH */
    dict *watched_keys;         /* WATCHED keys for MULTI/EXEC CAS */
    struct evictionPoolEntry *eviction_pool;    /* Eviction pool of keys */
    int id;                     /* Database ID */
    long long avg_ttl;          /* Average TTL, just for stats */

    sds rdb_filename;
    sds rdb_tmpfilename;
    long long version;
    int flags;
    void *rdb_cached;
    void *cursor_key;
    long long dirty;            /* Changes to DB from the last save. */
    long long dirty_before_bgsave;  /* Used to restore dirty on failed BGSAVE */
    
    sds aof_filename;           /* Name of the AOF file for this db. */
    int aof_enabled;            /* If aof file write enabled. */
    int aof_fd;                 /* File descriptor of currently AOF file. */
    sds aof_buf;                /* AOF buffer, written before entering the event loop. */
    off_t aof_current_size;     /* AOF current size. */
    long long aof_flush_postponed_start;    /* Time in second of postponed AOF flush. */
    unsigned long aof_delayed_fsync;    /* Delayed AOF fsync() counter. */
    long long aof_last_fsync;    /* Time in second of last fsync(). */
    int aof_last_write_status;   /* VR_OK or VR_ERROR */
    int aof_last_write_errno;    /* Valid if aof_last_write_status is ERR */
    int aof_last_write_error_log;   /* Time in second of the last write error message in log for this aof file. */

    struct bigkeyDumpHelper *bkdhelper; /* Used to dump the big keys to rdb file. */

    pthread_rwlock_t rwl;        /* Read Write lock for this db. */
    
#ifdef HAVE_DEBUG_LOG
	long long lock_start_time;
    long long times_rdbSaveRio;
	long long times_rdbSaveMicroseconds;
#endif
} redisDb;


#ifdef HAVE_DEBUG_LOG

#define lockDbRead(db) do {                                         \
    pthread_rwlock_rdlock(&db->rwl);                                \
    db->lock_start_time = dusec_now();                              \
} while (0)

#define lockDbWrite(db) do {                                        \
    pthread_rwlock_wrlock(&db->rwl);                                \
    db->lock_start_time = dusec_now();                              \
} while (0)

#define unlockDb(db) do {                                           \
    long long lock_used_time = dusec_now() - db->lock_start_time;   \
    pthread_rwlock_unlock(&db->rwl);                                \
    if (lock_used_time > 2000) {                                    \
        log_debug(LOG_NOTICE, "Locked time : %lld", lock_used_time);\
    }                                                               \
} while (0)

#else

#define lockDbRead(db) do {                                         \
    pthread_rwlock_rdlock(&db->rwl);                                \
} while (0)

#define lockDbWrite(db) do {                                        \
    pthread_rwlock_wrlock(&db->rwl);                                \
} while (0)

#define unlockDb(db) do {                                           \
    pthread_rwlock_unlock(&db->rwl);                                \
} while (0)

#endif


extern dictType dbDictType;
extern dictType keyptrDictType;
extern dictType keylistDictType;

int redisDbInit(redisDb *db, int idx);
int redisDbDeinit(redisDb *db);

robj *lookupKey(redisDb *db, robj *key);
robj *lookupKeyRead(redisDb *db, robj *key);
robj *lookupKeyWrite(redisDb *db, robj *key, int *expired);
robj *lookupKeyReadOrReply(struct client *c, robj *key, robj *reply);
robj *lookupKeyWriteOrReply(struct client *c, robj *key, robj *reply, int *expired);
void dbAdd(redisDb *db, robj *key, robj *val);
void dbOverwrite(redisDb *db, robj *key, robj *val);
void setKey(redisDb *db, robj *key, robj *val, int *expired);
int dbExists(redisDb *db, robj *key);
robj *dbRandomKey(redisDb *db);
int dbDelete(redisDb *db, robj *key);
robj *dbUnshareStringValue(redisDb *db, robj *key, robj *o);
long long emptyDb(void(callback)(void*));
int selectDb(struct client *c, int id);
void signalModifiedKey(redisDb *db, robj *key);
void signalFlushedDb(int dbid);
void flushidbCommand(struct client *c);
void flushdbCommand(struct client *c);
void flushallCommand(struct client *c);
void delCommand(struct client *c);
void existsCommand(struct client *c);
void selectCommand(struct client *c);
void randomkeyCommand(struct client *c);
void keysCommand(struct client *c);
void scanCallback(void *privdata, const dictEntry *de);
int parseScanCursorOrReply(struct client *c, robj *o, unsigned long *cursor);
void scanGenericCommand(struct client *c, int scantype);
void scanCommand(struct client *c);
void dbsizeCommand(struct client *c);
void lastsaveCommand(struct client *c);
void typeCommand(struct client *c);
void shutdownCommand(struct client *c);
void renameGenericCommand(struct client *c, int nx);
void renameCommand(struct client *c);
void renamenxCommand(struct client *c);
void moveCommand(struct client *c);
int removeExpire(redisDb *db, robj *key);
void setExpire(redisDb *db, robj *key, long long when);
long long getExpire(redisDb *db, robj *key);
void propagateExpire(redisDb *db, robj *key);
int checkIfExpired(redisDb *db, robj *key);
int expireIfNeeded(redisDb *db, robj *key);
void expireGenericCommand(struct client *c, long long basetime, int unit);
void expireCommand(struct client *c);
void expireatCommand(struct client *c);
void pexpireCommand(struct client *c);
void pexpireatCommand(struct client *c);
void ttlGenericCommand(struct client *c, int output_ms);
void ttlCommand(struct client *c);
void pttlCommand(struct client *c);
void persistCommand(struct client *c);
int *getKeysUsingCommandTable(struct redisCommand *cmd,robj **argv, int argc, int *numkeys);
int *getKeysFromCommand(struct redisCommand *cmd, robj **argv, int argc, int *numkeys);
void getKeysFreeResult(int *result);
int *zunionInterGetKeys(struct redisCommand *cmd, robj **argv, int argc, int *numkeys);
int *evalGetKeys(struct redisCommand *cmd, robj **argv, int argc, int *numkeys);
int *sortGetKeys(struct redisCommand *cmd, robj **argv, int argc, int *numkeys);
int *migrateGetKeys(struct redisCommand *cmd, robj **argv, int argc, int *numkeys);

redisDb *fetchInternalDbByKey(int logic_dbid, robj *key);
redisDb *fetchInternalDbById(int logic_dbid, int idx);

int fetchInternalDbByKeyForClient(struct client *c, robj *key);
int fetchInternalDbByIdForClient(struct client *c, int idx);

void tryResizeHashTablesForDb(redisDb *db);
int incrementallyRehashForDb(redisDb *db);
void activeExpireCycle(vr_backend *backend, int type);
int activeExpireCycleTryExpire(redisDb *db, dictEntry *de, long long now);
void databasesCron(vr_backend *backend);

#endif
