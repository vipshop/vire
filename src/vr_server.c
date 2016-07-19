#include <sys/utsname.h>

#include <vr_core.h>

/* Global vars */
struct vr_server server; /* server global state */

/* Our shared "common" objects */
struct sharedObjectsStruct shared;

unsigned int
dictStrHash(const void *key) {
    return dictGenHashFunction((unsigned char*)key, strlen((char*)key));
}

unsigned int
dictStrCaseHash(const void *key) {
    return dictGenCaseHashFunction((unsigned char*)key, strlen((char*)key));
}

unsigned int
dictSdsHash(const void *key) {
    return dictGenHashFunction((unsigned char*)key, sdslen((char*)key));
}

unsigned int
dictSdsCaseHash(const void *key) {
    return dictGenCaseHashFunction((unsigned char*)key, sdslen((char*)key));
}

int
dictStrKeyCompare(void *privdata, const void *key1,
        const void *key2)
{
    int l1,l2;
    DICT_NOTUSED(privdata);

    l1 = strlen((char*)key1);
    l2 = strlen((char*)key2);
    if (l1 != l2) return 0;
    return memcmp(key1, key2, l1) == 0;
}

/* A case insensitive version used for the config option lookup table and other
 * places where case insensitive non binary-safe comparison is needed. */
int
dictStrKeyCaseCompare(void *privdata, const void *key1,
        const void *key2)
{
    DICT_NOTUSED(privdata);

    return strcasecmp(key1, key2) == 0;
}

int
dictSdsKeyCompare(void *privdata, const void *key1,
        const void *key2)
{
    int l1,l2;
    DICT_NOTUSED(privdata);

    l1 = sdslen((sds)key1);
    l2 = sdslen((sds)key2);
    if (l1 != l2) return 0;
    return memcmp(key1, key2, l1) == 0;
}

/* A case insensitive version used for the command lookup table and other
 * places where case insensitive non binary-safe comparison is needed. */
int
dictSdsKeyCaseCompare(void *privdata, const void *key1,
        const void *key2)
{
    DICT_NOTUSED(privdata);

    return strcasecmp(key1, key2) == 0;
}

void *
dictSdsKeyDupFromStr(void *privdata, const void *key)
{
    DICT_NOTUSED(privdata);

    return sdsnew(key); /* key is c string */
}

void
dictSdsDestructor(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);

    sdsfree(val);
}

void
dictObjectDestructor(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);

    if (val == NULL) return; /* Values of swapped out keys as set to NULL */
    freeObject(val);
}

int
dictEncObjKeyCompare(void *privdata, const void *key1,
        const void *key2)
{
    robj *o1 = (robj*) key1, *o2 = (robj*) key2;
    robj *o1_new, *o2_new;
    int cmp;

    if (o1->encoding == OBJ_ENCODING_INT &&
        o2->encoding == OBJ_ENCODING_INT)
            return o1->ptr == o2->ptr;

    o1_new = getDecodedObject(o1);
    o2_new = getDecodedObject(o2);
    cmp = dictSdsKeyCompare(privdata,o1_new->ptr,o2_new->ptr);
    if (o1_new != o1)  freeObject(o1_new);
    if (o2_new != o2)  freeObject(o2_new);
    return cmp;
}

unsigned int
dictEncObjHash(const void *key) {
    robj *o = (robj*) key;

    if (sdsEncodedObject(o)) {
        return dictGenHashFunction(o->ptr, sdslen((sds)o->ptr));
    } else {
        if (o->encoding == OBJ_ENCODING_INT) {
            char buf[32];
            int len;

            len = ll2string(buf,32,(long)o->ptr);
            return dictGenHashFunction((unsigned char*)buf, len);
        } else {
            unsigned int hash;
            robj *o_new;

            o_new = getDecodedObject(o);
            hash = dictGenHashFunction(o_new->ptr, sdslen((sds)o_new->ptr));
            if (o_new!= o) freeObject(o_new);
            return hash;
        }
    }
}

unsigned int
dictObjHash(const void *key) {
    const robj *o = key;
    return dictGenHashFunction(o->ptr, sdslen((sds)o->ptr));
}

int
dictObjKeyCompare(void *privdata, const void *key1,
        const void *key2)
{
    const robj *o1 = key1, *o2 = key2;
    return dictSdsKeyCompare(privdata,o1->ptr,o2->ptr);
}

void
dictListDestructor(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);
    listRelease((list*)val);
}

/* Hash type hash table (note that small hashes are represented with ziplists) */
dictType hashDictType = {
    dictEncObjHash,             /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictEncObjKeyCompare,       /* key compare */
    dictObjectDestructor,  /* key destructor */
    dictObjectDestructor   /* val destructor */
};

/* Sets type hash table */
dictType setDictType = {
    dictEncObjHash,            /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictEncObjKeyCompare,      /* key compare */
    dictObjectDestructor, /* key destructor */
    NULL                       /* val destructor */
};

/* Sorted sets hash (note: a skiplist is used in addition to the hash table) */
dictType zsetDictType = {
    dictEncObjHash,            /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictEncObjKeyCompare,      /* key compare */
    dictObjectDestructor,      /* key destructor */
    NULL                       /* val destructor */
};

/* =========================== Server initialization ======================== */

static void createSharedObjects(void) {
    int j;
    robj **obj;

    shared.crlf = createObject(OBJ_STRING,sdsnew("\r\n"));
    shared.ok = createObject(OBJ_STRING,sdsnew("+OK\r\n"));
    shared.err = createObject(OBJ_STRING,sdsnew("-ERR\r\n"));
    shared.emptybulk = createObject(OBJ_STRING,sdsnew("$0\r\n\r\n"));
    shared.czero = createObject(OBJ_STRING,sdsnew(":0\r\n"));
    shared.cone = createObject(OBJ_STRING,sdsnew(":1\r\n"));
    shared.cnegone = createObject(OBJ_STRING,sdsnew(":-1\r\n"));
    shared.nullbulk = createObject(OBJ_STRING,sdsnew("$-1\r\n"));
    shared.nullmultibulk = createObject(OBJ_STRING,sdsnew("*-1\r\n"));
    shared.emptymultibulk = createObject(OBJ_STRING,sdsnew("*0\r\n"));
    shared.pong = createObject(OBJ_STRING,sdsnew("+PONG\r\n"));
    shared.queued = createObject(OBJ_STRING,sdsnew("+QUEUED\r\n"));
    shared.emptyscan = createObject(OBJ_STRING,sdsnew("*2\r\n$1\r\n0\r\n*0\r\n"));
    shared.wrongtypeerr = createObject(OBJ_STRING,sdsnew(
        "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"));
    shared.nokeyerr = createObject(OBJ_STRING,sdsnew(
        "-ERR no such key\r\n"));
    shared.syntaxerr = createObject(OBJ_STRING,sdsnew(
        "-ERR syntax error\r\n"));
    shared.sameobjecterr = createObject(OBJ_STRING,sdsnew(
        "-ERR source and destination objects are the same\r\n"));
    shared.outofrangeerr = createObject(OBJ_STRING,sdsnew(
        "-ERR index out of range\r\n"));
    shared.noscripterr = createObject(OBJ_STRING,sdsnew(
        "-NOSCRIPT No matching script. Please use EVAL.\r\n"));
    shared.loadingerr = createObject(OBJ_STRING,sdsnew(
        "-LOADING Redis is loading the dataset in memory\r\n"));
    shared.slowscripterr = createObject(OBJ_STRING,sdsnew(
        "-BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.\r\n"));
    shared.masterdownerr = createObject(OBJ_STRING,sdsnew(
        "-MASTERDOWN Link with MASTER is down and slave-serve-stale-data is set to 'no'.\r\n"));
    shared.bgsaveerr = createObject(OBJ_STRING,sdsnew(
        "-MISCONF Redis is configured to save RDB snapshots, but is currently not able to persist on disk. Commands that may modify the data set are disabled. Please check Redis logs for details about the error.\r\n"));
    shared.roslaveerr = createObject(OBJ_STRING,sdsnew(
        "-READONLY You can't write against a read only slave.\r\n"));
    shared.noautherr = createObject(OBJ_STRING,sdsnew(
        "-NOAUTH Authentication required.\r\n"));
    shared.oomerr = createObject(OBJ_STRING,sdsnew(
        "-OOM command not allowed when used memory > 'maxmemory'.\r\n"));
    shared.execaborterr = createObject(OBJ_STRING,sdsnew(
        "-EXECABORT Transaction discarded because of previous errors.\r\n"));
    shared.noreplicaserr = createObject(OBJ_STRING,sdsnew(
        "-NOREPLICAS Not enough good slaves to write.\r\n"));
    shared.busykeyerr = createObject(OBJ_STRING,sdsnew(
        "-BUSYKEY Target key name already exists.\r\n"));
    shared.space = createObject(OBJ_STRING,sdsnew(" "));
    shared.colon = createObject(OBJ_STRING,sdsnew(":"));
    shared.plus = createObject(OBJ_STRING,sdsnew("+"));

    for (j = 0; j < PROTO_SHARED_SELECT_CMDS; j++) {
        char dictid_str[64];
        int dictid_len;

        dictid_len = ll2string(dictid_str,sizeof(dictid_str),j);
        shared.select[j] = createObject(OBJ_STRING,
            sdscatprintf(sdsempty(),
                "*2\r\n$6\r\nSELECT\r\n$%d\r\n%s\r\n",
                dictid_len, dictid_str));
    }
    shared.messagebulk = createStringObject("$7\r\nmessage\r\n",13);
    shared.pmessagebulk = createStringObject("$8\r\npmessage\r\n",14);
    shared.subscribebulk = createStringObject("$9\r\nsubscribe\r\n",15);
    shared.unsubscribebulk = createStringObject("$11\r\nunsubscribe\r\n",18);
    shared.psubscribebulk = createStringObject("$10\r\npsubscribe\r\n",17);
    shared.punsubscribebulk = createStringObject("$12\r\npunsubscribe\r\n",19);
    shared.del = createStringObject("DEL",3);
    shared.rpop = createStringObject("RPOP",4);
    shared.lpop = createStringObject("LPOP",4);
    shared.lpush = createStringObject("LPUSH",5);
    for (j = 0; j < OBJ_SHARED_INTEGERS; j++) {
        shared.integers[j] = createObject(OBJ_STRING,(void*)(long)j);
        shared.integers[j]->encoding = OBJ_ENCODING_INT;
    }
    for (j = 0; j < OBJ_SHARED_BULKHDR_LEN; j++) {
        shared.mbulkhdr[j] = createObject(OBJ_STRING,
            sdscatprintf(sdsempty(),"*%d\r\n",j));
        shared.bulkhdr[j] = createObject(OBJ_STRING,
            sdscatprintf(sdsempty(),"$%d\r\n",j));
    }
    /* The following two shared objects, minstring and maxstrings, are not
     * actually used for their value but as a special object meaning
     * respectively the minimum possible string and the maximum possible
     * string in string comparisons for the ZRANGEBYLEX command. */
    shared.minstring = createStringObject("minstring",9);
    shared.maxstring = createStringObject("maxstring",9);

    shared.outofcomplexitylimit = createObject(OBJ_STRING,sdsnew(
        "-ERR Out of max time complexity limit.\r\n"));

    /* Set this objects to constant */
    for (obj = &shared; *obj != NULL; obj ++) {
        (*obj)->constant = 1;
    }
}

rstatus_t
init_server(struct instance *nci)
{
    rstatus_t status;
    uint32_t i;
    redisDb *db;

    conf = conf_create(nci->conf_filename);

    server.pid = getpid();
    server.configfile = getAbsolutePath(nci->conf_filename);
    server.hz = 10;
    server.dblnum = cserver->databases;
    server.dbinum = cserver->internal_dbs_per_databases;
    server.dbnum = server.dblnum*server.dbinum;
    array_init(&server.dbs, server.dbnum, sizeof(redisDb));
    server.pidfile = nci->pid_filename;
    server.executable = NULL;
    server.activerehashing = CONFIG_DEFAULT_ACTIVE_REHASHING;
    get_random_hex_chars(server.runid, CONFIG_RUN_ID_SIZE);
    server.arch_bits = (sizeof(long) == 8) ? 64 : 32;
    server.requirepass = NULL;

    server.starttime = time(NULL);
    
    server.client_max_querybuf_len = PROTO_MAX_QUERYBUF_LEN;

    server.commands = dictCreate(&commandTableDictType,NULL);
    populateCommandTable();
    server.delCommand = lookupCommandByCString("del");
    server.multiCommand = lookupCommandByCString("multi");
    server.lpushCommand = lookupCommandByCString("lpush");
    server.lpopCommand = lookupCommandByCString("lpop");
    server.rpopCommand = lookupCommandByCString("rpop");
    server.sremCommand = lookupCommandByCString("srem");
    server.execCommand = lookupCommandByCString("exec");

    for (i = 0; i < server.dbnum; i ++) {
        db = array_push(&server.dbs);
        redisDbInit(db);
    }

    server.clients = listCreate();
    
    server.monitors = listCreate();

    server.loading = 0;

    server.lua_timedout = 0;

    server.aof_state = AOF_OFF;

    server.stop_writes_on_bgsave_err = 0;

    server.ready_keys = listCreate();

    server.system_memory_size = vr_alloc_get_memory_size();

    server.rdb_child_pid = -1;
    server.aof_child_pid = -1;

    server.hash_max_ziplist_entries = OBJ_HASH_MAX_ZIPLIST_ENTRIES;
    server.hash_max_ziplist_value = OBJ_HASH_MAX_ZIPLIST_VALUE;
    server.list_max_ziplist_size = OBJ_LIST_MAX_ZIPLIST_SIZE;
    server.list_compress_depth = OBJ_LIST_COMPRESS_DEPTH;
    server.set_max_intset_entries = OBJ_SET_MAX_INTSET_ENTRIES;
    server.zset_max_ziplist_entries = OBJ_ZSET_MAX_ZIPLIST_ENTRIES;
    server.zset_max_ziplist_value = OBJ_ZSET_MAX_ZIPLIST_VALUE;
    server.hll_sparse_max_bytes = CONFIG_DEFAULT_HLL_SPARSE_MAX_BYTES;

    server.notify_keyspace_events = 0;

    slowlogInit();
    vr_replication_init();
    
    createSharedObjects();

    server.port = cserver->port;
    
    /* Init worker first */
    status = workers_init(nci->thread_num);
    if (status != VR_OK) {
        log_error("init worker threads failed");
        return VR_ERROR;
    }

    /* Init master after worker init */
    status = master_init(conf);
    if (status != VR_OK) {
        log_error("init master thread failed");
        return VR_ERROR;
    }

    log_debug(LOG_NOTICE, "memory alloc lock type: %s", malloc_lock_type());
    log_debug(LOG_NOTICE, "malloc lib: %s", VR_MALLOC_LIB);

    log_debug(LOG_NOTICE, "stats lock type: %s", STATS_LOCK_TYPE);

    return VR_OK;
}

unsigned int getLRUClock(void) {
    return (vr_msec_now()/LRU_CLOCK_RESOLUTION) & LRU_CLOCK_MAX;
}

/* This is an helper function for freeMemoryIfNeeded(), it is used in order
 * to populate the evictionPool with a few entries every time we want to
 * expire a key. Keys with idle time smaller than one of the current
 * keys are added. Keys are always added if there are free entries.
 *
 * We insert keys on place in ascending order, so keys with the smaller
 * idle time are on the left, and keys with the higher idle time on the
 * right. */

#define EVICTION_SAMPLES_ARRAY_SIZE 16
void evictionPoolPopulate(dict *sampledict, dict *keydict, 
    struct evictionPoolEntry *pool, int maxmemory_samples) {
    int j, k, count;
    dictEntry *_samples[EVICTION_SAMPLES_ARRAY_SIZE];
    dictEntry **samples;

    /* Try to use a static buffer: this function is a big hit...
     * Note: it was actually measured that this helps. */
    if (maxmemory_samples <= EVICTION_SAMPLES_ARRAY_SIZE) {
        samples = _samples;
    } else {
        samples = vr_alloc(sizeof(samples[0])*maxmemory_samples);
    }

    count = dictGetSomeKeys(sampledict,samples,maxmemory_samples);
    for (j = 0; j < count; j++) {
        unsigned long long idle;
        sds key;
        robj *o;
        dictEntry *de;

        de = samples[j];
        key = dictGetKey(de);
        /* If the dictionary we are sampling from is not the main
         * dictionary (but the expires one) we need to lookup the key
         * again in the key dictionary to obtain the value object. */
        if (sampledict != keydict) de = dictFind(keydict, key);
        o = dictGetVal(de);
        idle = estimateObjectIdleTime(o);

        /* Insert the element inside the pool.
         * First, find the first empty bucket or the first populated
         * bucket that has an idle time smaller than our idle time. */
        k = 0;
        while (k < MAXMEMORY_EVICTION_POOL_SIZE &&
               pool[k].key &&
               pool[k].idle < idle) k++;
        if (k == 0 && pool[MAXMEMORY_EVICTION_POOL_SIZE-1].key != NULL) {
            /* Can't insert if the element is < the worst element we have
             * and there are no empty buckets. */
            continue;
        } else if (k < MAXMEMORY_EVICTION_POOL_SIZE && pool[k].key == NULL) {
            /* Inserting into empty position. No setup needed before insert. */
        } else {
            /* Inserting in the middle. Now k points to the first element
             * greater than the element to insert.  */
            if (pool[MAXMEMORY_EVICTION_POOL_SIZE-1].key == NULL) {
                /* Free space on the right? Insert at k shifting
                 * all the elements from k to end to the right. */
                memmove(pool+k+1,pool+k,
                    sizeof(pool[0])*(MAXMEMORY_EVICTION_POOL_SIZE-k-1));
            } else {
                /* No free space on right? Insert at k-1 */
                k--;
                /* Shift all elements on the left of k (included) to the
                 * left, so we discard the element with smaller idle time. */
                sdsfree(pool[0].key);
                memmove(pool,pool+1,sizeof(pool[0])*k);
            }
        }
        pool[k].key = sdsdup(key);
        pool[k].idle = idle;
    }
    if (samples != _samples) vr_free(samples);
}

int freeMemoryIfNeeded(vr_eventloop *vel) {
    size_t mem_used, mem_tofree, mem_freed;
    mstime_t latency, eviction_latency;
    int keys_freed = 0;
    long long maxmemory;
    int maxmemory_policy, maxmemory_samples;
    int ret;

    maxmemory = vel->cc.maxmemory;
    if (vr_alloc_used_memory() <= maxmemory)
        return VR_OK;

    conf_server_get(CONFIG_SOPN_MAXMEMORYP, &maxmemory_policy);
    if (maxmemory_policy == MAXMEMORY_NO_EVICTION)
        return VR_ERROR; /* We need to free memory, but policy forbids. */

    conf_server_get(CONFIG_SOPN_MAXMEMORYS, &maxmemory_samples);
    while (1) {
        int j, k;

        for (j = 0; j < server.dbnum; j++) {
            long bestval = 0; /* just to prevent warning */
            sds bestkey = NULL;
            dictEntry *de;
            redisDb *db = array_get(&server.dbs, j);
            dict *dict;

            lockDbWrite(db);
            if (maxmemory_policy == MAXMEMORY_ALLKEYS_LRU ||
                maxmemory_policy == MAXMEMORY_ALLKEYS_RANDOM)
            {
                dict = db->dict;
            } else {
                dict = db->expires;
            }
            if (dictSize(dict) == 0) {
                unlockDb(db);
                continue;
            }

            /* volatile-random and allkeys-random policy */
            if (maxmemory_policy == MAXMEMORY_ALLKEYS_RANDOM ||
                maxmemory_policy == MAXMEMORY_VOLATILE_RANDOM)
            {
                de = dictGetRandomKey(dict);
                bestkey = dictGetKey(de);
            }

            /* volatile-lru and allkeys-lru policy */
            else if (maxmemory_policy == MAXMEMORY_ALLKEYS_LRU ||
                maxmemory_policy == MAXMEMORY_VOLATILE_LRU)
            {
                struct evictionPoolEntry *pool = db->eviction_pool;

                while(bestkey == NULL) {
                    evictionPoolPopulate(dict, db->dict, db->eviction_pool, maxmemory_samples);
                    /* Go backward from best to worst element to evict. */
                    for (k = MAXMEMORY_EVICTION_POOL_SIZE-1; k >= 0; k--) {
                        if (pool[k].key == NULL) continue;
                        de = dictFind(dict,pool[k].key);

                        /* Remove the entry from the pool. */
                        sdsfree(pool[k].key);
                        /* Shift all elements on its right to left. */
                        memmove(pool+k,pool+k+1,
                            sizeof(pool[0])*(MAXMEMORY_EVICTION_POOL_SIZE-k-1));
                        /* Clear the element on the right which is empty
                         * since we shifted one position to the left.  */
                        pool[MAXMEMORY_EVICTION_POOL_SIZE-1].key = NULL;
                        pool[MAXMEMORY_EVICTION_POOL_SIZE-1].idle = 0;

                        /* If the key exists, is our pick. Otherwise it is
                         * a ghost and we need to try the next element. */
                        if (de) {
                            bestkey = dictGetKey(de);
                            break;
                        } else {
                            /* Ghost... */
                            continue;
                        }
                    }
                }
            }

            /* volatile-ttl */
            else if (maxmemory_policy == MAXMEMORY_VOLATILE_TTL) {
                for (k = 0; k < maxmemory_samples; k++) {
                    sds thiskey;
                    long thisval;

                    de = dictGetRandomKey(dict);
                    thiskey = dictGetKey(de);
                    thisval = (long) dictGetVal(de);

                    /* Expire sooner (minor expire unix timestamp) is better
                     * candidate for deletion */
                    if (bestkey == NULL || thisval < bestval) {
                        bestkey = thiskey;
                        bestval = thisval;
                    }
                }
            }

            /* Finally remove the selected key. */
            if (bestkey) {
                robj *keyobj = createStringObject(bestkey,sdslen(bestkey));
                dbDelete(db,keyobj);
                freeObject(keyobj);
                keys_freed++;
            }
            
            unlockDb(db);

            conf_server_get(CONFIG_SOPN_MAXMEMORY, &maxmemory);
            if (vr_alloc_used_memory() <= maxmemory) {
                goto stop;
            }
        }
        
        if (!keys_freed) {
            return VR_ERROR; /* nothing to free... */
        }

        update_stats_add(vel->stats, evictedkeys, keys_freed);
        keys_freed = 0;
    }

stop:
    update_stats_add(vel->stats, evictedkeys, keys_freed);
    return VR_OK;
}

/* The PING command. It works in a different way if the client is in
 * in Pub/Sub mode. */
void pingCommand(client *c) {
    /* The command takes zero or one arguments. */
    if (c->argc > 2) {
        addReplyErrorFormat(c,"wrong number of arguments for '%s' command",
            c->cmd->name);
        return;
    }

    if (c->flags & CLIENT_PUBSUB) {
        addReply(c,shared.mbulkhdr[2]);
        addReplyBulkCBuffer(c,"pong",4);
        if (c->argc == 1)
            addReplyBulkCBuffer(c,"",0);
        else
            addReplyBulk(c,c->argv[1]);
    } else {
        if (c->argc == 1)
            addReply(c,shared.pong);
        else
            addReplyBulk(c,c->argv[1]);
    }
}

/* Return zero if strings are the same, non-zero if they are not.
 * The comparison is performed in a way that prevents an attacker to obtain
 * information about the nature of the strings just monitoring the execution
 * time of the function.
 *
 * Note that limiting the comparison length to strings up to 512 bytes we
 * can avoid leaking any information about the password length and any
 * possible branch misprediction related leak.
 */
int time_independent_strcmp(char *a, char *b) {
    char bufa[CONFIG_AUTHPASS_MAX_LEN], bufb[CONFIG_AUTHPASS_MAX_LEN];
    /* The above two strlen perform len(a) + len(b) operations where either
     * a or b are fixed (our password) length, and the difference is only
     * relative to the length of the user provided string, so no information
     * leak is possible in the following two lines of code. */
    unsigned int alen = strlen(a);
    unsigned int blen = strlen(b);
    unsigned int j;
    int diff = 0;

    /* We can't compare strings longer than our static buffers.
     * Note that this will never pass the first test in practical circumstances
     * so there is no info leak. */
    if (alen > sizeof(bufa) || blen > sizeof(bufb)) return 1;

    memset(bufa,0,sizeof(bufa));        /* Constant time. */
    memset(bufb,0,sizeof(bufb));        /* Constant time. */
    /* Again the time of the following two copies is proportional to
     * len(a) + len(b) so no info is leaked. */
    memcpy(bufa,a,alen);
    memcpy(bufb,b,blen);

    /* Always compare all the chars in the two buffers without
     * conditional expressions. */
    for (j = 0; j < sizeof(bufa); j++) {
        diff |= (bufa[j] ^ bufb[j]);
    }
    /* Length must be equal as well. */
    diff |= alen ^ blen;
    return diff; /* If zero strings are the same. */
}

void authCommand(client *c) {
    if (!server.requirepass) {
        addReplyError(c,"Client sent AUTH, but no password is set");
    } else if (!time_independent_strcmp(c->argv[1]->ptr, server.requirepass)) {
      c->authenticated = 1;
      addReply(c,shared.ok);
    } else {
      c->authenticated = 0;
      addReplyError(c,"invalid password");
    }
}

int htNeedsResize(dict *dict) {
    long long size, used;

    size = dictSlots(dict);
    used = dictSize(dict);
    return (size && used && size > DICT_HT_INITIAL_SIZE &&
            (used*100/size < HASHTABLE_MIN_FILL));
}

struct keys_statistics {
    long long keys_all;
    long long vkeys_all;
    long long avg_ttl_all;
    int nexist;
};

/* Create the string returned by the INFO command. This is decoupled
 * by the INFO command itself as we need to report the same information
 * on memory corruption problems. */
sds genVireInfoString(vr_eventloop *vel, char *section) {
    sds info = sdsempty();
    time_t uptime = time(NULL)-server.starttime;
    int j, k, numcommands;
    struct rusage self_ru;
    unsigned long lol, bib;
    int allsections = 0, defsections = 0;
    int sections = 0;
    struct array *kss = NULL;  /* type: keys_statistics */

    if (section == NULL) section = "default";
    allsections = strcasecmp(section,"all") == 0;
    defsections = strcasecmp(section,"default") == 0;

    getrusage(RUSAGE_SELF, &self_ru);

    /* Server */
    if (allsections || defsections || !strcasecmp(section,"server")) {
        static int call_uname = 1;
        static struct utsname name;
        char *mode;

        mode = "standalone";

        if (sections++) info = sdscat(info,"\r\n");

        if (call_uname) {
            /* Uname can be slow and is always the same output. Cache it. */
            uname(&name);
            call_uname = 0;
        }

        info = sdscatprintf(info,
            "# Server\r\n"
            "vire_version:%s\r\n"
            "vire_mode:%s\r\n"
            "os:%s %s %s\r\n"
            "arch_bits:%d\r\n"
            "multiplexing_api:%s\r\n"
            "gcc_version:%d.%d.%d\r\n"
            "process_id:%ld\r\n"
            "run_id:%s\r\n"
            "tcp_port:%d\r\n"
            "uptime_in_seconds:%jd\r\n"
            "uptime_in_days:%jd\r\n"
            "hz:%d\r\n"
            "executable:%s\r\n"
            "config_file:%s\r\n"
            "databases:%d\r\n"
            "internal_databases:%d\r\n",
            VR_VERSION_STRING,
            mode,
            name.sysname, name.release, name.machine,
            server.arch_bits,
            aeGetApiName(),
#ifdef __GNUC__
            __GNUC__,__GNUC_MINOR__,__GNUC_PATCHLEVEL__,
#else
            0,0,0,
#endif
            (long) getpid(),
            server.runid,
            server.port,
            (intmax_t)uptime,
            (intmax_t)(uptime/(3600*24)),
            server.hz,
            server.executable ? server.executable : "",
            server.configfile ? server.configfile : "",
            server.dblnum,
            server.dbinum);
    }

    /* Clients */
    if (allsections || defsections || !strcasecmp(section,"clients")) {
        if (sections++) info = sdscat(info,"\r\n");
        info = sdscatprintf(info,
            "# Clients\r\n"
            "connected_clients:%d\r\n",
            current_clients());
    }

    /* Memory */
    if (allsections || defsections || !strcasecmp(section,"memory")) {
        uint32_t idx;
        vr_worker *worker;
        char hmem[64];
        char peak_hmem[64];
        char total_system_hmem[64];
        char used_memory_lua_hmem[64];
        char used_memory_rss_hmem[64];
        char maxmemory_hmem[64];
        size_t vr_used_memory = vr_alloc_used_memory();
        size_t total_system_mem = server.system_memory_size;
        const char *evict_policy;
        size_t peak_memory = 0, peak_memory_for_one_worker;
        long long maxmemory;
        int maxmemory_policy;

        /* Peak memory is updated from time to time by workerCron() so it
         * may happen that the instantaneous value is slightly bigger than
         * the peak value. This may confuse users, so we update the peak
         * if found smaller than the current memory usage. */
        for (idx = 0; idx < array_n(&workers); idx ++) {
            worker = array_get(&workers, idx);
            update_stats_get(worker->vel.stats, peak_memory, 
                &peak_memory_for_one_worker);
            if (peak_memory < peak_memory_for_one_worker)
                peak_memory = peak_memory_for_one_worker;
        }
        if (vr_used_memory > peak_memory) {
            peak_memory = vr_used_memory;
            update_stats_set(vel->stats, peak_memory, vr_used_memory);
        }

        conf_server_get(CONFIG_SOPN_MAXMEMORY,&maxmemory);
        conf_server_get(CONFIG_SOPN_MAXMEMORYP,&maxmemory_policy);
        evict_policy = get_evictpolicy_strings(maxmemory_policy);
    
        bytesToHuman(hmem,vr_used_memory);
        bytesToHuman(peak_hmem,peak_memory);
        bytesToHuman(total_system_hmem,total_system_mem);
        bytesToHuman(used_memory_rss_hmem,vel->resident_set_size);
        bytesToHuman(maxmemory_hmem,maxmemory);

        if (sections++) info = sdscat(info,"\r\n");
        info = sdscatprintf(info,
            "# Memory\r\n"
            "used_memory:%zu\r\n"
            "used_memory_human:%s\r\n"
            "used_memory_rss:%zu\r\n"
            "used_memory_rss_human:%s\r\n"
            "used_memory_peak:%zu\r\n"
            "used_memory_peak_human:%s\r\n"
            "total_system_memory:%lu\r\n"
            "total_system_memory_human:%s\r\n"
            "maxmemory:%lld\r\n"
            "maxmemory_human:%s\r\n"
            "maxmemory_policy:%s\r\n"
            "mem_fragmentation_ratio:%.2f\r\n"
            "mem_allocator:%s\r\n",
            vr_used_memory,
            hmem,
            vel->resident_set_size,
            used_memory_rss_hmem,
            peak_memory,
            peak_hmem,
            (unsigned long)total_system_mem,
            total_system_hmem,
            maxmemory,
            maxmemory_hmem,
            evict_policy,
            (float)vel->resident_set_size/vr_used_memory,
            VR_MALLOC_LIB
            );
    }

    /* Stats */
    if (allsections || defsections || !strcasecmp(section,"stats")) {
        uint32_t idx;
        vr_worker *worker;
        vr_stats *stats;
        long long stat_numconnections=0, stat_numcommands=0;
        long long stat_net_input_bytes=0, stat_net_output_bytes=0;
        long long stat_rejected_conn=0;
        long long stat_expiredkeys=0;
        long long stat_evictedkeys=0;
        long long stat_keyspace_hits=0, stat_keyspace_misses=0;
        long long stat_numcommands_ops=0;
        float stat_net_input_bytes_ops=0, stat_net_output_bytes_ops=0;

        for (idx = 0; idx < array_n(&workers); idx ++) {
            long long stats_value;
            worker = array_get(&workers, idx);
            stats = worker->vel.stats;

            update_stats_get(stats, numcommands, &stats_value);
            stat_numcommands += stats_value;
            update_stats_get(stats, numconnections, &stats_value);
            stat_numconnections += stats_value;
            update_stats_get(stats, expiredkeys, &stats_value);
            stat_expiredkeys += stats_value;
            update_stats_get(stats, evictedkeys, &stats_value);
            stat_evictedkeys += stats_value;
            update_stats_get(stats, net_input_bytes, &stats_value);
            stat_net_input_bytes += stats_value;
            update_stats_get(stats, net_output_bytes, &stats_value);
            stat_net_output_bytes += stats_value;
            update_stats_get(stats, keyspace_hits, &stats_value);
            stat_keyspace_hits += stats_value;
            update_stats_get(stats, keyspace_misses, &stats_value);
            stat_keyspace_misses += stats_value;
            
            stat_numcommands_ops += getInstantaneousMetric(stats, STATS_METRIC_COMMAND);
            stat_net_input_bytes_ops += (float)getInstantaneousMetric(stats, STATS_METRIC_NET_INPUT)/1024;
            stat_net_output_bytes_ops += (float)getInstantaneousMetric(stats, STATS_METRIC_NET_OUTPUT)/1024;
        }
        update_stats_get(master.vel.stats, rejected_conn, &stat_rejected_conn);
        
        if (sections++) info = sdscat(info,"\r\n");
        info = sdscatprintf(info,
            "# Stats\r\n"
            "total_connections_received:%lld\r\n"
            "total_commands_processed:%lld\r\n"
            "instantaneous_ops_per_sec:%lld\r\n"
            "total_net_input_bytes:%lld\r\n"
            "total_net_output_bytes:%lld\r\n"
            "instantaneous_input_kbps:%.2f\r\n"
            "instantaneous_output_kbps:%.2f\r\n"
            "rejected_connections:%lld\r\n"
            "expired_keys:%lld\r\n"
            "evicted_keys:%lld\r\n"
            "keyspace_hits:%lld\r\n"
            "keyspace_misses:%lld\r\n",
            stat_numconnections,
            stat_numcommands,
            stat_numcommands_ops,
            stat_net_input_bytes,
            stat_net_output_bytes,
            stat_net_input_bytes_ops,
            stat_net_output_bytes_ops,
            stat_rejected_conn,
            stat_expiredkeys,
            stat_evictedkeys,
            stat_keyspace_hits,
            stat_keyspace_misses);
    }

    /* CPU */
    if (allsections || defsections || !strcasecmp(section,"cpu")) {
        if (sections++) info = sdscat(info,"\r\n");
        info = sdscatprintf(info,
        "# CPU\r\n"
        "used_cpu_sys:%.2f\r\n"
        "used_cpu_user:%.2f\r\n",
        (float)self_ru.ru_stime.tv_sec+(float)self_ru.ru_stime.tv_usec/1000000,
        (float)self_ru.ru_utime.tv_sec+(float)self_ru.ru_utime.tv_usec/1000000);
    }
    
    /* Internal */
    if (allsections || !strcasecmp(section,"internal")) {
        redisDb *db;
        struct keys_statistics *ks;
        long long keys, vkeys, avg_ttl;
        
        kss = array_create(server.dblnum, sizeof(struct keys_statistics));
        
        if (sections++) info = sdscat(info,"\r\n");
        info = sdscatprintf(info, "# Internal\r\n");
        for (j = 0; j < server.dblnum; j++) {
            ks = array_push(kss);
            ks->keys_all = ks->vkeys_all = ks->avg_ttl_all = 0;
            ks->nexist = 0;
            for (k = 0; k < server.dbinum; k ++) {
                db = array_get(&server.dbs, (uint32_t)(j*server.dbinum+k));
                lockDbRead(db);
                keys = dictSize(db->dict);
                vkeys = dictSize(db->expires);
                avg_ttl = db->avg_ttl;
                unlockDb(db);
                if (keys || vkeys) {
                    info = sdscatprintf(info,
                        "db%d-%d:keys=%lld,expires=%lld,avg_ttl=%lld\r\n",
                        j, k, keys, vkeys, db->avg_ttl);
                }
                ks->keys_all += keys;
                ks->vkeys_all += vkeys;
                ks->avg_ttl_all += avg_ttl;
                if (avg_ttl > 0) ks->nexist ++;
            }
        }
    }

    /* Key space */
    if (allsections || defsections || !strcasecmp(section,"keyspace")) {
        redisDb *db;
        struct keys_statistics *ks;
        long long keys_all, vkeys_all, avg_ttl_all;
        int nexist;
        
        if (sections++) info = sdscat(info,"\r\n");
        info = sdscatprintf(info, "# Keyspace\r\n");
        if (kss == NULL) {
            for (j = 0; j < server.dblnum; j++) {
                keys_all = vkeys_all = avg_ttl_all = 0;
                nexist = 0;
                for (k = 0; k < server.dbinum; k ++) {
                    db = array_get(&server.dbs, (uint32_t)(j*server.dbinum+k));
                    lockDbRead(db);
                    keys_all += dictSize(db->dict);
                    vkeys_all += dictSize(db->expires);
                    avg_ttl_all += db->avg_ttl;
                    if (db->avg_ttl > 0) nexist ++;
                    unlockDb(db);
                }
                if (keys_all || vkeys_all) {
                    info = sdscatprintf(info,
                        "db%d:keys=%lld,expires=%lld,avg_ttl=%lld\r\n",
                        j, keys_all, vkeys_all, nexist>0?(avg_ttl_all/nexist):0);
                }
            }
        } else {
            for (j = 0; j < server.dblnum; j ++) {
                ks = array_get(kss, j);
                if (ks->keys_all || ks->vkeys_all) {
                    info = sdscatprintf(info,
                        "db%d:keys=%lld,expires=%lld,avg_ttl=%lld\r\n",
                        j, ks->keys_all, ks->vkeys_all, 
                        ks->nexist>0?(ks->avg_ttl_all/ks->nexist):0);
                }
            }
        }
    }

    if (kss != NULL) {
        kss->nelem = 0;
        array_destroy(kss);
        kss = NULL;
    }

    return info;
}

void infoCommand(client *c) {
    char *section = c->argc == 2 ? c->argv[1]->ptr : "default";

    if (c->argc > 2) {
        addReply(c,shared.syntaxerr);
        return;
    }
    addReplyBulkSds(c, genVireInfoString(c->vel, section));
}

void echoCommand(client *c) {
    addReplyBulk(c,c->argv[1]);
}

void timeCommand(client *c) {
    struct timeval tv;

    /* gettimeofday() can only fail if &tv is a bad address so we
     * don't check for errors. */
    gettimeofday(&tv,NULL);
    addReplyMultiBulkLen(c,2);
    addReplyBulkLongLong(c,tv.tv_sec);
    addReplyBulkLongLong(c,tv.tv_usec);
}

/* This function will try to raise the max number of open files accordingly to
 * the configured max number of clients. It also reserves a number of file
 * descriptors (CONFIG_MIN_RESERVED_FDS) for extra operations of
 * persistence, listening sockets, log files and so forth.
 *
 * If it will not be possible to set the limit accordingly to the configured
 * max number of clients, the function will do the reverse setting
 * server.maxclients to the value that we can actually handle. */
int adjustOpenFilesLimit(int maxclients) {
    rlim_t maxfiles, finallimit = 0;
    rlim_t oldlimit;
    int finalmaxclients;
    struct rlimit limit;
    int threads;

    conf_server_get(CONFIG_SOPN_THREADS,&threads);
    maxfiles = maxclients+threads*2+CONFIG_MIN_RESERVED_FDS;
    if (getrlimit(RLIMIT_NOFILE,&limit) == -1) {
        log_warn("Unable to obtain the current NOFILE limit (%s), assuming 1024 and setting the max clients configuration accordingly.",
            strerror(errno));
        oldlimit = 1024;
        finallimit = oldlimit;
    } else {
        oldlimit = limit.rlim_cur;

        /* Set the max number of files if the current limit is not enough
         * for our needs. */
        if (oldlimit < maxfiles) {
            rlim_t bestlimit;
            int setrlimit_error = 0;

            /* Try to set the file limit to match 'maxfiles' or at least
             * to the higher value supported less than maxfiles. */
            bestlimit = maxfiles;
            while(bestlimit > oldlimit) {
                rlim_t decr_step = 16;

                limit.rlim_cur = bestlimit;
                limit.rlim_max = bestlimit;
                if (setrlimit(RLIMIT_NOFILE,&limit) != -1) break;
                setrlimit_error = errno;

                /* We failed to set file limit to 'bestlimit'. Try with a
                 * smaller limit decrementing by a few FDs per iteration. */
                if (bestlimit < decr_step) break;
                bestlimit -= decr_step;
            }

            /* Assume that the limit we get initially is still valid if
             * our last try was even lower. */
            if (bestlimit < oldlimit) bestlimit = oldlimit;

            finallimit = bestlimit;
            if (bestlimit < maxfiles) {
                log_warn("You requested maxclients of %d "
                    "requiring at least %llu max file descriptors.",
                    maxclients,
                    (unsigned long long) maxfiles);
                log_warn("Server can't set maximum open files "
                    "to %llu because of OS error: %s.",
                    (unsigned long long) maxfiles, strerror(setrlimit_error));
                log_warn("Current maximum open files is %llu. ",
                    (unsigned long long) bestlimit);
            } else {
                log_warn("Increased maximum number of open files "
                    "to %llu (it was originally set to %llu).",
                    (unsigned long long) maxfiles,
                    (unsigned long long) oldlimit);
            }
        } else {
            finallimit = maxfiles;
        }
    }

    finalmaxclients = finallimit-threads*2-CONFIG_MIN_RESERVED_FDS;
    if (finalmaxclients < 1) {
        log_warn("Your current 'ulimit -n' "
            "of %llu is not enough for the server to start. "
            "Please increase your open file limit to at least "
            "%llu. Exiting.",
            (unsigned long long) oldlimit,
            (unsigned long long) maxfiles);
        return -1;
    }
    
    if (finallimit < maxfiles) {
        conf_value *cv = conf_value_create(CONF_VALUE_TYPE_STRING);
        cv->value = sdsfromlonglong((long long)finalmaxclients);
        conf_server_set(CONFIG_SOPN_MAXCLIENTS,cv);
        conf_value_destroy(cv);
        log_warn("maxclients has been reduced to %d to compensate for "
            "low ulimit. "
            "If you need higher maxclients increase 'ulimit -n'.",
            finalmaxclients);
    }
    
    return (int)finallimit;
}
