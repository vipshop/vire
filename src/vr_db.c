#include <signal.h>
#include <ctype.h>

#include <vr_core.h>

/* Db->dict, keys are sds strings, vals are Redis objects. */
dictType dbDictType = {
    dictSdsHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCompare,          /* key compare */
    dictSdsDestructor,          /* key destructor */
    dictObjectDestructor   /* val destructor */
};

/* Db->expires */
dictType keyptrDictType = {
    dictSdsHash,               /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictSdsKeyCompare,         /* key compare */
    NULL,                      /* key destructor */
    NULL                       /* val destructor */
};

/* Keylist hash table type has unencoded redis objects as keys and
 * lists as values. It's used for blocking operations (BLPOP) and to
 * map swapped keys to a list of clients waiting for this keys to be loaded. */
dictType keylistDictType = {
    dictObjHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictObjKeyCompare,          /* key compare */
    dictObjectDestructor,       /* key destructor */
    dictListDestructor          /* val destructor */
};

/* Create a new eviction pool. */
static struct evictionPoolEntry *evictionPoolAlloc(void) {
    struct evictionPoolEntry *ep;
    int j;

    ep = dalloc(sizeof(*ep)*MAXMEMORY_EVICTION_POOL_SIZE);
    for (j = 0; j < MAXMEMORY_EVICTION_POOL_SIZE; j++) {
        ep[j].idle = 0;
        ep[j].key = NULL;
    }
    return ep;
}

/*-----------------------------------------------------------------------------
 * C-level DB API
 *----------------------------------------------------------------------------*/

int redisDbInit(redisDb *db)
{
    db->dict = dictCreate(&dbDictType,NULL);
    db->expires = dictCreate(&keyptrDictType,NULL);
    db->blocking_keys = dictCreate(&keylistDictType,NULL);
    db->ready_keys = dictCreate(&setDictType,NULL);
    db->watched_keys = dictCreate(&keylistDictType,NULL);
    db->eviction_pool = evictionPoolAlloc();
    db->avg_ttl = 0;

    pthread_rwlock_init(&db->rwl, NULL);

    return VR_OK;
}

int 
redisDbDeinit(redisDb *db)
{
    pthread_rwlock_destroy(&db->rwl);
    return VR_OK;
}

int
lockDbRead(redisDb *db)
{
    pthread_rwlock_rdlock(&db->rwl);
    return VR_OK;
}

int
lockDbWrite(redisDb *db)
{
    pthread_rwlock_wrlock(&db->rwl);
    return VR_OK;
}

int
unlockDb(redisDb *db)
{
    pthread_rwlock_unlock(&db->rwl);
    return VR_OK;
}

robj *lookupKey(redisDb *db, robj *key) {
    dictEntry *de = dictFind(db->dict,key->ptr);
    if (de) {
        robj *val = dictGetVal(de);

        /* Update the access time for the ageing algorithm.
         * Don't do it if we have a saving child, as this will trigger
         * a copy on write madness. */
        if (server.rdb_child_pid == -1 && server.aof_child_pid == -1)
            //val->lru = LRU_CLOCK();
            val->lru = 0;
        return val;
    } else {
        return NULL;
    }
}

robj *lookupKeyRead(redisDb *db, robj *key) {
    if (checkIfExpired(db, key)) return NULL;
    return lookupKey(db,key);
}

robj *lookupKeyWrite(redisDb *db, robj *key, int *expired) {
    if (expired) *expired = expireIfNeeded(db,key);
    return lookupKey(db,key);
}

robj *lookupKeyReadOrReply(client *c, robj *key, robj *reply) {
    robj *o = lookupKeyRead(c->db, key);
    if (!o) addReply(c,reply);
    return o;
}

robj *lookupKeyWriteOrReply(client *c, robj *key, robj *reply, int *expired) {
    robj *o = lookupKeyWrite(c->db, key, expired);
    if (!o) addReply(c,reply);
    return o;
}

/* Add the key to the DB. It's up to the caller to increment the reference
 * counter of the value if needed.
 *
 * The program is aborted if the key already exists. 
 * Val object must be independent. */
void dbAdd(redisDb *db, robj *key, robj *val) {
    sds copy = sdsdup(key->ptr);
    int retval = dictAdd(db->dict, copy, val);
    serverAssertWithInfo(NULL,key,retval == DICT_OK);
    if (val->type == OBJ_LIST) signalListAsReady(db, key);
 }

/* Overwrite an existing key with a new value. Incrementing the reference
 * count of the new value is up to the caller.
 * This function does not modify the expire time of the existing key.
 *
 * The program is aborted if the key was not already present. 
 * Val object must be independent. */
void dbOverwrite(redisDb *db, robj *key, robj *val) {
    dictEntry *de = dictFind(db->dict,key->ptr);

    serverAssertWithInfo(NULL,key,de != NULL);
    dictReplace(db->dict, key->ptr, val);
}

/* High level Set operation. This function can be used in order to set
 * a key, whatever it was existing or not, to a new object.
 *
 * 1) Val object must be independent.
 * 2) Clients WATCHing for the destination key notified.
 * 3) The expire time of the key is reset (the key is made persistent). */
void setKey(redisDb *db, robj *key, robj *val, int *expired) {
    if (lookupKeyWrite(db,key,expired) == NULL) {
        dbAdd(db,key,val);
    } else {
        dbOverwrite(db,key,val);
    }
    
    removeExpire(db,key);
}

int dbExists(redisDb *db, robj *key) {
    return dictFind(db->dict,key->ptr) != NULL;
}

/* Return a random key, in form of a Redis object.
 * If there are no keys, NULL is returned.
 *
 * The function makes sure to return keys not already expired. */
robj *dbRandomKey(redisDb *db) {
    dictEntry *de;

    while(1) {
        sds key;
        robj *keyobj;

        lockDbRead(db);
        de = dictGetRandomKey(db->dict);
        if (de == NULL) {
            unlockDb(db);
            return NULL;
        }

        key = dictGetKey(de);
        keyobj = createStringObject(key,sdslen(key));
        if (dictFind(db->expires,key)) {
            if (checkIfExpired(db,keyobj)) {
                unlockDb(db);
                freeObject(keyobj);
                continue; /* search for another key. This expired. */
            }
        }
        unlockDb(db);
        return keyobj;
    }
}

/* Delete a key, value, and associated expiration entry if any, from the DB */
int dbDelete(redisDb *db, robj *key) {
    /* Deleting an entry from the expires dict will not free the sds of
     * the key, because it is shared with the main dictionary. */
    if (dictSize(db->expires) > 0) dictDelete(db->expires,key->ptr);
    if (dictDelete(db->dict,key->ptr) == DICT_OK) {
        return 1;
    } else {
        return 0;
    }
}

robj *dbUnshareStringValue(redisDb *db, robj *key, robj *o) {    
    ASSERT(o->type == OBJ_STRING);
    if (o->constant || o->encoding != OBJ_ENCODING_RAW) {
        robj *decoded, *new;
        decoded = getDecodedObject(o);
        new = createRawStringObject(decoded->ptr, sdslen(decoded->ptr));
        if (decoded != o) freeObject(decoded);
        dbOverwrite(db,key,new);
        return new;
    }
    return o;
}

long long emptyDb(void(callback)(void*)) {
    int j;
    long long removed = 0;
    redisDb *db;

    for (j = 0; j < server.dbnum; j++) {
        db = darray_get(&server.dbs, (uint32_t)j);
        removed += dictSize(db->dict);
        dictEmpty(db->dict,callback);
        dictEmpty(db->expires,callback);
    }
    
    return removed;
}

int selectDb(client *c, int id) {
    redisDb *db;
    
    if (id < 0 || id >= server.dblnum)
        return VR_ERROR;

    c->dictid = id;
    return VR_OK;
}

/*-----------------------------------------------------------------------------
 * Hooks for key space changes.
 *
 * Every time a key in the database is modified the function
 * signalModifiedKey() is called.
 *
 * Every time a DB is flushed the function signalFlushDb() is called.
 *----------------------------------------------------------------------------*/

void signalModifiedKey(redisDb *db, robj *key) {
    touchWatchedKey(db,key);
}

void signalFlushedDb(int dbid) {
    touchWatchedKeysOnFlush(dbid);
}

/*-----------------------------------------------------------------------------
 * Type agnostic commands operating on the key space
 *----------------------------------------------------------------------------*/

void flushdbCommand(client *c) {
    int idx;

    for (idx = 0; idx < server.dbinum; idx ++) {
        fetchInternalDbById(c, idx);
        lockDbWrite(c->db);
        c->vel->dirty += dictSize(c->db->dict);
        signalFlushedDb(c->db->id);
        dictEmpty(c->db->dict,NULL);
        dictEmpty(c->db->expires,NULL);
        unlockDb(c->db);
    }

    addReply(c,shared.ok);
}

void flushallCommand(client *c) {
    int idx;
    redisDb *db;

    for (idx = 0; idx < server.dbnum; idx ++) {
        db = darray_get(&server.dbs, (uint32_t)idx);
        lockDbWrite(db);
        dictEmpty(db->dict,NULL);
        dictEmpty(db->expires,NULL);
        unlockDb(db);
    }

    addReply(c,shared.ok);
}

void delCommand(client *c) {
    int deleted = 0, j;
    int expired = 0;

    for (j = 1; j < c->argc; j++) {
        fetchInternalDbByKey(c, c->argv[j]);
        lockDbWrite(c->db);
        expired += expireIfNeeded(c->db,c->argv[j]);
        if (dbDelete(c->db,c->argv[j])) {
            signalModifiedKey(c->db,c->argv[j]);
            notifyKeyspaceEvent(NOTIFY_GENERIC,
                "del",c->argv[j],c->db->id);
            c->vel->dirty++;
            deleted++;
        }
        unlockDb(c->db);
    }
    addReplyLongLong(c,deleted);

    if (expired > 0) {
        update_stats_add(c->vel->stats, expiredkeys, expired);
    }
}

/* EXISTS key1 key2 ... key_N.
 * Return value is the number of keys existing. */
void existsCommand(client *c) {
    long long count = 0;
    int j;

    for (j = 1; j < c->argc; j++) {
        fetchInternalDbByKey(c,c->argv[j]);
        lockDbRead(c->db);
        if (checkIfExpired(c->db,c->argv[j])) {
            unlockDb(c->db);
            continue;
        }
        if (dbExists(c->db,c->argv[j])) count++;
        unlockDb(c->db);
    }
    addReplyLongLong(c,count);

    update_stats_add(c->vel->stats, keyspace_hits, count);
    update_stats_add(c->vel->stats, keyspace_misses, c->argc-1-count);
}

void selectCommand(client *c) {
    long id;

    if (getLongFromObjectOrReply(c, c->argv[1], &id,
        "invalid DB index") != VR_OK)
        return;

    if (selectDb(c,id) == VR_ERROR) {
        addReplyError(c,"invalid DB index");
    } else {
        addReply(c,shared.ok);
    }
}

void randomkeyCommand(client *c) {
    robj *key;
    int idx, retry_count = 0;

    idx = random()%server.dbinum;

retry:
    fetchInternalDbById(c, idx);
    if ((key = dbRandomKey(c->db)) == NULL) {
        if (retry_count++ < server.dbinum) {
            if (++idx >= server.dbinum) {
                idx = 0;
            }
            goto retry;
        }

        addReply(c,shared.nullbulk);
        return;
    }

    addReplyBulk(c,key);
    freeObject(key);
}

void keysCommand(client *c) {
    dictIterator *di;
    dictEntry *de;
    sds pattern = c->argv[1]->ptr;
    int plen = sdslen(pattern), allkeys;
    unsigned long numkeys = 0;
    void *replylen;
    int idx;
    long long keys_count = 0;
    unsigned long expired = 0;
    long long max_time_complexity_limit;

    /* Check if it is reach the max-time-complexity-limit */
    for (idx = 0; idx < server.dbinum; idx ++) {
        fetchInternalDbById(c, idx);
        lockDbWrite(c->db);
        keys_count += dictSize(c->db->dict);
        unlockDb(c->db);
    }

    max_time_complexity_limit = c->vel->cc.max_time_complexity_limit;
    if (max_time_complexity_limit && 
        keys_count > max_time_complexity_limit) {
        addReply(c,shared.outofcomplexitylimit);
        return;
    }

    replylen = addDeferredMultiBulkLength(c);
    for (idx = 0; idx < server.dbinum; idx ++) {
        fetchInternalDbById(c,idx);
        lockDbWrite(c->db);
        di = dictGetSafeIterator(c->db->dict);
        allkeys = (pattern[0] == '*' && pattern[1] == '\0');
        while((de = dictNext(di)) != NULL) {
            sds key = dictGetKey(de);
            robj *keyobj;

            if (allkeys || stringmatchlen(pattern,plen,key,sdslen(key),0)) {
                keyobj = createStringObject(key,sdslen(key));
                if (expireIfNeeded(c->db,keyobj) == 0) {
                    addReplyBulk(c,keyobj);
                    numkeys++;
                } else {
                    expired ++;
                }
                freeObject(keyobj);
            }
        }
        dictReleaseIterator(di);
        unlockDb(c->db);
    }
    setDeferredMultiBulkLength(c,replylen,numkeys);
}

/* This callback is used by scanGenericCommand in order to collect elements
 * returned by the dictionary iterator into a list. */
void scanCallback(void *privdata, const dictEntry *de) {
    void **pd = (void**) privdata;
    dlist *keys = pd[0];
    robj *o = pd[1];
    robj *key, *val = NULL;

    if (o == NULL) {
        sds sdskey = dictGetKey(de);
        key = createStringObject(sdskey, sdslen(sdskey));
    } else if (o->type == OBJ_SET) {
        key = dictGetKey(de);
        key = dupStringObjectUnconstant(key);
    } else if (o->type == OBJ_HASH) {
        key = dictGetKey(de);
        key = dupStringObjectUnconstant(key);
        val = dictGetVal(de);
        val = dupStringObjectUnconstant(val);
    } else if (o->type == OBJ_ZSET) {
        key = dictGetKey(de);
        key = dupStringObjectUnconstant(key);
        val = createStringObjectFromLongDouble(*(double*)dictGetVal(de),0);
    } else {
        serverPanic("Type not handled in SCAN callback.");
    }

    dlistAddNodeTail(keys, key);
    if (val) dlistAddNodeTail(keys, val);
}

/* Try to parse a SCAN cursor stored at object 'o':
 * if the cursor is valid, store it as unsigned integer into *cursor and
 * returns VR_OK. Otherwise return VR_ERROR and send an error to the
 * client. */
int parseScanCursorOrReply(client *c, robj *o, unsigned long *cursor) {
    char *eptr;

    /* Use strtoul() because we need an *unsigned* long, so
     * getLongLongFromObject() does not cover the whole cursor space. */
    errno = 0;
    *cursor = strtoul(o->ptr, &eptr, 10);
    if (isspace(((char*)o->ptr)[0]) || eptr[0] != '\0' || errno == ERANGE)
    {
        addReplyError(c, "invalid cursor");
        return VR_ERROR;
    }
    return VR_OK;
}

/* This command implements SCAN, HSCAN and SSCAN commands.
 * If object 'o' is passed, then it must be a Hash or Set object, otherwise
 * if 'o' is NULL the command will operate on the dictionary associated with
 * the current database.
 *
 * When 'o' is not NULL the function assumes that the first argument in
 * the client arguments vector is a key so it skips it before iterating
 * in order to parse options.
 *
 * In the case of a Hash object the function returns both the field and value
 * of every element on the Hash. */
void scanGenericCommand(client *c, int scantype) {
    int i, j;
    dlist *keys = dlistCreate();
    dlistNode *node, *nextnode;
    long count = 10;
    sds pat = NULL;
    int patlen = 0, use_pattern = 0;
    unsigned long cursor;
    robj *o;
    dict *ht;

    /* Set i to the first option argument. The previous one is the cursor. */
    i = (scantype == SCAN_TYPE_KEY) ? 2 : 3; /* Skip the key argument if needed. */
    if (parseScanCursorOrReply(c,c->argv[i-1],&cursor) == VR_ERROR) return;

    /* Step 1: Parse options. */
    while (i < c->argc) {
        j = c->argc - i;
        if (!strcasecmp(c->argv[i]->ptr, "count") && j >= 2) {
            if (getLongFromObjectOrReply(c, c->argv[i+1], &count, NULL)
                != VR_OK)
            {
                goto cleanup;
            }

            if (count < 1) {
                addReply(c,shared.syntaxerr);
                goto cleanup;
            }

            i += 2;
        } else if (!strcasecmp(c->argv[i]->ptr, "match") && j >= 2) {
            pat = c->argv[i+1]->ptr;
            patlen = sdslen(pat);

            /* The pattern always matches if it is exactly "*", so it is
             * equivalent to disabling it. */
            use_pattern = !(pat[0] == '*' && patlen == 1);

            i += 2;
        } else {
            addReply(c,shared.syntaxerr);
            goto cleanup;
        }
    }

    if (scantype == SCAN_TYPE_KEY) {
        o = NULL;
        if (c->scanid == -1 || cursor == 0) c->scanid = 0;
        fetchInternalDbById(c, c->scanid);
        lockDbRead(c->db);
    } else if (scantype == SCAN_TYPE_HASH || 
        scantype == SCAN_TYPE_SET ||
        scantype == SCAN_TYPE_ZSET) {
        fetchInternalDbByKey(c, c->argv[1]);
        lockDbRead(c->db);
        if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptyscan)) == NULL) {
            unlockDb(c->db);
            update_stats_add(c->vel->stats, keyspace_misses, 1);
            return;
        }
    }

    switch (scantype) {
    case SCAN_TYPE_KEY:
        ASSERT(o == NULL);
        break;
    case SCAN_TYPE_HASH:
        if (checkType(c,o,OBJ_HASH)) {
            unlockDb(c->db);
            update_stats_add(c->vel->stats, keyspace_hits, 1);
            return;
        }
        break;
    case SCAN_TYPE_SET:
        if (checkType(c,o,OBJ_SET)) {
            unlockDb(c->db);
            update_stats_add(c->vel->stats, keyspace_hits, 1);
            return;
        }
        break;
    case SCAN_TYPE_ZSET:
        if (checkType(c,o,OBJ_ZSET)) {
            unlockDb(c->db);
            update_stats_add(c->vel->stats, keyspace_hits, 1);
            return;
        }
        break;
    }

    /* Object must be NULL (to iterate keys names), or the type of the object
     * must be Set, Sorted Set, or Hash. */
    ASSERT(o == NULL || o->type == OBJ_SET || o->type == OBJ_HASH ||
                o->type == OBJ_ZSET);

scan_retry:
    
    /* Step 2: Iterate the collection.
     *
     * Note that if the object is encoded with a ziplist, intset, or any other
     * representation that is not a hash table, we are sure that it is also
     * composed of a small number of elements. So to avoid taking state we
     * just return everything inside the object in a single call, setting the
     * cursor to zero to signal the end of the iteration. */

    /* Handle the case of a hash table. */
    ht = NULL;
    if (scantype == SCAN_TYPE_KEY) {
        ht = c->db->dict;
    } else if (o->type == OBJ_SET && o->encoding == OBJ_ENCODING_HT) {
        ht = o->ptr;
    } else if (o->type == OBJ_HASH && o->encoding == OBJ_ENCODING_HT) {
        ht = o->ptr;
        count *= 2; /* We return key / value for this type. */
    } else if (o->type == OBJ_ZSET && o->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = o->ptr;
        ht = zs->dict;
        count *= 2; /* We return key / value for this type. */
    }

    if (ht) {
        void *privdata[2];
        /* We set the max number of iterations to ten times the specified
         * COUNT, so if the hash table is in a pathological state (very
         * sparsely populated) we avoid to block too much time at the cost
         * of returning no or very few elements. */
        long maxiterations = count*10;

        /* We pass two pointers to the callback: the list to which it will
         * add new elements, and the object containing the dictionary so that
         * it is possible to fetch more data in a type-dependent way. */
        privdata[0] = keys;
        privdata[1] = o;
        do {
            cursor = dictScan(ht, cursor, scanCallback, privdata);
        } while (cursor &&
              maxiterations-- &&
              dlistLength(keys) < (unsigned long)count);
    } else if (o->type == OBJ_SET) {
        int pos = 0;
        int64_t ll;

        while(intsetGet(o->ptr,pos++,&ll))
            dlistAddNodeTail(keys,createStringObjectFromLongLong(ll));
        cursor = 0;
    } else if (o->type == OBJ_HASH || o->type == OBJ_ZSET) {
        unsigned char *p = ziplistIndex(o->ptr,0);
        unsigned char *vstr;
        unsigned int vlen;
        long long vll;

        while(p) {
            ziplistGet(p,&vstr,&vlen,&vll);
            dlistAddNodeTail(keys,
                (vstr != NULL) ? createStringObject((char*)vstr,vlen) :
                                 createStringObjectFromLongLong(vll));
            p = ziplistNext(o->ptr,p);
        }
        cursor = 0;
    } else {
        serverPanic("Not handled encoding in SCAN.");
    }

    unlockDb(c->db);
    if (scantype == SCAN_TYPE_KEY) {
        if (cursor == 0) {
            if (c->scanid < (server.dbinum - 1)) {
                c->scanid ++;
                fetchInternalDbById(c, c->scanid);
                lockDbRead(c->db);
                goto scan_retry;
            } else {
                c->scanid = -1;
            }
        }
    } else if (scantype == SCAN_TYPE_HASH || 
        scantype == SCAN_TYPE_SET ||
        scantype == SCAN_TYPE_ZSET) {
        update_stats_add(c->vel->stats, keyspace_hits, 1);
    }

    /* Step 3: Filter elements. */
    node = dlistFirst(keys);
    while (node) {
        robj *kobj = dlistNodeValue(node);
        nextnode = dlistNextNode(node);
        int filter = 0;

        /* Filter element if it does not match the pattern. */
        if (!filter && use_pattern) {
            if (sdsEncodedObject(kobj)) {
                if (!stringmatchlen(pat, patlen, kobj->ptr, sdslen(kobj->ptr), 0))
                    filter = 1;
            } else {
                char buf[LONG_STR_SIZE];
                int len;

                ASSERT(kobj->encoding == OBJ_ENCODING_INT);
                len = ll2string(buf,sizeof(buf),(long)kobj->ptr);
                if (!stringmatchlen(pat, patlen, buf, len, 0)) filter = 1;
            }
        }

        /* Filter element if it is an expired key. */
        if (!filter && o == NULL && checkIfExpired(c->db,kobj)) filter = 1;

        /* Remove the element and its associted value if needed. */
        if (filter) {
            freeObject(kobj);
            dlistDelNode(keys, node);
        }

        /* If this is a hash or a sorted set, we have a flat list of
         * key-value elements, so if this element was filtered, remove the
         * value, or skip it if it was not filtered: we only match keys. */
        if (o && (o->type == OBJ_ZSET || o->type == OBJ_HASH)) {
            node = nextnode;
            nextnode = dlistNextNode(node);
            if (filter) {
                kobj = dlistNodeValue(node);
                freeObject(kobj);
                dlistDelNode(keys, node);
            }
        }
        node = nextnode;
    }

    /* Step 4: Reply to the client. */
    addReplyMultiBulkLen(c, 2);
    addReplyBulkLongLong(c,cursor);

    addReplyMultiBulkLen(c, dlistLength(keys));
    while ((node = dlistFirst(keys)) != NULL) {
        robj *kobj = dlistNodeValue(node);
        addReplyBulk(c, kobj);
        freeObject(kobj);
        dlistDelNode(keys, node);
    }

cleanup:
    dlistSetFreeMethod(keys,freeObjectVoid);
    dlistRelease(keys);
}

/* The SCAN command completely relies on scanGenericCommand. */
void scanCommand(client *c) {
    scanGenericCommand(c,SCAN_TYPE_KEY);
}

void dbsizeCommand(client *c) {
    int idx;
    unsigned long count = 0;

    for (idx = 0; idx < server.dbinum; idx ++) {
        fetchInternalDbById(c, idx);
        lockDbRead(c->db);
        count += dictSize(c->db->dict);
        unlockDb(c->db);
    }
    
    addReplyLongLong(c,count);
}

void lastsaveCommand(client *c) {
    addReplyLongLong(c,server.lastsave);
}

void typeCommand(client *c) {
    robj *o;
    char *type;

    fetchInternalDbByKey(c, c->argv[1]);
    lockDbRead(c->db);
    o = lookupKeyRead(c->db,c->argv[1]);
    if (o == NULL) {
        type = "none";
        unlockDb(c->db);
        addReplyStatus(c,type);
        update_stats_add(c->vel->stats, keyspace_misses, 1);
        return;
    } else {
        switch(o->type) {
        case OBJ_STRING: type = "string"; break;
        case OBJ_LIST: type = "list"; break;
        case OBJ_SET: type = "set"; break;
        case OBJ_ZSET: type = "zset"; break;
        case OBJ_HASH: type = "hash"; break;
        default: type = "unknown"; break;
        }
    }

    unlockDb(c->db);
    addReplyStatus(c,type);
    update_stats_add(c->vel->stats, keyspace_hits, 1);
}

void shutdownCommand(client *c) {
    int flags = 0;

    if (c->argc > 2) {
        addReply(c,shared.syntaxerr);
        return;
    } else if (c->argc == 2) {
        if (!strcasecmp(c->argv[1]->ptr,"nosave")) {
            flags |= SHUTDOWN_NOSAVE;
        } else if (!strcasecmp(c->argv[1]->ptr,"save")) {
            flags |= SHUTDOWN_SAVE;
        } else {
            addReply(c,shared.syntaxerr);
            return;
        }
    }
    /* When SHUTDOWN is called while the server is loading a dataset in
     * memory we need to make sure no attempt is performed to save
     * the dataset on shutdown (otherwise it could overwrite the current DB
     * with half-read data).
     *
     * Also when in Sentinel mode clear the SAVE flag and force NOSAVE. */
    if (server.loading)
        flags = (flags & ~SHUTDOWN_SAVE) | SHUTDOWN_NOSAVE;
    //if (prepareForShutdown(flags) == VR_OK) exit(0);
    addReplyError(c,"Errors trying to SHUTDOWN. Check logs.");
}

void renameGenericCommand(client *c, int nx) {
    robj *o;
    long long expire;
    int samekey = 0;

    /* When source and dest key is the same, no operation is performed,
     * if the key exists, however we still return an error on unexisting key. */
    if (sdscmp(c->argv[1]->ptr,c->argv[2]->ptr) == 0) samekey = 1;

    if ((o = lookupKeyWriteOrReply(c,c->argv[1],shared.nokeyerr,NULL)) == NULL)
        return;

    if (samekey) {
        addReply(c,nx ? shared.czero : shared.ok);
        return;
    }

    incrRefCount(o);
    expire = getExpire(c->db,c->argv[1]);
    if (lookupKeyWrite(c->db,c->argv[2],NULL) != NULL) {
        if (nx) {
            decrRefCount(o);
            addReply(c,shared.czero);
            return;
        }
        /* Overwrite: delete the old key before creating the new one
         * with the same name. */
        dbDelete(c->db,c->argv[2]);
    }
    dbAdd(c->db,c->argv[2],o);
    if (expire != -1) setExpire(c->db,c->argv[2],expire);
    dbDelete(c->db,c->argv[1]);
    signalModifiedKey(c->db,c->argv[1]);
    signalModifiedKey(c->db,c->argv[2]);
    notifyKeyspaceEvent(NOTIFY_GENERIC,"rename_from",
        c->argv[1],c->db->id);
    notifyKeyspaceEvent(NOTIFY_GENERIC,"rename_to",
        c->argv[2],c->db->id);
    server.dirty++;
    addReply(c,nx ? shared.cone : shared.ok);
}

void renameCommand(client *c) {
    renameGenericCommand(c,0);
}

void renamenxCommand(client *c) {
    renameGenericCommand(c,1);
}

void moveCommand(client *c) {
    robj *o;
    redisDb *src, *dst;
    int srcid;
    long long dbid, expire;

    /* Obtain source and target DB pointers */
    src = c->db;
    srcid = c->db->id;

    if (getLongLongFromObject(c->argv[2],&dbid) == VR_ERROR ||
        dbid < INT_MIN || dbid > INT_MAX ||
        selectDb(c,dbid) == VR_ERROR)
    {
        addReply(c,shared.outofrangeerr);
        return;
    }
    dst = c->db;
    selectDb(c,srcid); /* Back to the source DB */

    /* If the user is moving using as target the same
     * DB as the source DB it is probably an error. */
    if (src == dst) {
        addReply(c,shared.sameobjecterr);
        return;
    }

    /* Check if the element exists and get a reference */
    o = lookupKeyWrite(c->db,c->argv[1],NULL);
    if (!o) {
        addReply(c,shared.czero);
        return;
    }
    expire = getExpire(c->db,c->argv[1]);

    /* Return zero if the key already exists in the target DB */
    if (lookupKeyWrite(dst,c->argv[1],NULL) != NULL) {
        addReply(c,shared.czero);
        return;
    }
    dbAdd(dst,c->argv[1],o);
    if (expire != -1) setExpire(dst,c->argv[1],expire);
    incrRefCount(o);

    /* OK! key moved, free the entry in the source DB */
    dbDelete(src,c->argv[1]);
    server.dirty++;
    addReply(c,shared.cone);
}

/*-----------------------------------------------------------------------------
 * Expires API
 *----------------------------------------------------------------------------*/

int removeExpire(redisDb *db, robj *key) {
    /* An expire may only be removed if there is a corresponding entry in the
     * main dict. Otherwise, the key will never be freed. */
    serverAssertWithInfo(NULL,key,dictFind(db->dict,key->ptr) != NULL);
    return dictDelete(db->expires,key->ptr) == DICT_OK;
}

void setExpire(redisDb *db, robj *key, long long when) {
    dictEntry *kde, *de;

    /* Reuse the sds from the main dict in the expire dict */
    kde = dictFind(db->dict,key->ptr);
    serverAssertWithInfo(NULL,key,kde != NULL);
    de = dictReplaceRaw(db->expires,dictGetKey(kde));
    dictSetSignedIntegerVal(de,when);
}

/* Return the expire time of the specified key, or -1 if no expire
 * is associated with this key (i.e. the key is non volatile) */
long long getExpire(redisDb *db, robj *key) {
    dictEntry *de;

    /* No expire? return ASAP */
    if (dictSize(db->expires) == 0 ||
       (de = dictFind(db->expires,key->ptr)) == NULL) return -1;

    /* The entry was found in the expire dict, this means it should also
     * be present in the main dict (safety check). */
    serverAssertWithInfo(NULL,key,dictFind(db->dict,key->ptr) != NULL);
    return dictGetSignedIntegerVal(de);
}

/* Propagate expires into slaves and the AOF file.
 * When a key expires in the master, a DEL operation for this key is sent
 * to all the slaves and the AOF file if enabled.
 *
 * This way the key expiry is centralized in one place, and since both
 * AOF and the master->slave link guarantee operation ordering, everything
 * will be consistent even if we allow write operations against expiring
 * keys. */
void propagateExpire(redisDb *db, robj *key) {
    robj *argv[2];

    argv[0] = shared.del;
    argv[1] = key;
    incrRefCount(argv[0]);
    incrRefCount(argv[1]);

    if (server.aof_state != AOF_OFF)
        feedAppendOnlyFile(server.delCommand,db->id,argv,2);
    replicationFeedSlaves(repl.slaves,db->id,argv,2);

    decrRefCount(argv[0]);
    decrRefCount(argv[1]);
}

/* Check if the key exists in the db and had expired */
int checkIfExpired(redisDb *db, robj *key) {
    long long when;

    when = getExpire(db,key);
    if (when > 0 && vr_msec_now() > when) {
        return 1;
    }

    return 0;
}

int expireIfNeeded(redisDb *db, robj *key) {
    long long when = getExpire(db,key);
    long long now;

    if (when < 0) return 0; /* No expire for this key */

    /* Don't expire anything while loading. It will be done later. */
    if (server.loading) return 0;

    /* If we are in the context of a Lua script, we claim that time is
     * blocked to when the Lua script started. This way a key can expire
     * only the first time it is accessed and not in the middle of the
     * script execution, making propagation to slaves / AOF consistent.
     * See issue #1525 on Github for more information. */
    now = server.lua_caller ? server.lua_time_start : vr_msec_now();

    /* If we are running in the context of a slave, return ASAP:
     * the slave key expiration is controlled by the master that will
     * send us synthesized DEL operations for expired keys.
     *
     * Still we try to return the right information to the caller,
     * that is, 0 if we think the key should be still valid, 1 if
     * we think the key is expired at this time. */
    if (repl.masterhost != NULL) return now > when;

    /* Return when this key has not expired */
    if (now <= when) return 0;

    /* Delete the key */
    //propagateExpire(db,key);
    notifyKeyspaceEvent(NOTIFY_EXPIRED,
        "expired",key,db->id);
    return dbDelete(db,key);
}

/*-----------------------------------------------------------------------------
 * Expires Commands
 *----------------------------------------------------------------------------*/

/* This is the generic command implementation for EXPIRE, PEXPIRE, EXPIREAT
 * and PEXPIREAT. Because the commad second argument may be relative or absolute
 * the "basetime" argument is used to signal what the base time is (either 0
 * for *AT variants of the command, or the current time for relative expires).
 *
 * unit is either UNIT_SECONDS or UNIT_MILLISECONDS, and is only used for
 * the argv[2] parameter. The basetime is always specified in milliseconds. */
void expireGenericCommand(client *c, long long basetime, int unit) {
    robj *key = c->argv[1], *param = c->argv[2];
    long long when; /* unix time in milliseconds when the key will expire. */
    int expired = 0;

    if (getLongLongFromObjectOrReply(c, param, &when, NULL) != VR_OK)
        return;

    if (unit == UNIT_SECONDS) when *= 1000;
    when += basetime;

    fetchInternalDbByKey(c, key);
    lockDbWrite(c->db);
    /* No key, return zero. */
    if (lookupKeyWrite(c->db,key,&expired) == NULL) {
        unlockDb(c->db);
        if (expired) update_stats_add(c->vel->stats,expiredkeys,1);
        addReply(c,shared.czero);
        return;
    }

    /* EXPIRE with negative TTL, or EXPIREAT with a timestamp into the past
     * should never be executed as a DEL when load the AOF or in the context
     * of a slave instance.
     *
     * Instead we take the other branch of the IF statement setting an expire
     * (possibly in the past) and wait for an explicit DEL from the master. */
    if (when <= vr_msec_now() && !server.loading && !repl.masterhost) {
        robj *aux;

        serverAssertWithInfo(c,key,dbDelete(c->db,key));
        c->vel->dirty++;

        /* Replicate/AOF this as an explicit DEL. */
        aux = dupStringObjectUnconstant(key);
        rewriteClientCommandVector(c,2,shared.del,aux);
        signalModifiedKey(c->db,key);
        notifyKeyspaceEvent(NOTIFY_GENERIC,"del",key,c->db->id);
        unlockDb(c->db);
        if (expired) update_stats_add(c->vel->stats,expiredkeys,1);
        addReply(c, shared.cone);
        return;
    } else {
        setExpire(c->db,key,when);
        addReply(c,shared.cone);
        signalModifiedKey(c->db,key);
        notifyKeyspaceEvent(NOTIFY_GENERIC,"expire",key,c->db->id);
        unlockDb(c->db);
        if (expired) update_stats_add(c->vel->stats,expiredkeys,1);
        c->vel->dirty++;
        return;
    }
}

void expireCommand(client *c) {
    expireGenericCommand(c,vr_msec_now(),UNIT_SECONDS);
}

void expireatCommand(client *c) {
    expireGenericCommand(c,0,UNIT_SECONDS);
}

void pexpireCommand(client *c) {
    expireGenericCommand(c,vr_msec_now(),UNIT_MILLISECONDS);
}

void pexpireatCommand(client *c) {
    expireGenericCommand(c,0,UNIT_MILLISECONDS);
}

void ttlGenericCommand(client *c, int output_ms) {
    long long expire, ttl = -1;

    fetchInternalDbByKey(c, c->argv[1]);
    lockDbRead(c->db);
    /* If the key does not exist at all, return -2 */
    if (lookupKeyRead(c->db,c->argv[1]) == NULL) {
        unlockDb(c->db);
        update_stats_add(c->vel->stats, keyspace_misses, 1);
        addReplyLongLong(c,-2);
        return;
    }
    /* The key exists. Return -1 if it has no expire, or the actual
     * TTL value otherwise. */
    expire = getExpire(c->db,c->argv[1]);
    unlockDb(c->db);
    update_stats_add(c->vel->stats, keyspace_hits, 1);
    
    if (expire != -1) {
        ttl = expire-vr_msec_now();
        if (ttl < 0) ttl = 0;
    }
    if (ttl == -1) {
        addReplyLongLong(c,-1);
    } else {
        addReplyLongLong(c,output_ms ? ttl : ((ttl+500)/1000));
    }
}

void ttlCommand(client *c) {
    ttlGenericCommand(c, 0);
}

void pttlCommand(client *c) {
    ttlGenericCommand(c, 1);
}

void persistCommand(client *c) {
    dictEntry *de;

    fetchInternalDbByKey(c, c->argv[1]);
    lockDbWrite(c->db);
    de = dictFind(c->db->dict,c->argv[1]->ptr);
    if (de == NULL) {
        addReply(c,shared.czero);
    } else {
        if (removeExpire(c->db,c->argv[1])) {
            addReply(c,shared.cone);
            c->vel->dirty++;
        } else {
            addReply(c,shared.czero);
        }
    }
    unlockDb(c->db);
}

/* -----------------------------------------------------------------------------
 * API to get key arguments from commands
 * ---------------------------------------------------------------------------*/

/* The base case is to use the keys position as given in the command table
 * (firstkey, lastkey, step). */
int *getKeysUsingCommandTable(struct redisCommand *cmd,robj **argv, int argc, int *numkeys) {
    int j, i = 0, last, *keys;
    UNUSED(argv);

    if (cmd->firstkey == 0) {
        *numkeys = 0;
        return NULL;
    }
    last = cmd->lastkey;
    if (last < 0) last = argc+last;
    keys = dalloc(sizeof(int)*((last - cmd->firstkey)+1));
    for (j = cmd->firstkey; j <= last; j += cmd->keystep) {
        ASSERT(j < argc);
        keys[i++] = j;
    }
    *numkeys = i;
    return keys;
}

/* Return all the arguments that are keys in the command passed via argc / argv.
 *
 * The command returns the positions of all the key arguments inside the array,
 * so the actual return value is an heap allocated array of integers. The
 * length of the array is returned by reference into *numkeys.
 *
 * 'cmd' must be point to the corresponding entry into the redisCommand
 * table, according to the command name in argv[0].
 *
 * This function uses the command table if a command-specific helper function
 * is not required, otherwise it calls the command-specific function. */
int *getKeysFromCommand(struct redisCommand *cmd, robj **argv, int argc, int *numkeys) {
    if (cmd->getkeys_proc) {
        return cmd->getkeys_proc(cmd,argv,argc,numkeys);
    } else {
        return getKeysUsingCommandTable(cmd,argv,argc,numkeys);
    }
}

/* Free the result of getKeysFromCommand. */
void getKeysFreeResult(int *result) {
    dfree(result);
}

/* Helper function to extract keys from following commands:
 * ZUNIONSTORE <destkey> <num-keys> <key> <key> ... <key> <options>
 * ZINTERSTORE <destkey> <num-keys> <key> <key> ... <key> <options> */
int *zunionInterGetKeys(struct redisCommand *cmd, robj **argv, int argc, int *numkeys) {
    int i, num, *keys;
    UNUSED(cmd);

    num = atoi(argv[2]->ptr);
    /* Sanity check. Don't return any key if the command is going to
     * reply with syntax error. */
    if (num > (argc-3)) {
        *numkeys = 0;
        return NULL;
    }

    /* Keys in z{union,inter}store come from two places:
     * argv[1] = storage key,
     * argv[3...n] = keys to intersect */
    keys = dalloc(sizeof(int)*(num+1));

    /* Add all key positions for argv[3...n] to keys[] */
    for (i = 0; i < num; i++) keys[i] = 3+i;

    /* Finally add the argv[1] key position (the storage key target). */
    keys[num] = 1;
    *numkeys = num+1;  /* Total keys = {union,inter} keys + storage key */
    return keys;
}

/* Helper function to extract keys from the following commands:
 * EVAL <script> <num-keys> <key> <key> ... <key> [more stuff]
 * EVALSHA <script> <num-keys> <key> <key> ... <key> [more stuff] */
int *evalGetKeys(struct redisCommand *cmd, robj **argv, int argc, int *numkeys) {
    int i, num, *keys;
    UNUSED(cmd);

    num = atoi(argv[2]->ptr);
    /* Sanity check. Don't return any key if the command is going to
     * reply with syntax error. */
    if (num > (argc-3)) {
        *numkeys = 0;
        return NULL;
    }

    keys = dalloc(sizeof(int)*num);
    *numkeys = num;

    /* Add all key positions for argv[3...n] to keys[] */
    for (i = 0; i < num; i++) keys[i] = 3+i;

    return keys;
}

/* Helper function to extract keys from the SORT command.
 *
 * SORT <sort-key> ... STORE <store-key> ...
 *
 * The first argument of SORT is always a key, however a list of options
 * follow in SQL-alike style. Here we parse just the minimum in order to
 * correctly identify keys in the "STORE" option. */
int *sortGetKeys(struct redisCommand *cmd, robj **argv, int argc, int *numkeys) {
    int i, j, num, *keys, found_store = 0;
    UNUSED(cmd);

    num = 0;
    keys = dalloc(sizeof(int)*2); /* Alloc 2 places for the worst case. */

    keys[num++] = 1; /* <sort-key> is always present. */

    /* Search for STORE option. By default we consider options to don't
     * have arguments, so if we find an unknown option name we scan the
     * next. However there are options with 1 or 2 arguments, so we
     * provide a list here in order to skip the right number of args. */
    struct {
        char *name;
        int skip;
    } skiplist[] = {
        {"limit", 2},
        {"get", 1},
        {"by", 1},
        {NULL, 0} /* End of elements. */
    };

    for (i = 2; i < argc; i++) {
        for (j = 0; skiplist[j].name != NULL; j++) {
            if (!strcasecmp(argv[i]->ptr,skiplist[j].name)) {
                i += skiplist[j].skip;
                break;
            } else if (!strcasecmp(argv[i]->ptr,"store") && i+1 < argc) {
                /* Note: we don't increment "num" here and continue the loop
                 * to be sure to process the *last* "STORE" option if multiple
                 * ones are provided. This is same behavior as SORT. */
                found_store = 1;
                keys[num] = i+1; /* <store-key> */
                break;
            }
        }
    }
    *numkeys = num + found_store;
    return keys;
}

int *migrateGetKeys(struct redisCommand *cmd, robj **argv, int argc, int *numkeys) {
    int i, num, first, *keys;
    UNUSED(cmd);

    /* Assume the obvious form. */
    first = 3;
    num = 1;

    /* But check for the extended one with the KEYS option. */
    if (argc > 6) {
        for (i = 6; i < argc; i++) {
            if (!strcasecmp(argv[i]->ptr,"keys") &&
                sdslen(argv[3]->ptr) == 0)
            {
                first = i+1;
                num = argc-first;
                break;
            }
        }
    }

    keys = dalloc(sizeof(int)*num);
    for (i = 0; i < num; i++) keys[i] = first+i;
    *numkeys = num;
    return keys;
}

int fetchInternalDbByKey(client *c, robj *key) {
    c->db = darray_get(&server.dbs, (hash_crc16(key->ptr,stringObjectLen(key))&0x3FFF)%server.dbinum+c->dictid*server.dbinum);
    return VR_OK;
}

int fetchInternalDbById(client *c, int idx) {
    c->db = darray_get(&server.dbs, idx+c->dictid*server.dbinum);
    return VR_OK;
}

/* If the percentage of used slots in the HT reaches HASHTABLE_MIN_FILL
 * we resize the hash table to save memory */
void tryResizeHashTablesForDb(int dbid) {
    redisDb *db;

    db = darray_get(&server.dbs, dbid);
    lockDbWrite(db);
    if (htNeedsResize(db->dict))
        dictResize(db->dict);
    if (htNeedsResize(db->expires))
        dictResize(db->expires);
    unlockDb(db);
}

/* Our hash table implementation performs rehashing incrementally while
 * we write/read from the hash table. Still if the server is idle, the hash
 * table will use two tables for a long time. So we try to use 1 millisecond
 * of CPU time at every call of this function to perform some rehahsing.
 *
 * The function returns 1 if some rehashing was performed, otherwise 0
 * is returned. */
int incrementallyRehashForDb(int dbid) {
    redisDb *db;

    db = darray_get(&server.dbs, dbid);
    lockDbWrite(db);
    
    /* Keys dictionary */
    if (dictIsRehashing(db->dict)) {
        dictRehashMilliseconds(db->dict,1);
        unlockDb(db);
        return 1; /* already used our millisecond for this loop... */
    }
    /* Expires */
    if (dictIsRehashing(db->expires)) {
        dictRehashMilliseconds(db->expires,1);
        unlockDb(db);
        return 1; /* already used our millisecond for this loop... */
    }

    unlockDb(db);
    return 0;
}

/* Try to expire a few timed out keys. The algorithm used is adaptive and
 * will use few CPU cycles if there are few expiring keys, otherwise
 * it will get more aggressive to avoid that too much memory is used by
 * keys that can be removed from the keyspace.
 *
 * No more than CRON_DBS_PER_CALL databases are tested at every
 * iteration.
 *
 * This kind of call is used when Redis detects that timelimit_exit is
 * true, so there is more work to do, and we do it more incrementally from
 * the beforeSleep() function of the event loop.
 *
 * Expire cycle type:
 *
 * If type is ACTIVE_EXPIRE_CYCLE_FAST the function will try to run a
 * "fast" expire cycle that takes no longer than EXPIRE_FAST_CYCLE_DURATION
 * microseconds, and is not repeated again before the same amount of time.
 *
 * If type is ACTIVE_EXPIRE_CYCLE_SLOW, that normal expire cycle is
 * executed, where the time limit is a percentage of the REDIS_HZ period
 * as specified by the REDIS_EXPIRELOOKUPS_TIME_PERC define. */

void activeExpireCycle(vr_backend *backend, int type) {
    int j, iteration = 0;
    int dbs_per_call = CRON_DBS_PER_CALL;
    long long start = vr_usec_now(), timelimit;
    long long expired_total = 0;

    if (type == ACTIVE_EXPIRE_CYCLE_FAST) {
        /* Don't start a fast cycle if the previous cycle did not exited
         * for time limt. Also don't repeat a fast cycle for the same period
         * as the fast cycle total duration itself. */
        if (!backend->timelimit_exit) return;
        if (start < backend->last_fast_cycle + ACTIVE_EXPIRE_CYCLE_FAST_DURATION*2) return;
        backend->last_fast_cycle = start;
    }

    /* We usually should test CRON_DBS_PER_CALL per iteration, with
     * two exceptions:
     *
     * 1) Don't test more DBs than we have.
     * 2) If last time we hit the time limit, we want to scan all DBs
     * in this iteration, as there is work to do in some DB and we don't want
     * expired keys to use memory for too much time. */
    if (dbs_per_call > server.dbnum || backend->timelimit_exit)
        dbs_per_call = server.dbnum;

    /* We can use at max ACTIVE_EXPIRE_CYCLE_SLOW_TIME_PERC percentage of CPU time
     * per iteration. Since this function gets called with a frequency of
     * server.hz times per second, the following is the max amount of
     * microseconds we can spend in this function. */
    timelimit = 1000000*ACTIVE_EXPIRE_CYCLE_SLOW_TIME_PERC/server.hz/100;
    backend->timelimit_exit = 0;
    if (timelimit <= 0) timelimit = 1;

    if (type == ACTIVE_EXPIRE_CYCLE_FAST)
        timelimit = ACTIVE_EXPIRE_CYCLE_FAST_DURATION; /* in microseconds. */

    for (j = 0; j < dbs_per_call; j++) {
        int expired;
        redisDb *db = darray_get(&server.dbs, backend->current_db%server.dbnum);

        /* Increment the DB now so we are sure if we run out of time
         * in the current DB we'll restart from the next. This allows to
         * distribute the time evenly across DBs. */
        backend->current_db++;
        
        lockDbWrite(db);
        /* Continue to expire if at the end of the cycle more than 25%
         * of the keys were expired. */
        do {
            unsigned long num, slots;
            long long now, ttl_sum;
            int ttl_samples;
            /* If there is nothing to expire try next DB ASAP. */
            if ((num = dictSize(db->expires)) == 0) {
                db->avg_ttl = 0;
                break;
            }
            slots = dictSlots(db->expires);
            now = vr_msec_now();

            /* When there are less than 1% filled slots getting random
             * keys is expensive, so stop here waiting for better times...
             * The dictionary will be resized asap. */
            if (num && slots > DICT_HT_INITIAL_SIZE &&
                (num*100/slots < 1)) break;

            /* The main collection cycle. Sample random keys among keys
             * with an expire set, checking for expired ones. */
            expired = 0;
            ttl_sum = 0;
            ttl_samples = 0;

            if (num > ACTIVE_EXPIRE_CYCLE_LOOKUPS_PER_LOOP)
                num = ACTIVE_EXPIRE_CYCLE_LOOKUPS_PER_LOOP;

            while (num--) {
                dictEntry *de;
                long long ttl;

                if ((de = dictGetRandomKey(db->expires)) == NULL) break;
                ttl = dictGetSignedIntegerVal(de)-now;
                if (activeExpireCycleTryExpire(db,de,now)) expired++;
                if (ttl > 0) {
                    /* We want the average TTL of keys yet not expired. */
                    ttl_sum += ttl;
                    ttl_samples++;
                }
            }

            expired_total += expired;

            /* Update the average TTL stats for this database. */
            if (ttl_samples) {
                long long avg_ttl = ttl_sum/ttl_samples;

                /* Do a simple running average with a few samples.
                 * We just use the current estimate with a weight of 2%
                 * and the previous estimate with a weight of 98%. */
                if (db->avg_ttl == 0) db->avg_ttl = avg_ttl;
                db->avg_ttl = (db->avg_ttl/50)*49 + (avg_ttl/50);
            }

            /* We can't block forever here even if there are many keys to
             * expire. So after a given amount of milliseconds return to the
             * caller waiting for the other active expire cycle. */
            iteration++;
            if ((iteration & 0xf) == 0) { /* check once every 16 iterations. */
                long long elapsed = vr_usec_now()-start;

                //latencyAddSampleIfNeeded("expire-cycle",elapsed/1000);
                if (elapsed > timelimit) backend->timelimit_exit = 1;
            }
            if (backend->timelimit_exit) {
                unlockDb(db);

                if (expired_total > 0) {
                    update_stats_add(backend->vel.stats, expiredkeys, expired_total);
                }
                return;
            }
            /* We don't repeat the cycle if there are less than 25% of keys
             * found expired in the current DB. */
        } while (expired > ACTIVE_EXPIRE_CYCLE_LOOKUPS_PER_LOOP/4);
        unlockDb(db);
    }

    if (expired_total > 0) {
        update_stats_add(backend->vel.stats, expiredkeys, expired_total);
    }
}

int activeExpireCycleTryExpire(redisDb *db, dictEntry *de, long long now) {
    long long t = dictGetSignedIntegerVal(de);
    if (now > t) {
        sds key = dictGetKey(de);
        robj *keyobj = createStringObject(key,sdslen(key));
        dbDelete(db,keyobj);
        freeObject(keyobj);
        return 1;
    } else {
        return 0;
    }
}

/* This function handles 'background' operations we are required to do
 * incrementally in Redis databases, such as active key expiring, resizing,
 * rehashing. */
void databasesCron(vr_backend *backend) {
    /* Expire keys by random sampling. Not required for slaves
     * as master will synthesize DELs for us. */
    if (repl.masterhost == NULL)
        activeExpireCycle(backend, ACTIVE_EXPIRE_CYCLE_SLOW);

    /* Perform hash tables rehashing if needed, but only if there are no
     * other processes saving the DB on disk. Otherwise rehashing is bad
     * as will cause a lot of copy-on-write of memory pages. */
    if (server.rdb_child_pid == -1 && server.aof_child_pid == -1) {
        int dbs_per_call = CRON_DBS_PER_CALL;
        int j;

        /* Don't test more DBs than we have. */
        if (dbs_per_call > server.dbnum) dbs_per_call = server.dbnum;

        /* Resize */
        for (j = 0; j < dbs_per_call; j++) {
            tryResizeHashTablesForDb(backend->resize_db%server.dbnum);
            backend->resize_db++;
        }

        /* Rehash */
        if (server.activerehashing) {
            for (j = 0; j < dbs_per_call; j++) {
                int work_done = incrementallyRehashForDb(backend->rehash_db%server.dbnum);
                backend->rehash_db++;
                if (work_done) {
                    /* If the function did some work, stop here, we'll do
                     * more at the next cron loop. */
                    break;
                }
            }
        }
    }
}

