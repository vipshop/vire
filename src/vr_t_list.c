#include <vr_core.h>

/*-----------------------------------------------------------------------------
 * List API
 *----------------------------------------------------------------------------*/

/* The function pushes an element to the specified list object 'subject',
 * at head or tail position as specified by 'where'.
 *
 * There is no need for the caller to increment the refcount of 'value' as
 * the function takes care of it if needed. */
void listTypePush(robj *subject, robj *value, int where) {
    if (subject->encoding == OBJ_ENCODING_QUICKLIST) {
        robj *value_new;
        int pos = (where == LIST_HEAD) ? QUICKLIST_HEAD : QUICKLIST_TAIL;
        value_new = getDecodedObject(value);
        size_t len = sdslen(value_new->ptr);
        quicklistPush(subject->ptr, value_new->ptr, len, pos);
        if (value_new != value) freeObject(value_new);
    } else {
        serverPanic("Unknown list encoding");
    }
}

void *listPopSaver(unsigned char *data, unsigned int sz) {
    return createStringObject((char*)data,sz);
}

robj *listTypePop(robj *subject, int where) {
    long long vlong;
    robj *value = NULL;

    int ql_where = where == LIST_HEAD ? QUICKLIST_HEAD : QUICKLIST_TAIL;
    if (subject->encoding == OBJ_ENCODING_QUICKLIST) {
        if (quicklistPopCustom(subject->ptr, ql_where, (unsigned char **)&value,
                               NULL, &vlong, listPopSaver)) {
            if (!value)
                value = createStringObjectFromLongLong(vlong);
        }
    } else {
        serverPanic("Unknown list encoding");
    }
    return value;
}

unsigned long listTypeLength(robj *subject) {
    if (subject->encoding == OBJ_ENCODING_QUICKLIST) {
        return quicklistCount(subject->ptr);
    } else {
        serverPanic("Unknown list encoding");
    }
}

/* Initialize an iterator at the specified index. */
listTypeIterator *listTypeInitIterator(robj *subject, long index,
                                       unsigned char direction) {
    listTypeIterator *li = dalloc(sizeof(listTypeIterator));
    li->subject = subject;
    li->encoding = subject->encoding;
    li->direction = direction;
    li->iter = NULL;
    /* LIST_HEAD means start at TAIL and move *towards* head.
     * LIST_TAIL means start at HEAD and move *towards tail. */
    int iter_direction =
        direction == LIST_HEAD ? AL_START_TAIL : AL_START_HEAD;
    if (li->encoding == OBJ_ENCODING_QUICKLIST) {
        li->iter = quicklistGetIteratorAtIdx(li->subject->ptr,
                                             iter_direction, index);
    } else {
        serverPanic("Unknown list encoding");
    }
    return li;
}

/* Clean up the iterator. */
void listTypeReleaseIterator(listTypeIterator *li) {
    dfree(li->iter);
    dfree(li);
}

/* Stores pointer to current the entry in the provided entry structure
 * and advances the position of the iterator. Returns 1 when the current
 * entry is in fact an entry, 0 otherwise. */
int listTypeNext(listTypeIterator *li, listTypeEntry *entry) {
    /* Protect from converting when iterating */
    ASSERT(li->subject->encoding == li->encoding);

    entry->li = li;
    if (li->encoding == OBJ_ENCODING_QUICKLIST) {
        return quicklistNext(li->iter, &entry->entry);
    } else {
        serverPanic("Unknown list encoding");
    }
    return 0;
}

/* Return entry or NULL at the current position of the iterator. */
robj *listTypeGet(listTypeEntry *entry) {
    robj *value = NULL;
    if (entry->li->encoding == OBJ_ENCODING_QUICKLIST) {
        if (entry->entry.value) {
            value = createStringObject((char *)entry->entry.value,
                                       entry->entry.sz);
        } else {
            value = createStringObjectFromLongLong(entry->entry.longval);
        }
    } else {
        serverPanic("Unknown list encoding");
    }
    return value;
}

void listTypeInsert(listTypeEntry *entry, robj *value, int where) {
    if (entry->li->encoding == OBJ_ENCODING_QUICKLIST) {
        value = getDecodedObject(value);
        sds str = value->ptr;
        size_t len = sdslen(str);
        if (where == LIST_TAIL) {
            quicklistInsertAfter((quicklist *)entry->entry.quicklist,
                                 &entry->entry, str, len);
        } else if (where == LIST_HEAD) {
            quicklistInsertBefore((quicklist *)entry->entry.quicklist,
                                  &entry->entry, str, len);
        }
        decrRefCount(value);
    } else {
        serverPanic("Unknown list encoding");
    }
}

/* Compare the given object with the entry at the current position. */
int listTypeEqual(listTypeEntry *entry, robj *o) {
    if (entry->li->encoding == OBJ_ENCODING_QUICKLIST) {
        serverAssertWithInfo(NULL,o,sdsEncodedObject(o));
        return quicklistCompare(entry->entry.zi,o->ptr,sdslen(o->ptr));
    } else {
        serverPanic("Unknown list encoding");
    }
}

/* Delete the element pointed to. */
void listTypeDelete(listTypeIterator *iter, listTypeEntry *entry) {
    if (entry->li->encoding == OBJ_ENCODING_QUICKLIST) {
        quicklistDelEntry(iter->iter, &entry->entry);
    } else {
        serverPanic("Unknown list encoding");
    }
}

/* Create a quicklist from a single ziplist */
void listTypeConvert(robj *subject, int enc) {
    serverAssertWithInfo(NULL,subject,subject->type==OBJ_LIST);
    serverAssertWithInfo(NULL,subject,subject->encoding==OBJ_ENCODING_ZIPLIST);

    if (enc == OBJ_ENCODING_QUICKLIST) {
        size_t zlen = server.list_max_ziplist_size;
        int depth = server.list_compress_depth;
        subject->ptr = quicklistCreateFromZiplist(zlen, depth, subject->ptr);
        subject->encoding = OBJ_ENCODING_QUICKLIST;
    } else {
        serverPanic("Unsupported list conversion");
    }
}

/*-----------------------------------------------------------------------------
 * List Commands
 *----------------------------------------------------------------------------*/

void pushGenericCommand(client *c, int where) {
    int j, waiting = 0, pushed = 0;
    robj *lobj;
    int expired = 0;

    fetchInternalDbByKey(c, c->argv[1]);
    lockDbWrite(c->db);
    lobj = lookupKeyWrite(c->db,c->argv[1],&expired);
    if (lobj && lobj->type != OBJ_LIST) {
        unlockDb(c->db);
        if (expired) update_stats_add(c->vel->stats,expiredkeys,1);
        addReply(c,shared.wrongtypeerr);
        return;
    }

    for (j = 2; j < c->argc; j++) {
        c->argv[j] = tryObjectEncoding(c->argv[j]);
        if (!lobj) {
            lobj = createQuicklistObject();
            quicklistSetOptions(lobj->ptr, server.list_max_ziplist_size,
                                server.list_compress_depth);
            dbAdd(c->db,c->argv[1],lobj);
        }
        listTypePush(lobj,c->argv[j],where);
        pushed++;
    }
    addReplyLongLong(c, waiting + (lobj ? listTypeLength(lobj) : 0));
    if (pushed) {
        char *event = (where == LIST_HEAD) ? "lpush" : "rpush";

        signalModifiedKey(c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_LIST,event,c->argv[1],c->db->id);
    }
    c->vel->dirty += pushed;

    unlockDb(c->db);
    if (expired) update_stats_add(c->vel->stats,expiredkeys,1);
}

void lpushCommand(client *c) {
    pushGenericCommand(c,LIST_HEAD);
}

void rpushCommand(client *c) {
    pushGenericCommand(c,LIST_TAIL);
}

void pushxGenericCommand(client *c, robj *refval, robj *val, int where) {
    robj *subject;
    listTypeIterator *iter;
    listTypeEntry entry;
    int inserted = 0;

    if ((subject = lookupKeyWriteOrReply(c,c->argv[1],shared.czero,NULL)) == NULL ||
        checkType(c,subject,OBJ_LIST)) return;

    if (refval != NULL) {
        /* Seek refval from head to tail */
        iter = listTypeInitIterator(subject,0,LIST_TAIL);
        while (listTypeNext(iter,&entry)) {
            if (listTypeEqual(&entry,refval)) {
                listTypeInsert(&entry,val,where);
                inserted = 1;
                break;
            }
        }
        listTypeReleaseIterator(iter);

        if (inserted) {
            signalModifiedKey(c->db,c->argv[1]);
            notifyKeyspaceEvent(NOTIFY_LIST,"linsert",
                                c->argv[1],c->db->id);
            server.dirty++;
        } else {
            /* Notify client of a failed insert */
            addReply(c,shared.cnegone);
            return;
        }
    } else {
        char *event = (where == LIST_HEAD) ? "lpush" : "rpush";

        listTypePush(subject,val,where);
        signalModifiedKey(c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_LIST,event,c->argv[1],c->db->id);
        server.dirty++;
    }

    addReplyLongLong(c,listTypeLength(subject));
}

void lpushxCommand(client *c) {
    c->argv[2] = tryObjectEncoding(c->argv[2]);
    pushxGenericCommand(c,NULL,c->argv[2],LIST_HEAD);
}

void rpushxCommand(client *c) {
    c->argv[2] = tryObjectEncoding(c->argv[2]);
    pushxGenericCommand(c,NULL,c->argv[2],LIST_TAIL);
}

void linsertCommand(client *c) {
    c->argv[4] = tryObjectEncoding(c->argv[4]);
    if (strcasecmp(c->argv[2]->ptr,"after") == 0) {
        pushxGenericCommand(c,c->argv[3],c->argv[4],LIST_TAIL);
    } else if (strcasecmp(c->argv[2]->ptr,"before") == 0) {
        pushxGenericCommand(c,c->argv[3],c->argv[4],LIST_HEAD);
    } else {
        addReply(c,shared.syntaxerr);
    }
}

void llenCommand(client *c) {
    robj *o;
    
    fetchInternalDbByKey(c, c->argv[1]);
    lockDbRead(c->db);
    o = lookupKeyReadOrReply(c,c->argv[1],shared.czero);
    if (o == NULL) {
        unlockDb(c->db);
        update_stats_add(c->vel->stats, keyspace_misses, 1);
        return;
    } else if(checkType(c,o,OBJ_LIST)) {
        unlockDb(c->db);
        update_stats_add(c->vel->stats, keyspace_hits, 1);
        return;
    }

    addReplyLongLong(c,listTypeLength(o));
    unlockDb(c->db);
    update_stats_add(c->vel->stats, keyspace_hits, 1);
}

void lindexCommand(client *c) {
    robj *o;
    long index;
    robj *value = NULL;

    if ((getLongFromObjectOrReply(c, c->argv[2], &index, NULL) != VR_OK))
        return;

    fetchInternalDbByKey(c,c->argv[1]);
    lockDbRead(c->db);
    o = lookupKeyReadOrReply(c,c->argv[1],shared.nullbulk);
    if (o == NULL) {
        unlockDb(c->db);
        update_stats_add(c->vel->stats, keyspace_misses, 1);
        return;
    } else if(checkType(c,o,OBJ_LIST)) {
        unlockDb(c->db);
        update_stats_add(c->vel->stats, keyspace_hits, 1);
        return;
    }
    if (o->encoding == OBJ_ENCODING_QUICKLIST) {
        quicklistEntry entry;
        if (quicklistIndex(o->ptr, index, &entry)) {
            if (entry.value) {
                value = createStringObject((char*)entry.value,entry.sz);
            } else {
                value = createStringObjectFromLongLong(entry.longval);
            }
            addReplyBulk(c,value);
            freeObject(value);
        } else {
            addReply(c,shared.nullbulk);
        }
    } else {
        serverPanic("Unknown list encoding");
    }
    unlockDb(c->db);
    update_stats_add(c->vel->stats, keyspace_hits, 1);
}

void lsetCommand(client *c) {
    robj *o;
    long index;
    robj *value = c->argv[3];
    int expired = 0;

    if ((getLongFromObjectOrReply(c, c->argv[2], &index, NULL) != VR_OK))
        return;

    fetchInternalDbByKey(c, c->argv[1]);
    lockDbWrite(c->db);
    o = lookupKeyWriteOrReply(c,c->argv[1],shared.nokeyerr,&expired);
    if (o == NULL || checkType(c,o,OBJ_LIST)) {
        unlockDb(c->db);
        if (expired) update_stats_add(c->vel->stats,expiredkeys,1);
        return;
    }
    if (o->encoding == OBJ_ENCODING_QUICKLIST) {
        quicklist *ql = o->ptr;
        int replaced = quicklistReplaceAtIndex(ql, index,
                                               value->ptr, sdslen(value->ptr));
        if (!replaced) {
            addReply(c,shared.outofrangeerr);
        } else {
            addReply(c,shared.ok);
            signalModifiedKey(c->db,c->argv[1]);
            notifyKeyspaceEvent(NOTIFY_LIST,"lset",c->argv[1],c->db->id);
            c->vel->dirty++;
        }
    } else {
        serverPanic("Unknown list encoding");
    }

    unlockDb(c->db);
    if (expired) update_stats_add(c->vel->stats,expiredkeys,1);
}

void popGenericCommand(client *c, int where) {
    robj *o, *value;
    int expired = 0;

    fetchInternalDbByKey(c, c->argv[1]);
    lockDbWrite(c->db);
    o = lookupKeyWriteOrReply(c,c->argv[1],shared.nullbulk,&expired);
    if (o == NULL || checkType(c,o,OBJ_LIST)) {
        unlockDb(c->db);
        if (expired) update_stats_add(c->vel->stats,expiredkeys,1);
        return;
    }

    value = listTypePop(o,where);
    if (value == NULL) {
        addReply(c,shared.nullbulk);
    } else {
        char *event = (where == LIST_HEAD) ? "lpop" : "rpop";

        addReplyBulk(c,value);
        freeObject(value);
        notifyKeyspaceEvent(NOTIFY_LIST,event,c->argv[1],c->db->id);
        if (listTypeLength(o) == 0) {
            notifyKeyspaceEvent(NOTIFY_GENERIC,"del",
                                c->argv[1],c->db->id);
            dbDelete(c->db,c->argv[1]);
        }
        signalModifiedKey(c->db,c->argv[1]);
        c->vel->dirty++;
    }
    unlockDb(c->db);
    if (expired) update_stats_add(c->vel->stats,expiredkeys,1);
}

void lpopCommand(client *c) {
    popGenericCommand(c,LIST_HEAD);
}

void rpopCommand(client *c) {
    popGenericCommand(c,LIST_TAIL);
}

void lrangeCommand(client *c) {
    robj *o;
    long start, end, llen, rangelen;

    if ((getLongFromObjectOrReply(c, c->argv[2], &start, NULL) != VR_OK) ||
        (getLongFromObjectOrReply(c, c->argv[3], &end, NULL) != VR_OK)) return;

    fetchInternalDbByKey(c, c->argv[1]);
    lockDbRead(c->db);
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptymultibulk)) == NULL) {
        unlockDb(c->db);
        update_stats_add(c->vel->stats, keyspace_misses, 1);
        return;
    } else if (checkType(c,o,OBJ_LIST)) {
        unlockDb(c->db);
        update_stats_add(c->vel->stats, keyspace_hits, 1);
        return;
    }

    llen = listTypeLength(o);

    /* convert negative indexes */
    if (start < 0) start = llen+start;
    if (end < 0) end = llen+end;
    if (start < 0) start = 0;

    /* Invariant: start >= 0, so this test will be true when end < 0.
     * The range is empty when start > end or start >= length. */
    if (start > end || start >= llen) {
        unlockDb(c->db);
        update_stats_add(c->vel->stats, keyspace_hits, 1);
        addReply(c,shared.emptymultibulk);
        return;
    }
    if (end >= llen) end = llen-1;
    rangelen = (end-start)+1;

    /* Return the result in form of a multi-bulk reply */
    addReplyMultiBulkLen(c,rangelen);
    if (o->encoding == OBJ_ENCODING_QUICKLIST) {
        listTypeIterator *iter = listTypeInitIterator(o, start, LIST_TAIL);

        while(rangelen--) {
            listTypeEntry entry;
            listTypeNext(iter, &entry);
            quicklistEntry *qe = &entry.entry;
            if (qe->value) {
                addReplyBulkCBuffer(c,qe->value,qe->sz);
            } else {
                addReplyBulkLongLong(c,qe->longval);
            }
        }
        listTypeReleaseIterator(iter);
    } else {
        serverPanic("List encoding is not QUICKLIST!");
    }

    unlockDb(c->db);
    update_stats_add(c->vel->stats, keyspace_hits, 1);
}

void ltrimCommand(client *c) {
    robj *o;
    long start, end, llen, ltrim, rtrim;
    int expired = 0;

    if ((getLongFromObjectOrReply(c, c->argv[2], &start, NULL) != VR_OK) ||
        (getLongFromObjectOrReply(c, c->argv[3], &end, NULL) != VR_OK)) return;

    fetchInternalDbByKey(c, c->argv[1]);
    lockDbWrite(c->db);
    if ((o = lookupKeyWriteOrReply(c,c->argv[1],shared.ok,&expired)) == NULL ||
        checkType(c,o,OBJ_LIST)) {
        unlockDb(c->db);
        if (expired) update_stats_add(c->vel->stats, expiredkeys, 1);
        return;
    }
    llen = listTypeLength(o);

    /* convert negative indexes */
    if (start < 0) start = llen+start;
    if (end < 0) end = llen+end;
    if (start < 0) start = 0;

    /* Invariant: start >= 0, so this test will be true when end < 0.
     * The range is empty when start > end or start >= length. */
    if (start > end || start >= llen) {
        /* Out of range start or start > end result in empty list */
        ltrim = llen;
        rtrim = 0;
    } else {
        if (end >= llen) end = llen-1;
        ltrim = start;
        rtrim = llen-end-1;
    }

    /* Remove list elements to perform the trim */
    if (o->encoding == OBJ_ENCODING_QUICKLIST) {
        quicklistDelRange(o->ptr,0,ltrim);
        quicklistDelRange(o->ptr,-rtrim,rtrim);
    } else {
        serverPanic("Unknown list encoding");
    }

    notifyKeyspaceEvent(NOTIFY_LIST,"ltrim",c->argv[1],c->db->id);
    if (listTypeLength(o) == 0) {
        dbDelete(c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_GENERIC,"del",c->argv[1],c->db->id);
    }
    signalModifiedKey(c->db,c->argv[1]);
    c->vel->dirty++;
    addReply(c,shared.ok);
    unlockDb(c->db);
    if (expired) update_stats_add(c->vel->stats, expiredkeys, 1);
}

void lremCommand(client *c) {
    robj *subject, *obj;
    obj = c->argv[3];
    long toremove;
    long removed = 0;
    int expired = 0;

    if ((getLongFromObjectOrReply(c, c->argv[2], &toremove, NULL) != VR_OK))
        return;

    fetchInternalDbByKey(c, c->argv[1]);
    lockDbWrite(c->db);
    subject = lookupKeyWriteOrReply(c,c->argv[1],shared.czero,&expired);
    if (subject == NULL || checkType(c,subject,OBJ_LIST)) {
        unlockDb(c->db);
        if (expired) update_stats_add(c->vel->stats, expiredkeys, 1);
        return;
    }
    listTypeIterator *li;
    if (toremove < 0) {
        toremove = -toremove;
        li = listTypeInitIterator(subject,-1,LIST_HEAD);
    } else {
        li = listTypeInitIterator(subject,0,LIST_TAIL);
    }

    listTypeEntry entry;
    while (listTypeNext(li,&entry)) {
        if (listTypeEqual(&entry,obj)) {
            listTypeDelete(li, &entry);
            c->vel->dirty++;
            removed++;
            if (toremove && removed == toremove) break;
        }
    }
    listTypeReleaseIterator(li);

    if (removed) {
        signalModifiedKey(c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_GENERIC,"lrem",c->argv[1],c->db->id);
    }

    if (listTypeLength(subject) == 0) {
        dbDelete(c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_GENERIC,"del",c->argv[1],c->db->id);
    }

    addReplyLongLong(c,removed);
    unlockDb(c->db);
    if (expired) update_stats_add(c->vel->stats, expiredkeys, 1);
}

/* This is the semantic of this command:
 *  RPOPLPUSH srclist dstlist:
 *    IF LLEN(srclist) > 0
 *      element = RPOP srclist
 *      LPUSH dstlist element
 *      RETURN element
 *    ELSE
 *      RETURN nil
 *    END
 *  END
 *
 * The idea is to be able to get an element from a list in a reliable way
 * since the element is not just returned but pushed against another list
 * as well. This command was originally proposed by Ezra Zygmuntowicz.
 */

void rpoplpushHandlePush(client *c, robj *dstkey, robj *dstobj, robj *value) {
    /* Create the list if the key does not exist */
    if (!dstobj) {
        dstobj = createQuicklistObject();
        quicklistSetOptions(dstobj->ptr, server.list_max_ziplist_size,
                            server.list_compress_depth);
        dbAdd(c->db,dstkey,dstobj);
    }
    signalModifiedKey(c->db,dstkey);
    listTypePush(dstobj,value,LIST_HEAD);
    notifyKeyspaceEvent(NOTIFY_LIST,"lpush",dstkey,c->db->id);
    /* Always send the pushed value to the client. */
    addReplyBulk(c,value);
}

void rpoplpushCommand(client *c) {
    robj *sobj, *value;
    if ((sobj = lookupKeyWriteOrReply(c,c->argv[1],shared.nullbulk,NULL)) == NULL ||
        checkType(c,sobj,OBJ_LIST)) return;

    if (listTypeLength(sobj) == 0) {
        /* This may only happen after loading very old RDB files. Recent
         * versions of Redis delete keys of empty lists. */
        addReply(c,shared.nullbulk);
    } else {
        robj *dobj = lookupKeyWrite(c->db,c->argv[2],NULL);
        robj *touchedkey = c->argv[1];

        if (dobj && checkType(c,dobj,OBJ_LIST)) return;
        value = listTypePop(sobj,LIST_TAIL);
        /* We saved touched key, and protect it, since rpoplpushHandlePush
         * may change the client command argument vector (it does not
         * currently). */
        incrRefCount(touchedkey);
        rpoplpushHandlePush(c,c->argv[2],dobj,value);

        /* listTypePop returns an object with its refcount incremented */
        decrRefCount(value);

        /* Delete the source list when it is empty */
        notifyKeyspaceEvent(NOTIFY_LIST,"rpop",touchedkey,c->db->id);
        if (listTypeLength(sobj) == 0) {
            dbDelete(c->db,touchedkey);
            notifyKeyspaceEvent(NOTIFY_GENERIC,"del",
                                touchedkey,c->db->id);
        }
        signalModifiedKey(c->db,touchedkey);
        decrRefCount(touchedkey);
        server.dirty++;
    }
}

/*-----------------------------------------------------------------------------
 * Blocking POP operations
 *----------------------------------------------------------------------------*/

/* This is how the current blocking POP works, we use BLPOP as example:
 * - If the user calls BLPOP and the key exists and contains a non empty list
 *   then LPOP is called instead. So BLPOP is semantically the same as LPOP
 *   if blocking is not required.
 * - If instead BLPOP is called and the key does not exists or the list is
 *   empty we need to block. In order to do so we remove the notification for
 *   new data to read in the client socket (so that we'll not serve new
 *   requests if the blocking request is not served). Also we put the client
 *   in a dictionary (db->blocking_keys) mapping keys to a list of clients
 *   blocking for this keys.
 * - If a PUSH operation against a key with blocked clients waiting is
 *   performed, we mark this key as "ready", and after the current command,
 *   MULTI/EXEC block, or script, is executed, we serve all the clients waiting
 *   for this list, from the one that blocked first, to the last, accordingly
 *   to the number of elements we have in the ready list.
 */

/* Set a client in blocking mode for the specified key, with the specified
 * timeout */
void blockForKeys(client *c, robj **keys, int numkeys, mstime_t timeout, robj *target) {
    dictEntry *de;
    dlist *l;
    int j;

    c->bpop.timeout = timeout;
    c->bpop.target = target;

    if (target != NULL) incrRefCount(target);

    for (j = 0; j < numkeys; j++) {
        /* If the key already exists in the dict ignore it. */
        if (dictAdd(c->bpop.keys,keys[j],NULL) != DICT_OK) continue;
        incrRefCount(keys[j]);

        /* And in the other "side", to map keys -> clients */
        de = dictFind(c->db->blocking_keys,keys[j]);
        if (de == NULL) {
            int retval;

            /* For every key we take a list of clients blocked for it */
            l = dlistCreate();
            retval = dictAdd(c->db->blocking_keys,keys[j],l);
            incrRefCount(keys[j]);
            serverAssertWithInfo(c,keys[j],retval == DICT_OK);
        } else {
            l = dictGetVal(de);
        }
        dlistAddNodeTail(l,c);
    }
    blockClient(c,BLOCKED_LIST);
}

/* Unblock a client that's waiting in a blocking operation such as BLPOP.
 * You should never call this function directly, but unblockClient() instead. */
void unblockClientWaitingData(client *c) {
    dictEntry *de;
    dictIterator *di;
    dlist *l;

    serverAssertWithInfo(c,NULL,dictSize(c->bpop.keys) != 0);
    di = dictGetIterator(c->bpop.keys);
    /* The client may wait for multiple keys, so unblock it for every key. */
    while((de = dictNext(di)) != NULL) {
        robj *key = dictGetKey(de);

        /* Remove this client from the list of clients waiting for this key. */
        l = dictFetchValue(c->db->blocking_keys,key);
        serverAssertWithInfo(c,key,l != NULL);
        dlistDelNode(l,dlistSearchKey(l,c));
        /* If the list is empty we need to remove it to avoid wasting memory */
        if (dlistLength(l) == 0)
            dictDelete(c->db->blocking_keys,key);
    }
    dictReleaseIterator(di);

    /* Cleanup the client structure */
    dictEmpty(c->bpop.keys,NULL);
    if (c->bpop.target) {
        decrRefCount(c->bpop.target);
        c->bpop.target = NULL;
    }
}

/* If the specified key has clients blocked waiting for list pushes, this
 * function will put the key reference into the server.ready_keys list.
 * Note that db->ready_keys is a hash table that allows us to avoid putting
 * the same key again and again in the list in case of multiple pushes
 * made by a script or in the context of MULTI/EXEC.
 *
 * The list will be finally processed by handleClientsBlockedOnLists() */
void signalListAsReady(redisDb *db, robj *key) {
    int ret;
    readyList *rl;

    /* No clients blocking for this key? No need to queue it. */
    if (dictFind(db->blocking_keys,key) == NULL) return;

    /* Key was already signaled? No need to queue it again. */
    if (dictFind(db->ready_keys,key) != NULL) return;

    /* Ok, we need to queue this key into server.ready_keys. */
    rl = dalloc(sizeof(*rl));
    rl->key = key;
    rl->db = db;
    incrRefCount(key);
    dlistAddNodeTail(server.ready_keys,rl);

    /* We also add the key in the db->ready_keys dictionary in order
     * to avoid adding it multiple times into a list with a simple O(1)
     * check. */
    incrRefCount(key);
    ret = dictAdd(db->ready_keys,key,NULL);
    ASSERT(ret == DICT_OK);
}

/* This is a helper function for handleClientsBlockedOnLists(). It's work
 * is to serve a specific client (receiver) that is blocked on 'key'
 * in the context of the specified 'db', doing the following:
 *
 * 1) Provide the client with the 'value' element.
 * 2) If the dstkey is not NULL (we are serving a BRPOPLPUSH) also push the
 *    'value' element on the destination list (the LPUSH side of the command).
 * 3) Propagate the resulting BRPOP, BLPOP and additional LPUSH if any into
 *    the AOF and replication channel.
 *
 * The argument 'where' is LIST_TAIL or LIST_HEAD, and indicates if the
 * 'value' element was popped fron the head (BLPOP) or tail (BRPOP) so that
 * we can propagate the command properly.
 *
 * The function returns VR_OK if we are able to serve the client, otherwise
 * VR_ERROR is returned to signal the caller that the list POP operation
 * should be undone as the client was not served: This only happens for
 * BRPOPLPUSH that fails to push the value to the destination key as it is
 * of the wrong type. */
int serveClientBlockedOnList(client *receiver, robj *key, robj *dstkey, redisDb *db, robj *value, int where)
{
    robj *argv[3];

    if (dstkey == NULL) {
        /* Propagate the [LR]POP operation. */
        argv[0] = (where == LIST_HEAD) ? shared.lpop :
                                          shared.rpop;
        argv[1] = key;
        propagate((where == LIST_HEAD) ?
            server.lpopCommand : server.rpopCommand,
            db->id,argv,2,PROPAGATE_AOF|PROPAGATE_REPL);

        /* BRPOP/BLPOP */
        addReplyMultiBulkLen(receiver,2);
        addReplyBulk(receiver,key);
        addReplyBulk(receiver,value);
    } else {
        /* BRPOPLPUSH */
        robj *dstobj =
            lookupKeyWrite(receiver->db,dstkey,NULL);
        if (!(dstobj &&
             checkType(receiver,dstobj,OBJ_LIST)))
        {
            /* Propagate the RPOP operation. */
            argv[0] = shared.rpop;
            argv[1] = key;
            propagate(server.rpopCommand,
                db->id,argv,2,
                PROPAGATE_AOF|
                PROPAGATE_REPL);
            rpoplpushHandlePush(receiver,dstkey,dstobj,
                value);
            /* Propagate the LPUSH operation. */
            argv[0] = shared.lpush;
            argv[1] = dstkey;
            argv[2] = value;
            propagate(server.lpushCommand,
                db->id,argv,3,
                PROPAGATE_AOF|
                PROPAGATE_REPL);
        } else {
            /* BRPOPLPUSH failed because of wrong
             * destination type. */
            return VR_ERROR;
        }
    }
    return VR_OK;
}

/* This function should be called by Redis every time a single command,
 * a MULTI/EXEC block, or a Lua script, terminated its execution after
 * being called by a client.
 *
 * All the keys with at least one client blocked that received at least
 * one new element via some PUSH operation are accumulated into
 * the server.ready_keys list. This function will run the list and will
 * serve clients accordingly. Note that the function will iterate again and
 * again as a result of serving BRPOPLPUSH we can have new blocking clients
 * to serve because of the PUSH side of BRPOPLPUSH. */
void handleClientsBlockedOnLists(void) {
    while(dlistLength(server.ready_keys) != 0) {
        dlist *l;

        /* Point server.ready_keys to a fresh list and save the current one
         * locally. This way as we run the old list we are free to call
         * signalListAsReady() that may push new elements in server.ready_keys
         * when handling clients blocked into BRPOPLPUSH. */
        l = server.ready_keys;
        server.ready_keys = dlistCreate();

        while(dlistLength(l) != 0) {
            dlistNode *ln = dlistFirst(l);
            readyList *rl = ln->value;

            /* First of all remove this key from db->ready_keys so that
             * we can safely call signalListAsReady() against this key. */
            dictDelete(rl->db->ready_keys,rl->key);

            /* If the key exists and it's a list, serve blocked clients
             * with data. */
            robj *o = lookupKeyWrite(rl->db,rl->key,NULL);
            if (o != NULL && o->type == OBJ_LIST) {
                dictEntry *de;

                /* We serve clients in the same order they blocked for
                 * this key, from the first blocked to the last. */
                de = dictFind(rl->db->blocking_keys,rl->key);
                if (de) {
                    dlist *clients = dictGetVal(de);
                    int numclients = dlistLength(clients);

                    while(numclients--) {
                        dlistNode *clientnode = dlistFirst(clients);
                        client *receiver = clientnode->value;
                        robj *dstkey = receiver->bpop.target;
                        int where = (receiver->lastcmd &&
                                     receiver->lastcmd->proc == blpopCommand) ?
                                    LIST_HEAD : LIST_TAIL;
                        robj *value = listTypePop(o,where);

                        if (value) {
                            /* Protect receiver->bpop.target, that will be
                             * freed by the next unblockClient()
                             * call. */
                            if (dstkey) incrRefCount(dstkey);
                            unblockClient(receiver);

                            if (serveClientBlockedOnList(receiver,
                                rl->key,dstkey,rl->db,value,
                                where) == VR_ERROR)
                            {
                                /* If we failed serving the client we need
                                 * to also undo the POP operation. */
                                    listTypePush(o,value,where);
                            }

                            if (dstkey) decrRefCount(dstkey);
                            decrRefCount(value);
                        } else {
                            break;
                        }
                    }
                }

                if (listTypeLength(o) == 0) {
                    dbDelete(rl->db,rl->key);
                }
                /* We don't call signalModifiedKey() as it was already called
                 * when an element was pushed on the list. */
            }

            /* Free this item. */
            decrRefCount(rl->key);
            dfree(rl);
            dlistDelNode(l,ln);
        }
        dlistRelease(l); /* We have the new list on place at this point. */
    }
}

/* Blocking RPOP/LPOP */
void blockingPopGenericCommand(client *c, int where) {
    robj *o;
    mstime_t timeout;
    int j;

    if (getTimeoutFromObjectOrReply(c,c->argv[c->argc-1],&timeout,UNIT_SECONDS)
        != VR_OK) return;

    for (j = 1; j < c->argc-1; j++) {
        o = lookupKeyWrite(c->db,c->argv[j],NULL);
        if (o != NULL) {
            if (o->type != OBJ_LIST) {
                addReply(c,shared.wrongtypeerr);
                return;
            } else {
                if (listTypeLength(o) != 0) {
                    /* Non empty list, this is like a non normal [LR]POP. */
                    char *event = (where == LIST_HEAD) ? "lpop" : "rpop";
                    robj *value = listTypePop(o,where);
                    ASSERT(value != NULL);

                    addReplyMultiBulkLen(c,2);
                    addReplyBulk(c,c->argv[j]);
                    addReplyBulk(c,value);
                    decrRefCount(value);
                    notifyKeyspaceEvent(NOTIFY_LIST,event,
                                        c->argv[j],c->db->id);
                    if (listTypeLength(o) == 0) {
                        dbDelete(c->db,c->argv[j]);
                        notifyKeyspaceEvent(NOTIFY_GENERIC,"del",
                                            c->argv[j],c->db->id);
                    }
                    signalModifiedKey(c->db,c->argv[j]);
                    server.dirty++;

                    /* Replicate it as an [LR]POP instead of B[LR]POP. */
                    rewriteClientCommandVector(c,2,
                        (where == LIST_HEAD) ? shared.lpop : shared.rpop,
                        c->argv[j]);
                    return;
                }
            }
        }
    }

    /* If we are inside a MULTI/EXEC and the list is empty the only thing
     * we can do is treating it as a timeout (even with timeout 0). */
    if (c->flags & CLIENT_MULTI) {
        addReply(c,shared.nullmultibulk);
        return;
    }

    /* If the list is empty or the key does not exists we must block */
    blockForKeys(c, c->argv + 1, c->argc - 2, timeout, NULL);
}

void blpopCommand(client *c) {
    blockingPopGenericCommand(c,LIST_HEAD);
}

void brpopCommand(client *c) {
    blockingPopGenericCommand(c,LIST_TAIL);
}

void brpoplpushCommand(client *c) {
    mstime_t timeout;

    if (getTimeoutFromObjectOrReply(c,c->argv[3],&timeout,UNIT_SECONDS)
        != VR_OK) return;

    robj *key = lookupKeyWrite(c->db, c->argv[1],NULL);

    if (key == NULL) {
        if (c->flags & CLIENT_MULTI) {
            /* Blocking against an empty list in a multi state
             * returns immediately. */
            addReply(c, shared.nullbulk);
        } else {
            /* The list is empty and the client blocks. */
            blockForKeys(c, c->argv + 1, 1, timeout, c->argv[2]);
        }
    } else {
        if (key->type != OBJ_LIST) {
            addReply(c, shared.wrongtypeerr);
        } else {
            /* The list exists and has elements, so
             * the regular rpoplpushCommand is executed. */
            serverAssertWithInfo(c,key,listTypeLength(key) > 0);
            rpoplpushCommand(c);
        }
    }
}
