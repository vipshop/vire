#include <vr_core.h>

/*-----------------------------------------------------------------------------
 * Set Commands
 *----------------------------------------------------------------------------*/

void sunionDiffGenericCommand(client *c, robj **setkeys, int setnum,
                              robj *dstkey, int op);

/* Factory method to return a set that *can* hold "value". When the object has
 * an integer-encodable value, an intset will be returned. Otherwise a regular
 * hash table. */
robj *setTypeCreate(robj *value) {
    if (isObjectRepresentableAsLongLong(value,NULL) == VR_OK)
        return createIntsetObject();
    return createSetObject();
}

/* Add the specified value into a set. The function takes care of incrementing
 * the reference count of the object if needed in order to retain a copy.
 *
 * If the value was already member of the set, nothing is done and 0 is
 * returned, otherwise the new element is added and 1 is returned. */
int setTypeAdd(robj *subject, robj *value) {
    long long llval;
    robj *obj;
    if (subject->encoding == OBJ_ENCODING_HT) {
        obj = dupStringObjectUnconstant(value);
        if (dictAdd(subject->ptr,obj,NULL) == DICT_OK) {
            return 1;
        } else {
            freeObject(obj);
        }
    } else if (subject->encoding == OBJ_ENCODING_INTSET) {
        if (isObjectRepresentableAsLongLong(value,&llval) == VR_OK) {
            uint8_t success = 0;
            subject->ptr = intsetAdd(subject->ptr,llval,&success);
            if (success) {
                /* Convert to regular set when the intset contains
                 * too many entries. */
                if (intsetLen(subject->ptr) > server.set_max_intset_entries)
                    setTypeConvert(subject,OBJ_ENCODING_HT);
                return 1;
            }
        } else {
            /* Failed to get integer from object, convert to regular set. */
            setTypeConvert(subject,OBJ_ENCODING_HT);
            obj = dupStringObjectUnconstant(value);
            /* The set *was* an intset and this value is not integer
             * encodable, so dictAdd should always work. */
            serverAssertWithInfo(NULL,obj,
                dictAdd(subject->ptr,obj,NULL) == DICT_OK);
            return 1;
        }
    } else {
        serverPanic("Unknown set encoding");
    }
    return 0;
}

int setTypeRemove(robj *setobj, robj *value) {
    long long llval;
    if (setobj->encoding == OBJ_ENCODING_HT) {
        if (dictDelete(setobj->ptr,value) == DICT_OK) {
            if (htNeedsResize(setobj->ptr)) dictResize(setobj->ptr);
            return 1;
        }
    } else if (setobj->encoding == OBJ_ENCODING_INTSET) {
        if (isObjectRepresentableAsLongLong(value,&llval) == VR_OK) {
            int success;
            setobj->ptr = intsetRemove(setobj->ptr,llval,&success);
            if (success) return 1;
        }
    } else {
        serverPanic("Unknown set encoding");
    }
    return 0;
}

int setTypeIsMember(robj *subject, robj *value) {
    long long llval;
    if (subject->encoding == OBJ_ENCODING_HT) {
        return dictFind((dict*)subject->ptr,value) != NULL;
    } else if (subject->encoding == OBJ_ENCODING_INTSET) {
        if (isObjectRepresentableAsLongLong(value,&llval) == VR_OK) {
            return intsetFind((intset*)subject->ptr,llval);
        }
    } else {
        serverPanic("Unknown set encoding");
    }
    return 0;
}

setTypeIterator *setTypeInitIterator(robj *subject) {
    setTypeIterator *si = dalloc(sizeof(setTypeIterator));
    si->subject = subject;
    si->encoding = subject->encoding;
    if (si->encoding == OBJ_ENCODING_HT) {
        si->di = dictGetIterator(subject->ptr);
    } else if (si->encoding == OBJ_ENCODING_INTSET) {
        si->ii = 0;
    } else {
        serverPanic("Unknown set encoding");
    }
    return si;
}

void setTypeReleaseIterator(setTypeIterator *si) {
    if (si->encoding == OBJ_ENCODING_HT)
        dictReleaseIterator(si->di);
    dfree(si);
}

/* Move to the next entry in the set. Returns the object at the current
 * position.
 *
 * Since set elements can be internally be stored as redis objects or
 * simple arrays of integers, setTypeNext returns the encoding of the
 * set object you are iterating, and will populate the appropriate pointer
 * (objele) or (llele) accordingly.
 *
 * Note that both the objele and llele pointers should be passed and cannot
 * be NULL since the function will try to defensively populate the non
 * used field with values which are easy to trap if misused.
 *
 * When there are no longer elements -1 is returned.
 * Returned objects ref count is not incremented, so this function is
 * copy on write friendly. */
int setTypeNext(setTypeIterator *si, robj **objele, int64_t *llele) {
    if (si->encoding == OBJ_ENCODING_HT) {
        dictEntry *de = dictNext(si->di);
        if (de == NULL) return -1;
        *objele = dictGetKey(de);
        *llele = -123456789; /* Not needed. Defensive. */
    } else if (si->encoding == OBJ_ENCODING_INTSET) {
        if (!intsetGet(si->subject->ptr,si->ii++,llele))
            return -1;
        *objele = NULL; /* Not needed. Defensive. */
    } else {
        serverPanic("Wrong set encoding in setTypeNext");
    }
    return si->encoding;
}

/* The not copy on write friendly version but easy to use version
 * of setTypeNext() is setTypeNextObject(), returning new objects
 * or the returned objects. So if you don't
 * retain a pointer to this object you should call freeObject() against 
 * it if  si->encoding == OBJ_ENCODING_INTSET.
 *
 * This function is the way to go for write operations where COW is not
 * an issue as the result will be anyway of incrementing the ref count. */
robj *setTypeNextObject(setTypeIterator *si) {
    int64_t intele;
    robj *objele;
    int encoding;

    encoding = setTypeNext(si,&objele,&intele);
    switch(encoding) {
        case -1:    return NULL;
        case OBJ_ENCODING_INTSET:
            return createStringObjectFromLongLong(intele);
        case OBJ_ENCODING_HT:
            return objele;
        default:
            serverPanic("Unsupported encoding");
    }
    return NULL; /* just to suppress warnings */
}

/* Return random element from a non empty set.
 * The returned element can be a int64_t value if the set is encoded
 * as an "intset" blob of integers, or a redis object if the set
 * is a regular set.
 *
 * The caller provides both pointers to be populated with the right
 * object. The return value of the function is the object->encoding
 * field of the object and is used by the caller to check if the
 * int64_t pointer or the redis object pointer was populated.
 *
 * Note that both the objele and llele pointers should be passed and cannot
 * be NULL since the function will try to defensively populate the non
 * used field with values which are easy to trap if misused.
 *
 * When an object is returned (the set was a real set) the ref count
 * of the object is not incremented so this function can be considered
 * copy on write friendly. */
int setTypeRandomElement(robj *setobj, robj **objele, int64_t *llele) {
    if (setobj->encoding == OBJ_ENCODING_HT) {
        dictEntry *de = dictGetRandomKey(setobj->ptr);
        *objele = dictGetKey(de);
        *llele = -123456789; /* Not needed. Defensive. */
    } else if (setobj->encoding == OBJ_ENCODING_INTSET) {
        *llele = intsetRandom(setobj->ptr);
        *objele = NULL; /* Not needed. Defensive. */
    } else {
        serverPanic("Unknown set encoding");
    }
    return setobj->encoding;
}

unsigned long setTypeSize(robj *subject) {
    if (subject->encoding == OBJ_ENCODING_HT) {
        return dictSize((dict*)subject->ptr);
    } else if (subject->encoding == OBJ_ENCODING_INTSET) {
        return intsetLen((intset*)subject->ptr);
    } else {
        serverPanic("Unknown set encoding");
    }
}

/* Convert the set to specified encoding. The resulting dict (when converting
 * to a hash table) is presized to hold the number of elements in the original
 * set. */
void setTypeConvert(robj *setobj, int enc) {
    setTypeIterator *si;
    serverAssertWithInfo(NULL,setobj,setobj->type == OBJ_SET &&
                             setobj->encoding == OBJ_ENCODING_INTSET);

    if (enc == OBJ_ENCODING_HT) {
        int64_t intele;
        dict *d = dictCreate(&setDictType,NULL);
        robj *element;

        /* Presize the dict to avoid rehashing */
        dictExpand(d,intsetLen(setobj->ptr));

        /* To add the elements we extract integers and create redis objects */
        si = setTypeInitIterator(setobj);
        while (setTypeNext(si,&element,&intele) != -1) {
            element = createStringObjectFromLongLong(intele);
            serverAssertWithInfo(NULL,element,
                dictAdd(d,element,NULL) == DICT_OK);
        }
        setTypeReleaseIterator(si);

        setobj->encoding = OBJ_ENCODING_HT;
        dfree(setobj->ptr);
        setobj->ptr = d;
    } else {
        serverPanic("Unsupported set conversion");
    }
}

void saddCommand(client *c) {
    robj *set;
    int j, added = 0;
    int expired = 0;

    fetchInternalDbByKey(c, c->argv[1]);
    lockDbWrite(c->db);
    set = lookupKeyWrite(c->db,c->argv[1],&expired);
    if (set == NULL) {
        set = setTypeCreate(c->argv[2]);
        dbAdd(c->db,c->argv[1],set);
    } else {
        if (set->type != OBJ_SET) {
            unlockDb(c->db);
            if (expired) update_stats_add(c->vel->stats,expiredkeys,1);
            addReply(c,shared.wrongtypeerr);
            return;
        }
    }

    for (j = 2; j < c->argc; j++) {
        c->argv[j] = tryObjectEncoding(c->argv[j]);
        if (setTypeAdd(set,c->argv[j])) added++;
    }
    if (added) {
        signalModifiedKey(c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_SET,"sadd",c->argv[1],c->db->id);
    }
    c->vel->dirty += added;
    addReplyLongLong(c,added);
    unlockDb(c->db);
    if (expired) update_stats_add(c->vel->stats,expiredkeys,1);
}

void sremCommand(client *c) {
    robj *set;
    int j, deleted = 0, keyremoved = 0;
    int expired = 0;
    
    fetchInternalDbByKey(c, c->argv[1]);
    lockDbWrite(c->db);
    if ((set = lookupKeyWriteOrReply(c,c->argv[1],shared.czero,&expired)) == NULL ||
        checkType(c,set,OBJ_SET)) {
        unlockDb(c->db);
        if (expired) update_stats_add(c->vel->stats,expiredkeys,1);
        return;
    }

    for (j = 2; j < c->argc; j++) {
        if (setTypeRemove(set,c->argv[j])) {
            deleted++;
            if (setTypeSize(set) == 0) {
                dbDelete(c->db,c->argv[1]);
                keyremoved = 1;
                break;
            }
        }
    }
    if (deleted) {
        signalModifiedKey(c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_SET,"srem",c->argv[1],c->db->id);
        if (keyremoved)
            notifyKeyspaceEvent(NOTIFY_GENERIC,"del",c->argv[1],
                                c->db->id);
        c->vel->dirty += deleted;
    }
    addReplyLongLong(c,deleted);
    unlockDb(c->db);
    if (expired) update_stats_add(c->vel->stats,expiredkeys,1);
}

void smoveCommand(client *c) {
    robj *srcset, *dstset, *ele;
    srcset = lookupKeyWrite(c->db,c->argv[1],NULL);
    dstset = lookupKeyWrite(c->db,c->argv[2],NULL);
    ele = c->argv[3] = tryObjectEncoding(c->argv[3]);

    /* If the source key does not exist return 0 */
    if (srcset == NULL) {
        addReply(c,shared.czero);
        return;
    }

    /* If the source key has the wrong type, or the destination key
     * is set and has the wrong type, return with an error. */
    if (checkType(c,srcset,OBJ_SET) ||
        (dstset && checkType(c,dstset,OBJ_SET))) return;

    /* If srcset and dstset are equal, SMOVE is a no-op */
    if (srcset == dstset) {
        addReply(c,setTypeIsMember(srcset,ele) ? shared.cone : shared.czero);
        return;
    }

    /* If the element cannot be removed from the src set, return 0. */
    if (!setTypeRemove(srcset,ele)) {
        addReply(c,shared.czero);
        return;
    }
    notifyKeyspaceEvent(NOTIFY_SET,"srem",c->argv[1],c->db->id);

    /* Remove the src set from the database when empty */
    if (setTypeSize(srcset) == 0) {
        dbDelete(c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_GENERIC,"del",c->argv[1],c->db->id);
    }
    signalModifiedKey(c->db,c->argv[1]);
    signalModifiedKey(c->db,c->argv[2]);
    server.dirty++;

    /* Create the destination set when it doesn't exist */
    if (!dstset) {
        dstset = setTypeCreate(ele);
        dbAdd(c->db,c->argv[2],dstset);
    }

    /* An extra key has changed when ele was successfully added to dstset */
    if (setTypeAdd(dstset,ele)) {
        server.dirty++;
        notifyKeyspaceEvent(NOTIFY_SET,"sadd",c->argv[2],c->db->id);
    }
    addReply(c,shared.cone);
}

void sismemberCommand(client *c) {
    robj *set;

    fetchInternalDbByKey(c, c->argv[1]);
    lockDbRead(c->db);
    if ((set = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL) {
        unlockDb(c->db);
        update_stats_add(c->vel->stats, keyspace_misses, 1);
        return;
    } else if (checkType(c,set,OBJ_SET)) {
        unlockDb(c->db);
        update_stats_add(c->vel->stats, keyspace_hits, 1);
        return;
    }
    
    c->argv[2] = tryObjectEncoding(c->argv[2]);
    if (setTypeIsMember(set,c->argv[2]))
        addReply(c,shared.cone);
    else
        addReply(c,shared.czero);

    unlockDb(c->db);
    update_stats_add(c->vel->stats, keyspace_hits, 1);
}

void scardCommand(client *c) {
    robj *o;

    fetchInternalDbByKey(c, c->argv[1]);
    lockDbRead(c->db);
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL) {
        unlockDb(c->db);
        update_stats_add(c->vel->stats, keyspace_misses, 1);
        return;
    } else if (checkType(c,o,OBJ_SET)) {
        unlockDb(c->db);
        update_stats_add(c->vel->stats, keyspace_hits, 1);
        return;
    }
    
    addReplyLongLong(c,setTypeSize(o));
    unlockDb(c->db);
    update_stats_add(c->vel->stats, keyspace_hits, 1);
}

void smembersGenericCommand(client *c, robj *set)
{
    setTypeIterator *si;
    robj *eleobj;
    int64_t intobj;
    int encoding;
    
    addReplyMultiBulkLen(c, setTypeSize(set));
    si = setTypeInitIterator(set);
    while ((encoding = setTypeNext(si,&eleobj,&intobj)) != -1) {
        if (encoding == OBJ_ENCODING_HT) {
            addReplyBulk(c, eleobj);
        } else if (encoding == OBJ_ENCODING_INTSET) {
            addReplyBulkLongLong(c, intobj);
        }
    }
    setTypeReleaseIterator(si);
}

/* Handle the "SPOP key <count>" variant. The normal version of the
 * command is handled by the spopCommand() function itself. */

/* How many times bigger should be the set compared to the remaining size
 * for us to use the "create new set" strategy? Read later in the
 * implementation for more info. */
#define SPOP_MOVE_STRATEGY_MUL 5

void spopWithCountCommand(client *c) {
    long l;
    unsigned long count, size;
    robj *set;
    int expired = 0;

    /* Get the count argument */
    if (getLongFromObjectOrReply(c,c->argv[2],&l,NULL) != VR_OK) return;
    if (l >= 0) {
        count = (unsigned) l;
    } else {
        addReply(c,shared.outofrangeerr);
        return;
    }

    fetchInternalDbByKey(c, c->argv[1]);
    lockDbWrite(c->db);
    /* Make sure a key with the name inputted exists, and that it's type is
     * indeed a set. Otherwise, return nil */
    if ((set = lookupKeyWriteOrReply(c,c->argv[1],shared.emptymultibulk,&expired))
        == NULL || checkType(c,set,OBJ_SET)) {
        unlockDb(c->db);
        if (expired) update_stats_add(c->vel->stats,expiredkeys,1);
        return;
    }

    /* If count is zero, serve an empty multibulk ASAP to avoid special
     * cases later. */
    if (count == 0) {
        unlockDb(c->db);
        if (expired) update_stats_add(c->vel->stats,expiredkeys,1);
        addReply(c,shared.emptymultibulk);
        return;
    }

    size = setTypeSize(set);

    /* Generate an SPOP keyspace notification */
    notifyKeyspaceEvent(NOTIFY_SET,"spop",c->argv[1],c->db->id);
    c->vel->dirty += count;

    /* CASE 1:
     * The number of requested elements is greater than or equal to
     * the number of elements inside the set: simply return the whole set. */
    if (count >= size) {
        robj *aux;
        
        /* We just return the entire set */
        smembersGenericCommand(c, set);

        /* Delete the set as it is now empty */
        dbDelete(c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_GENERIC,"del",c->argv[1],c->db->id);

        /* Propagate this command as an DEL operation */
        aux = dupStringObjectUnconstant(c->argv[1]);
        rewriteClientCommandVector(c,2,shared.del,aux);
        signalModifiedKey(c->db,c->argv[1]);
        c->vel->dirty++;
        unlockDb(c->db);
        if (expired) update_stats_add(c->vel->stats,expiredkeys,1);
        return;
    }

    /* Case 2 and 3 require to replicate SPOP as a set of SERM commands.
     * Prepare our replication argument vector. Also send the array length
     * which is common to both the code paths. */
    robj *propargv[3];
    propargv[0] = createStringObject("SREM",4);
    propargv[1] = c->argv[1];
    addReplyMultiBulkLen(c,count);

    /* Common iteration vars. */
    robj *objele;
    int encoding;
    int64_t llele;
    unsigned long remaining = size-count; /* Elements left after SPOP. */

    /* If we are here, the number of requested elements is less than the
     * number of elements inside the set. Also we are sure that count < size.
     * Use two different strategies.
     *
     * CASE 2: The number of elements to return is small compared to the
     * set size. We can just extract random elements and return them to
     * the set. */
    if (remaining*SPOP_MOVE_STRATEGY_MUL > count) {
        while(count--) {
            encoding = setTypeRandomElement(set,&objele,&llele);
            if (encoding == OBJ_ENCODING_INTSET) {
                objele = createStringObjectFromLongLong(llele);
            } else {
                objele = dupStringObjectUnconstant(objele);
            }

            /* Return the element to the client and remove from the set. */
            addReplyBulk(c,objele);
            setTypeRemove(set,objele);

            /* Replicate/AOF this command as an SREM operation */
            propargv[2] = objele;
            alsoPropagate(server.sremCommand,c->db->id,propargv,3,
                PROPAGATE_AOF|PROPAGATE_REPL);
            freeObject(objele);
        }
    } else {
    /* CASE 3: The number of elements to return is very big, approaching
     * the size of the set itself. After some time extracting random elements
     * from such a set becomes computationally expensive, so we use
     * a different strategy, we extract random elements that we don't
     * want to return (the elements that will remain part of the set),
     * creating a new set as we do this (that will be stored as the original
     * set). Then we return the elements left in the original set and
     * release it. */
        robj *newset = NULL;

        /* Create a new set with just the remaining elements. */
        while(remaining--) {
            encoding = setTypeRandomElement(set,&objele,&llele);
            if (encoding == OBJ_ENCODING_INTSET)
                objele = createStringObjectFromLongLong(llele);
            
            if (!newset) newset = setTypeCreate(objele);
            setTypeAdd(newset,objele);
            setTypeRemove(set,objele);
            if (encoding == OBJ_ENCODING_INTSET)
                freeObject(objele);
        }

        /* Tranfer the old set to the client. */
        setTypeIterator *si;
        si = setTypeInitIterator(set);
        while((encoding = setTypeNext(si,&objele,&llele)) != -1) {
            if (encoding == OBJ_ENCODING_INTSET)
                objele = createStringObjectFromLongLong(llele);
            addReplyBulk(c,objele);

            /* Replicate/AOF this command as an SREM operation */
            propargv[2] = objele;
            alsoPropagate(server.sremCommand,c->db->id,propargv,3,
                PROPAGATE_AOF|PROPAGATE_REPL);
            if (encoding == OBJ_ENCODING_INTSET)
                freeObject(objele);
        }
        setTypeReleaseIterator(si);

        /* Assign the new set as the key value. */
        dbOverwrite(c->db,c->argv[1],newset);
    }

    /* Don't propagate the command itself even if we incremented the
     * dirty counter. We don't want to propagate an SPOP command since
     * we propagated the command as a set of SREMs operations using
     * the alsoPropagate() API. */
    freeObject(propargv[0]);
    preventCommandPropagation(c);
    unlockDb(c->db);
    if (expired) update_stats_add(c->vel->stats,expiredkeys,1);
}

void spopCommand(client *c) {
    robj *set, *ele, *aux1, *aux2;
    int64_t llele;
    int encoding;
    int expired = 0;

    if (c->argc == 3) {
        spopWithCountCommand(c);
        return;
    } else if (c->argc > 3) {
        addReply(c,shared.syntaxerr);
        return;
    }

    fetchInternalDbByKey(c, c->argv[1]);
    lockDbWrite(c->db);
    /* Make sure a key with the name inputted exists, and that it's type is
     * indeed a set */
    if ((set = lookupKeyWriteOrReply(c,c->argv[1],shared.nullbulk,&expired)) == NULL ||
        checkType(c,set,OBJ_SET)) {
        unlockDb(c->db);
        if (expired) update_stats_add(c->vel->stats,expiredkeys,1);
        return;
    }
    
    /* Get a random element from the set */
    encoding = setTypeRandomElement(set,&ele,&llele);

    /* Remove the element from the set */
    if (encoding == OBJ_ENCODING_INTSET) {
        ele = createStringObjectFromLongLong(llele);
        set->ptr = intsetRemove(set->ptr,llele,NULL);
    } else {
        ele = dupStringObjectUnconstant(ele);
        setTypeRemove(set,ele);
    }

    notifyKeyspaceEvent(NOTIFY_SET,"spop",c->argv[1],c->db->id);

    /* Replicate/AOF this command as an SREM operation */
    aux1 = createStringObject("SREM",4);
    aux2 = dupStringObjectUnconstant(c->argv[1]);
    rewriteClientCommandVector(c,3,aux1,aux2,ele);

    /* Add the element to the reply */
    addReplyBulk(c,ele);

    /* Delete the set if it's empty */
    if (setTypeSize(set) == 0) {
        dbDelete(c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_GENERIC,"del",c->argv[1],c->db->id);
    }

    /* Set has been modified */
    signalModifiedKey(c->db,c->argv[1]);
    c->vel->dirty++;
    unlockDb(c->db);
    if (expired) update_stats_add(c->vel->stats,expiredkeys,1);
}

/* handle the "SRANDMEMBER key <count>" variant. The normal version of the
 * command is handled by the srandmemberCommand() function itself. */

/* How many times bigger should be the set compared to the requested size
 * for us to don't use the "remove elements" strategy? Read later in the
 * implementation for more info. */
#define SRANDMEMBER_SUB_STRATEGY_MUL 3

void srandmemberWithCountCommand(client *c) {
    long l;
    unsigned long count, size;
    int uniq = 1;
    robj *set, *ele;
    int64_t llele;
    int encoding;

    dict *d;

    if (getLongFromObjectOrReply(c,c->argv[2],&l,NULL) != VR_OK) return;
    if (l >= 0) {
        count = (unsigned) l;
    } else {
        /* A negative count means: return the same elements multiple times
         * (i.e. don't remove the extracted element after every extraction). */
        count = -l;
        uniq = 0;
    }

    if ((set = lookupKeyReadOrReply(c,c->argv[1],shared.emptymultibulk))
        == NULL || checkType(c,set,OBJ_SET)) return;
    size = setTypeSize(set);

    /* If count is zero, serve it ASAP to avoid special cases later. */
    if (count == 0) {
        addReply(c,shared.emptymultibulk);
        return;
    }

    /* CASE 1: The count was negative, so the extraction method is just:
     * "return N random elements" sampling the whole set every time.
     * This case is trivial and can be served without auxiliary data
     * structures. */
    if (!uniq) {
        addReplyMultiBulkLen(c,count);
        while(count--) {
            encoding = setTypeRandomElement(set,&ele,&llele);
            if (encoding == OBJ_ENCODING_INTSET) {
                addReplyBulkLongLong(c,llele);
            } else {
                addReplyBulk(c,ele);
            }
        }
        return;
    }

    /* CASE 2:
     * The number of requested elements is greater than the number of
     * elements inside the set: simply return the whole set. */
    if (count >= size) {
        //sunionDiffGenericCommand(c,c->argv+1,1,NULL,SET_OP_UNION);
        smembersGenericCommand(c, set);
        return;
    }

    /* For CASE 3 and CASE 4 we need an auxiliary dictionary. */
    d = dictCreate(&setDictType,NULL);

    /* CASE 3:
     * The number of elements inside the set is not greater than
     * SRANDMEMBER_SUB_STRATEGY_MUL times the number of requested elements.
     * In this case we create a set from scratch with all the elements, and
     * subtract random elements to reach the requested number of elements.
     *
     * This is done because if the number of requsted elements is just
     * a bit less than the number of elements in the set, the natural approach
     * used into CASE 3 is highly inefficient. */
    if (count*SRANDMEMBER_SUB_STRATEGY_MUL > size) {
        setTypeIterator *si;

        /* Add all the elements into the temporary dictionary. */
        si = setTypeInitIterator(set);
        while((encoding = setTypeNext(si,&ele,&llele)) != -1) {
            int retval = DICT_ERR;

            if (encoding == OBJ_ENCODING_INTSET) {
                retval = dictAdd(d,createStringObjectFromLongLong(llele),NULL);
            } else {
                retval = dictAdd(d,dupStringObject(ele),NULL);
            }
            ASSERT(retval == DICT_OK);
        }
        setTypeReleaseIterator(si);
        ASSERT(dictSize(d) == size);

        /* Remove random elements to reach the right count. */
        while(size > count) {
            dictEntry *de;

            de = dictGetRandomKey(d);
            dictDelete(d,dictGetKey(de));
            size--;
        }
    }

    /* CASE 4: We have a big set compared to the requested number of elements.
     * In this case we can simply get random elements from the set and add
     * to the temporary set, trying to eventually get enough unique elements
     * to reach the specified count. */
    else {
        unsigned long added = 0;

        while(added < count) {
            encoding = setTypeRandomElement(set,&ele,&llele);
            if (encoding == OBJ_ENCODING_INTSET) {
                ele = createStringObjectFromLongLong(llele);
            } else {
                ele = dupStringObject(ele);
            }
            /* Try to add the object to the dictionary. If it already exists
             * free it, otherwise increment the number of objects we have
             * in the result dictionary. */
            if (dictAdd(d,ele,NULL) == DICT_OK)
                added++;
            else
                decrRefCount(ele);
        }
    }

    /* CASE 3 & 4: send the result to the user. */
    {
        dictIterator *di;
        dictEntry *de;

        addReplyMultiBulkLen(c,count);
        di = dictGetIterator(d);
        while((de = dictNext(di)) != NULL)
            addReplyBulk(c,dictGetKey(de));
        dictReleaseIterator(di);
        dictRelease(d);
    }
}

void srandmemberCommand(client *c) {
    robj *set, *ele;
    int64_t llele;
    int encoding;

    if (c->argc == 3) {
        srandmemberWithCountCommand(c);
        return;
    } else if (c->argc > 3) {
        addReply(c,shared.syntaxerr);
        return;
    }

    if ((set = lookupKeyReadOrReply(c,c->argv[1],shared.nullbulk)) == NULL ||
        checkType(c,set,OBJ_SET)) return;

    encoding = setTypeRandomElement(set,&ele,&llele);
    if (encoding == OBJ_ENCODING_INTSET) {
        addReplyBulkLongLong(c,llele);
    } else {
        addReplyBulk(c,ele);
    }
}

void smembersCommand(client *c) {
    robj *set;

    fetchInternalDbByKey(c, c->argv[1]);
    lockDbRead(c->db);
    set = lookupKeyReadOrReply(c,c->argv[1],shared.emptymultibulk);
    if (set == NULL) {
        unlockDb(c->db);
        update_stats_add(c->vel->stats, keyspace_misses, 1);
        return;
    } else if(checkType(c,set,OBJ_SET)) {
        unlockDb(c->db);
        update_stats_add(c->vel->stats, keyspace_hits, 1);
        return;
    }

    smembersGenericCommand(c, set);
    unlockDb(c->db);
    update_stats_add(c->vel->stats, keyspace_hits, 1);
}

int qsortCompareSetsByCardinality(const void *s1, const void *s2) {
    return setTypeSize(*(robj**)s1)-setTypeSize(*(robj**)s2);
}

/* This is used by SDIFF and in this case we can receive NULL that should
 * be handled as empty sets. */
int qsortCompareSetsByRevCardinality(const void *s1, const void *s2) {
    robj *o1 = *(robj**)s1, *o2 = *(robj**)s2;

    return  (o2 ? setTypeSize(o2) : 0) - (o1 ? setTypeSize(o1) : 0);
}

void sinterGenericCommand(client *c, robj **setkeys,
                          unsigned long setnum, robj *dstkey) {
    setTypeIterator *si;
    robj *eleobj, *dstset = NULL;
    int64_t intobj;
    unsigned long j, cardinality = 0;
    int encoding;
    robj *setobj, *min_len_set;
    unsigned long min_len = -1;
    unsigned long min_len_idx = 0;

    for (j = 0; j < setnum; j++) {
        fetchInternalDbByKey(c,setkeys[j]);
        lockDbRead(c->db);
        setobj = lookupKeyRead(c->db,setkeys[j]);
        if (!setobj) {
            unlockDb(c->db);
            update_stats_add(c->vel->stats,keyspace_misses,1);
            if (dstkey) {
                fetchInternalDbByKey(c,dstkey);
                lockDbWrite(c->db);
                if (dbDelete(c->db,dstkey)) {
                    signalModifiedKey(c->db,dstkey);
                    c->vel->dirty++;
                }
                unlockDb(c->db);
                addReply(c,shared.czero);
            } else {
                addReply(c,shared.emptymultibulk);
            }
            return;
        }
        if (checkType(c,setobj,OBJ_SET)) {
            unlockDb(c->db);
            update_stats_add(c->vel->stats,keyspace_hits,1);
            return;
        }

        if (min_len == -1 || setTypeSize(setobj) < min_len) {
            min_len = setTypeSize(setobj);
            min_len_idx = j;
        }

        unlockDb(c->db);
        update_stats_add(c->vel->stats,keyspace_hits,1);
    }

    min_len_set = createIntsetObject();
    fetchInternalDbByKey(c,setkeys[min_len_idx]);
    lockDbRead(c->db);
    setobj = lookupKeyRead(c->db,setkeys[min_len_idx]);
    if (!setobj) {
        unlockDb(c->db);
        freeObject(min_len_set);
        goto done;
    }
    if (checkType(c,setobj,OBJ_SET)) {
        unlockDb(c->db);
        freeObject(min_len_set);
        return;
    }
    si = setTypeInitIterator(setobj);
    while((eleobj = setTypeNextObject(si)) != NULL) {
        setTypeAdd(min_len_set,eleobj);
        if (si->encoding == OBJ_ENCODING_INTSET) 
            freeObject(eleobj); /* free this object for intset type */
    }
    setTypeReleaseIterator(si);
    unlockDb(c->db);

    dstset = createIntsetObject();

    /* Iterate all the elements of the first (smallest) set, and test
     * the element against all the other sets, if at least one set does
     * not include the element it is discarded */
    si = setTypeInitIterator(min_len_set);
    while((encoding = setTypeNext(si,&eleobj,&intobj)) != -1) {
        for (j = 0; j < setnum; j++) {
            if (j == min_len_idx) continue;
            fetchInternalDbByKey(c,setkeys[j]);
            lockDbRead(c->db);
            setobj = lookupKeyRead(c->db,setkeys[j]);
            if (!setobj) {
                unlockDb(c->db);
                freeObject(min_len_set);
                if (dstset) {
                    freeObject(dstset);
                    dstset = NULL;
                }
                setTypeReleaseIterator(si);
                goto done;
            }
            if (checkType(c,setobj,OBJ_SET)) {
                unlockDb(c->db);
                freeObject(min_len_set);
                if (dstset) {
                    freeObject(dstset);
                    dstset = NULL;
                }
                setTypeReleaseIterator(si);
                return;
            }
            
            if (encoding == OBJ_ENCODING_INTSET) {
                /* intset with intset is simple... and fast */
                if (setobj->encoding == OBJ_ENCODING_INTSET &&
                    !intsetFind((intset*)setobj->ptr,intobj))
                {
                    unlockDb(c->db);
                    break;
                /* in order to compare an integer with an object we
                 * have to use the generic function, creating an object
                 * for this */
                } else if (setobj->encoding == OBJ_ENCODING_HT) {
                    eleobj = createStringObjectFromLongLong(intobj);
                    if (!setTypeIsMember(setobj,eleobj)) {
                        unlockDb(c->db);
                        freeObject(eleobj);
                        break;
                    }
                    freeObject(eleobj);
                }
            } else if (encoding == OBJ_ENCODING_HT) {
                /* Optimization... if the source object is integer
                 * encoded AND the target set is an intset, we can get
                 * a much faster path. */
                if (eleobj->encoding == OBJ_ENCODING_INT &&
                    setobj->encoding == OBJ_ENCODING_INTSET &&
                    !intsetFind((intset*)setobj->ptr,(long)eleobj->ptr))
                {
                    unlockDb(c->db);
                    break;
                /* else... object to object check is easy as we use the
                 * type agnostic API here. */
                } else if (!setTypeIsMember(setobj,eleobj)) {
                    unlockDb(c->db);
                    break;
                }
            }
            unlockDb(c->db);
        }

        /* Only take action when all sets contain the member */
        if (j == setnum) {
            if (encoding == OBJ_ENCODING_INTSET) {
                eleobj = createStringObjectFromLongLong(intobj);
                setTypeAdd(dstset,eleobj);
                freeObject(eleobj);
            } else {
                setTypeAdd(dstset,eleobj);
            }
            cardinality ++;
        }
    }
    setTypeReleaseIterator(si);
    freeObject(min_len_set);

done:
    if (dstkey) {
        fetchInternalDbByKey(c,dstkey);
        lockDbWrite(c->db);
        /* Store the resulting set into the target, if the intersection
            * is not an empty set. */
        int deleted = dbDelete(c->db,dstkey);
        if (dstset && setTypeSize(dstset) > 0) {
            dbAdd(c->db,dstkey,dstset);
            addReplyLongLong(c,setTypeSize(dstset));
            notifyKeyspaceEvent(NOTIFY_SET,"sinterstore",
                dstkey,c->db->id);
        } else {
            if (dstset) {
                freeObject(dstset);
                dstset = NULL;
            }
            addReply(c,shared.czero);
            if (deleted)
                notifyKeyspaceEvent(NOTIFY_GENERIC,"del",
                    dstkey,c->db->id);
        }
        signalModifiedKey(c->db,dstkey);
        unlockDb(c->db);
        c->vel->dirty++;
    } else {
        addReplyMultiBulkLen(c,cardinality);
        if (dstset && setTypeSize(dstset) > 0) {
            si = setTypeInitIterator(dstset);
            while((eleobj = setTypeNextObject(si)) != NULL) {
                addReplyBulk(c,eleobj);
                if (si->encoding == OBJ_ENCODING_INTSET) 
                    freeObject(eleobj); /* free this object for intset type */
            }
            setTypeReleaseIterator(si);
        }
        if (dstset) freeObject(dstset);
    }
}

void sinterCommand(client *c) {
    sinterGenericCommand(c,c->argv+1,c->argc-1,NULL);
}

void sinterstoreCommand(client *c) {
    sinterGenericCommand(c,c->argv+2,c->argc-2,c->argv[1]);
}

#define SET_OP_UNION 0
#define SET_OP_DIFF 1
#define SET_OP_INTER 2

void sunionDiffGenericCommand(client *c, robj **setkeys, int setnum,
                              robj *dstkey, int op) {
    setTypeIterator *si;
    robj *ele, *dstset = NULL;
    int j, cardinality = 0;
    int diff_algo = 1;
    robj *setobj;
    long long algo_one_work = 0, algo_two_work = 0;
    long long first_length = 0;

    for (j = 0; j < setnum; j++) {
        fetchInternalDbByKey(c,setkeys[j]);
        lockDbRead(c->db);
        setobj = lookupKeyRead(c->db,setkeys[j]);
        if (!setobj) {
            unlockDb(c->db);
            update_stats_add(c->vel->stats,keyspace_misses,1);
            continue;
        }
        if (checkType(c,setobj,OBJ_SET)) {
            unlockDb(c->db);
            update_stats_add(c->vel->stats,keyspace_hits,1);
            return;
        }
        
        if (op == SET_OP_DIFF) {
            if (j == 0) first_length = setTypeSize(setobj);
            algo_one_work += first_length;
            algo_two_work += setTypeSize(setobj);
        }

        unlockDb(c->db);
        update_stats_add(c->vel->stats,keyspace_hits,1);
    }

    /* Select what DIFF algorithm to use.
     *
     * Algorithm 1 is O(N*M) where N is the size of the element first set
     * and M the total number of sets.
     *
     * Algorithm 2 is O(N) where N is the total number of elements in all
     * the sets.
     *
     * We compute what is the best bet with the current input here. */
    if (op == SET_OP_DIFF) {
        /* Algorithm 1 has better constant times and performs less operations
         * if there are elements in common. Give it some advantage. */
        algo_one_work /= 2;
        diff_algo = (algo_one_work <= algo_two_work) ? 1 : 2;
    }

    /* We need a temp set object to store our union. If the dstkey
     * is not NULL (that is, we are inside an SUNIONSTORE operation) then
     * this set object will be the resulting object to set into the target key*/
    dstset = createIntsetObject();

    if (op == SET_OP_UNION) {
        /* Union is trivial, just add every element of every set to the
         * temporary set. */
        for (j = 0; j < setnum; j++) {
            fetchInternalDbByKey(c,setkeys[j]);
            lockDbRead(c->db);
            setobj = lookupKeyRead(c->db,setkeys[j]);

            if (!setobj) {
                unlockDb(c->db);
                continue;
            }
            if (checkType(c,setobj,OBJ_SET)) {
                unlockDb(c->db);
                freeObject(dstset);
                return;
            }

            si = setTypeInitIterator(setobj);
            while((ele = setTypeNextObject(si)) != NULL) {
                if (setTypeAdd(dstset,ele)) cardinality++;
                if (si->encoding == OBJ_ENCODING_INTSET)
                    freeObject(ele); /* free this object for intset type */
            }
            setTypeReleaseIterator(si);
            unlockDb(c->db);
        }
    } else if (op == SET_OP_DIFF && diff_algo == 1) {
        /* DIFF Algorithm 1:
         *
         * We perform the diff by iterating all the elements of the first set,
         * and only adding it to the target set if the element does not exist
         * into all the other sets.
         *
         * This way we perform at max N*M operations, where N is the size of
         * the first set, and M the number of sets. */
        robj *first_set;
        
        first_set = createIntsetObject();
        fetchInternalDbByKey(c,setkeys[0]);
        lockDbRead(c->db);
        setobj = lookupKeyRead(c->db,setkeys[0]);
        if (!setobj) {
            unlockDb(c->db);
            freeObject(first_set);
            goto done;
        }
        if (checkType(c,setobj,OBJ_SET)) {
            unlockDb(c->db);
            freeObject(dstset);
            freeObject(first_set);
            return;
        }
        si = setTypeInitIterator(setobj);
        while((ele = setTypeNextObject(si)) != NULL) {
            setTypeAdd(first_set,ele);
            if (si->encoding == OBJ_ENCODING_INTSET) 
                freeObject(ele); /* free this object for intset type */
        }
        setTypeReleaseIterator(si);
        unlockDb(c->db);

        si = setTypeInitIterator(first_set);
        while((ele = setTypeNextObject(si)) != NULL) {
            for (j = 1; j < setnum; j++) {
                fetchInternalDbByKey(c,setkeys[j]);
                lockDbRead(c->db);
                setobj = lookupKeyRead(c->db,setkeys[j]);
                if (!setobj) {
                    unlockDb(c->db);
                    continue;
                }
                if (checkType(c,setobj,OBJ_SET)) {
                    unlockDb(c->db);
                    freeObject(dstset);
                    freeObject(first_set);
                    if (si->encoding == OBJ_ENCODING_INTSET) 
                        freeObject(ele); /* free this object for intset type */
                    setTypeReleaseIterator(si);
                    return;
                }
                if (setTypeIsMember(setobj,ele)) {
                    unlockDb(c->db);
                    break;
                }
                unlockDb(c->db);
            }

            if (j == setnum) {
                /* There is no other set with this element. Add it. */
                setTypeAdd(dstset,ele);
                cardinality++;
            }

            if (si->encoding == OBJ_ENCODING_INTSET) 
                freeObject(ele); /* free this object for intset type */
        }
        setTypeReleaseIterator(si);
        freeObject(first_set);
    } else if (op == SET_OP_DIFF && diff_algo == 2) {
        /* DIFF Algorithm 2:
            *
            * Add all the elements of the first set to the auxiliary set.
            * Then remove all the elements of all the next sets from it.
            *
            * This is O(N) where N is the sum of all the elements in every
            * set. */
        for (j = 0; j < setnum; j++) {
            fetchInternalDbByKey(c,setkeys[0]);
            lockDbRead(c->db);
            setobj = lookupKeyRead(c->db,setkeys[0]);
            if (!setobj) {
                if (j == 0) {
                    unlockDb(c->db);
                    goto done;
                }
                
                unlockDb(c->db);
                continue; /* non existing keys are like empty sets */
            }
            if (checkType(c,setobj,OBJ_SET)) {
                unlockDb(c->db);
                freeObject(dstset);
                return;
            }

            si = setTypeInitIterator(setobj);
            while((ele = setTypeNextObject(si)) != NULL) {
                if (j == 0) {
                    if (setTypeAdd(dstset,ele)) cardinality++;
                } else {
                    if (setTypeRemove(dstset,ele)) cardinality--;
                }
                if (si->encoding == OBJ_ENCODING_INTSET) 
                    freeObject(ele); /* free this object for intset type */
            }
            setTypeReleaseIterator(si);

            /* Exit if result set is empty as any additional removal
                    * of elements will have no effect. */
            if (cardinality == 0) {
                unlockDb(c->db);
                break;
            }
            unlockDb(c->db);
        }
    }

done:
    /* Output the content of the resulting set, if not in STORE mode */
    if (!dstkey) {
        addReplyMultiBulkLen(c,cardinality);
        si = setTypeInitIterator(dstset);
        while((ele = setTypeNextObject(si)) != NULL) {
            addReplyBulk(c,ele);
            if (si->encoding == OBJ_ENCODING_INTSET) 
                freeObject(ele); /* free this object for intset type */
        }
        setTypeReleaseIterator(si);
        freeObject(dstset);
    } else {
        fetchInternalDbByKey(c,dstkey);
        lockDbWrite(c->db);
        /* If we have a target key where to store the resulting set
         * create this key with the result set inside */
        int deleted = dbDelete(c->db,dstkey);
        if (setTypeSize(dstset) > 0) {
            dbAdd(c->db,dstkey,dstset);
            addReplyLongLong(c,setTypeSize(dstset));
            notifyKeyspaceEvent(NOTIFY_SET,
                op == SET_OP_UNION ? "sunionstore" : "sdiffstore",
                dstkey,c->db->id);
        } else {
            freeObject(dstset);
            addReply(c,shared.czero);
            if (deleted)
                notifyKeyspaceEvent(NOTIFY_GENERIC,"del",
                    dstkey,c->db->id);
        }
        unlockDb(c->db);
        signalModifiedKey(c->db,dstkey);
        c->vel->dirty++;
    }
}

void sunionCommand(client *c) {
    sunionDiffGenericCommand(c,c->argv+1,c->argc-1,NULL,SET_OP_UNION);
}

void sunionstoreCommand(client *c) {
    sunionDiffGenericCommand(c,c->argv+2,c->argc-2,c->argv[1],SET_OP_UNION);
}

void sdiffCommand(client *c) {
    sunionDiffGenericCommand(c,c->argv+1,c->argc-1,NULL,SET_OP_DIFF);
}

void sdiffstoreCommand(client *c) {
    sunionDiffGenericCommand(c,c->argv+2,c->argc-2,c->argv[1],SET_OP_DIFF);
}

void sscanCommand(client *c) {
    scanGenericCommand(c,SCAN_TYPE_SET);
}
