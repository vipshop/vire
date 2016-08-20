#include <vr_core.h>

/*-----------------------------------------------------------------------------
 * String Commands
 *----------------------------------------------------------------------------*/

static int checkStringLength(client *c, long long size) {
    if (size > 512*1024*1024) {
        addReplyError(c,"string exceeds maximum allowed size (512MB)");
        return VR_ERROR;
    }
    return VR_OK;
}

/* The setGenericCommand() function implements the SET operation with different
 * options and variants. This function is called in order to implement the
 * following commands: SET, SETEX, PSETEX, SETNX.
 *
 * 'flags' changes the behavior of the command (NX or XX, see belove).
 *
 * 'expire' represents an expire to set in form of a Redis object as passed
 * by the user. It is interpreted according to the specified 'unit'.
 *
 * 'ok_reply' and 'abort_reply' is what the function will reply to the client
 * if the operation is performed, or when it is not because of NX or
 * XX flags.
 *
 * If ok_reply is NULL "+OK" is used.
 * If abort_reply is NULL, "$-1" is used. */

#define OBJ_SET_NO_FLAGS 0
#define OBJ_SET_NX (1<<0)     /* Set if key not exists. */
#define OBJ_SET_XX (1<<1)     /* Set if key exists. */
#define OBJ_SET_EX (1<<2)     /* Set if time in seconds is given */
#define OBJ_SET_PX (1<<3)     /* Set if time in ms in given */

void setGenericCommand(client *c, int flags, robj *key, robj *val, robj *expire, int unit, robj *ok_reply, robj *abort_reply) {
    long long milliseconds = 0; /* initialized to avoid any harmness warning */
    int expired = 0;
    int exist;

    if (expire) {
        if (getLongLongFromObjectOrReply(c, expire, &milliseconds, NULL) != VR_OK)
            return;
        if (milliseconds <= 0) {
            addReplyErrorFormat(c,"invalid expire time in %s",c->cmd->name);
            return;
        }
        if (unit == UNIT_SECONDS) milliseconds *= 1000;
    }

    fetchInternalDbByKey(c,key);
    lockDbWrite(c->db);
    if (lookupKeyWrite(c->db,key,&expired) == NULL)
        exist = 0;
    else
        exist = 1;

    if ((flags & OBJ_SET_NX && exist) ||
        (flags & OBJ_SET_XX && !exist))
    {
        unlockDb(c->db);
        if (expired) update_stats_add(c->vel->stats, expiredkeys, 1);
        addReply(c, abort_reply ? abort_reply : shared.nullbulk);
        return;
    }

    setKey(c->db,key,dupStringObjectUnconstant(val),NULL);
    c->vel->dirty++;
    if (expire) setExpire(c->db,key,vr_msec_now()+milliseconds);
    notifyKeyspaceEvent(NOTIFY_STRING,"set",key,c->db->id);
    if (expire) notifyKeyspaceEvent(NOTIFY_GENERIC,
        "expire",key,c->db->id);
    addReply(c, ok_reply ? ok_reply : shared.ok);
    unlockDb(c->db);
    if (expired) update_stats_add(c->vel->stats,expiredkeys,1);
}

/* SET key value [NX] [XX] [EX <seconds>] [PX <milliseconds>] */
void setCommand(client *c) {
    int j;
    robj *expire = NULL;
    int unit = UNIT_SECONDS;
    int flags = OBJ_SET_NO_FLAGS;

    for (j = 3; j < c->argc; j++) {
        char *a = c->argv[j]->ptr;
        robj *next = (j == c->argc-1) ? NULL : c->argv[j+1];

        if ((a[0] == 'n' || a[0] == 'N') &&
            (a[1] == 'x' || a[1] == 'X') && a[2] == '\0' &&
            !(flags & OBJ_SET_XX))
        {
            flags |= OBJ_SET_NX;
        } else if ((a[0] == 'x' || a[0] == 'X') &&
                   (a[1] == 'x' || a[1] == 'X') && a[2] == '\0' &&
                   !(flags & OBJ_SET_NX))
        {
            flags |= OBJ_SET_XX;
        } else if ((a[0] == 'e' || a[0] == 'E') &&
                   (a[1] == 'x' || a[1] == 'X') && a[2] == '\0' &&
                   !(flags & OBJ_SET_PX) && next)
        {
            flags |= OBJ_SET_EX;
            unit = UNIT_SECONDS;
            expire = next;
            j++;
        } else if ((a[0] == 'p' || a[0] == 'P') &&
                   (a[1] == 'x' || a[1] == 'X') && a[2] == '\0' &&
                   !(flags & OBJ_SET_EX) && next)
        {
            flags |= OBJ_SET_PX;
            unit = UNIT_MILLISECONDS;
            expire = next;
            j++;
        } else {
            addReply(c,shared.syntaxerr);
            return;
        }
    }

    c->argv[2] = tryObjectEncoding(c->argv[2]);
    setGenericCommand(c,flags,c->argv[1],c->argv[2],expire,unit,NULL,NULL);
}

void setnxCommand(client *c) {
    c->argv[2] = tryObjectEncoding(c->argv[2]);
    setGenericCommand(c,OBJ_SET_NX,c->argv[1],c->argv[2],NULL,0,shared.cone,shared.czero);
}

void setexCommand(client *c) {
    c->argv[3] = tryObjectEncoding(c->argv[3]);
    setGenericCommand(c,OBJ_SET_NO_FLAGS,c->argv[1],c->argv[3],c->argv[2],UNIT_SECONDS,NULL,NULL);
}

void psetexCommand(client *c) {
    c->argv[3] = tryObjectEncoding(c->argv[3]);
    setGenericCommand(c,OBJ_SET_NO_FLAGS,c->argv[1],c->argv[3],c->argv[2],UNIT_MILLISECONDS,NULL,NULL);
}

int getGenericCommand(client *c) {
    robj *o;
    
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.nullbulk)) == NULL) {
        return VR_OK;
    }
    
    if (o->type != OBJ_STRING) {
        addReply(c,shared.wrongtypeerr);
        return VR_ERROR;
    } else {
        addReplyBulk(c,o);
        return VR_OK;
    }
}

void getCommand(client *c) {
    robj *o;

    fetchInternalDbByKey(c,c->argv[1]);
    lockDbRead(c->db);
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.nullbulk)) == NULL) {
        unlockDb(c->db);
        update_stats_add(c->vel->stats, keyspace_misses, 1);
        return;
    }

    if (o->type != OBJ_STRING) {
        addReply(c,shared.wrongtypeerr);
    } else {
        addReplyBulk(c,o);
    }
    
    unlockDb(c->db);
    update_stats_add(c->vel->stats, keyspace_hits, 1);
}

void getsetCommand(client *c) {
    robj *key, *val;
    int expired = 0;
    int exist;

    key = c->argv[1];
    c->argv[2] = tryObjectEncoding(c->argv[2]);
    
    fetchInternalDbByKey(c,key);
    lockDbWrite(c->db);
    val = lookupKeyWriteOrReply(c,key,shared.nullbulk,&expired);
    if (val == NULL) {
        exist = 0;
        dbAdd(c->db,key,dupStringObjectUnconstant(c->argv[2]));
    } else {    
        exist = 1;
        if (val->type != OBJ_STRING) {
            addReply(c,shared.wrongtypeerr);
            goto end;
        }

        addReplyBulk(c,val);
        dbOverwrite(c->db,key,dupStringObjectUnconstant(c->argv[2]));
        removeExpire(c->db,key);
    }

    notifyKeyspaceEvent(NOTIFY_STRING,"set",c->argv[1],c->db->id);
    c->vel->dirty++;
    
end:
    unlockDb(c->db);
    if (expired) update_stats_add(c->vel->stats, expiredkeys, 1);
    if (exist)
        update_stats_add(c->vel->stats, keyspace_hits, 1);
    else
        update_stats_add(c->vel->stats, keyspace_misses, 1);
}

void setrangeCommand(client *c) {
    robj *o;
    long offset;
    sds value = c->argv[3]->ptr;
    int expired = 0;

    if (getLongFromObjectOrReply(c,c->argv[2],&offset,NULL) != VR_OK)
        return;

    if (offset < 0) {
        addReplyError(c,"offset is out of range");
        return;
    }

    fetchInternalDbByKey(c, c->argv[1]);
    lockDbWrite(c->db);
    o = lookupKeyWrite(c->db,c->argv[1],&expired);
    if (o == NULL) {
        /* Return 0 when setting nothing on a non-existing string */
        if (sdslen(value) == 0) {
            unlockDb(c->db);
            if (expired) update_stats_add(c->vel->stats,expiredkeys,1);
            addReply(c,shared.czero);
            return;
        }

        /* Return when the resulting string exceeds allowed size */
        if (checkStringLength(c,offset+sdslen(value)) != VR_OK) {
            unlockDb(c->db);
            if (expired) update_stats_add(c->vel->stats,expiredkeys,1);
            return;
        }

        o = createObject(OBJ_STRING,sdsnewlen(NULL, offset+sdslen(value)));
        dbAdd(c->db,c->argv[1],o);
    } else {
        size_t olen;

        /* Key exists, check type */
        if (checkType(c,o,OBJ_STRING)) {
            unlockDb(c->db);
            if (expired) update_stats_add(c->vel->stats,expiredkeys,1);
            return;
        }

        /* Return existing string length when setting nothing */
        olen = stringObjectLen(o);
        if (sdslen(value) == 0) {
            unlockDb(c->db);
            if (expired) update_stats_add(c->vel->stats,expiredkeys,1);
            addReplyLongLong(c,olen);
            return;
        }

        /* Return when the resulting string exceeds allowed size */
        if (checkStringLength(c,offset+sdslen(value)) != VR_OK) {
            unlockDb(c->db);
            if (expired) update_stats_add(c->vel->stats,expiredkeys,1);
            return;
        }

        /* Create a copy when the object is shared or encoded. */
        o = dbUnshareStringValue(c->db,c->argv[1],o);
    }

    if (sdslen(value) > 0) {
        o->ptr = sdsgrowzero(o->ptr,offset+sdslen(value));
        memcpy((char*)o->ptr+offset,value,sdslen(value));
        signalModifiedKey(c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_STRING,
            "setrange",c->argv[1],c->db->id);
        c->vel->dirty++;
    }
    addReplyLongLong(c,sdslen(o->ptr));
    unlockDb(c->db);
    if (expired) update_stats_add(c->vel->stats,expiredkeys,1);
}

void getrangeCommand(client *c) {
    robj *o;
    long long start, end;
    char *str, llbuf[32];
    size_t strlen;

    if (getLongLongFromObjectOrReply(c,c->argv[2],&start,NULL) != VR_OK)
        return;
    if (getLongLongFromObjectOrReply(c,c->argv[3],&end,NULL) != VR_OK)
        return;

    fetchInternalDbByKey(c, c->argv[1]);
    lockDbRead(c->db);
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptybulk)) == NULL) {
        unlockDb(c->db);
        update_stats_add(c->vel->stats, keyspace_misses, 1);
        return;
    } else if (checkType(c,o,OBJ_STRING)) {
        unlockDb(c->db);
        update_stats_add(c->vel->stats, keyspace_hits, 1);
        return;
    }

    if (o->encoding == OBJ_ENCODING_INT) {
        str = llbuf;
        strlen = ll2string(llbuf,sizeof(llbuf),(long)o->ptr);
    } else {
        str = o->ptr;
        strlen = sdslen(str);
    }

    /* Convert negative indexes */
    if (start < 0) start = strlen+start;
    if (end < 0) end = strlen+end;
    if (start < 0) start = 0;
    if (end < 0) end = 0;
    if ((unsigned long long)end >= strlen) end = strlen-1;

    /* Precondition: end >= 0 && end < strlen, so the only condition where
     * nothing can be returned is: start > end. */
    if (start > end || strlen == 0) {
        addReply(c,shared.emptybulk);
    } else {
        addReplyBulkCBuffer(c,(char*)str+start,end-start+1);
    }
    unlockDb(c->db);
    update_stats_add(c->vel->stats, keyspace_hits, 1);
}

void mgetCommand(client *c) {
    int j;

    addReplyMultiBulkLen(c,c->argc-1);
    for (j = 1; j < c->argc; j++) {
        fetchInternalDbByKey(c,c->argv[j]);
        lockDbRead(c->db);
        robj *o = lookupKeyRead(c->db,c->argv[j]);
        if (o == NULL) {
            unlockDb(c->db);
            update_stats_add(c->vel->stats, keyspace_misses, 1);
            addReply(c,shared.nullbulk);
        } else {
            if (o->type != OBJ_STRING) {
                addReply(c,shared.nullbulk);
            } else {
                addReplyBulk(c,o);
            }
            unlockDb(c->db);
            update_stats_add(c->vel->stats, keyspace_hits, 1);
        }
    }
}

void msetGenericCommand(client *c, int nx) {
    int j, busykeys = 0;

    if ((c->argc % 2) == 0) {
        addReplyError(c,"wrong number of arguments for MSET");
        return;
    }
    /* Handle the NX flag. The MSETNX semantic is to return zero and don't
     * set nothing at all if at least one already key exists. */
    if (nx) {
        for (j = 1; j < c->argc; j += 2) {
            if (lookupKeyWrite(c->db,c->argv[j],NULL) != NULL) {
                busykeys++;
            }
        }
        if (busykeys) {
            addReply(c, shared.czero);
            return;
        }
    }

    for (j = 1; j < c->argc; j += 2) {
        c->argv[j+1] = tryObjectEncoding(c->argv[j+1]);
        setKey(c->db,c->argv[j],c->argv[j+1],NULL);
        notifyKeyspaceEvent(NOTIFY_STRING,"set",c->argv[j],c->db->id);
    }
    server.dirty += (c->argc-1)/2;
    addReply(c, nx ? shared.cone : shared.ok);
}

void msetCommand(client *c) {
    int j;
    int expired = 0, expired_total = 0;

    if ((c->argc % 2) == 0) {
        addReplyError(c,"wrong number of arguments for MSET");
        return;
    }

    for (j = 1; j < c->argc; j += 2) {
        c->argv[j+1] = tryObjectEncoding(c->argv[j+1]);
        fetchInternalDbByKey(c,c->argv[j]);
        lockDbWrite(c->db);
        setKey(c->db,c->argv[j],dupStringObjectUnconstant(c->argv[j+1]),&expired);
        unlockDb(c->db);
        if (expired) expired_total ++;
        notifyKeyspaceEvent(NOTIFY_STRING,"set",c->argv[j],c->db->id);
    }

    if (expired_total) update_stats_add(c->vel->stats,expiredkeys,expired_total);
    c->vel->dirty += (c->argc-1)/2;
    addReply(c, shared.ok);
}

void msetnxCommand(client *c) {
    msetGenericCommand(c,1);
}

void incrDecrCommand(client *c, long long incr) {
    long long value, oldvalue;
    robj *o, *new;
    int expired = 0;

    fetchInternalDbByKey(c, c->argv[1]);
    lockDbWrite(c->db);
    o = lookupKeyWrite(c->db,c->argv[1],&expired);
    if (o != NULL && checkType(c,o,OBJ_STRING)) goto end;
    if (getLongLongFromObjectOrReply(c,o,&value,NULL) != VR_OK) goto end;

    oldvalue = value;
    if ((incr < 0 && oldvalue < 0 && incr < (LLONG_MIN-oldvalue)) ||
        (incr > 0 && oldvalue > 0 && incr > (LLONG_MAX-oldvalue))) {
        addReplyError(c,"increment or decrement would overflow");
        goto end;
    }
    value += incr;

    if (o && o->refcount == 1 && o->encoding == OBJ_ENCODING_INT &&
        (value < 0 || value >= OBJ_SHARED_INTEGERS) &&
        value >= LONG_MIN && value <= LONG_MAX)
    {
        new = o;
        o->ptr = (void*)((long)value);
    } else {
        new = createStringObjectFromLongLong(value);
        if (o) {
            dbOverwrite(c->db,c->argv[1],new);
        } else {
            dbAdd(c->db,c->argv[1],new);
        }
    }
    signalModifiedKey(c->db,c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_STRING,"incrby",c->argv[1],c->db->id);
    c->vel->dirty++;
    addReply(c,shared.colon);
    addReply(c,new);
    addReply(c,shared.crlf);
    
end:
    unlockDb(c->db);
    if (expired) update_stats_add(c->vel->stats, expiredkeys, 1);
}

void incrCommand(client *c) {
    incrDecrCommand(c,1);
}

void decrCommand(client *c) {
    incrDecrCommand(c,-1);
}

void incrbyCommand(client *c) {
    long long incr;

    if (getLongLongFromObjectOrReply(c, c->argv[2], &incr, NULL) != VR_OK) return;
    incrDecrCommand(c,incr);
}

void decrbyCommand(client *c) {
    long long incr;

    if (getLongLongFromObjectOrReply(c, c->argv[2], &incr, NULL) != VR_OK) return;
    incrDecrCommand(c,-incr);
}

void incrbyfloatCommand(client *c) {
    long double incr, value;
    robj *o, *new, *aux;
    int expired = 0;

    fetchInternalDbByKey(c, c->argv[1]);
    lockDbWrite(c->db);
    o = lookupKeyWrite(c->db,c->argv[1],&expired);
    if (o != NULL && checkType(c,o,OBJ_STRING)) goto end;
    if (getLongDoubleFromObjectOrReply(c,o,&value,NULL) != VR_OK ||
        getLongDoubleFromObjectOrReply(c,c->argv[2],&incr,NULL) != VR_OK)
        goto end;

    value += incr;
    if (isnan(value) || isinf(value)) {
        addReplyError(c,"increment would produce NaN or Infinity");
        goto end;
    }
    new = createStringObjectFromLongDouble(value,1);
    if (o)
        dbOverwrite(c->db,c->argv[1],new);
    else
        dbAdd(c->db,c->argv[1],new);
    signalModifiedKey(c->db,c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_STRING,"incrbyfloat",c->argv[1],c->db->id);
    c->vel->dirty++;
    addReplyBulk(c,new);

    /* Always replicate INCRBYFLOAT as a SET command with the final value
     * in order to make sure that differences in float precision or formatting
     * will not create differences in replicas or after an AOF restart. */
    aux = createStringObject("SET",3);
    rewriteClientCommandArgument(c,0,aux);
    aux = dupStringObjectUnconstant(new);
    rewriteClientCommandArgument(c,2,aux);

end:
    unlockDb(c->db);
    if (expired) update_stats_add(c->vel->stats,expiredkeys,1);
}

void appendCommand(client *c) {
    size_t totlen;
    robj *o, *append;
    int expired = 0;

    fetchInternalDbByKey(c, c->argv[1]);
    lockDbWrite(c->db);
    o = lookupKeyWrite(c->db,c->argv[1],&expired);
    if (o == NULL) {
        /* Create the key */
        c->argv[2] = tryObjectEncoding(c->argv[2]);
        dbAdd(c->db,c->argv[1],dupStringObjectUnconstant(c->argv[2]));
        totlen = stringObjectLen(c->argv[2]);
    } else {    
        /* Key exists, check type */
        if (checkType(c,o,OBJ_STRING))
            goto end;

        /* "append" is an argument, so always an sds */
        append = c->argv[2];
        totlen = stringObjectLen(o)+sdslen(append->ptr);
        if (checkStringLength(c,totlen) != VR_OK)
            goto end;

        /* Append the value */
        o = dbUnshareStringValue(c->db,c->argv[1],o);
        o->ptr = sdscatlen(o->ptr,append->ptr,sdslen(append->ptr));
        totlen = sdslen(o->ptr);
    }
    signalModifiedKey(c->db,c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_STRING,"append",c->argv[1],c->db->id);
    c->vel->dirty++;
    addReplyLongLong(c,totlen);

end:
    unlockDb(c->db);
    if (expired) update_stats_add(c->vel->stats,expiredkeys,1);
}

void strlenCommand(client *c) {
    robj *o;
    
    fetchInternalDbByKey(c, c->argv[1]);
    lockDbRead(c->db);
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL) {
        unlockDb(c->db);
        update_stats_add(c->vel->stats, keyspace_misses, 1);
        return;
    } else if(checkType(c,o,OBJ_STRING)) {
        unlockDb(c->db);
        update_stats_add(c->vel->stats, keyspace_hits, 1);
        return;
    }
    
    addReplyLongLong(c,stringObjectLen(o));
    unlockDb(c->db);
    update_stats_add(c->vel->stats, keyspace_hits, 1);
}
