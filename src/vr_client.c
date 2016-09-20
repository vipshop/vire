#include <vr_core.h>

int ncurr_cconn = 0;       /* current # client connections */

static void setProtocolError(client *c, int pos);

/* Return the size consumed from the allocator, for the specified SDS string,
 * including internal fragmentation. This function is used in order to compute
 * the client output buffer size. */
size_t sdsZmallocSize(sds s) {
    void *sh = sdsAllocPtr(s);
    return dmalloc_size(sh);
}

void *dupClientReplyValue(void *o) {
    return o;
}

void freeClientReplyValue(void *o) {
    freeObject(o);
}

int listMatchObjects(void *a, void *b) {
    return equalStringObjects(a,b);
}

client *createClient(vr_eventloop *vel, struct conn *conn) {
    client *c = dalloc(sizeof(client));

    /* passing -1 as fd it is possible to create a non connected client.
     * This is useful since all the commands needs to be executed
     * in the context of a client. When commands are executed in other
     * contexts (for instance a Lua script) we need a non connected client. */
    if (conn->sd != -1) {
        vr_set_nonblocking(conn->sd);
        vr_set_tcpnodelay(conn->sd);
        if (server.tcpkeepalive)
            vr_set_tcpkeepalive(conn->sd,server.tcpkeepalive,0,0);
        if (aeCreateFileEvent(vel->el,conn->sd,AE_READABLE,
            readQueryFromClient, c) == AE_ERR)
        {
            log_error("Unrecoverable error creating client ipfd file event.");
            dfree(c);
            return NULL;
        }
    }

    selectDb(c,0);
    c->id = vel->next_client_id++;
    c->conn = conn;
    c->vel = vel;
    c->scanid = -1;
    c->name = NULL;
    c->bufpos = 0;
    c->querybuf = sdsempty();
    c->querybuf_peak = 0;
    c->reqtype = 0;
    c->argc = 0;
    c->argv = NULL;
    c->cmd = c->lastcmd = NULL;
    c->multibulklen = 0;
    c->bulklen = -1;
    c->sentlen = 0;
    c->flags = 0;
    c->ctime = c->lastinteraction = vel->unixtime;
    c->authenticated = 0;
    c->replstate = REPL_STATE_NONE;
    c->repl_put_online_on_ack = 0;
    c->reploff = 0;
    c->repl_ack_off = 0;
    c->repl_ack_time = 0;
    c->slave_listening_port = 0;
    c->slave_capa = SLAVE_CAPA_NONE;
    c->reply = dlistCreate();
    c->reply_bytes = 0;
    c->obuf_soft_limit_reached_time = 0;
    dlistSetFreeMethod(c->reply,freeClientReplyValue);
    dlistSetDupMethod(c->reply,dupClientReplyValue);
    c->btype = BLOCKED_NONE;
    c->bpop.timeout = 0;
    c->bpop.keys = dictCreate(&setDictType,NULL);
    c->bpop.target = NULL;
    c->bpop.numreplicas = 0;
    c->bpop.reploffset = 0;
    c->woff = 0;
    c->watched_keys = dlistCreate();
    c->pubsub_channels = dictCreate(&setDictType,NULL);
    c->pubsub_patterns = dlistCreate();
    c->peerid = NULL;
    c->curidx = -1;
    c->taridx = -1;
    c->steps = 0;
    c->cache = NULL;
    dlistSetFreeMethod(c->pubsub_patterns,decrRefCountVoid);
    dlistSetMatchMethod(c->pubsub_patterns,listMatchObjects);
    if (conn->sd != -1) dlistAddNodeTail(vel->clients,c);
    initClientMultiState(c);
    return c;
}

/* This function is called every time we are going to transmit new data
 * to the client. The behavior is the following:
 *
 * If the client should receive new data (normal clients will) the function
 * returns VR_OK, and make sure to install the write handler in our event
 * loop so that when the socket is writable new data gets written.
 *
 * If the client should not receive new data, because it is a fake client
 * (used to load AOF in memory), a master or because the setup of the write
 * handler failed, the function returns VR_ERROR.
 *
 * The function may return VR_OK without actually installing the write
 * event handler in the following cases:
 *
 * 1) The event handler should already be installed since the output buffer
 *    already contained something.
 * 2) The client is a slave but not yet online, so we want to just accumulate
 *    writes in the buffer but not actually sending them yet.
 *
 * Typically gets called every time a reply is built, before adding more
 * data to the clients output buffers. If the function returns VR_ERROR no
 * data should be appended to the output buffers. */
int prepareClientToWrite(client *c) {
    /* If it's the Lua client we always return ok without installing any
     * handler since there is no socket at all. */
    if (c->flags & CLIENT_LUA) return VR_OK;

    /* CLIENT REPLY OFF / SKIP handling: don't send replies. */
    if (c->flags & (CLIENT_REPLY_OFF|CLIENT_REPLY_SKIP)) return VR_ERROR;

    /* Masters don't receive replies, unless CLIENT_MASTER_FORCE_REPLY flag
     * is set. */
    if ((c->flags & CLIENT_MASTER) &&
        !(c->flags & CLIENT_MASTER_FORCE_REPLY)) return VR_ERROR;

    if (c->conn->sd <= 0) return VR_ERROR; /* Fake client for AOF loading. */

    /* Schedule the client to write the output buffers to the socket only
     * if not already done (there were no pending writes already and the client
     * was yet not flagged), and, for slaves, if the slave can actually
     * receive writes at this stage. */
    if (!clientHasPendingReplies(c) &&
        !(c->flags & CLIENT_PENDING_WRITE) &&
        (c->replstate == REPL_STATE_NONE ||
         (c->replstate == SLAVE_STATE_ONLINE && !c->repl_put_online_on_ack)))
    {
        /* Here instead of installing the write handler, we just flag the
         * client and put it into a list of clients that have something
         * to write to the socket. This way before re-entering the event
         * loop, we can try to directly write to the client sockets avoiding
         * a system call. We'll only really install the write handler if
         * we'll not be able to write the whole reply at once. */
        c->flags |= CLIENT_PENDING_WRITE;
        dlistAddNodeHead(c->vel->clients_pending_write,c);
    }

    /* Authorize the caller to queue in the output buffer of this client. */
    return VR_OK;
}

/* Create a duplicate of the last object in the reply list when
 * it is not exclusively owned by the reply list. */
robj *dupLastObjectIfNeeded(dlist *reply) {
    robj *new, *cur;
    dlistNode *ln;
    ASSERT(dlistLength(reply) > 0);
    ln = dlistLast(reply);
    cur = dlistNodeValue(ln);
    if (cur->constant) {
        new = dupStringObject(cur);
        dlistNodeValue(ln) = new;
    }
    return dlistNodeValue(ln);
}

/* -----------------------------------------------------------------------------
 * Low level functions to add more data to output buffers.
 * -------------------------------------------------------------------------- */

int _addReplyToBuffer(client *c, const char *s, size_t len) {
    size_t available = sizeof(c->buf)-c->bufpos;

    if (c->flags & CLIENT_CLOSE_AFTER_REPLY) return VR_OK;

    /* If there already are entries in the reply list, we cannot
     * add anything more to the static buffer. */
    if (dlistLength(c->reply) > 0) return VR_ERROR;

    /* Check that the buffer has enough space available for this string. */
    if (len > available) return VR_ERROR;

    memcpy(c->buf+c->bufpos,s,len);
    c->bufpos+=len;
    return VR_OK;
}

void _addReplyObjectToList(client *c, robj *o) {
    robj *tail, *obj;

    if (c->flags & CLIENT_CLOSE_AFTER_REPLY) return;

    if (dlistLength(c->reply) == 0) {
        if (o->constant)
            obj = o;
        else
            obj = dupStringObject(o);
        dlistAddNodeTail(c->reply,obj);
        c->reply_bytes += getStringObjectSdsUsedMemory(obj);
    } else {
        tail = dlistNodeValue(dlistLast(c->reply));

        /* Append to this object when possible. */
        if (tail->ptr != NULL &&
            tail->encoding == OBJ_ENCODING_RAW &&
            sdslen(tail->ptr)+sdslen(o->ptr) <= PROTO_REPLY_CHUNK_BYTES)
        {
            c->reply_bytes -= sdsZmallocSize(tail->ptr);
            tail = dupLastObjectIfNeeded(c->reply);
            tail->ptr = sdscatlen(tail->ptr,o->ptr,sdslen(o->ptr));
            c->reply_bytes += sdsZmallocSize(tail->ptr);
        } else {
            if (o->constant)
                obj = o;
            else
                obj = dupStringObject(o);
            dlistAddNodeTail(c->reply,obj);
            c->reply_bytes += getStringObjectSdsUsedMemory(obj);
        }
    }
    asyncCloseClientOnOutputBufferLimitReached(c);
}

/* This method takes responsibility over the sds. When it is no longer
 * needed it will be free'd, otherwise it ends up in a robj. */
void _addReplySdsToList(client *c, sds s) {
    robj *tail;

    if (c->flags & CLIENT_CLOSE_AFTER_REPLY) {
        sdsfree(s);
        return;
    }

    if (dlistLength(c->reply) == 0) {
        dlistAddNodeTail(c->reply,createObject(OBJ_STRING,s));
        c->reply_bytes += sdsZmallocSize(s);
    } else {
        tail = dlistNodeValue(dlistLast(c->reply));

        /* Append to this object when possible. */
        if (tail->ptr != NULL && tail->encoding == OBJ_ENCODING_RAW &&
            sdslen(tail->ptr)+sdslen(s) <= PROTO_REPLY_CHUNK_BYTES)
        {
            c->reply_bytes -= sdsZmallocSize(tail->ptr);
            tail = dupLastObjectIfNeeded(c->reply);
            tail->ptr = sdscatlen(tail->ptr,s,sdslen(s));
            c->reply_bytes += sdsZmallocSize(tail->ptr);
            sdsfree(s);
        } else {
            dlistAddNodeTail(c->reply,createObject(OBJ_STRING,s));
            c->reply_bytes += sdsZmallocSize(s);
        }
    }
    asyncCloseClientOnOutputBufferLimitReached(c);
}

void _addReplyStringToList(client *c, const char *s, size_t len) {
    robj *tail;

    if (c->flags & CLIENT_CLOSE_AFTER_REPLY) return;

    if (dlistLength(c->reply) == 0) {
        robj *o = createStringObject(s,len);

        dlistAddNodeTail(c->reply,o);
        c->reply_bytes += getStringObjectSdsUsedMemory(o);
    } else {
        tail = dlistNodeValue(dlistLast(c->reply));

        /* Append to this object when possible. */
        if (tail->ptr != NULL && tail->encoding == OBJ_ENCODING_RAW &&
            sdslen(tail->ptr)+len <= PROTO_REPLY_CHUNK_BYTES)
        {
            c->reply_bytes -= sdsZmallocSize(tail->ptr);
            tail = dupLastObjectIfNeeded(c->reply);
            tail->ptr = sdscatlen(tail->ptr,s,len);
            c->reply_bytes += sdsZmallocSize(tail->ptr);
        } else {
            robj *o = createStringObject(s,len);

            dlistAddNodeTail(c->reply,o);
            c->reply_bytes += getStringObjectSdsUsedMemory(o);
        }
    }
    asyncCloseClientOnOutputBufferLimitReached(c);
}

/* -----------------------------------------------------------------------------
 * Higher level functions to queue data on the client output buffer.
 * The following functions are the ones that commands implementations will call.
 * -------------------------------------------------------------------------- */

void addReply(client *c, robj *obj) {
    if (prepareClientToWrite(c) != VR_OK) return;

    /* This is an important place where we can avoid copy-on-write
     * when there is a saving child running, avoiding touching the
     * refcount field of the object if it's not needed.
     *
     * If the encoding is RAW and there is room in the static buffer
     * we'll be able to send the object to the client without
     * messing with its page. */
    if (sdsEncodedObject(obj)) {
        if (_addReplyToBuffer(c,obj->ptr,sdslen(obj->ptr)) != VR_OK)
            _addReplyObjectToList(c,obj);
    } else if (obj->encoding == OBJ_ENCODING_INT) {
        robj *obj_new;
        /* Optimization: if there is room in the static buffer for 32 bytes
         * (more than the max chars a 64 bit integer can take as string) we
         * avoid decoding the object and go for the lower level approach. */
        if (dlistLength(c->reply) == 0 && (sizeof(c->buf) - c->bufpos) >= 32) {
            char buf[32];
            int len;

            len = ll2string(buf,sizeof(buf),(long)obj->ptr);
            if (_addReplyToBuffer(c,buf,len) == VR_OK)
                return;
            /* else... continue with the normal code path, but should never
             * happen actually since we verified there is room. */
        }
        obj_new = getDecodedObject(obj);
        if (_addReplyToBuffer(c,obj_new->ptr,sdslen(obj_new->ptr)) != VR_OK)
            _addReplyObjectToList(c,obj_new);
        if (obj_new != obj) freeObject(obj_new);
    } else {
        serverPanic("Wrong obj->encoding in addReply()");
    }
}

void addReplySds(client *c, sds s) {
    if (prepareClientToWrite(c) != VR_OK) {
        /* The caller expects the sds to be free'd. */
        sdsfree(s);
        return;
    }
    if (_addReplyToBuffer(c,s,sdslen(s)) == VR_OK) {
        sdsfree(s);
    } else {
        /* This method free's the sds when it is no longer needed. */
        _addReplySdsToList(c,s);
    }
}

void addReplyString(client *c, const char *s, size_t len) {
    if (prepareClientToWrite(c) != VR_OK) return;
    if (_addReplyToBuffer(c,s,len) != VR_OK)
        _addReplyStringToList(c,s,len);
}

void addReplyErrorLength(client *c, const char *s, size_t len) {
    addReplyString(c,"-ERR ",5);
    addReplyString(c,s,len);
    addReplyString(c,"\r\n",2);
}

void addReplyError(client *c, const char *err) {
    addReplyErrorLength(c,err,strlen(err));
}

void addReplyErrorFormat(client *c, const char *fmt, ...) {
    size_t l, j;
    va_list ap;
    va_start(ap,fmt);
    sds s = sdscatvprintf(sdsempty(),fmt,ap);
    va_end(ap);
    /* Make sure there are no newlines in the string, otherwise invalid protocol
     * is emitted. */
    l = sdslen(s);
    for (j = 0; j < l; j++) {
        if (s[j] == '\r' || s[j] == '\n') s[j] = ' ';
    }
    addReplyErrorLength(c,s,sdslen(s));
    sdsfree(s);
}

void addReplyStatusLength(client *c, const char *s, size_t len) {
    addReplyString(c,"+",1);
    addReplyString(c,s,len);
    addReplyString(c,"\r\n",2);
}

void addReplyStatus(client *c, const char *status) {
    addReplyStatusLength(c,status,strlen(status));
}

void addReplyStatusFormat(client *c, const char *fmt, ...) {
    va_list ap;
    va_start(ap,fmt);
    sds s = sdscatvprintf(sdsempty(),fmt,ap);
    va_end(ap);
    addReplyStatusLength(c,s,sdslen(s));
    sdsfree(s);
}

/* Adds an empty object to the reply list that will contain the multi bulk
 * length, which is not known when this function is called. */
void *addDeferredMultiBulkLength(client *c) {
    /* Note that we install the write event here even if the object is not
     * ready to be sent, since we are sure that before returning to the
     * event loop setDeferredMultiBulkLength() will be called. */
    if (prepareClientToWrite(c) != VR_OK) return NULL;
    dlistAddNodeTail(c->reply,createObject(OBJ_STRING,NULL));
    return dlistLast(c->reply);
}

/* Populate the length object and try gluing it to the next chunk. */
void setDeferredMultiBulkLength(client *c, void *node, long length) {
    dlistNode *ln = (dlistNode*)node;
    robj *len, *next;

    /* Abort when *node is NULL (see addDeferredMultiBulkLength). */
    if (node == NULL) return;

    len = dlistNodeValue(ln);
    len->ptr = sdscatprintf(sdsempty(),"*%ld\r\n",length);
    len->encoding = OBJ_ENCODING_RAW; /* in case it was an EMBSTR. */
    c->reply_bytes += sdsZmallocSize(len->ptr);
    if (ln->next != NULL) {
        next = dlistNodeValue(ln->next);

        /* Only glue when the next node is non-NULL (an sds in this case) */
        if (next->ptr != NULL) {
            c->reply_bytes -= sdsZmallocSize(len->ptr);
            c->reply_bytes -= getStringObjectSdsUsedMemory(next);
            len->ptr = sdscatlen(len->ptr,next->ptr,sdslen(next->ptr));
            c->reply_bytes += sdsZmallocSize(len->ptr);
            dlistDelNode(c->reply,ln->next);
        }
    }
    asyncCloseClientOnOutputBufferLimitReached(c);
}

/* Add a double as a bulk reply */
void addReplyDouble(client *c, double d) {
    char dbuf[128], sbuf[128];
    int dlen, slen;
    if (isinf(d)) {
        /* Libc in odd systems (Hi Solaris!) will format infinite in a
         * different way, so better to handle it in an explicit way. */
        addReplyBulkCString(c, d > 0 ? "inf" : "-inf");
    } else {
        dlen = snprintf(dbuf,sizeof(dbuf),"%.17g",d);
        slen = snprintf(sbuf,sizeof(sbuf),"$%d\r\n%s\r\n",dlen,dbuf);
        addReplyString(c,sbuf,slen);
    }
}

/* Add a long double as a bulk reply, but uses a human readable formatting
 * of the double instead of exposing the crude behavior of doubles to the
 * dear user. */
void addReplyHumanLongDouble(client *c, long double d) {
    robj *o = createStringObjectFromLongDouble(d,1);
    addReplyBulk(c,o);
    decrRefCount(o);
}

/* Add a long long as integer reply or bulk len / multi bulk count.
 * Basically this is used to output <prefix><long long><crlf>. */
void addReplyLongLongWithPrefix(client *c, long long ll, char prefix) {
    char buf[128];
    int len;

    /* Things like $3\r\n or *2\r\n are emitted very often by the protocol
     * so we have a few shared objects to use if the integer is small
     * like it is most of the times. */
    if (prefix == '*' && ll < OBJ_SHARED_BULKHDR_LEN && ll >= 0) {
        addReply(c,shared.mbulkhdr[ll]);
        return;
    } else if (prefix == '$' && ll < OBJ_SHARED_BULKHDR_LEN && ll >= 0) {
        addReply(c,shared.bulkhdr[ll]);
        return;
    }

    buf[0] = prefix;
    len = ll2string(buf+1,sizeof(buf)-1,ll);
    buf[len+1] = '\r';
    buf[len+2] = '\n';
    addReplyString(c,buf,len+3);
}

void addReplyLongLong(client *c, long long ll) {
    if (ll == 0)
        addReply(c,shared.czero);
    else if (ll == 1)
        addReply(c,shared.cone);
    else
        addReplyLongLongWithPrefix(c,ll,':');
}

void addReplyMultiBulkLen(client *c, long length) {
    if (length < OBJ_SHARED_BULKHDR_LEN)
        addReply(c,shared.mbulkhdr[length]);
    else
        addReplyLongLongWithPrefix(c,length,'*');
}

/* Create the length prefix of a bulk reply, example: $2234 */
void addReplyBulkLen(client *c, robj *obj) {
    size_t len;

    if (sdsEncodedObject(obj)) {
        len = sdslen(obj->ptr);
    } else {
        long n = (long)obj->ptr;

        /* Compute how many bytes will take this integer as a radix 10 string */
        len = 1;
        if (n < 0) {
            len++;
            n = -n;
        }
        while((n = n/10) != 0) {
            len++;
        }
    }

    if (len < OBJ_SHARED_BULKHDR_LEN)
        addReply(c,shared.bulkhdr[len]);
    else
        addReplyLongLongWithPrefix(c,len,'$');
}

/* Add a Redis Object as a bulk reply */
void addReplyBulk(client *c, robj *obj) {
    addReplyBulkLen(c,obj);
    addReply(c,obj);
    addReply(c,shared.crlf);
}

/* Add a C buffer as bulk reply */
void addReplyBulkCBuffer(client *c, const void *p, size_t len) {
    addReplyLongLongWithPrefix(c,len,'$');
    addReplyString(c,p,len);
    addReply(c,shared.crlf);
}

/* Add sds to reply (takes ownership of this sds and frees it) */
void addReplyBulkSds(client *c, sds s)  {
    addReplySds(c,sdscatfmt(sdsempty(),"$%u\r\n",
        (unsigned long)sdslen(s)));
    addReplySds(c,s);
    addReply(c,shared.crlf);
}

/* Add a C nul term string as bulk reply */
void addReplyBulkCString(client *c, const char *s) {
    if (s == NULL) {
        addReply(c,shared.nullbulk);
    } else {
        addReplyBulkCBuffer(c,s,strlen(s));
    }
}

/* Add a long long as a bulk reply */
void addReplyBulkLongLong(client *c, long long ll) {
    char buf[64];
    int len;

    len = ll2string(buf,64,ll);
    addReplyBulkCBuffer(c,buf,len);
}

/* Copy 'src' client output buffers into 'dst' client output buffers.
 * The function takes care of freeing the old output buffers of the
 * destination client. */
void copyClientOutputBuffer(client *dst, client *src) {
    dlistRelease(dst->reply);
    dst->reply = dlistDup(src->reply);
    memcpy(dst->buf,src->buf,src->bufpos);
    dst->bufpos = src->bufpos;
    dst->reply_bytes = src->reply_bytes;
}

/* Return true if the specified client has pending reply buffers to write to
 * the socket. */
int clientHasPendingReplies(client *c) {
    return c->bufpos || dlistLength(c->reply);
}

static void freeClientArgv(client *c) {
    int j;
    for (j = 0; j < c->argc; j++)
        freeObject(c->argv[j]);
    c->argc = 0;
    c->cmd = NULL;
}

/* Close all the slaves connections. This is useful in chained replication
 * when we resync with our own master and want to force all our slaves to
 * resync with us as well. */
void disconnectSlaves(void) {
    while (dlistLength(repl.slaves)) {
        dlistNode *ln = dlistFirst(repl.slaves);
        freeClient((client*)ln->value);
    }
}

/* Remove the specified client from eventloop lists where the client could
 * be referenced from this eventloop, not including the Pub/Sub channels.
 * This is used by clients jump between workers. */
void unlinkClientFromEventloop(client *c) {
    dlistNode *ln;
    vr_eventloop *vel = c->vel;

    c->vel = NULL;

    if (c->steps >= 1) return;
    
    /* If this is marked as current client unset it. */
    if (vel->current_client == c) vel->current_client = NULL;

    /* Certain operations must be done only if the client has an active socket.
     * If the client was already unlinked or if it's a "fake client" the
     * fd is already set to -1. */
    if (c->conn->sd != -1) {
        /* Remove from the list of active clients. */
        ln = dlistSearchKey(vel->clients,c);
        ASSERT(ln != NULL);
        dlistDelNode(vel->clients,ln);

        /* Unregister async I/O handlers and close the socket. */
        aeDeleteFileEvent(vel->el,c->conn->sd,AE_READABLE);
        aeDeleteFileEvent(vel->el,c->conn->sd,AE_WRITABLE);
    }

    /* Remove from the list of pending writes if needed. */
    if (c->flags & CLIENT_PENDING_WRITE) {
        ln = dlistSearchKey(vel->clients_pending_write,c);
        ASSERT(ln != NULL);
        dlistDelNode(vel->clients_pending_write,ln);
        c->flags &= ~CLIENT_PENDING_WRITE;
    }

    /* When client was just unblocked because of a blocking operation,
     * remove it from the list of unblocked clients. */
    if (c->flags & CLIENT_UNBLOCKED) {
        ln = dlistSearchKey(vel->unblocked_clients,c);
        ASSERT(ln != NULL);
        dlistDelNode(vel->unblocked_clients,ln);
        c->flags &= ~CLIENT_UNBLOCKED;
    }
}

void linkClientToEventloop(client *c,vr_eventloop *vel) {
    dlistPush(vel->clients,c);
    c->vel = vel;
    if (aeCreateFileEvent(vel->el,c->conn->sd,AE_READABLE,
        readQueryFromClient,c) == AE_ERR)
    {
        freeClient(c);
        return;
    }

    /* Handle the remain query buffer */
    processInputBuffer(c);
    if (c->flags&CLIENT_JUMP) {
        dispatch_conn_exist(c,c->taridx);
    } else {
        if (clientHasPendingReplies(c) && 
            !(c->flags&CLIENT_PENDING_WRITE)) {
            if (aeCreateFileEvent(vel->el, c->conn->sd, AE_WRITABLE,
                sendReplyToClient, c) == AE_ERR)
            {
                freeClientAsync(c);
            }
        }
    }
}

/* Remove the specified client from global lists where the client could
 * be referenced, not including the Pub/Sub channels.
 * This is used by freeClient() and replicationCacheMaster(). */
void unlinkClient(client *c) {
    dlistNode *ln;

    /* If this is marked as current client unset it. */
    if (c->vel->current_client == c) c->vel->current_client = NULL;

    /* Certain operations must be done only if the client has an active socket.
     * If the client was already unlinked or if it's a "fake client" the
     * fd is already set to -1. */
    if (c->conn->sd != -1) {
        /* Remove from the list of active clients. */
        ln = dlistSearchKey(c->vel->clients,c);
        ASSERT(ln != NULL);
        dlistDelNode(c->vel->clients,ln);

        /* Unregister async I/O handlers and close the socket. */
        aeDeleteFileEvent(c->vel->el,c->conn->sd,AE_READABLE);
        aeDeleteFileEvent(c->vel->el,c->conn->sd,AE_WRITABLE);
        conn_put(c->conn);
        c->conn = NULL;
    }

    /* Remove from the list of pending writes if needed. */
    if (c->flags & CLIENT_PENDING_WRITE) {
        ln = dlistSearchKey(c->vel->clients_pending_write,c);
        ASSERT(ln != NULL);
        dlistDelNode(c->vel->clients_pending_write,ln);
        c->flags &= ~CLIENT_PENDING_WRITE;
    }

    /* When client was just unblocked because of a blocking operation,
     * remove it from the list of unblocked clients. */
    if (c->flags & CLIENT_UNBLOCKED) {
        ln = dlistSearchKey(c->vel->unblocked_clients,c);
        ASSERT(ln != NULL);
        dlistDelNode(c->vel->unblocked_clients,ln);
        c->flags &= ~CLIENT_UNBLOCKED;
    }
}

void freeClient(client *c) {
    dlistNode *ln;

    /* If it is our master that's beging disconnected we should make sure
     * to cache the state to try a partial resynchronization later.
     *
     * Note that before doing this we make sure that the client is not in
     * some unexpected state, by checking its flags. */
    if (repl.role == REPLICATION_ROLE_MASTER && c->flags & CLIENT_MASTER) {
        log_warn("connection with master lost.");
        if (!(c->flags & (CLIENT_CLOSE_AFTER_REPLY|
                          CLIENT_CLOSE_ASAP|
                          CLIENT_BLOCKED|
                          CLIENT_UNBLOCKED)))
        {
            replicationCacheMaster(c);
            return;
        }
    }

    /* Log link disconnection with slave */
    if ((c->flags & CLIENT_SLAVE) && !(c->flags & CLIENT_MONITOR)) {
        log_warn("connection with slave %s lost.",
            replicationGetSlaveName(c));
    }

    /* Free the query buffer */
    sdsfree(c->querybuf);
    c->querybuf = NULL;

    /* Deallocate structures used to block on blocking ops. */
    if (c->flags & CLIENT_BLOCKED) unblockClient(c);
    dictRelease(c->bpop.keys);

    /* UNWATCH all the keys */
    unwatchAllKeys(c);
    dlistRelease(c->watched_keys);

    /* Unsubscribe from all the pubsub channels */
    pubsubUnsubscribeAllChannels(c,0);
    pubsubUnsubscribeAllPatterns(c,0);
    dictRelease(c->pubsub_channels);
    dlistRelease(c->pubsub_patterns);

    /* Free data structures. */
    dlistRelease(c->reply);
    freeClientArgv(c);

    /* Unlink the client: this will close the socket, remove the I/O
     * handlers, and remove references of the client from different
     * places where active clients may be referenced. */
    unlinkClient(c);

    /* Master/slave cleanup Case 1:
     * we lost the connection with a slave. */
    if (c->flags & CLIENT_SLAVE) {
        if (c->replstate == SLAVE_STATE_SEND_BULK) {
            if (c->repldbfd != -1) close(c->repldbfd);
            if (c->replpreamble) sdsfree(c->replpreamble);
        }
        dlist *l = (c->flags & CLIENT_MONITOR) ? server.monitors : repl.slaves;
        ln = dlistSearchKey(l,c);
        ASSERT(ln != NULL);
        dlistDelNode(l,ln);
        /* We need to remember the time when we started to have zero
         * attached slaves, as after some time we'll free the replication
         * backlog. */
        if (c->flags & CLIENT_SLAVE && dlistLength(repl.slaves) == 0)
            repl.repl_no_slaves_since = c->vel->unixtime;
        refreshGoodSlavesCount();
    }

    /* Master/slave cleanup Case 2:
     * we lost the connection with the master. */
    if (c->flags & CLIENT_MASTER) replicationHandleMasterDisconnection();

    /* If this client was scheduled for async freeing we need to remove it
     * from the queue. */
    if (c->flags & CLIENT_CLOSE_ASAP) {
        ln = dlistSearchKey(c->vel->clients_to_close,c);
        ASSERT(ln != NULL);
        dlistDelNode(c->vel->clients_to_close,ln);
    }

    /* Release other dynamically allocated client structure fields,
     * and finally release the client structure itself. */
    if (c->name) freeObject(c->name);
    if (c->argv) dfree(c->argv);
    freeClientMultiState(c);
    sdsfree(c->peerid);
    dfree(c);
}

/* Schedule a client to free it at a safe time in the serverCron() function.
 * This function is useful when we need to terminate a client but we are in
 * a context where calling freeClient() is not possible, because the client
 * should be valid for the continuation of the flow of the program. */
void freeClientAsync(client *c) {
    if (c->flags & CLIENT_CLOSE_ASAP || c->flags & CLIENT_LUA) return;
    c->flags |= CLIENT_CLOSE_ASAP;
    dlistAddNodeTail(c->vel->clients_to_close,c);
}

void freeClientsInAsyncFreeQueue(vr_eventloop *vel) {
    while (dlistLength(vel->clients_to_close)) {
        dlistNode *ln = dlistFirst(vel->clients_to_close);
        client *c = dlistNodeValue(ln);

        c->flags &= ~CLIENT_CLOSE_ASAP;
        freeClient(c);
        dlistDelNode(vel->clients_to_close,ln);
    }
}

/* Write data in output buffers to client. Return VR_OK if the client
 * is still valid after the call, VR_ERROR if it was freed. */
int writeToClient(int fd, client *c, int handler_installed) {
    ssize_t nwritten = 0, totwritten = 0;
    size_t objlen;
    size_t objmem;
    robj *o;
    long long maxmemory;

    maxmemory = c->vel->cc.maxmemory;
    while(clientHasPendingReplies(c)) {
        if (c->bufpos > 0) {
            nwritten = vr_write(fd,c->buf+c->sentlen,c->bufpos-c->sentlen);
            if (nwritten <= 0) break;
            c->sentlen += nwritten;
            totwritten += nwritten;

            /* If the buffer was sent, set bufpos to zero to continue with
             * the remainder of the reply. */
            if ((int)c->sentlen == c->bufpos) {
                c->bufpos = 0;
                c->sentlen = 0;
            }
        } else {
            o = dlistNodeValue(dlistFirst(c->reply));
            objlen = sdslen(o->ptr);
            objmem = getStringObjectSdsUsedMemory(o);

            if (objlen == 0) {
                dlistDelNode(c->reply,dlistFirst(c->reply));
                c->reply_bytes -= objmem;
                continue;
            }

            nwritten = vr_write(fd, ((char*)o->ptr)+c->sentlen,objlen-c->sentlen);
            if (nwritten <= 0) break;
            c->sentlen += nwritten;
            totwritten += nwritten;

            /* If we fully sent the object on head go to the next one */
            if (c->sentlen == objlen) {
                dlistDelNode(c->reply,dlistFirst(c->reply));
                c->sentlen = 0;
                c->reply_bytes -= objmem;
            }
        }
        /* Note that we avoid to send more than NET_MAX_WRITES_PER_EVENT
         * bytes, in a single threaded server it's a good idea to serve
         * other clients as well, even if a very large request comes from
         * super fast link that is always able to accept data (in real world
         * scenario think about 'KEYS *' against the loopback interface).
         *
         * However if we are over the maxmemory limit we ignore that and
         * just deliver as much data as it is possible to deliver. */
        if (totwritten > NET_MAX_WRITES_PER_EVENT &&
            (maxmemory == 0 || dalloc_used_memory() < maxmemory)) 
            break;
    }
    if (nwritten == -1) {
        if (errno == EAGAIN) {
            nwritten = 0;
        } else {
            log_debug(LOG_VERB,
                "error writing to client: %s", strerror(errno));
            freeClient(c);
            return VR_ERROR;
        }
    }
    if (totwritten > 0) {
        update_stats_add(c->vel->stats, net_output_bytes, (long long)totwritten);
        /* For clients representing masters we don't count sending data
         * as an interaction, since we always send REPLCONF ACK commands
         * that take some time to just fill the socket output buffer.
         * We just rely on data / pings received for timeout detection. */
        if (!(c->flags & CLIENT_MASTER)) c->lastinteraction = c->vel->unixtime;
    }
    if (!clientHasPendingReplies(c)) {
        c->sentlen = 0;
        if (handler_installed) aeDeleteFileEvent(c->vel->el,c->conn->sd,AE_WRITABLE);

        /* Close connection after entire reply has been sent. */
        if (c->flags & CLIENT_CLOSE_AFTER_REPLY) {
            freeClient(c);
            return VR_ERROR;
        }
    }
    return VR_OK;
}

/* Write event handler. Just send data to the client. */
void sendReplyToClient(aeEventLoop *el, int fd, void *privdata, int mask) {
    UNUSED(el);
    UNUSED(mask);
    writeToClient(fd,privdata,1);
}

/* This function is called just before entering the event loop, in the hope
 * we can just write the replies to the client output buffer without any
 * need to use a syscall in order to install the writable event handler,
 * get it called, and so forth. */
int handleClientsWithPendingWrites(vr_eventloop *vel) {
    dlistIter li;
    dlistNode *ln;
    int processed = dlistLength(vel->clients_pending_write);

    dlistRewind(vel->clients_pending_write,&li);
    while((ln = dlistNext(&li))) {
        client *c = dlistNodeValue(ln);
        c->flags &= ~CLIENT_PENDING_WRITE;
        dlistDelNode(vel->clients_pending_write,ln);

        /* Try to write buffers to the client socket. */
        if (writeToClient(c->conn->sd,c,0) == VR_ERROR) continue;

        /* If there is nothing left, do nothing. Otherwise install
         * the write handler. */
        if (clientHasPendingReplies(c) &&
            aeCreateFileEvent(vel->el, c->conn->sd, AE_WRITABLE,
                sendReplyToClient, c) == AE_ERR)
        {
            freeClientAsync(c);
        }
    }
    return processed;
}

/* resetClient prepare the client to process the next command */
void resetClient(client *c) {
    redisCommandProc *prevcmd = c->cmd ? c->cmd->proc : NULL;

    if (c->flags&CLIENT_JUMP)
        return;

    freeClientArgv(c);
    c->reqtype = 0;
    c->multibulklen = 0;
    c->bulklen = -1;

    /* Remove the CLIENT_REPLY_SKIP flag if any so that the reply
     * to the next command will be sent, but set the flag if the command
     * we just processed was "CLIENT REPLY SKIP". */
    c->flags &= ~CLIENT_REPLY_SKIP;
    if (c->flags & CLIENT_REPLY_SKIP_NEXT) {
        c->flags |= CLIENT_REPLY_SKIP;
        c->flags &= ~CLIENT_REPLY_SKIP_NEXT;
    }
}

int processInlineBuffer(client *c) {
    char *newline;
    int argc, j;
    sds *argv, aux;
    size_t querylen;

    /* Search for end of line */
    newline = strchr(c->querybuf,'\n');

    /* Nothing to do without a \r\n */
    if (newline == NULL) {
        if (sdslen(c->querybuf) > PROTO_INLINE_MAX_SIZE) {
            addReplyError(c,"Protocol error: too big inline request");
            setProtocolError(c,0);
        }
        return VR_ERROR;
    }

    /* Handle the \r\n case. */
    if (newline && newline != c->querybuf && *(newline-1) == '\r')
        newline--;

    /* Split the input buffer up to the \r\n */
    querylen = newline-(c->querybuf);
    aux = sdsnewlen(c->querybuf,querylen);
    argv = sdssplitargs(aux,&argc);
    sdsfree(aux);
    if (argv == NULL) {
        addReplyError(c,"Protocol error: unbalanced quotes in request");
        setProtocolError(c,0);
        return VR_ERROR;
    }

    /* Newline from slaves can be used to refresh the last ACK time.
     * This is useful for a slave to ping back while loading a big
     * RDB file. */
    if (querylen == 0 && c->flags & CLIENT_SLAVE)
        c->repl_ack_time = c->vel->unixtime;

    /* Leave data after the first line of the query in the buffer */
    sdsrange(c->querybuf,querylen+2,-1);

    /* Setup argv array on client structure */
    if (argc) {
        if (c->argv) dfree(c->argv);
        c->argv = dalloc(sizeof(robj*)*argc);
    }

    /* Create redis objects for all arguments. */
    for (c->argc = 0, j = 0; j < argc; j++) {
        if (sdslen(argv[j])) {
            c->argv[c->argc] = createObject(OBJ_STRING,argv[j]);
            c->argc++;
        } else {
            sdsfree(argv[j]);
        }
    }
    dfree(argv);
    return VR_OK;
}

/* Helper function. Trims query buffer to make the function that processes
 * multi bulk requests idempotent. */
static void setProtocolError(client *c, int pos) {
    if (log_loggable(LOG_VERB)) {
        sds client = catClientInfoString(sdsempty(),c);
        log_debug(LOG_VERB,
            "Protocol error from client: %s", client);
        sdsfree(client);
    }
    c->flags |= CLIENT_CLOSE_AFTER_REPLY;
    sdsrange(c->querybuf,pos,-1);
}

int processMultibulkBuffer(client *c) {
    char *newline = NULL;
    int pos = 0, ok;
    long long ll;

    if (c->multibulklen == 0) {
        /* The client should have been reset */
        serverAssertWithInfo(c,NULL,c->argc == 0);

        /* Multi bulk length cannot be read without a \r\n */
        newline = strchr(c->querybuf,'\r');
        if (newline == NULL) {
            if (sdslen(c->querybuf) > PROTO_INLINE_MAX_SIZE) {
                addReplyError(c,"Protocol error: too big mbulk count string");
                setProtocolError(c,0);
            }
            return VR_ERROR;
        }

        /* Buffer should also contain \n */
        if (newline-(c->querybuf) > ((signed)sdslen(c->querybuf)-2))
            return VR_ERROR;

        /* We know for sure there is a whole line since newline != NULL,
         * so go ahead and find out the multi bulk length. */
        serverAssertWithInfo(c,NULL,c->querybuf[0] == '*');
        ok = string2ll(c->querybuf+1,newline-(c->querybuf+1),&ll);
        if (!ok || ll > 1024*1024) {
            addReplyError(c,"Protocol error: invalid multibulk length");
            setProtocolError(c,pos);
            return VR_ERROR;
        }

        pos = (newline-c->querybuf)+2;
        if (ll <= 0) {
            sdsrange(c->querybuf,pos,-1);
            return VR_OK;
        }

        c->multibulklen = ll;

        /* Setup argv array on client structure */
        if (c->argv) dfree(c->argv);
        c->argv = dalloc(sizeof(robj*)*c->multibulklen);
    }

    serverAssertWithInfo(c,NULL,c->multibulklen > 0);
    while(c->multibulklen) {
        /* Read bulk length if unknown */
        if (c->bulklen == -1) {
            newline = strchr(c->querybuf+pos,'\r');
            if (newline == NULL) {
                if (sdslen(c->querybuf) > PROTO_INLINE_MAX_SIZE) {
                    addReplyError(c,
                        "Protocol error: too big bulk count string");
                    setProtocolError(c,0);
                    return VR_ERROR;
                }
                break;
            }

            /* Buffer should also contain \n */
            if (newline-(c->querybuf) > ((signed)sdslen(c->querybuf)-2))
                break;

            if (c->querybuf[pos] != '$') {
                addReplyErrorFormat(c,
                    "Protocol error: expected '$', got '%c'",
                    c->querybuf[pos]);
                setProtocolError(c,pos);
                return VR_ERROR;
            }

            ok = string2ll(c->querybuf+pos+1,newline-(c->querybuf+pos+1),&ll);
            if (!ok || ll < 0 || ll > 512*1024*1024) {
                addReplyError(c,"Protocol error: invalid bulk length");
                setProtocolError(c,pos);
                return VR_ERROR;
            }

            pos += newline-(c->querybuf+pos)+2;
            if (ll >= PROTO_MBULK_BIG_ARG) {
                size_t qblen;

                /* If we are going to read a large object from network
                 * try to make it likely that it will start at c->querybuf
                 * boundary so that we can optimize object creation
                 * avoiding a large copy of data. */
                sdsrange(c->querybuf,pos,-1);
                pos = 0;
                qblen = sdslen(c->querybuf);
                /* Hint the sds library about the amount of bytes this string is
                 * going to contain. */
                if (qblen < (size_t)ll+2)
                    c->querybuf = sdsMakeRoomFor(c->querybuf,ll+2-qblen);
            }
            c->bulklen = ll;
        }

        /* Read bulk argument */
        if (sdslen(c->querybuf)-pos < (unsigned)(c->bulklen+2)) {
            /* Not enough data (+2 == trailing \r\n) */
            break;
        } else {
            /* Optimization: if the buffer contains JUST our bulk element
             * instead of creating a new object by *copying* the sds we
             * just use the current sds string. */
            if (pos == 0 &&
                c->bulklen >= PROTO_MBULK_BIG_ARG &&
                (signed) sdslen(c->querybuf) == c->bulklen+2)
            {
                c->argv[c->argc++] = createObject(OBJ_STRING,c->querybuf);
                sdsIncrLen(c->querybuf,-2); /* remove CRLF */
                /* Assume that if we saw a fat argument we'll see another one
                 * likely... */
                c->querybuf = sdsnewlen(NULL,c->bulklen+2);
                sdsclear(c->querybuf);
                pos = 0;
            } else {
                c->argv[c->argc++] =
                    createStringObject(c->querybuf+pos,c->bulklen);
                pos += c->bulklen+2;
            }
            c->bulklen = -1;
            c->multibulklen--;
        }
    }

    /* Trim to pos */
    if (pos) sdsrange(c->querybuf,pos,-1);

    /* We're done when c->multibulk == 0 */
    if (c->multibulklen == 0) return VR_OK;

    /* Still not read to process the command */
    return VR_ERROR;
}

void processInputBuffer(client *c) {
    c->vel->current_client = c;
    /* Keep processing while there is something in the input buffer */
    while(sdslen(c->querybuf)) {
        /* Return if clients are paused. */
        if (!(c->flags & CLIENT_SLAVE) && clientsArePaused(c->vel)) break;

        /* Immediately abort if the client is in the middle of something. */
        if (c->flags & CLIENT_BLOCKED) break;

        /* CLIENT_CLOSE_AFTER_REPLY closes the connection once the reply is
         * written to the client. Make sure to not let the reply grow after
         * this flag has been set (i.e. don't process more commands). */
        if (c->flags & CLIENT_CLOSE_AFTER_REPLY) break;

        /* Determine request type when unknown. */
        if (!c->reqtype) {
            if (c->querybuf[0] == '*') {
                c->reqtype = PROTO_REQ_MULTIBULK;
            } else {
                c->reqtype = PROTO_REQ_INLINE;
            }
        }

        if (c->reqtype == PROTO_REQ_INLINE) {
            if (processInlineBuffer(c) != VR_OK) break;
        } else if (c->reqtype == PROTO_REQ_MULTIBULK) {
            if (processMultibulkBuffer(c) != VR_OK) break;
        } else {
            serverPanic("Unknown request type");
        }

        /* Multibulk processing could see a <= 0 length. */
        if (c->argc == 0) {
            resetClient(c);
        } else {
            /* Only reset the client when the command was executed. */
            if (processCommand(c) == VR_OK)
                resetClient(c);
            /* freeMemoryIfNeeded may flush slave output buffers. This may result
             * into a slave, that may be the active client, to be freed. */
            if (c->vel->current_client == NULL) break;

            /* If this client need to jump to another worker,
             * break this while loop. When this client jumped finished, 
             * continue handle the remain query buffer. */
            if (c->flags&CLIENT_JUMP) break;
        }
    }
    c->vel->current_client = NULL;
}

void readQueryFromClient(aeEventLoop *el, int fd, void *privdata, int mask) {
    client *c = (client*) privdata;
    int nread, readlen;
    size_t qblen;
    UNUSED(el);
    UNUSED(mask);

    readlen = PROTO_IOBUF_LEN;
    /* If this is a multi bulk request, and we are processing a bulk reply
     * that is large enough, try to maximize the probability that the query
     * buffer contains exactly the SDS string representing the object, even
     * at the risk of requiring more read(2) calls. This way the function
     * processMultiBulkBuffer() can avoid copying buffers to create the
     * Redis Object representing the argument. */
    if (c->reqtype == PROTO_REQ_MULTIBULK && c->multibulklen && c->bulklen != -1
        && c->bulklen >= PROTO_MBULK_BIG_ARG)
    {
        int remaining = (unsigned)(c->bulklen+2)-sdslen(c->querybuf);

        if (remaining < readlen) readlen = remaining;
    }

    qblen = sdslen(c->querybuf);
    if (c->querybuf_peak < qblen) c->querybuf_peak = qblen;
    c->querybuf = sdsMakeRoomFor(c->querybuf, readlen);
    nread = vr_read(fd, c->querybuf+qblen, readlen);
    if (nread == -1) {
        if (errno == EAGAIN) {
            return;
        } else {
            log_debug(LOG_VERB, "reading from client: %s",strerror(errno));
            freeClient(c);
            return;
        }
    } else if (nread == 0) {
        log_debug(LOG_VERB, "client closed connection");
        freeClient(c);
        return;
    }

    sdsIncrLen(c->querybuf,nread);
    c->lastinteraction = c->vel->unixtime;
    if (c->flags & CLIENT_MASTER) c->reploff += nread;
    update_stats_add(c->vel->stats, net_input_bytes, nread);
    if (sdslen(c->querybuf) > server.client_max_querybuf_len) {
        sds ci = catClientInfoString(sdsempty(),c), bytes = sdsempty();

        bytes = sdscatrepr(bytes,c->querybuf,64);
        log_warn("closing client that reached max query buffer length: %s (qbuf initial bytes: %s)", ci, bytes);
        sdsfree(ci);
        sdsfree(bytes);
        freeClient(c);
        return;
    }
    processInputBuffer(c);

    if (c->flags&CLIENT_JUMP) {
        dispatch_conn_exist(c,c->taridx);
    }
}

void getClientsMaxBuffers(vr_eventloop *vel, unsigned long *longest_output_list,
                          unsigned long *biggest_input_buffer) {
    client *c;
    dlistNode *ln;
    dlistIter li;
    unsigned long lol = 0, bib = 0;

    dlistRewind(vel->clients,&li);
    while ((ln = dlistNext(&li)) != NULL) {
        c = dlistNodeValue(ln);

        if (dlistLength(c->reply) > lol) lol = dlistLength(c->reply);
        if (sdslen(c->querybuf) > bib) bib = sdslen(c->querybuf);
    }
    *longest_output_list = lol;
    *biggest_input_buffer = bib;
}

/* A Redis "Peer ID" is a colon separated ip:port pair.
 * For IPv4 it's in the form x.y.z.k:port, example: "127.0.0.1:1234".
 * For IPv6 addresses we use [] around the IP part, like in "[::1]:1234".
 * For Unix sockets we use path:0, like in "/tmp/redis:0".
 *
 * A Peer ID always fits inside a buffer of NET_PEER_ID_LEN bytes, including
 * the null term.
 *
 * On failure the function still populates 'peerid' with the "?:0" string
 * in case you want to relax error checking or need to display something
 * anyway (see anetPeerToString implementation for more info). */
void genClientPeerId(client *client, char *peerid,
                            size_t peerid_len) {
    if (client->flags & CLIENT_UNIX_SOCKET) {
        /* Unix socket client. */
        snprintf(peerid,peerid_len,"%s:0",server.unixsocket);
    } else {
        /* TCP client. */
        vr_net_format_peer(client->conn->sd,peerid,peerid_len);
    }
}

/* This function returns the client peer id, by creating and caching it
 * if client->peerid is NULL, otherwise returning the cached value.
 * The Peer ID never changes during the life of the client, however it
 * is expensive to compute. */
char *getClientPeerId(client *c) {
    char peerid[VR_INET_PEER_ID_LEN];

    if (c->peerid == NULL) {
        genClientPeerId(c,peerid,sizeof(peerid));
        c->peerid = sdsnew(peerid);
    }
    return c->peerid;
}

/* Concatenate a string representing the state of a client in an human
 * readable format, into the sds string 's'. */
sds catClientInfoString(sds s, client *client) {
    char flags[16], events[3], *p;
    int emask;

    p = flags;
    if (client->flags & CLIENT_SLAVE) {
        if (client->flags & CLIENT_MONITOR)
            *p++ = 'O';
        else
            *p++ = 'S';
    }
    if (client->flags & CLIENT_MASTER) *p++ = 'M';
    if (client->flags & CLIENT_MULTI) *p++ = 'x';
    if (client->flags & CLIENT_BLOCKED) *p++ = 'b';
    if (client->flags & CLIENT_DIRTY_CAS) *p++ = 'd';
    if (client->flags & CLIENT_CLOSE_AFTER_REPLY) *p++ = 'c';
    if (client->flags & CLIENT_UNBLOCKED) *p++ = 'u';
    if (client->flags & CLIENT_CLOSE_ASAP) *p++ = 'A';
    if (client->flags & CLIENT_UNIX_SOCKET) *p++ = 'U';
    if (client->flags & CLIENT_READONLY) *p++ = 'r';
    if (p == flags) *p++ = 'N';
    *p++ = '\0';

    emask = client->conn->sd == -1 ? 0 : aeGetFileEvents(client->vel->el,client->conn->sd);
    p = events;
    if (emask & AE_READABLE) *p++ = 'r';
    if (emask & AE_WRITABLE) *p++ = 'w';
    *p = '\0';
    
    return sdscatfmt(s,
        "oid=%i id=%U addr=%s fd=%i name=%s age=%I idle=%I flags=%s db=%i sub=%i psub=%i multi=%i qbuf=%U qbuf-free=%U obl=%U oll=%U omem=%U events=%s cmd=%s",
        client->curidx,
        (unsigned long long) client->id,
        getClientPeerId(client),
        client->conn->sd,
        client->name ? (char*)client->name->ptr : "",
        (long long)(client->vel->unixtime - client->ctime),
        (long long)(client->vel->unixtime - client->lastinteraction),
        flags,
        client->dictid,
        (int) dictSize(client->pubsub_channels),
        (int) dlistLength(client->pubsub_patterns),
        (client->flags & CLIENT_MULTI) ? client->mstate.count : -1,
        (unsigned long long) sdslen(client->querybuf),
        (unsigned long long) sdsavail(client->querybuf),
        (unsigned long long) client->bufpos,
        (unsigned long long) dlistLength(client->reply),
        (unsigned long long) getClientOutputBufferMemoryUsage(client),
        events,
        client->lastcmd ? client->lastcmd->name : "NULL");
}

sds getAllClientsInfoString(vr_eventloop *vel) {
    dlistNode *ln;
    dlistIter li;
    client *client;
    sds o = sdsnewlen(NULL,200*dlistLength(vel->clients));
    sdsclear(o);
    dlistRewind(vel->clients,&li);
    while ((ln = dlistNext(&li)) != NULL) {
        client = dlistNodeValue(ln);
        o = catClientInfoString(o,client);
        o = sdscatlen(o,"\n",1);
    }
    return o;
}

struct clientkilldata {
    sds addr;
    int type;
    uint64_t id;
    int skipme;
    int killed;
    int close_this_client;
};

void clientCommand(client *c) {
    dlistNode *ln;
    dlistIter li;
    client *client;

    if (!strcasecmp(c->argv[1]->ptr,"list") && c->argc == 2) {
        /* CLIENT LIST */
        sds str = c->cache;
        sds o = getAllClientsInfoString(c->vel);

        str = sdscatsds(str?str:sdsempty(),o);

        if (c->steps >= (darray_n(&workers) - 1)) {
            addReplyBulkCBuffer(c,str,sdslen(str));
            c->steps = 0;
            c->taridx = -1;
            sdsfree(str);
            c->cache = NULL;
            c->flags &= ~CLIENT_JUMP;
        } else {
            if (!(c->flags&CLIENT_JUMP))
                c->flags |= CLIENT_JUMP;
            c->taridx = worker_get_next_idx(c->curidx);
            c->cache = str;
        }
        sdsfree(o);
        return;
    } else if (!strcasecmp(c->argv[1]->ptr,"kill")) {
        /* CLIENT KILL <ip:port>
         * CLIENT KILL <option> [value] ... <option> [value] */
        struct clientkilldata *ckd;

        if (c->steps == 0) {
            ckd = dalloc(sizeof(struct clientkilldata));
            ckd->addr = NULL;
            ckd->type = -1;
            ckd->id = 0;
            ckd->skipme = 1;
            ckd->killed = 0;
            ckd->close_this_client = 0;

            if (c->argc == 3) {
                /* Old style syntax: CLIENT KILL <addr> */
                ckd->addr = sdsnew(c->argv[2]->ptr);
                ckd->skipme = 0; /* With the old form, you can kill yourself. */
            } else if (c->argc > 3) {
                int i = 2; /* Next option index. */
    
                /* New style syntax: parse options. */
                while(i < c->argc) {
                    int moreargs = c->argc > i+1;
    
                    if (!strcasecmp(c->argv[i]->ptr,"id") && moreargs) {
                        long long tmp;
    
                        if (getLongLongFromObjectOrReply(c,c->argv[i+1],&tmp,NULL)
                            != VR_OK) {
                            if (ckd->addr) sdsfree(ckd->addr);
                            dfree(ckd);
                            return;
                        }
                        ckd->id = tmp;
                    } else if (!strcasecmp(c->argv[i]->ptr,"type") && moreargs) {
                        ckd->type = getClientTypeByName(c->argv[i+1]->ptr);
                        if (ckd->type == -1) {
                            if (ckd->addr) sdsfree(ckd->addr);
                            dfree(ckd);
                            addReplyErrorFormat(c,"Unknown client type '%s'",
                                (char*) c->argv[i+1]->ptr);
                            return;
                        }
                    } else if (!strcasecmp(c->argv[i]->ptr,"addr") && moreargs) {
                        ckd->addr = sdsnew(c->argv[i+1]->ptr);
                    } else if (!strcasecmp(c->argv[i]->ptr,"skipme") && moreargs) {
                        if (!strcasecmp(c->argv[i+1]->ptr,"yes")) {
                            ckd->skipme = 1;
                        } else if (!strcasecmp(c->argv[i+1]->ptr,"no")) {
                            ckd->skipme = 0;
                        } else {
                            if (ckd->addr) sdsfree(ckd->addr);
                            dfree(ckd);
                            addReply(c,shared.syntaxerr);
                            return;
                        }
                    } else {
                        if (ckd->addr) sdsfree(ckd->addr);
                        dfree(ckd);
                        addReply(c,shared.syntaxerr);
                        return;
                    }
                    i += 2;
                }
            } else {
                if (ckd->addr) sdsfree(ckd->addr);
                dfree(ckd);
                addReply(c,shared.syntaxerr);
                return;
            }

            if (!(c->flags&CLIENT_JUMP))
                c->flags |= CLIENT_JUMP;
            c->taridx = worker_get_next_idx(c->curidx);
            c->cache = ckd;
        } else {
            ckd = c->cache;
            c->taridx = worker_get_next_idx(c->curidx);
        }

        /* Iterate clients killing all the matching clients. */
        dlistRewind(c->vel->clients,&li);
        while ((ln = dlistNext(&li)) != NULL) {
            client = dlistNodeValue(ln);
            if (ckd->addr && strcmp(getClientPeerId(client),ckd->addr) != 0) continue;
            if (ckd->type != -1 && getClientType(client) != ckd->type) continue;
            if (ckd->id != 0 && client->id != ckd->id) continue;
            if (c == client && ckd->skipme) continue;

            /* Kill it. */
            if (c == client) {
                ckd->close_this_client = 1;
            } else {
                freeClient(client);
            }
            ckd->killed++;
        }

        if (c->steps >= (darray_n(&workers) - 1)) {
            /* Reply according to old/new format. */
            if (c->argc == 3) {
                if (ckd->killed == 0)
                    addReplyError(c,"No such client");
                else
                    addReply(c,shared.ok);
            } else {
                addReplyLongLong(c,ckd->killed);
            }
            
            c->steps = 0;
            c->taridx = -1;
            c->cache = NULL;
            c->flags &= ~CLIENT_JUMP;
            
            /* If this client has to be closed, flag it as CLOSE_AFTER_REPLY
             * only after we queued the reply to its output buffers. */
            if (ckd->close_this_client) c->flags |= CLIENT_CLOSE_AFTER_REPLY;

            if (ckd->addr) sdsfree(ckd->addr);
            dfree(ckd);
        }

        return;
    } else if (!strcasecmp(c->argv[1]->ptr,"setname") && c->argc == 3) {
        int j, len = sdslen(c->argv[2]->ptr);
        char *p = c->argv[2]->ptr;

        /* Setting the client name to an empty string actually removes
         * the current name. */
        if (len == 0) {
            if (c->name) freeObject(c->name);
            c->name = NULL;
            addReply(c,shared.ok);
            return;
        }

        /* Otherwise check if the charset is ok. We need to do this otherwise
         * CLIENT LIST format will break. You should always be able to
         * split by space to get the different fields. */
        for (j = 0; j < len; j++) {
            if (p[j] < '!' || p[j] > '~') { /* ASCII is assumed. */
                addReplyError(c,
                    "Client names cannot contain spaces, "
                    "newlines or special characters.");
                return;
            }
        }
        if (c->name) freeObject(c->name);
        c->name = dupStringObjectUnconstant(c->argv[2]);
        addReply(c,shared.ok);
        return;
    } else if (!strcasecmp(c->argv[1]->ptr,"getname") && c->argc == 2) {
        if (c->name)
            addReplyBulk(c,c->name);
        else
            addReply(c,shared.nullbulk);
        return;
    } else {
        addReplyError(c, "Syntax error, try CLIENT (LIST | KILL ip:port | SETNAME connection-name)");
        return;
    }

    if (!strcasecmp(c->argv[1]->ptr,"reply") && c->argc == 3) {
        /* CLIENT REPLY ON|OFF|SKIP */
        if (!strcasecmp(c->argv[2]->ptr,"on")) {
            c->flags &= ~(CLIENT_REPLY_SKIP|CLIENT_REPLY_OFF);
            addReply(c,shared.ok);
        } else if (!strcasecmp(c->argv[2]->ptr,"off")) {
            c->flags |= CLIENT_REPLY_OFF;
        } else if (!strcasecmp(c->argv[2]->ptr,"skip")) {
            if (!(c->flags & CLIENT_REPLY_OFF))
                c->flags |= CLIENT_REPLY_SKIP_NEXT;
        } else {
            addReply(c,shared.syntaxerr);
            return;
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"pause") && c->argc == 3) {
        long long duration;

        if (getTimeoutFromObjectOrReply(c,c->argv[2],&duration,UNIT_MILLISECONDS)
                                        != VR_OK) return;
        pauseClients(NULL, duration);
        addReply(c,shared.ok);
    } else {
        addReplyError(c, "Syntax error, try CLIENT (LIST | KILL ip:port | GETNAME | SETNAME connection-name)");
    }
}

/* Rewrite the command vector of the client. All the new objects should 
 * be independent. The old command vector is freed. */
void rewriteClientCommandVector(client *c, int argc, ...) {
    va_list ap;
    int j;
    robj **argv; /* The new argument vector */

    argv = dalloc(sizeof(robj*)*argc);
    va_start(ap,argc);
    for (j = 0; j < argc; j++) {
        robj *a;
        a = va_arg(ap, robj*);
        argv[j] = a;
    }
    /* We free the objects in the original vector at the end. */
    for (j = 0; j < c->argc; j++) freeObject(c->argv[j]);
    dfree(c->argv);
    /* Replace argv and argc with our new versions. */
    c->argv = argv;
    c->argc = argc;
    c->cmd = lookupCommandOrOriginal(c->argv[0]->ptr);
    serverAssertWithInfo(c,NULL,c->cmd != NULL);
    va_end(ap);
}

/* Completely replace the client command vector with the provided one. */
void replaceClientCommandVector(client *c, int argc, robj **argv) {
    freeClientArgv(c);
    dfree(c->argv);
    c->argv = argv;
    c->argc = argc;
    c->cmd = lookupCommandOrOriginal(c->argv[0]->ptr);
    serverAssertWithInfo(c,NULL,c->cmd != NULL);
}

/* Rewrite a single item in the command vector.
 * The new val should be independent, and the old freed.
 *
 * It is possible to specify an argument over the current size of the
 * argument vector: in this case the array of objects gets reallocated
 * and c->argc set to the max value. However it's up to the caller to
 *
 * 1. Make sure there are no "holes" and all the arguments are set.
 * 2. If the original argument vector was longer than the one we
 *    want to end with, it's up to the caller to set c->argc and
 *    free the no longer used objects on c->argv. */
void rewriteClientCommandArgument(client *c, int i, robj *newval) {
    robj *oldval;

    if (i >= c->argc) {
        c->argv = drealloc(c->argv,sizeof(robj*)*(i+1));
        c->argc = i+1;
        c->argv[i] = NULL;
    }
    oldval = c->argv[i];
    c->argv[i] = newval;
    if (oldval) freeObject(oldval);

    /* If this is the command name make sure to fix c->cmd. */
    if (i == 0) {
        c->cmd = lookupCommandOrOriginal(c->argv[0]->ptr);
        serverAssertWithInfo(c,NULL,c->cmd != NULL);
    }
}

/* This function returns the number of bytes that Redis is virtually
 * using to store the reply still not read by the client.
 * It is "virtual" since the reply output list may contain objects that
 * are shared and are not really using additional memory.
 *
 * The function returns the total sum of the length of all the objects
 * stored in the output list, plus the memory used to allocate every
 * list node. The static reply buffer is not taken into account since it
 * is allocated anyway.
 *
 * Note: this function is very fast so can be called as many time as
 * the caller wishes. The main usage of this function currently is
 * enforcing the client output length limits. */
unsigned long getClientOutputBufferMemoryUsage(client *c) {
    unsigned long list_item_size = sizeof(dlistNode)+sizeof(robj);

    return c->reply_bytes + (list_item_size*dlistLength(c->reply));
}

/* Get the class of a client, used in order to enforce limits to different
 * classes of clients.
 *
 * The function will return one of the following:
 * CLIENT_TYPE_NORMAL -> Normal client
 * CLIENT_TYPE_SLAVE  -> Slave or client executing MONITOR command
 * CLIENT_TYPE_PUBSUB -> Client subscribed to Pub/Sub channels
 * CLIENT_TYPE_MASTER -> The client representing our replication master.
 */
int getClientType(client *c) {
    if (c->flags & CLIENT_MASTER) return CLIENT_TYPE_MASTER;
    if ((c->flags & CLIENT_SLAVE) && !(c->flags & CLIENT_MONITOR))
        return CLIENT_TYPE_SLAVE;
    if (c->flags & CLIENT_PUBSUB) return CLIENT_TYPE_PUBSUB;
    return CLIENT_TYPE_NORMAL;
}

int getClientTypeByName(char *name) {
    if (!strcasecmp(name,"normal")) return CLIENT_TYPE_NORMAL;
    else if (!strcasecmp(name,"slave")) return CLIENT_TYPE_SLAVE;
    else if (!strcasecmp(name,"pubsub")) return CLIENT_TYPE_PUBSUB;
    else if (!strcasecmp(name,"master")) return CLIENT_TYPE_MASTER;
    else return -1;
}

char *getClientTypeName(int class) {
    switch(class) {
    case CLIENT_TYPE_NORMAL: return "normal";
    case CLIENT_TYPE_SLAVE:  return "slave";
    case CLIENT_TYPE_PUBSUB: return "pubsub";
    case CLIENT_TYPE_MASTER: return "master";
    default:                       return NULL;
    }
}

/* The function checks if the client reached output buffer soft or hard
 * limit, and also update the state needed to check the soft limit as
 * a side effect.
 *
 * Return value: non-zero if the client reached the soft or the hard limit.
 *               Otherwise zero is returned. */
int checkClientOutputBufferLimits(client *c) {
    int soft = 0, hard = 0, class;
    unsigned long used_mem = getClientOutputBufferMemoryUsage(c);

    class = getClientType(c);
    /* For the purpose of output buffer limiting, masters are handled
     * like normal clients. */
    if (class == CLIENT_TYPE_MASTER) class = CLIENT_TYPE_NORMAL;

    if (server.client_obuf_limits[class].hard_limit_bytes &&
        used_mem >= server.client_obuf_limits[class].hard_limit_bytes)
        hard = 1;
    if (server.client_obuf_limits[class].soft_limit_bytes &&
        used_mem >= server.client_obuf_limits[class].soft_limit_bytes)
        soft = 1;

    /* We need to check if the soft limit is reached continuously for the
     * specified amount of seconds. */
    if (soft) {
        if (c->obuf_soft_limit_reached_time == 0) {
            c->obuf_soft_limit_reached_time = c->vel->unixtime;
            soft = 0; /* First time we see the soft limit reached */
        } else {
            time_t elapsed = c->vel->unixtime - c->obuf_soft_limit_reached_time;

            if (elapsed <=
                server.client_obuf_limits[class].soft_limit_seconds) {
                soft = 0; /* The client still did not reached the max number of
                             seconds for the soft limit to be considered
                             reached. */
            }
        }
    } else {
        c->obuf_soft_limit_reached_time = 0;
    }
    return soft || hard;
}

/* Asynchronously close a client if soft or hard limit is reached on the
 * output buffer size. The caller can check if the client will be closed
 * checking if the client CLIENT_CLOSE_ASAP flag is set.
 *
 * Note: we need to close the client asynchronously because this function is
 * called from contexts where the client can't be freed safely, i.e. from the
 * lower level functions pushing data inside the client output buffers. */
void asyncCloseClientOnOutputBufferLimitReached(client *c) {
    ASSERT(c->reply_bytes < SIZE_MAX-(1024*64));
    if (c->reply_bytes == 0 || c->flags & CLIENT_CLOSE_ASAP) return;
    if (checkClientOutputBufferLimits(c)) {
        sds client = catClientInfoString(sdsempty(),c);

        freeClientAsync(c);
        log_warn("Client %s scheduled to be closed ASAP for overcoming of output buffer limits.", client);
        sdsfree(client);
    }
}

/* Helper function used by freeMemoryIfNeeded() in order to flush slaves
 * output buffers without returning control to the event loop.
 * This is also called by SHUTDOWN for a best-effort attempt to send
 * slaves the latest writes. */
void flushSlavesOutputBuffers(void) {
    dlistIter li;
    dlistNode *ln;

    dlistRewind(repl.slaves,&li);
    while((ln = dlistNext(&li))) {
        client *slave = dlistNodeValue(ln);
        int events;

        /* Note that the following will not flush output buffers of slaves
         * in STATE_ONLINE but having put_online_on_ack set to true: in this
         * case the writable event is never installed, since the purpose
         * of put_online_on_ack is to postpone the moment it is installed.
         * This is what we want since slaves in this state should not receive
         * writes before the first ACK. */
        events = aeGetFileEvents(repl.vel.el,slave->conn->sd);
        if (events & AE_WRITABLE &&
            slave->replstate == SLAVE_STATE_ONLINE &&
            clientHasPendingReplies(slave))
        {
            writeToClient(slave->conn->sd,slave,0);
        }
    }
}

/* Pause clients up to the specified unixtime (in ms). While clients
 * are paused no command is processed from clients, so the data set can't
 * change during that time.
 *
 * However while this function pauses normal and Pub/Sub clients, slaves are
 * still served, so this function can be used on server upgrades where it is
 * required that slaves process the latest bytes from the replication stream
 * before being turned to masters.
 *
 * This function is also internally used by Redis Cluster for the manual
 * failover procedure implemented by CLUSTER FAILOVER.
 *
 * The function always succeed, even if there is already a pause in progress.
 * In such a case, the pause is extended if the duration is more than the
 * time left for the previous duration. However if the duration is smaller
 * than the time left for the previous pause, no change is made to the
 * left duration. */
void pauseClients(vr_eventloop *vel, long long end) {
    if (vel == NULL) return;

    if (!vel->clients_paused || end > vel->clients_pause_end_time)
        vel->clients_pause_end_time = end;
    vel->clients_paused = 1;
}

/* Return non-zero if clients are currently paused. As a side effect the
 * function checks if the pause time was reached and clear it. */
int clientsArePaused(vr_eventloop *vel) {
    if (vel->clients_paused &&
        vel->clients_pause_end_time < vel->mstime)
    {
        dlistNode *ln;
        dlistIter li;
        client *c;

        vel->clients_paused = 0;

        /* Put all the clients in the unblocked clients queue in order to
         * force the re-processing of the input buffer if any. */
        dlistRewind(vel->clients,&li);
        while ((ln = dlistNext(&li)) != NULL) {
            c = dlistNodeValue(ln);

            /* Don't touch slaves and blocked clients. The latter pending
             * requests be processed when unblocked. */
            if (c->flags & (CLIENT_SLAVE|CLIENT_BLOCKED)) continue;
            c->flags |= CLIENT_UNBLOCKED;
            dlistAddNodeTail(vel->unblocked_clients,c);
        }
    }
    return vel->clients_paused;
}

/* This function is called by Redis in order to process a few events from
 * time to time while blocked into some not interruptible operation.
 * This allows to reply to clients with the -LOADING error while loading the
 * data set at startup or after a full resynchronization with the master
 * and so forth.
 *
 * It calls the event loop in order to process a few events. Specifically we
 * try to call the event loop 4 times as long as we receive acknowledge that
 * some event was processed, in order to go forward with the accept, read,
 * write, close sequence needed to serve a client.
 *
 * The function returns the total number of events processed. */
int processEventsWhileBlocked(vr_eventloop *vel) {
    int iterations = 4; /* See the function top-comment. */
    int count = 0;
    while (iterations--) {
        int events = 0;
        events += aeProcessEvents(vel->el, AE_FILE_EVENTS|AE_DONT_WAIT);
        events += handleClientsWithPendingWrites(vel);
        if (!events) break;
        count += events;
    }
    return count;
}

int
current_clients(void)
{
    int ccs;

#if defined(__ATOMIC_RELAXED) || defined(HAVE_ATOMIC)
    ccs = update_curr_clients_add(0);
#else
    pthread_mutex_lock(&curr_clients_mutex);
    ccs = ncurr_cconn;
    pthread_mutex_unlock(&curr_clients_mutex);
#endif

    return ccs;
}
