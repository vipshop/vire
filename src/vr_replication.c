#include <vr_core.h>

struct vr_replication repl;

int vr_replication_init(void)
{
    vr_eventloop_init(&repl.vel,1000);

    repl.role = REPLICATION_ROLE_MASTER;
    repl.master = NULL;
    repl.cached_master = NULL;
    repl.slaves = NULL;
    repl.repl_no_slaves_since = 0;
    repl.repl_min_slaves_to_write = 0;
    repl.repl_min_slaves_max_lag = 0;
    repl.repl_good_slaves_count = 0;
    repl.repl_state = REPL_STATE_NONE;
    repl.repl_down_since = 0;

    /* Replication partial resync backlog */
    repl.repl_backlog = NULL;
    repl.repl_backlog_size = CONFIG_DEFAULT_REPL_BACKLOG_SIZE;
    repl.repl_backlog_histlen = 0;
    repl.repl_backlog_idx = 0;
    repl.repl_backlog_off = 0;
    repl.repl_backlog_time_limit = CONFIG_DEFAULT_REPL_BACKLOG_TIME_LIMIT;
    repl.repl_no_slaves_since = time(NULL);

    repl.slaves = dlistCreate();

    return VR_OK;
}

void vr_replication_deinit(void)
{
    vr_eventloop_deinit(&repl.vel);

    if (repl.master != NULL) {
        freeClient(repl.master);
        repl.master = NULL;
    }

    if (repl.cached_master != NULL) {
        freeClient(repl.cached_master);
        repl.cached_master = NULL;
    }

    if (repl.repl_backlog != NULL) {
        dfree(repl.repl_backlog);
        repl.repl_backlog = NULL;
    }

    if (repl.slaves != NULL) {
        client *slave;
        while (slave = dlistPop(repl.slaves)) {
            freeClient(slave);
        }
        dlistRelease(repl.slaves);
        repl.slaves = NULL;
    }
}

/* This is called by unblockClient() to perform the blocking op type
 * specific cleanup. We just remove the client from the list of clients
 * waiting for replica acks. Never call it directly, call unblockClient()
 * instead. */
void unblockClientWaitingReplicas(client *c) {
    dlistNode *ln = dlistSearchKey(c->vel->clients_waiting_acks,c);
    ASSERT(ln != NULL);
    dlistDelNode(c->vel->clients_waiting_acks,ln);
}

/* ------------------------- MIN-SLAVES-TO-WRITE  --------------------------- */

/* This function counts the number of slaves with lag <= min-slaves-max-lag.
 * If the option is active, the server will prevent writes if there are not
 * enough connected slaves with the specified lag (or less). */
void refreshGoodSlavesCount(void) {
    dlistIter li;
    dlistNode *ln;
    int good = 0;

    if (!repl.repl_min_slaves_to_write ||
        !repl.repl_min_slaves_max_lag) return;

    dlistRewind(repl.slaves,&li);
    while((ln = dlistNext(&li))) {
        client *slave = ln->value;
        time_t lag = repl.vel.unixtime - slave->repl_ack_time;

        if (slave->replstate == SLAVE_STATE_ONLINE &&
            lag <= repl.repl_min_slaves_max_lag) good++;
    }
    repl.repl_good_slaves_count = good;
}

/* This function is called when the slave lose the connection with the
 * master into an unexpected way. */
void replicationHandleMasterDisconnection(void) {
    repl.master = NULL;
    repl.repl_state = REPL_STATE_CONNECT;
    repl.repl_down_since = repl.vel.unixtime;
    /* We lost connection with our master, don't disconnect slaves yet,
     * maybe we'll be able to PSYNC with our master later. We'll disconnect
     * the slaves only if we'll have to do a full resync with our master. */
}


/* ---------------------- MASTER CACHING FOR PSYNC -------------------------- */

/* In order to implement partial synchronization we need to be able to cache
 * our master's client structure after a transient disconnection.
 * It is cached into server.cached_master and flushed away using the following
 * functions. */

/* This function is called by freeClient() in order to cache the master
 * client structure instead of destryoing it. freeClient() will return
 * ASAP after this function returns, so every action needed to avoid problems
 * with a client that is really "suspended" has to be done by this function.
 *
 * The other functions that will deal with the cached master are:
 *
 * replicationDiscardCachedMaster() that will make sure to kill the client
 * as for some reason we don't want to use it in the future.
 *
 * replicationResurrectCachedMaster() that is used after a successful PSYNC
 * handshake in order to reactivate the cached master.
 */
void replicationCacheMaster(client *c) {
    ASSERT(repl.master != NULL && repl.cached_master == NULL);
    log_debug(LOG_NOTICE,"Caching the disconnected master state.");

    /* Unlink the client from the server structures. */
    unlinkClient(c);

    /* Save the master. Server.master will be set to null later by
     * replicationHandleMasterDisconnection(). */
    repl.cached_master = repl.master;

    /* Invalidate the Peer ID cache. */
    if (c->peerid) {
        sdsfree(c->peerid);
        c->peerid = NULL;
    }

    /* Caching the master happens instead of the actual freeClient() call,
     * so make sure to adjust the replication state. This function will
     * also set server.master to NULL. */
    replicationHandleMasterDisconnection();
}

/* Return the pointer to a string representing the slave ip:listening_port
 * pair. Mostly useful for logging, since we want to log a slave using its
 * IP address and it's listening port which is more clear for the user, for
 * example: "Closing connection with slave 10.1.2.3:6380". */
char *replicationGetSlaveName(client *c) {
    static char buf[VR_INET_PEER_ID_LEN];
    char ip[VR_INET_ADDRSTRLEN];

    ip[0] = '\0';
    buf[0] = '\0';
    
    return buf;
}

/* REPLCONF <option> <value> <option> <value> ...
 * This command is used by a slave in order to configure the replication
 * process before starting it with the SYNC command.
 *
 * Currently the only use of this command is to communicate to the master
 * what is the listening port of the Slave redis instance, so that the
 * master can accurately list slaves and their listening ports in
 * the INFO output.
 *
 * In the future the same command can be used in order to configure
 * the replication to initiate an incremental replication instead of a
 * full resync. */
void replconfCommand(client *c) {
    int j;

    if ((c->argc % 2) == 0) {
        /* Number of arguments must be odd to make sure that every
         * option has a corresponding value. */
        addReply(c,shared.syntaxerr);
        return;
    }

    /* Process every option-value pair. */
    for (j = 1; j < c->argc; j+=2) {
        if (!strcasecmp(c->argv[j]->ptr,"listening-port")) {
            long port;

            if ((getLongFromObjectOrReply(c,c->argv[j+1],
                    &port,NULL) != VR_OK))
                return;
            c->slave_listening_port = port;
        } else if (!strcasecmp(c->argv[j]->ptr,"capa")) {
            /* Ignore capabilities not understood by this master. */
            if (!strcasecmp(c->argv[j+1]->ptr,"eof"))
                c->slave_capa |= SLAVE_CAPA_EOF;
        } else if (!strcasecmp(c->argv[j]->ptr,"ack")) {
            /* REPLCONF ACK is used by slave to inform the master the amount
             * of replication stream that it processed so far. It is an
             * internal only command that normal clients should never use. */
            long long offset;

            if (!(c->flags & CLIENT_SLAVE)) return;
            if ((getLongLongFromObject(c->argv[j+1], &offset) != VR_OK))
                return;
            if (offset > c->repl_ack_off)
                c->repl_ack_off = offset;
            c->repl_ack_time = c->vel->unixtime;
            /* If this was a diskless replication, we need to really put
             * the slave online when the first ACK is received (which
             * confirms slave is online and ready to get more data). */
            if (c->repl_put_online_on_ack && c->replstate == SLAVE_STATE_ONLINE)
                putSlaveOnline(c);
            /* Note: this command does not reply anything! */
            return;
        } else if (!strcasecmp(c->argv[j]->ptr,"getack")) {
            /* REPLCONF GETACK is used in order to request an ACK ASAP
             * to the slave. */
            if (repl.masterhost && repl.master) replicationSendAck();
            /* Note: this command does not reply anything! */
        } else {
            addReplyErrorFormat(c,"Unrecognized REPLCONF option: %s",
                (char*)c->argv[j]->ptr);
            return;
        }
    }
    addReply(c,shared.ok);
}

/* This function puts a slave in the online state, and should be called just
 * after a slave received the RDB file for the initial synchronization, and
 * we are finally ready to send the incremental stream of commands.
 *
 * It does a few things:
 *
 * 1) Put the slave in ONLINE state (useless when the function is called
 *    because state is already ONLINE but repl_put_online_on_ack is true).
 * 2) Make sure the writable event is re-installed, since calling the SYNC
 *    command disables it, so that we can accumulate output buffer without
 *    sending it to the slave.
 * 3) Update the count of good slaves. */
void putSlaveOnline(client *slave) {
    slave->replstate = SLAVE_STATE_ONLINE;
    slave->repl_put_online_on_ack = 0;
    slave->repl_ack_time = slave->vel->unixtime; /* Prevent false timeout. */
    if (aeCreateFileEvent(slave->vel->el, slave->conn->sd, AE_WRITABLE,
        sendReplyToClient, slave) == AE_ERR) {
        log_warn("unable to register writable event for slave bulk transfer: %s", strerror(errno));
        freeClient(slave);
        return;
    }
    refreshGoodSlavesCount();
    log_debug(LOG_NOTICE,"Synchronization with slave %s succeeded",
        replicationGetSlaveName(slave));
}

/* Send a REPLCONF ACK command to the master to inform it about the current
 * processed offset. If we are not connected with a master, the command has
 * no effects. */
void replicationSendAck(void) {
    client *c = repl.master;

    if (c != NULL) {
        c->flags |= CLIENT_MASTER_FORCE_REPLY;
        addReplyMultiBulkLen(c,3);
        addReplyBulkCString(c,"REPLCONF");
        addReplyBulkCString(c,"ACK");
        addReplyBulkLongLong(c,c->reploff);
        c->flags &= ~CLIENT_MASTER_FORCE_REPLY;
    }
}

void replicationFeedMonitors(client *c, dlist *monitors, int dictid, robj **argv, int argc) {
    dlistNode *ln;
    dlistIter li;
    int j;
    sds cmdrepr = sdsnew("+");
    robj *cmdobj;
    struct timeval tv;

    gettimeofday(&tv,NULL);
    cmdrepr = sdscatprintf(cmdrepr,"%ld.%06ld ",(long)tv.tv_sec,(long)tv.tv_usec);
    if (c->flags & CLIENT_LUA) {
        cmdrepr = sdscatprintf(cmdrepr,"[%d lua] ",dictid);
    } else if (c->flags & CLIENT_UNIX_SOCKET) {
        cmdrepr = sdscatprintf(cmdrepr,"[%d unix:%s] ",dictid,server.unixsocket);
    } else {
        cmdrepr = sdscatprintf(cmdrepr,"[%d %s] ",dictid,getClientPeerId(c));
    }

    for (j = 0; j < argc; j++) {
        if (argv[j]->encoding == OBJ_ENCODING_INT) {
            cmdrepr = sdscatprintf(cmdrepr, "\"%ld\"", (long)argv[j]->ptr);
        } else {
            cmdrepr = sdscatrepr(cmdrepr,(char*)argv[j]->ptr,
                        sdslen(argv[j]->ptr));
        }
        if (j != argc-1)
            cmdrepr = sdscatlen(cmdrepr," ",1);
    }
    cmdrepr = sdscatlen(cmdrepr,"\r\n",2);
    cmdobj = createObject(OBJ_STRING,cmdrepr);

    dlistRewind(monitors,&li);
    while((ln = dlistNext(&li))) {
        client *monitor = ln->value;
        addReply(monitor,cmdobj);
    }
    decrRefCount(cmdobj);
}

/* Add data to the replication backlog.
 * This function also increments the global replication offset stored at
 * server.master_repl_offset, because there is no case where we want to feed
 * the backlog without incrementing the buffer. */
void feedReplicationBacklog(void *ptr, size_t len) {
    unsigned char *p = ptr;

    repl.master_repl_offset += len;

    /* This is a circular buffer, so write as much data we can at every
     * iteration and rewind the "idx" index if we reach the limit. */
    while(len) {
        size_t thislen = repl.repl_backlog_size - repl.repl_backlog_idx;
        if (thislen > len) thislen = len;
        memcpy(repl.repl_backlog+repl.repl_backlog_idx,p,thislen);
        repl.repl_backlog_idx += thislen;
        if (repl.repl_backlog_idx == repl.repl_backlog_size)
            repl.repl_backlog_idx = 0;
        len -= thislen;
        p += thislen;
        repl.repl_backlog_histlen += thislen;
    }
    if (repl.repl_backlog_histlen > repl.repl_backlog_size)
        repl.repl_backlog_histlen = repl.repl_backlog_size;
    /* Set the offset of the first byte we have in the backlog. */
    repl.repl_backlog_off = repl.master_repl_offset -
                              repl.repl_backlog_histlen + 1;
}

/* Wrapper for feedReplicationBacklog() that takes Redis string objects
 * as input. */
void feedReplicationBacklogWithObject(robj *o) {
    char llstr[LONG_STR_SIZE];
    void *p;
    size_t len;

    if (o->encoding == OBJ_ENCODING_INT) {
        len = ll2string(llstr,sizeof(llstr),(long)o->ptr);
        p = llstr;
    } else {
        len = sdslen(o->ptr);
        p = o->ptr;
    }
    feedReplicationBacklog(p,len);
}

void replicationFeedSlaves(dlist *slaves, int dictid, robj **argv, int argc) {
    dlistNode *ln;
    dlistIter li;
    int j, len;
    char llstr[LONG_STR_SIZE];

    /* If there aren't slaves, and there is no backlog buffer to populate,
     * we can return ASAP. */
    if (repl.repl_backlog == NULL && dlistLength(slaves) == 0) return;

    /* We can't have slaves attached and no backlog. */
    ASSERT(!(dlistLength(slaves) != 0 && repl.repl_backlog == NULL));

    /* Send SELECT command to every slave if needed. */
    if (repl.slaveseldb != dictid) {
        robj *selectcmd;

        /* For a few DBs we have pre-computed SELECT command. */
        if (dictid >= 0 && dictid < PROTO_SHARED_SELECT_CMDS) {
            selectcmd = shared.select[dictid];
        } else {
            int dictid_len;

            dictid_len = ll2string(llstr,sizeof(llstr),dictid);
            selectcmd = createObject(OBJ_STRING,
                sdscatprintf(sdsempty(),
                "*2\r\n$6\r\nSELECT\r\n$%d\r\n%s\r\n",
                dictid_len, llstr));
        }

        /* Add the SELECT command into the backlog. */
        if (repl.repl_backlog) feedReplicationBacklogWithObject(selectcmd);

        /* Send it to slaves. */
        dlistRewind(slaves,&li);
        while((ln = dlistNext(&li))) {
            client *slave = ln->value;
            if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_START) continue;
            addReply(slave,selectcmd);
        }

        if (dictid < 0 || dictid >= PROTO_SHARED_SELECT_CMDS)
            decrRefCount(selectcmd);
    }
    repl.slaveseldb = dictid;

    /* Write the command to the replication backlog if any. */
    if (repl.repl_backlog) {
        char aux[LONG_STR_SIZE+3];

        /* Add the multi bulk reply length. */
        aux[0] = '*';
        len = ll2string(aux+1,sizeof(aux)-1,argc);
        aux[len+1] = '\r';
        aux[len+2] = '\n';
        feedReplicationBacklog(aux,len+3);

        for (j = 0; j < argc; j++) {
            long objlen = stringObjectLen(argv[j]);

            /* We need to feed the buffer with the object as a bulk reply
             * not just as a plain string, so create the $..CRLF payload len
             * and add the final CRLF */
            aux[0] = '$';
            len = ll2string(aux+1,sizeof(aux)-1,objlen);
            aux[len+1] = '\r';
            aux[len+2] = '\n';
            feedReplicationBacklog(aux,len+3);
            feedReplicationBacklogWithObject(argv[j]);
            feedReplicationBacklog(aux+len+1,2);
        }
    }

    /* Write the command to every slave. */
    dlistRewind(repl.slaves,&li);
    while((ln = dlistNext(&li))) {
        client *slave = ln->value;

        /* Don't feed slaves that are still waiting for BGSAVE to start */
        if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_START) continue;

        /* Feed slaves that are waiting for the initial SYNC (so these commands
         * are queued in the output buffer until the initial SYNC completes),
         * or are already in sync with the master. */

        /* Add the multi bulk length. */
        addReplyMultiBulkLen(slave,argc);

        /* Finally any additional argument that was not stored inside the
         * static buffer if any (from j to argc). */
        for (j = 0; j < argc; j++)
            addReplyBulk(slave,argv[j]);
    }
}

