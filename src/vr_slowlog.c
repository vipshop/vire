#include <vr_core.h>

static pthread_rwlock_t rwlocker;
static dlist *slowlog;                  /* SLOWLOG list of commands */
static long long slowlog_entry_id;     /* SLOWLOG current entry ID */

/* Create a new slowlog entry.
 * Incrementing the ref count of all the objects retained is up to
 * this function. */
slowlogEntry *slowlogCreateEntry(robj **argv, int argc, long long duration) {
    slowlogEntry *se = dalloc(sizeof(*se));
    int j, slargc = argc;

    if (slargc > SLOWLOG_ENTRY_MAX_ARGC) slargc = SLOWLOG_ENTRY_MAX_ARGC;
    se->argc = slargc;
    se->argv = dalloc(sizeof(robj*)*slargc);
    for (j = 0; j < slargc; j++) {
        /* Logging too many arguments is a useless memory waste, so we stop
         * at SLOWLOG_ENTRY_MAX_ARGC, but use the last argument to specify
         * how many remaining arguments there were in the original command. */
        if (slargc != argc && j == slargc-1) {
            se->argv[j] = createObject(OBJ_STRING,
                sdscatprintf(sdsempty(),"... (%d more arguments)",
                argc-slargc+1));
        } else {
            /* Trim too long strings as well... */
            if (argv[j]->type == OBJ_STRING &&
                sdsEncodedObject(argv[j]) &&
                sdslen(argv[j]->ptr) > SLOWLOG_ENTRY_MAX_STRING)
            {
                sds s = sdsnewlen(argv[j]->ptr, SLOWLOG_ENTRY_MAX_STRING);

                s = sdscatprintf(s,"... (%lu more bytes)",
                    (unsigned long)
                    sdslen(argv[j]->ptr) - SLOWLOG_ENTRY_MAX_STRING);
                se->argv[j] = createObject(OBJ_STRING,s);
            } else {
                se->argv[j] = dupStringObjectUnconstant(argv[j]);
            }
        }
    }
    se->time = time(NULL);
    se->duration = duration;
    return se;
}

/* Free a slow log entry. The argument is void so that the prototype of this
 * function matches the one of the 'free' method of adlist.c.
 *
 * This function will take care to release all the retained object. */
void slowlogFreeEntry(void *septr) {
    slowlogEntry *se = septr;
    int j;

    for (j = 0; j < se->argc; j++)
        freeObject(se->argv[j]);
    dfree(se->argv);
    dfree(se);
}

/* Initialize the slow log. This function should be called a single time
 * at server startup. */
void slowlogInit(void) {
    pthread_rwlock_init(&rwlocker,NULL);
    slowlog = dlistCreate();
    slowlog_entry_id = 0;
    dlistSetFreeMethod(slowlog,slowlogFreeEntry);
}

/* Push a new entry into the slow log.
 * This function will make sure to trim the slow log accordingly to the
 * configured max length. */
void slowlogPushEntryIfNeeded(vr_eventloop *vel, robj **argv, int argc, long long duration) {
    long long slowlog_log_slower_than;
    int slowlog_max_len;
    
    slowlog_log_slower_than = vel->cc.slowlog_log_slower_than;
    if (slowlog_log_slower_than < 0) return; /* Slowlog disabled */
    if (duration >= slowlog_log_slower_than) {
        slowlogEntry *se = slowlogCreateEntry(argv,argc,duration);
        pthread_rwlock_wrlock(&rwlocker);
        se->id = slowlog_entry_id++;
        dlistAddNodeHead(slowlog,se);
        pthread_rwlock_unlock(&rwlocker);
    }

    conf_server_get(CONFIG_SOPN_SLOWLOGML,&slowlog_max_len);
    /* Remove old entries if needed. */
    pthread_rwlock_wrlock(&rwlocker);
    while (dlistLength(slowlog) > slowlog_max_len)
        dlistDelNode(slowlog,dlistLast(slowlog));
    pthread_rwlock_unlock(&rwlocker);
}

/* Remove all the entries from the current slow log. */
void slowlogReset(void) {
    pthread_rwlock_wrlock(&rwlocker);
    while (dlistLength(slowlog) > 0)
        dlistDelNode(slowlog,dlistLast(slowlog));
    pthread_rwlock_unlock(&rwlocker);
}

/* The SLOWLOG command. Implements all the subcommands needed to handle the
 * Redis slow log. */
void slowlogCommand(client *c) {
    if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr,"reset")) {
        slowlogReset();
        addReply(c,shared.ok);
    } else if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr,"len")) {
        unsigned long len;
        pthread_rwlock_rdlock(&rwlocker);
        len = dlistLength(slowlog);
        pthread_rwlock_unlock(&rwlocker);
        addReplyLongLong(c,len);
    } else if ((c->argc == 2 || c->argc == 3) &&
               !strcasecmp(c->argv[1]->ptr,"get"))
    {
        long count = 10, sent = 0;
        dlistIter li;
        void *totentries;
        dlistNode *ln;
        slowlogEntry *se;

        if (c->argc == 3 &&
            getLongFromObjectOrReply(c,c->argv[2],&count,NULL) != VR_OK)
            return;

        pthread_rwlock_rdlock(&rwlocker);
        dlistRewind(slowlog,&li);
        totentries = addDeferredMultiBulkLength(c);
        while(count-- && (ln = dlistNext(&li))) {
            int j;

            se = ln->value;
            addReplyMultiBulkLen(c,4);
            addReplyLongLong(c,se->id);
            addReplyLongLong(c,se->time);
            addReplyLongLong(c,se->duration);
            addReplyMultiBulkLen(c,se->argc);
            for (j = 0; j < se->argc; j++)
                addReplyBulk(c,se->argv[j]);
            sent++;
        }
        pthread_rwlock_unlock(&rwlocker);
        setDeferredMultiBulkLength(c,totentries,sent);
    } else {
        addReplyError(c,
            "Unknown SLOWLOG subcommand or wrong # of args. Try GET, RESET, LEN.");
    }
}

