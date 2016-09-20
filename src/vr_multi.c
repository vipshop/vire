#include <vr_core.h>

/* ===================== WATCH (CAS alike for MULTI/EXEC) ===================
 *
 * The implementation uses a per-DB hash table mapping keys to list of clients
 * WATCHing those keys, so that given a key that is going to be modified
 * we can mark all the associated clients as dirty.
 *
 * Also every client contains a list of WATCHed keys so that's possible to
 * un-watch such keys when the client is freed or when UNWATCH is called. */

/* In the client->watched_keys list we need to use watchedKey structures
 * as in order to identify a key in Redis we need both the key name and the
 * DB */
typedef struct watchedKey {
    robj *key;
    redisDb *db;
} watchedKey;

/* Unwatch all the keys watched by this client. To clean the EXEC dirty
 * flag is up to the caller. */
void unwatchAllKeys(client *c) {
    dlistIter li;
    dlistNode *ln;

    if (dlistLength(c->watched_keys) == 0) return;
    dlistRewind(c->watched_keys,&li);
    while((ln = dlistNext(&li))) {
        dlist *clients;
        watchedKey *wk;

        /* Lookup the watched key -> clients list and remove the client
         * from the list */
        wk = dlistNodeValue(ln);
        clients = dictFetchValue(wk->db->watched_keys, wk->key);
        serverAssertWithInfo(c,NULL,clients != NULL);
        dlistDelNode(clients,dlistSearchKey(clients,c));
        /* Kill the entry at all if this was the only client */
        if (dlistLength(clients) == 0)
            dictDelete(wk->db->watched_keys, wk->key);
        /* Remove this watched key from the client->watched list */
        dlistDelNode(c->watched_keys,ln);
        decrRefCount(wk->key);
        dfree(wk);
    }
}

/* Client state initialization for MULTI/EXEC */
void initClientMultiState(client *c) {
    c->mstate.commands = NULL;
    c->mstate.count = 0;
}

/* Release all the resources associated with MULTI/EXEC state */
void freeClientMultiState(client *c) {
    int j;

    for (j = 0; j < c->mstate.count; j++) {
        int i;
        multiCmd *mc = c->mstate.commands+j;

        for (i = 0; i < mc->argc; i++)
            decrRefCount(mc->argv[i]);
        dfree(mc->argv);
    }
    if (c->mstate.commands) dfree(c->mstate.commands);
}

/* Add a new command into the MULTI commands queue */
void queueMultiCommand(client *c) {
    multiCmd *mc;
    int j;

    c->mstate.commands = drealloc(c->mstate.commands,
            sizeof(multiCmd)*(c->mstate.count+1));
    mc = c->mstate.commands+c->mstate.count;
    mc->cmd = c->cmd;
    mc->argc = c->argc;
    mc->argv = dalloc(sizeof(robj*)*c->argc);
    memcpy(mc->argv,c->argv,sizeof(robj*)*c->argc);
    for (j = 0; j < c->argc; j++)
        incrRefCount(mc->argv[j]);
    c->mstate.count++;
}

/* Flag the transacation as DIRTY_EXEC so that EXEC will fail.
 * Should be called every time there is an error while queueing a command. */
void flagTransaction(client *c) {
    if (c->flags & CLIENT_MULTI)
        c->flags |= CLIENT_DIRTY_EXEC;
}

void execCommand(client *c) {
    addReply(c,shared.ok);
}

void discardCommand(client *c) {
    if (!(c->flags & CLIENT_MULTI)) {
        addReplyError(c,"DISCARD without MULTI");
        return;
    }
    discardTransaction(c);
    addReply(c,shared.ok);
}

void discardTransaction(client *c) {
    freeClientMultiState(c);
    initClientMultiState(c);
    c->flags &= ~(CLIENT_MULTI|CLIENT_DIRTY_CAS|CLIENT_DIRTY_EXEC);
    unwatchAllKeys(c);
}

void multiCommand(client *c) {
    if (c->flags & CLIENT_MULTI) {
        addReplyError(c,"MULTI calls can not be nested");
        return;
    }
    c->flags |= CLIENT_MULTI;
    addReply(c,shared.ok);
}

/* Watch for the specified key */
void watchForKey(client *c, robj *key) {
    dlist *clients = NULL;
    dlistIter li;
    dlistNode *ln;
    watchedKey *wk;

    /* Check if we are already watching for this key */
    dlistRewind(c->watched_keys,&li);
    while((ln = dlistNext(&li))) {
        wk = dlistNodeValue(ln);
        if (wk->db == c->db && equalStringObjects(key,wk->key))
            return; /* Key already watched */
    }
    /* This key is not already watched in this DB. Let's add it */
    clients = dictFetchValue(c->db->watched_keys,key);
    if (!clients) {
        clients = dlistCreate();
        dictAdd(c->db->watched_keys,key,clients);
        incrRefCount(key);
    }
    dlistAddNodeTail(clients,c);
    /* Add the new key to the list of keys watched by this client */
    wk = dalloc(sizeof(*wk));
    wk->key = key;
    wk->db = c->db;
    incrRefCount(key);
    dlistAddNodeTail(c->watched_keys,wk);
}

void watchCommand(client *c) {
    int j;

    if (c->flags & CLIENT_MULTI) {
        addReplyError(c,"WATCH inside MULTI is not allowed");
        return;
    }
    for (j = 1; j < c->argc; j++)
        watchForKey(c,c->argv[j]);
    addReply(c,shared.ok);
}

/* "Touch" a key, so that if this key is being WATCHed by some client the
 * next EXEC will fail. */
void touchWatchedKey(redisDb *db, robj *key) {
    dlist *clients;
    dlistIter li;
    dlistNode *ln;

    if (dictSize(db->watched_keys) == 0) return;
    clients = dictFetchValue(db->watched_keys, key);
    if (!clients) return;

    /* Mark all the clients watching this key as CLIENT_DIRTY_CAS */
    /* Check if we are already watching for this key */
    dlistRewind(clients,&li);
    while((ln = dlistNext(&li))) {
        client *c = dlistNodeValue(ln);

        c->flags |= CLIENT_DIRTY_CAS;
    }
}

/* On FLUSHDB or FLUSHALL all the watched keys that are present before the
 * flush but will be deleted as effect of the flushing operation should
 * be touched. "dbid" is the DB that's getting the flush. -1 if it is
 * a FLUSHALL operation (all the DBs flushed). */
void touchWatchedKeysOnFlush(int dbid) {
    dlistIter li1, li2;
    dlistNode *ln;

    /* For every client, check all the waited keys */
    dlistRewind(server.clients,&li1);
    while((ln = dlistNext(&li1))) {
        client *c = dlistNodeValue(ln);
        dlistRewind(c->watched_keys,&li2);
        while((ln = dlistNext(&li2))) {
            watchedKey *wk = dlistNodeValue(ln);

            /* For every watched key matching the specified DB, if the
             * key exists, mark the client as dirty, as the key will be
             * removed. */
            if (dbid == -1 || wk->db->id == dbid) {
                if (dictFind(wk->db->dict, wk->key->ptr) != NULL)
                    c->flags |= CLIENT_DIRTY_CAS;
            }
        }
    }
}

