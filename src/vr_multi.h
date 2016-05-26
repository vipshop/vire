#ifndef _VR_MULTI_H_
#define _VR_MULTI_H_

/* Client MULTI/EXEC state */
typedef struct multiCmd {
    robj **argv;
    int argc;
    struct redisCommand *cmd;
} multiCmd;

typedef struct multiState {
    multiCmd *commands;     /* Array of MULTI commands */
    int count;              /* Total number of MULTI commands */
    int minreplicas;        /* MINREPLICAS for synchronous replication */
    time_t minreplicas_timeout; /* MINREPLICAS timeout as unixtime. */
} multiState;

void unwatchAllKeys(struct client *c);
void initClientMultiState(struct client *c);
void freeClientMultiState(struct client *c);
void queueMultiCommand(struct client *c);

void flagTransaction(struct client *c);
void execCommand(struct client *c);
void discardCommand(struct client *c);
void discardTransaction(struct client *c);
void multiCommand(struct client *c);
void watchForKey(struct client *c, robj *key);
void watchCommand(struct client *c);
void touchWatchedKey(redisDb *db, robj *key);
void touchWatchedKeysOnFlush(int dbid) ;

#endif
