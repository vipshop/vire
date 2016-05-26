#ifndef _VR_BLOCK_H_
#define _VR_BLOCK_H_

/* This structure holds the blocking operation state for a client.
 * The fields used depend on client->btype. */
typedef struct blockingState {
    /* Generic fields. */
    long long timeout;       /* Blocking operation timeout. If UNIX current time
                             * is > timeout then the operation timed out. */

    /* BLOCKED_LIST */
    dict *keys;             /* The keys we are waiting to terminate a blocking
                             * operation such as BLPOP. Otherwise NULL. */
    robj *target;           /* The key that should receive the element,
                             * for BRPOPLPUSH. */

    /* BLOCKED_WAIT */
    int numreplicas;        /* Number of replicas we are waiting for ACK. */
    long long reploffset;   /* Replication offset to reach. */
} blockingState;

void blockClient(struct client *c, int btype);
void unblockClient(struct client *c);
int getTimeoutFromObjectOrReply(struct client *c, robj *object, long long *timeout, int unit);

#endif
