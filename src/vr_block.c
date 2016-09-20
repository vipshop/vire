#include <vr_core.h>


/* Unblock a client calling the right function depending on the kind
 * of operation the client is blocking for. */
void unblockClient(client *c) {
    if (c->btype == BLOCKED_LIST) {
        unblockClientWaitingData(c);
    } else if (c->btype == BLOCKED_WAIT) {
        unblockClientWaitingReplicas(c);
    } else {
        serverPanic("Unknown btype in unblockClient().");
    }
    /* Clear the flags, and put the client in the unblocked list so that
     * we'll process new commands in its query buffer ASAP. */
    c->flags &= ~CLIENT_BLOCKED;
    c->btype = BLOCKED_NONE;
    c->vel->bpop_blocked_clients--;
    /* The client may already be into the unblocked list because of a previous
     * blocking operation, don't add back it into the list multiple times. */
    if (!(c->flags & CLIENT_UNBLOCKED)) {
        c->flags |= CLIENT_UNBLOCKED;
        dlistAddNodeTail(c->vel->unblocked_clients,c);
    }
}

/* Get a timeout value from an object and store it into 'timeout'.
 * The final timeout is always stored as milliseconds as a time where the
 * timeout will expire, however the parsing is performed according to
 * the 'unit' that can be seconds or milliseconds.
 *
 * Note that if the timeout is zero (usually from the point of view of
 * commands API this means no timeout) the value stored into 'timeout'
 * is zero. */
int getTimeoutFromObjectOrReply(client *c, robj *object, long long *timeout, int unit) {
    long long tval;

    if (getLongLongFromObjectOrReply(c,object,&tval,
        "timeout is not an integer or out of range") != VR_OK)
        return VR_ERROR;

    if (tval < 0) {
        addReplyError(c,"timeout is negative");
        return VR_ERROR;
    }

    if (tval > 0) {
        if (unit == UNIT_SECONDS) tval *= 1000;
        tval += vr_msec_now();
    }
    *timeout = tval;

    return VR_OK;
}

/* Block a client for the specific operation type. Once the CLIENT_BLOCKED
 * flag is set client query buffer is not longer processed, but accumulated,
 * and will be processed when the client is unblocked. */
void blockClient(client *c, int btype) {
    c->flags |= CLIENT_BLOCKED;
    c->btype = btype;
    c->vel->bpop_blocked_clients++;
}
