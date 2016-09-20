#include <vr_core.h>

/* Unsubscribe a client from a channel. Returns 1 if the operation succeeded, or
 * 0 if the client was not subscribed to the specified channel. */
int pubsubUnsubscribeChannel(client *c, robj *channel, int notify) {
    dictEntry *de;
    dlist *clients;
    dlistNode *ln;
    int retval = 0;

    /* Remove the channel from the client -> channels hash table */
    incrRefCount(channel); /* channel may be just a pointer to the same object
                            we have in the hash tables. Protect it... */
    if (dictDelete(c->pubsub_channels,channel) == DICT_OK) {
        retval = 1;
        /* Remove the client from the channel -> clients list hash table */
        de = dictFind(c->vel->pubsub_channels,channel);
        serverAssertWithInfo(c,NULL,de != NULL);
        clients = dictGetVal(de);
        ln = dlistSearchKey(clients,c);
        serverAssertWithInfo(c,NULL,ln != NULL);
        dlistDelNode(clients,ln);
        if (dlistLength(clients) == 0) {
            /* Free the list and associated hash entry at all if this was
             * the latest client, so that it will be possible to abuse
             * Redis PUBSUB creating millions of channels. */
            dictDelete(c->vel->pubsub_channels,channel);
        }
    }
    /* Notify the client */
    if (notify) {
        addReply(c,shared.mbulkhdr[3]);
        addReply(c,shared.unsubscribebulk);
        addReplyBulk(c,channel);
        addReplyLongLong(c,dictSize(c->pubsub_channels)+
                       dlistLength(c->pubsub_patterns));

    }
    decrRefCount(channel); /* it is finally safe to release it */
    return retval;
}

/* Unsubscribe from all the channels. Return the number of channels the
 * client was subscribed to. */
int pubsubUnsubscribeAllChannels(client *c, int notify) {
    dictIterator *di = dictGetSafeIterator(c->pubsub_channels);
    dictEntry *de;
    int count = 0;

    while((de = dictNext(di)) != NULL) {
        robj *channel = dictGetKey(de);

        count += pubsubUnsubscribeChannel(c,channel,notify);
    }
    /* We were subscribed to nothing? Still reply to the client. */
    if (notify && count == 0) {
        addReply(c,shared.mbulkhdr[3]);
        addReply(c,shared.unsubscribebulk);
        addReply(c,shared.nullbulk);
        addReplyLongLong(c,dictSize(c->pubsub_channels)+
                       dlistLength(c->pubsub_patterns));
    }
    dictReleaseIterator(di);
    return count;
}

/* Unsubscribe a client from a channel. Returns 1 if the operation succeeded, or
 * 0 if the client was not subscribed to the specified channel. */
int pubsubUnsubscribePattern(client *c, robj *pattern, int notify) {
    dlistNode *ln;
    pubsubPattern pat;
    int retval = 0;

    incrRefCount(pattern); /* Protect the object. May be the same we remove */
    if ((ln = dlistSearchKey(c->pubsub_patterns,pattern)) != NULL) {
        retval = 1;
        dlistDelNode(c->pubsub_patterns,ln);
        pat.client = c;
        pat.pattern = pattern;
        ln = dlistSearchKey(c->vel->pubsub_patterns,&pat);
        dlistDelNode(c->vel->pubsub_patterns,ln);
    }
    /* Notify the client */
    if (notify) {
        addReply(c,shared.mbulkhdr[3]);
        addReply(c,shared.punsubscribebulk);
        addReplyBulk(c,pattern);
        addReplyLongLong(c,dictSize(c->pubsub_channels)+
                       dlistLength(c->pubsub_patterns));
    }
    decrRefCount(pattern);
    return retval;
}

/* Unsubscribe from all the patterns. Return the number of patterns the
 * client was subscribed from. */
int pubsubUnsubscribeAllPatterns(client *c, int notify) {
    dlistNode *ln;
    dlistIter li;
    int count = 0;

    dlistRewind(c->pubsub_patterns,&li);
    while ((ln = dlistNext(&li)) != NULL) {
        robj *pattern = ln->value;

        count += pubsubUnsubscribePattern(c,pattern,notify);
    }
    if (notify && count == 0) {
        /* We were subscribed to nothing? Still reply to the client. */
        addReply(c,shared.mbulkhdr[3]);
        addReply(c,shared.punsubscribebulk);
        addReply(c,shared.nullbulk);
        addReplyLongLong(c,dictSize(c->pubsub_channels)+
                       dlistLength(c->pubsub_patterns));
    }
    return count;
}

/*-----------------------------------------------------------------------------
 * Pubsub commands implementation
 *----------------------------------------------------------------------------*/


/* Subscribe a client to a channel. Returns 1 if the operation succeeded, or
 * 0 if the client was already subscribed to that channel. */
int pubsubSubscribeChannel(client *c, robj *channel) {
    dictEntry *de;
    dlist *clients = NULL;
    int retval = 0;

    /* Add the channel to the client -> channels hash table */
    if (dictAdd(c->pubsub_channels,channel,NULL) == DICT_OK) {
        retval = 1;
        incrRefCount(channel);
        /* Add the client to the channel -> list of clients hash table */
        de = dictFind(c->vel->pubsub_channels,channel);
        if (de == NULL) {
            clients = dlistCreate();
            dictAdd(c->vel->pubsub_channels,channel,clients);
            incrRefCount(channel);
        } else {
            clients = dictGetVal(de);
        }
        dlistAddNodeTail(clients,c);
    }
    /* Notify the client */
    addReply(c,shared.mbulkhdr[3]);
    addReply(c,shared.subscribebulk);
    addReplyBulk(c,channel);
    addReplyLongLong(c,clientSubscriptionsCount(c));
    return retval;
}

void subscribeCommand(client *c) {
    int j;

    for (j = 1; j < c->argc; j++)
        pubsubSubscribeChannel(c,c->argv[j]);
    c->flags |= CLIENT_PUBSUB;
}

/* Return the number of channels + patterns a client is subscribed to. */
int clientSubscriptionsCount(client *c) {
    return dictSize(c->pubsub_channels)+
           dlistLength(c->pubsub_patterns);
}

void unsubscribeCommand(client *c) {
    if (c->argc == 1) {
        pubsubUnsubscribeAllChannels(c,1);
    } else {
        int j;

        for (j = 1; j < c->argc; j++)
            pubsubUnsubscribeChannel(c,c->argv[j],1);
    }
    if (clientSubscriptionsCount(c) == 0) c->flags &= ~CLIENT_PUBSUB;
}

/* Subscribe a client to a pattern. Returns 1 if the operation succeeded, or 0 if the client was already subscribed to that pattern. */
int pubsubSubscribePattern(client *c, robj *pattern) {
    int retval = 0;

    if (dlistSearchKey(c->pubsub_patterns,pattern) == NULL) {
        retval = 1;
        pubsubPattern *pat;
        dlistAddNodeTail(c->pubsub_patterns,pattern);
        incrRefCount(pattern);
        pat = dalloc(sizeof(*pat));
        pat->pattern = getDecodedObject(pattern);
        pat->client = c;
        dlistAddNodeTail(c->vel->pubsub_patterns,pat);
    }
    /* Notify the client */
    addReply(c,shared.mbulkhdr[3]);
    addReply(c,shared.psubscribebulk);
    addReplyBulk(c,pattern);
    addReplyLongLong(c,clientSubscriptionsCount(c));
    return retval;
}

void psubscribeCommand(client *c) {
    int j;

    for (j = 1; j < c->argc; j++)
        pubsubSubscribePattern(c,c->argv[j]);
    c->flags |= CLIENT_PUBSUB;
}

void punsubscribeCommand(client *c) {
    if (c->argc == 1) {
        pubsubUnsubscribeAllPatterns(c,1);
    } else {
        int j;

        for (j = 1; j < c->argc; j++)
            pubsubUnsubscribePattern(c,c->argv[j],1);
    }
    if (clientSubscriptionsCount(c) == 0) c->flags &= ~CLIENT_PUBSUB;
}

/* Publish a message */
int pubsubPublishMessage(robj *channel, robj *message) {
    int receivers = 0;
    dictEntry *de;
    dlistNode *ln;
    dlistIter li;

    /* Send to clients listening for that channel */
    de = dictFind(server.pubsub_channels,channel);
    if (de) {
        dlist *list = dictGetVal(de);
        dlistNode *ln;
        dlistIter li;

        dlistRewind(list,&li);
        while ((ln = dlistNext(&li)) != NULL) {
            client *c = ln->value;

            addReply(c,shared.mbulkhdr[3]);
            addReply(c,shared.messagebulk);
            addReplyBulk(c,channel);
            addReplyBulk(c,message);
            receivers++;
        }
    }
    /* Send to clients listening to matching channels */
    if (dlistLength(server.pubsub_patterns)) {
        dlistRewind(server.pubsub_patterns,&li);
        channel = getDecodedObject(channel);
        while ((ln = dlistNext(&li)) != NULL) {
            pubsubPattern *pat = ln->value;

            if (stringmatchlen((char*)pat->pattern->ptr,
                                sdslen(pat->pattern->ptr),
                                (char*)channel->ptr,
                                sdslen(channel->ptr),0)) {
                addReply(pat->client,shared.mbulkhdr[4]);
                addReply(pat->client,shared.pmessagebulk);
                addReplyBulk(pat->client,pat->pattern);
                addReplyBulk(pat->client,channel);
                addReplyBulk(pat->client,message);
                receivers++;
            }
        }
        decrRefCount(channel);
    }
    return receivers;
}

