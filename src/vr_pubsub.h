#ifndef _VR_PUBSUB_H_
#define _VR_PUBSUB_H_

typedef struct pubsubPattern {
    client *client;
    robj *pattern;
} pubsubPattern;

int pubsubUnsubscribeChannel(client *c, robj *channel, int notify);
int pubsubUnsubscribeAllChannels(client *c, int notify);
int pubsubUnsubscribePattern(client *c, robj *pattern, int notify);
int pubsubUnsubscribeAllPatterns(client *c, int notify);
int pubsubSubscribeChannel(client *c, robj *channel);
int clientSubscriptionsCount(client *c);
void subscribeCommand(client *c);
void unsubscribeCommand(client *c);
void psubscribeCommand(client *c);
void punsubscribeCommand(client *c);
int pubsubSubscribePattern(client *c, robj *pattern);
int pubsubPublishMessage(robj *channel, robj *message);

#endif
