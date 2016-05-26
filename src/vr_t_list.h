#ifndef _VR_T_LIST_H_
#define _VR_T_LIST_H_

void listTypePush(robj *subject, robj *value, int where);
void *listPopSaver(unsigned char *data, unsigned int sz);
robj *listTypePop(robj *subject, int where);
unsigned long listTypeLength(robj *subject);
listTypeIterator *listTypeInitIterator(robj *subject, long index, unsigned char direction);
void listTypeReleaseIterator(listTypeIterator *li);
int listTypeNext(listTypeIterator *li, listTypeEntry *entry);
robj *listTypeGet(listTypeEntry *entry);
void listTypeInsert(listTypeEntry *entry, robj *value, int where);
int listTypeEqual(listTypeEntry *entry, robj *o);
void listTypeDelete(listTypeIterator *iter, listTypeEntry *entry);
void listTypeConvert(robj *subject, int enc);
void pushGenericCommand(client *c, int where);
void lpushCommand(client *c);
void rpushCommand(client *c);
void pushxGenericCommand(client *c, robj *refval, robj *val, int where);
void lpushxCommand(client *c);
void rpushxCommand(client *c);
void linsertCommand(client *c);
void llenCommand(client *c);
void lindexCommand(client *c);
void lsetCommand(client *c);
void popGenericCommand(client *c, int where);
void lpopCommand(client *c);
void rpopCommand(client *c);
void lrangeCommand(client *c);
void ltrimCommand(client *c);
void lremCommand(client *c);
void rpoplpushHandlePush(client *c, robj *dstkey, robj *dstobj, robj *value);
void rpoplpushCommand(client *c);
void blockForKeys(client *c, robj **keys, int numkeys, mstime_t timeout, robj *target);
void unblockClientWaitingData(client *c);
void signalListAsReady(redisDb *db, robj *key);
int serveClientBlockedOnList(client *receiver, robj *key, robj *dstkey, redisDb *db, robj *value, int where);
void handleClientsBlockedOnLists(void);
void blockingPopGenericCommand(client *c, int where);
void blpopCommand(client *c);
void brpopCommand(client *c);
void brpoplpushCommand(client *c);

#endif
