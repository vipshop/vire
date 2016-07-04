#ifndef _VR_T_SET_H_
#define _VR_T_SET_H_

robj *setTypeCreate(robj *value);
int setTypeAdd(robj *subject, robj *value);
int setTypeRemove(robj *setobj, robj *value);
int setTypeIsMember(robj *subject, robj *value);
setTypeIterator *setTypeInitIterator(robj *subject);
void setTypeReleaseIterator(setTypeIterator *si);
int setTypeNext(setTypeIterator *si, robj **objele, int64_t *llele);
robj *setTypeNextObject(setTypeIterator *si);
int setTypeRandomElement(robj *setobj, robj **objele, int64_t *llele);

unsigned long setTypeSize(robj *subject);
void setTypeConvert(robj *setobj, int enc);
void saddCommand(client *c);
void sremCommand(client *c);
void smoveCommand(client *c);
void sismemberCommand(client *c);
void scardCommand(client *c);
void spopWithCountCommand(client *c);
void spopCommand(client *c);
void srandmemberWithCountCommand(client *c);
void srandmemberCommand(client *c);
void smembersCommand(client *c);
int qsortCompareSetsByCardinality(const void *s1, const void *s2);
int qsortCompareSetsByRevCardinality(const void *s1, const void *s2);
void sinterGenericCommand(client *c, robj **setkeys, unsigned long setnum, robj *dstkey);
void sinterCommand(client *c);
void sinterstoreCommand(client *c);
void sunionDiffGenericCommand(client *c, robj **setkeys, int setnum, robj *dstkey, int op);
void sunionCommand(client *c);
void sunionstoreCommand(client *c);
void sdiffCommand(client *c);
void sdiffstoreCommand(client *c);
void sscanCommand(client *c);

#endif
