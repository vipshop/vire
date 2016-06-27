#ifndef _VR_T_HASH_H_
#define _VR_T_HASH_H_

void hashTypeTryConversion(robj *o, robj **argv, int start, int end);
void hashTypeTryObjectEncoding(robj *subject, robj **o1, robj **o2);
int hashTypeGetFromZiplist(robj *o, robj *field, unsigned char **vstr, unsigned int *vlen, long long *vll);
int hashTypeGetFromHashTable(robj *o, robj *field, robj **value);
robj *hashTypeGetObject(robj *o, robj *field);
size_t hashTypeGetValueLength(robj *o, robj *field);
int hashTypeExists(robj *o, robj *field);
int hashTypeSet(robj *o, robj *field, robj *value);
int hashTypeDelete(robj *o, robj *field);
unsigned long hashTypeLength(robj *o);
hashTypeIterator *hashTypeInitIterator(robj *subject);
void hashTypeReleaseIterator(hashTypeIterator *hi);
int hashTypeNext(hashTypeIterator *hi);
void hashTypeCurrentFromZiplist(hashTypeIterator *hi, int what, unsigned char **vstr, unsigned int *vlen, long long *vll);
void hashTypeCurrentFromHashTable(hashTypeIterator *hi, int what, robj **dst);
robj *hashTypeCurrentObject(hashTypeIterator *hi, int what);
robj *hashTypeLookupWriteOrCreate(client *c, robj *key, int *expired);
void hashTypeConvertZiplist(robj *o, int enc);
void hashTypeConvert(robj *o, int enc);
void hsetCommand(client *c);
void hsetnxCommand(client *c);
void hmsetCommand(client *c);
void hincrbyCommand(client *c);
void hincrbyfloatCommand(client *c);
void hgetCommand(client *c);
void hmgetCommand(client *c);
void hdelCommand(client *c);
void hlenCommand(client *c);
void hstrlenCommand(client *c);
void genericHgetallCommand(client *c, int flags);
void hkeysCommand(client *c);
void hvalsCommand(client *c);
void hgetallCommand(client *c);
void hexistsCommand(client *c);
void hscanCommand(client *c);

#endif
