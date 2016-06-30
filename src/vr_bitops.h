#ifndef _VR_BITOPS_H_
#define _VR_BITOPS_H_

size_t redisPopcount(void *s, long count);
long redisBitpos(void *s, unsigned long count, int bit);
void setUnsignedBitfield(unsigned char *p, uint64_t offset, uint64_t bits, uint64_t value);
void setSignedBitfield(unsigned char *p, uint64_t offset, uint64_t bits, int64_t value);
uint64_t getUnsignedBitfield(unsigned char *p, uint64_t offset, uint64_t bits);
int64_t getSignedBitfield(unsigned char *p, uint64_t offset, uint64_t bits);
int checkUnsignedBitfieldOverflow(uint64_t value, int64_t incr, uint64_t bits, int owtype, uint64_t *limit);
int checkSignedBitfieldOverflow(int64_t value, int64_t incr, uint64_t bits, int owtype, int64_t *limit);
void printBits(unsigned char *p, unsigned long count);
int getBitOffsetFromArgument(struct client *c, robj *o, size_t *offset, int hash, int bits);
int getBitfieldTypeFromArgument(struct client *c, robj *o, int *sign, int *bits);
robj *lookupStringForBitCommand(struct client *c, size_t maxbit, int *expired);
void setbitCommand(struct client *c);
void getbitCommand(struct client *c);
void bitopCommand(struct client *c);
void bitcountCommand(struct client *c);
void bitposCommand(struct client *c);
void bitfieldCommand(client *c);
#endif
