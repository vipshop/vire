#ifndef _VR_HYPERLOGLOG_H_
#define _VR_HYPERLOGLOG_H_

uint64_t MurmurHash64A (const void * key, int len, unsigned int seed);
int hllPatLen(unsigned char *ele, size_t elesize, long *regp);
int hllDenseAdd(uint8_t *registers, unsigned char *ele, size_t elesize);
double hllDenseSum(uint8_t *registers, double *PE, int *ezp);
int hllSparseToDense(robj *o);
int hllSparseAdd(robj *o, unsigned char *ele, size_t elesize);
double hllSparseSum(uint8_t *sparse, int sparselen, double *PE, int *ezp, int *invalid);
double hllRawSum(uint8_t *registers, double *PE, int *ezp);
int hllAdd(robj *o, unsigned char *ele, size_t elesize);
int hllMerge(uint8_t *max, robj *hll);
robj *createHLLObject(void);
int isHLLObjectOrReply(struct client *c, robj *o);
void pfaddCommand(struct client *c);
void pfcountCommand(struct client *c);
void pfmergeCommand(struct client *c);
void pfselftestCommand(struct client *c);
void pfdebugCommand(struct client *c);

#endif
