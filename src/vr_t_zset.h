#ifndef _VR_T_ZSET_H_
#define _VR_T_ZSET_H_

/* Struct to hold a inclusive/exclusive range spec by score comparison. */
typedef struct {
    double min, max;
    int minex, maxex; /* are min or max exclusive? */
} zrangespec;

/* Struct to hold an inclusive/exclusive range spec by lexicographic comparison. */
typedef struct {
    robj *min, *max;  /* May be set to shared.(minstring|maxstring) */
    int minex, maxex; /* are min or max exclusive? */
} zlexrangespec;

typedef struct {
    robj *subject;
    int type; /* Set, sorted set */
    int encoding;
    double weight;

    union {
        /* Set iterators. */
        union _iterset {
            struct {
                intset *is;
                int ii;
            } is;
            struct {
                dict *dict;
                dictIterator *di;
                dictEntry *de;
            } ht;
        } set;

        /* Sorted set iterators. */
        union _iterzset {
            struct {
                unsigned char *zl;
                unsigned char *eptr, *sptr;
            } zl;
            struct {
                zset *zs;
                zskiplistNode *node;
            } sl;
        } zset;
    } iter;
} zsetopsrc;

/* Store value retrieved from the iterator. */
typedef struct {
    int flags;
    unsigned char _buf[32]; /* Private buffer. */
    robj *ele;
    unsigned char *estr;
    unsigned int elen;
    long long ell;
    double score;
} zsetopval;

zskiplistNode *zslCreateNode(int level, double score, robj *obj);
zskiplist *zslCreate(void);
void zslFreeNode(zskiplistNode *node);
void zslFree(zskiplist *zsl);
int zslRandomLevel(void);
zskiplistNode *zslInsert(zskiplist *zsl, double score, robj *obj);
void zslDeleteNode(zskiplist *zsl, zskiplistNode *x, zskiplistNode **update);
int zslDelete(zskiplist *zsl, double score, robj *obj);
int zslValueLteMax(double value, zrangespec *spec);
int zslIsInRange(zskiplist *zsl, zrangespec *range);
zskiplistNode *zslFirstInRange(zskiplist *zsl, zrangespec *range);
zskiplistNode *zslLastInRange(zskiplist *zsl, zrangespec *range);
unsigned long zslDeleteRangeByScore(zskiplist *zsl, zrangespec *range, dict *dict);
unsigned long zslDeleteRangeByLex(zskiplist *zsl, zlexrangespec *range, dict *dict);
unsigned long zslDeleteRangeByRank(zskiplist *zsl, unsigned int start, unsigned int end, dict *dict);
unsigned long zslGetRank(zskiplist *zsl, double score, robj *o);
zskiplistNode* zslGetElementByRank(zskiplist *zsl, unsigned long rank);
int zslParseLexRangeItem(robj *item, robj **dest, int *ex);
void zslFreeLexRange(zlexrangespec *spec);
int compareStringObjectsForLexRange(robj *a, robj *b);
int zslIsInLexRange(zskiplist *zsl, zlexrangespec *range);
zskiplistNode *zslFirstInLexRange(zskiplist *zsl, zlexrangespec *range);
zskiplistNode *zslLastInLexRange(zskiplist *zsl, zlexrangespec *range);
double zzlGetScore(unsigned char *sptr);
robj *ziplistGetObject(unsigned char *sptr);
int zzlCompareElements(unsigned char *eptr, unsigned char *cstr, unsigned int clen);
unsigned int zzlLength(unsigned char *zl);
void zzlNext(unsigned char *zl, unsigned char **eptr, unsigned char **sptr);
void zzlPrev(unsigned char *zl, unsigned char **eptr, unsigned char **sptr);
int zzlIsInRange(unsigned char *zl, zrangespec *range);
unsigned char *zzlFirstInRange(unsigned char *zl, zrangespec *range);
unsigned char *zzlLastInRange(unsigned char *zl, zrangespec *range);
int zzlIsInLexRange(unsigned char *zl, zlexrangespec *range);
unsigned char *zzlFirstInLexRange(unsigned char *zl, zlexrangespec *range);

unsigned char *zzlLastInLexRange(unsigned char *zl, zlexrangespec *range);
unsigned char *zzlFind(unsigned char *zl, robj *ele, double *score);
unsigned char *zzlDelete(unsigned char *zl, unsigned char *eptr);
unsigned char *zzlInsertAt(unsigned char *zl, unsigned char *eptr, robj *ele, double score);
unsigned char *zzlInsert(unsigned char *zl, robj *ele, double score);
unsigned char *zzlDeleteRangeByScore(unsigned char *zl, zrangespec *range, unsigned long *deleted);
unsigned char *zzlDeleteRangeByLex(unsigned char *zl, zlexrangespec *range, unsigned long *deleted);
unsigned char *zzlDeleteRangeByRank(unsigned char *zl, unsigned int start, unsigned int end, unsigned long *deleted);
unsigned int zsetLength(robj *zobj);
void zsetConvert(robj *zobj, int encoding);
void zsetConvertToZiplistIfNeeded(robj *zobj, size_t maxelelen);
int zsetScore(robj *zobj, robj *member, double *score);

void zaddGenericCommand(client *c, int flags);
void zaddCommand(client *c);
void zincrbyCommand(client *c);
void zremCommand(client *c);
void zremrangeGenericCommand(client *c, int rangetype);
void zremrangebyrankCommand(client *c);
void zremrangebyscoreCommand(client *c);
void zremrangebylexCommand(client *c) ;

void zuiInitIterator(zsetopsrc *op);
void zuiClearIterator(zsetopsrc *op);
int zuiLength(zsetopsrc *op);

/* Check if the current value is valid. If so, store it in the passed structure
 * and move to the next element. If not valid, this means we have reached the
 * end of the structure and can abort. */
int zuiNext(zsetopsrc *op, zsetopval *val);
int zuiLongLongFromValue(zsetopval *val);
robj *zuiObjectFromValue(zsetopval *val);
int zuiBufferFromValue(zsetopval *val);

/* Find value pointed to by val in the source pointer to by op. When found,
 * return 1 and store its score in target. Return 0 otherwise. */
int zuiFind(zsetopsrc *op, zsetopval *val, double *score);
int zuiCompareByCardinality(const void *s1, const void *s2);
void zunionInterGenericCommand(client *c, robj *dstkey, int op);
void zunionstoreCommand(client *c);
void zinterstoreCommand(client *c);
void zrangeGenericCommand(client *c, int reverse);
void zrangeCommand(client *c);
void zrevrangeCommand(client *c);

/* This command implements ZRANGEBYSCORE, ZREVRANGEBYSCORE. */
void genericZrangebyscoreCommand(client *c, int reverse);
void zrangebyscoreCommand(client *c);
void zrevrangebyscoreCommand(client *c);
void zcountCommand(client *c);
void zlexcountCommand(client *c);

/* This command implements ZRANGEBYLEX, ZREVRANGEBYLEX. */
void genericZrangebylexCommand(client *c, int reverse);
void zrangebylexCommand(client *c);
void zrevrangebylexCommand(client *c);
void zcardCommand(client *c);
void zscoreCommand(client *c);
void zrankGenericCommand(client *c, int reverse);
void zrankCommand(client *c);
void zrevrankCommand(client *c);
void zscanCommand(client *c);

#endif
