#ifndef _DHASHTABLE_H__
#define _DHASHTABLE_H__

#define HT_OK 0
#define HT_ERR 1

typedef struct dhashtable {
    unsigned long long *table;
    unsigned long size;
    unsigned long used;
} dhashtable;

/* API */
dhashtable *dhashtableCreate(unsigned long size);
void dhashtableRelease(dhashtable *ht);
void dhashtableReset(dhashtable *ht);
int dhashtableExpandIfNeeded(dhashtable *ht, unsigned long size);

int dhashtableAdd(dhashtable *ht, const void *key);
long long dhashtableFind(dhashtable *ht, const void *key);

#endif /* _DHASHTABLE_H__ */
