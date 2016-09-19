#ifndef _DHASHTABLE_H__
#define _DHASHTABLE_H__

#define HT_OK 0
#define HT_ERR 1

typedef struct dhashtableEntry {
    char *key;
    size_t len;
    
    struct dhashtableEntry *next;
} dhashtableEntry;

typedef struct dhashtable {
    dhashtableEntry *table;
    unsigned long size;
    unsigned long used;
} dhashtable;

typedef struct dhashtableIterator {
    dhashtable *d;
    long index;
    dhashtableEntry *entry, *nextEntry;
} dhashtableIterator;

/* API */
dhashtable *dhashtableCreate(unsigned long size);
void dhashtableRelease(dhashtable *ht);

int dhashtableAdd(dhashtable *ht, char *key, size_t len);
dhashtableEntry *dhashtableFind(dhashtable *ht, const char *key, size_t len);

#endif /* _DHASHTABLE_H__ */
