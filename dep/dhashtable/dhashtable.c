#include <stdlib.h>
#include <stdio.h>

#include <dhashkit.h>
#include <dhashtable.h>

#define DHASHTABLE_SPREAD_TIMES 4
#define DHASHTABLE_EXPAND_TIMES 3

#define DHASHTABLE_INIT_SIZE 2862933

dhashtable *dhashtableCreate(unsigned long size)
{
    dhashtable *ht;

    if (size == 0)
        size = DHASHTABLE_INIT_SIZE;
    size *= DHASHTABLE_SPREAD_TIMES;

    ht = malloc(sizeof(dhashtable));
    if (ht == NULL)
        return NULL;

    ht->table = malloc(size*sizeof(unsigned long long));
    if (ht->table == NULL) {
        free(ht);
        return NULL;
    }

    ht->size = size;
    
    memset(ht->table,0,ht->size*sizeof(unsigned long long));
    ht->used = 0;

    return ht;
}

void dhashtableRelease(dhashtable *ht)
{
    if (ht != NULL) {
        if (ht->table != NULL) {
            free(ht->table);
        }
        free(ht);
    }
}

void dhashtableReset(dhashtable *ht)
{
    if (ht->used > 0) {
        memset(ht->table,0,ht->size*sizeof(unsigned long long));
        ht->used = 0;
    }
}

int dhashtableExpandIfNeeded(dhashtable *ht, unsigned long size)
{
    if (size*DHASHTABLE_EXPAND_TIMES > ht->size) {
        size *= DHASHTABLE_SPREAD_TIMES;
        ht->table = realloc(ht->table, size*sizeof(unsigned long long));
        if (ht->table == NULL)
            return HT_ERR;
        ht->size = size;
    }

    return HT_OK;
}

/* Add an element to the target hash table */
int dhashtableAdd(dhashtable *ht, const void *key)
{
    unsigned long i;
    unsigned long long k = (unsigned long long)key;
    unsigned long long idx = (k | 1) * 2862933555777941757ULL % ht->size;

    for (i = 0; i < ht->size; ++ i) {
        if (ht->table[idx] == 0) {
            ht->table[idx] = k;
            ++ ht->used;
            return HT_OK;
        } else if (ht->table[idx] == k) {
            /* Already exist */
            return HT_OK;
        }
        
        idx = (idx + 1) % ht->size;
    }

    /* Full */
    return HT_ERR;
}

long long dhashtableFind(dhashtable *ht, const void *key)
{
    unsigned long i;
    unsigned long long k = (unsigned long long)key;
    unsigned long long idx = (k | 1) * 2862933555777941757ULL % ht->size;

    for (i = 0; i < ht->size; ++ i) {
        if (ht->table[idx] == 0) {
            /* Not exist */
            return -1;
        }
        
        if (ht->table[idx] == k) {
            /* Exist */
            return idx;
        }
        idx = (idx + 1) % ht->size;
    }

    /* Full and not exist */
    return -1;
}
