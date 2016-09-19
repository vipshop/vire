#include <stdlib.h>

#include <dhashkit.h>
#include <dhashtable.h>

dhashtable *dhashtableCreate(unsigned long size)
{
    dhashtable *ht;

    if (size == 0)
        size = 10;

    ht = malloc(sizeof(dhashtable));
    if (ht == NULL)
        return NULL;

    ht->table = calloc(size,sizeof(dhashtableEntry));
    if (ht->table == NULL) {
        free(ht);
        return NULL;
    }

    ht->size = size;
    ht->used = 0;

    return ht;
}

void dhashtableRelease(dhashtable *ht)
{
    if (ht == NULL)
        return;

    if (ht->table != NULL) {
        unsigned int j;
        dhashtableEntry *entry, *next;
        for (j = 0; j < ht->size; j ++) {
            entry = ht->table + j;
            next = entry->next;
            while (next != NULL) {
                entry = next;
                next = next->next;
                free(entry);
            }
        }
        free(ht->table);
    }

    free(ht);
}

/* Add an element to the target hash table */
int dhashtableAdd(dhashtable *ht, char *key, size_t len)
{
    int idx;
    dhashtableEntry *entry;

    idx = hash_crc16(key,len)%ht->size;

    entry = ht->table + idx;
    if (entry->key == NULL) {
        entry->key = key;
        entry->len = len;
    } else {
        dhashtableEntry *prev;
        while (entry->next != NULL) {
            entry = entry->next;
        }
        prev = entry;
        entry = calloc(1,sizeof(dhashtableEntry));
        if (entry == NULL)
            return HT_ERR;
        entry->key = key;
        entry->len = len;
        prev->next = entry;
    }

    ht->used++;

    return HT_OK;
}

dhashtableEntry *dhashtableFind(dhashtable *ht, const char *key, size_t len)
{
    int idx;
    dhashtableEntry *entry;

    idx = hash_crc16(key,len)%ht->size;

    entry = ht->table + idx;

    if (entry->key == NULL)
        return NULL;

    if (entry->len != len || memcmp(key, entry->key, len)) {
        entry = entry->next;
    } else {
        return entry;
    }

    while (entry != NULL) {
        if (entry->len != len || memcmp(key, entry->key, len)) {
            entry = entry->next;
        } else {
            return entry;
        }
    }

    return NULL;
}
