#include <stdio.h>
#include <stdlib.h>
#include <math.h>

#include <dhashkit.h>

#define KETAMA_CONTINUUM_ADDITION   10  /* # extra slots to build into continuum */
#define KETAMA_POINTS_PER_SERVER    160 /* 40 points per hash */
#define KETAMA_MAX_HOSTLEN          86

static uint32_t
ketama_hash(const char *key, size_t key_length, uint32_t alignment)
{
    unsigned char results[16];

    md5_signature((const unsigned char*)key, (unsigned long)key_length, results);

    return ((uint32_t) (results[3 + alignment * 4] & 0xFF) << 24)
        | ((uint32_t) (results[2 + alignment * 4] & 0xFF) << 16)
        | ((uint32_t) (results[1 + alignment * 4] & 0xFF) << 8)
        | (results[0 + alignment * 4] & 0xFF);
}

static int
ketama_item_cmp(const void *t1, const void *t2)
{
    const struct continuum *ct1 = t1, *ct2 = t2;

    if (ct1->value == ct2->value) {
        return 0;
    } else if (ct1->value > ct2->value) {
        return 1;
    } else {
        return -1;
    }
}

uint32_t
ketama_dispatch(struct continuum *continuum, uint32_t ncontinuum, uint32_t hash)
{
    struct continuum *begin, *end, *left, *right, *middle;

    ASSERT(continuum != NULL);
    ASSERT(ncontinuum != 0);

    begin = left = continuum;
    end = right = continuum + ncontinuum;

    while (left < right) {
        middle = left + (right - left) / 2;
        if (middle->value < hash) {
          left = middle + 1;
        } else {
          right = middle;
        }
    }

    if (right == end) {
        right = begin;
    }

    return right->index;
}
