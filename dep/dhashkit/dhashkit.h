#ifndef _DHASHKIT_H_
#define _DHASHKIT_H_

#include <stdint.h>
#include <stdio.h>

#include <sys/types.h>

struct continuum {
    uint32_t index;  /* server index */
    uint32_t value;  /* hash value */
};

#define HASH_CODEC(ACTION)                      \
    ACTION( HASH_ONE_AT_A_TIME, one_at_a_time ) \
    ACTION( HASH_MD5,           md5           ) \
    ACTION( HASH_CRC16,         crc16         ) \
    ACTION( HASH_CRC32,         crc32         ) \
    ACTION( HASH_CRC32A,        crc32a        ) \
    ACTION( HASH_FNV1_64,       fnv1_64       ) \
    ACTION( HASH_FNV1A_64,      fnv1a_64      ) \
    ACTION( HASH_FNV1_32,       fnv1_32       ) \
    ACTION( HASH_FNV1A_32,      fnv1a_32      ) \
    ACTION( HASH_HSIEH,         hsieh         ) \
    ACTION( HASH_MURMUR,        murmur        ) \
    ACTION( HASH_JENKINS,       jenkins       ) \

#define DIST_CODEC(ACTION)                      \
    ACTION( DIST_KETAMA,        ketama        ) \
    ACTION( DIST_MODULA,        modula        ) \
    ACTION( DIST_RANDOM,        random        ) \

#define DEFINE_ACTION(_hash, _name) _hash,
typedef enum hash_type {
    HASH_CODEC( DEFINE_ACTION )
    HASH_SENTINEL
} hash_type_t;
#undef DEFINE_ACTION

#define DEFINE_ACTION(_dist, _name) _dist,
typedef enum dist_type {
    DIST_CODEC( DEFINE_ACTION )
    DIST_SENTINEL
} dist_type_t;
#undef DEFINE_ACTION

uint32_t hash_one_at_a_time(const char *key, size_t key_length);
void md5_signature(const unsigned char *key, unsigned long length, unsigned char *result);
uint32_t hash_md5(const char *key, size_t key_length);
uint32_t hash_crc16(const char *key, size_t key_length);
uint32_t hash_crc32(const char *key, size_t key_length);
uint32_t hash_crc32a(const char *key, size_t key_length);
uint32_t hash_fnv1_64(const char *key, size_t key_length);
uint32_t hash_fnv1a_64(const char *key, size_t key_length);
uint32_t hash_fnv1_32(const char *key, size_t key_length);
uint32_t hash_fnv1a_32(const char *key, size_t key_length);
uint32_t hash_hsieh(const char *key, size_t key_length);
uint32_t hash_jenkins(const char *key, size_t length);
uint32_t hash_murmur(const char *key, size_t length);

uint32_t ketama_dispatch(struct continuum *continuum, uint32_t ncontinuum, uint32_t hash);
uint32_t modula_dispatch(struct continuum *continuum, uint32_t ncontinuum, uint32_t hash);
uint32_t random_dispatch(struct continuum *continuum, uint32_t ncontinuum, uint32_t hash);


/*SHA-1 in CBy Steve Reid <steve@edmweb.com>100% Public Domain*/
typedef struct {
    uint32_t state[5];    
    uint32_t count[2];    
    unsigned char buffer[64];
} SHA1_CTX;

void SHA1Transform(uint32_t state[5], const unsigned char buffer[64]);
void SHA1Init(SHA1_CTX* context);
void SHA1Update(SHA1_CTX* context, const unsigned char* data, uint32_t len);
void SHA1Final(unsigned char digest[20], SHA1_CTX* context);

#endif
