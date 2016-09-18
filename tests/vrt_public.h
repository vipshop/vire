#ifndef _VRT_PUBLIC_H_
#define _VRT_PUBLIC_H_

#ifdef HAVE_CONFIG_H
# include <config.h>
#endif

#include <dspecialconfig.h>

#include <unistd.h>

#include <hiredis.h>

struct darray;
struct key_cache_pool;

#define VRT_TEST_OK     0
#define VRT_TEST_ERR    1

#define TEST_CMD_TYPE_STRING    (1<<0)
#define TEST_CMD_TYPE_LIST      (1<<1)
#define TEST_CMD_TYPE_SET       (1<<2)
#define TEST_CMD_TYPE_ZSET      (1<<3)
#define TEST_CMD_TYPE_HASH      (1<<4)
#define TEST_CMD_TYPE_SERVER    (1<<5)
#define TEST_CMD_TYPE_KEY       (1<<6)
#define TEST_CMD_TYPE_EXPIRE    (1<<7)

/* key types */
#define REDIS_STRING    0
#define REDIS_LIST      1
#define REDIS_SET       2
#define REDIS_ZSET      3
#define REDIS_HASH      4

/* State control API */
/* GCC version >= 4.7 */
#if defined(__ATOMIC_RELAXED)
#define update_state_add(_value, _n) __atomic_add_fetch(&_value, (_n), __ATOMIC_RELAXED)
#define update_state_sub(_value, _n) __atomic_sub_fetch(&_value, (_n), __ATOMIC_RELAXED)
#define update_state_set(_value, _n) __atomic_store_n(&_value, (_n), __ATOMIC_RELAXED)
#define update_state_get(_value, _v) do {         \
    __atomic_load(&_value, _v, __ATOMIC_RELAXED); \
} while(0)

#define TEST_STATE_LOCK_TYPE "__ATOMIC_RELAXED"
/* GCC version >= 4.1 */
#elif defined(HAVE_ATOMIC)
#define update_state_add(_value, _n) __sync_add_and_fetch(&_value, (_n))
#define update_state_sub(_value, _n) __sync_sub_and_fetch(&_value, (_n))
#define update_state_set(_value, _n) __sync_lock_test_and_set(&_value, (_n))
#define update_state_get(_value, _v) do {         \
    (*_v) = __sync_add_and_fetch(&_value, 0);     \
} while(0)

#define TEST_STATE_LOCK_TYPE "HAVE_ATOMIC"
#else
extern pthread_mutex_t state_locker;

#define update_state_add(_value, _n) do {   \
    pthread_mutex_lock(&state_locker);      \
    _value += (_n);                         \
    pthread_mutex_unlock(&state_locker);    \
} while(0)

#define update_state_sub(_value, _n) do {   \
    pthread_mutex_lock(&state_locker);      \
    _value -= (_n);                         \
    pthread_mutex_unlock(&state_locker);    \
} while(0)

#define update_state_set(_value, _n) do {   \
    pthread_mutex_lock(&state_locker);      \
    _value = (_n);                          \
    pthread_mutex_unlock(&state_locker);    \
} while(0)

#define update_state_get(_value, _v) do {   \
    pthread_mutex_lock(&state_locker);      \
    (*_v) = _value;                         \
    pthread_mutex_unlock(&state_locker);    \
} while(0)

#define TEST_STATE_LOCK_TYPE "pthread_mutex_lock"
#endif

typedef struct vire_instance {
    sds host;
    int port;
    
    sds dir;
    sds conf_file;
    sds pid_file;
    sds log_file;

    int running;
    int pid;
    redisContext *ctx;
} vire_instance;

void set_execute_file(char *file);

vire_instance *vire_instance_create(int port);
void vire_instance_destroy(vire_instance *vi);

int vire_server_run(vire_instance *vi);
void vire_server_stop(vire_instance *vi);

int create_work_dir(void);
int destroy_work_dir(void);

vire_instance *start_one_vire_instance(void);

void show_test_result(int result,char *test_content,char *errmsg);

typedef struct key_cache_array {
    long long cached_keys_count;
    long long ckeys_write_idx;
    long long max_pool_size;    /* Max keys count that can be cached in the ckeys array. */
    sds *ckeys;    /* Cached keys that may exist in the target redis/vire servers. */
    pthread_mutex_t pmutex;
} key_cache_array;

key_cache_array *key_cache_array_create(long long max_pool_size);
void key_cache_array_destroy(key_cache_array *kca);
int key_cache_array_input(key_cache_array *kca, char *key, size_t keylen);
sds key_cache_array_random(key_cache_array *kca);

long long get_longlong_from_info_reply(redisReply *reply, char *name);

redisReply *steal_hiredis_redisreply(redisReply *r);
int check_two_replys_if_same(redisReply *reply1, redisReply *reply2);
int sort_array_by_step(void **element, size_t elements, int step, int idx_cmp, int (*fcmp)(const void *,const void *));
int reply_string_binary_compare(const void *r1,const void *r2);

int parse_command_types(char *command_types_str);
struct darray *parse_command_list(char *command_list_str);

char *get_key_type_string(int keytype);

#endif
