#ifndef _VR_CONF_H_
#define _VR_CONF_H_

#define CONFIG_RUN_ID_SIZE 40
#define CONFIG_DEFAULT_ACTIVE_REHASHING 1

#define CONF_UNSET_NUM      -1
#define CONF_UNSET_PTR      NULL
#define CONF_UNSET_GROUP    (group_type_t) -1
#define CONF_UNSET_HASH     (hash_type_t) -1
#define CONF_UNSET_DIST     (dist_type_t) -1

typedef struct conf_option {
    char* name;
    int   (*set)(void *cf, struct conf_option *opt, void *data);
    int   offset;
}conf_option;

#define EVICTPOLICY_CODEC(ACTION)                           \
    ACTION( MAXMEMORY_VOLATILE_LRU,     volatile-lru)       \
    ACTION( MAXMEMORY_VOLATILE_RANDOM,  volatile-random)    \
    ACTION( MAXMEMORY_VOLATILE_TTL,     volatile-ttl)       \
    ACTION( MAXMEMORY_ALLKEYS_LRU,      allkeys-lru)        \
    ACTION( MAXMEMORY_ALLKEYS_RANDOM,   allkeys-random)     \
    ACTION( MAXMEMORY_NO_EVICTION,      noeviction)         \

#define DEFINE_ACTION(_policy, _name) _policy,
typedef enum evictpolicy_type {
    EVICTPOLICY_CODEC( DEFINE_ACTION )
    EVICTPOLICY_SENTINEL
} evictpolicy_type_t;
#undef DEFINE_ACTION

#define DEFINE_ACTION(_policy, _name) (char*)(#_name),
static char* evictpolicy_strings[] = {
    EVICTPOLICY_CODEC( DEFINE_ACTION )
    NULL
};
#undef DEFINE_ACTION

typedef struct conf_server {
    int           databases;
    int           internal_dbs_per_databases;
    long long     maxmemory;
    int           maxmemory_policy;
    int           maxmemory_samples;
} conf_server;

typedef struct vr_conf {
    sds           fname;             /* file name , absolute path */
    FILE          *fh;               /* file handle */

    dict          *organizations;    /* organizations */

    sds           listen;
    long long     maxmemory;
    int           threads;
    sds           dir;

    int           maxclients;

    conf_server   cserver;
}vr_conf;

typedef struct conf_value{
    int     type;
    void    *value;
}conf_value;

conf_value *conf_value_create(int type);
void conf_value_destroy(conf_value *cv);

vr_conf *conf_create(char *filename);
void conf_destroy(vr_conf *cf);

int conf_set_maxmemory(void *obj, conf_option *opt, void *data);
int conf_set_maxmemory_policy(void *obj, conf_option *opt, void *data);
int conf_set_number_non_zero(void *obj, conf_option *opt, void *data);

int conf_set_string(void *obj, conf_option *opt, void *data);
int conf_set_num(void *obj, conf_option *opt, void *data);
int conf_set_bool(void *obj, conf_option *opt, void *data);

#endif
