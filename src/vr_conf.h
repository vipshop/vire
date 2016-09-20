#ifndef _VR_CONF_H_
#define _VR_CONF_H_

/* Config server option name */
#define CONFIG_SOPN_DATABASES    "databases"
#define CONFIG_SOPN_IDPDATABASE  "internal-dbs-per-databases"
#define CONFIG_SOPN_MAXMEMORY    "maxmemory"
#define CONFIG_SOPN_MAXMEMORYP   "maxmemory-policy"
#define CONFIG_SOPN_MAXMEMORYS   "maxmemory-samples"
#define CONFIG_SOPN_MTCLIMIT     "max-time-complexity-limit"
#define CONFIG_SOPN_BIND         "bind"
#define CONFIG_SOPN_PORT         "port"
#define CONFIG_SOPN_THREADS      "threads"
#define CONFIG_SOPN_DIR          "dir"
#define CONFIG_SOPN_MAXCLIENTS   "maxclients"
#define CONFIG_SOPN_SLOWLOGLST   "slowlog-log-slower-than"
#define CONFIG_SOPN_SLOWLOGML    "slowlog-max-len"
#define CONFIG_SOPN_REQUIREPASS  "requirepass"
#define CONFIG_SOPN_ADMINPASS    "adminpass"
#define CONFIG_SOPN_COMMANDSNAP  "commands-need-adminpass"

#define CONFIG_RUN_ID_SIZE 40
#define CONFIG_DEFAULT_ACTIVE_REHASHING 1

#define CONFIG_DEFAULT_LOGICAL_DBNUM    6
#define CONFIG_DEFAULT_INTERNAL_DBNUM   6

#define CONFIG_DEFAULT_MAXMEMORY 0
#define CONFIG_DEFAULT_MAXMEMORY_SAMPLES 5
#define CONFIG_DEFAULT_MAX_CLIENTS 10000

#define CONFIG_DEFAULT_MAX_CLIENTS 10000

#define CONFIG_DEFAULT_THREADS_NUM (sysconf(_SC_NPROCESSORS_ONLN)>6?6:sysconf(_SC_NPROCESSORS_ONLN))

#define CONFIG_DEFAULT_HOST "0.0.0.0"

#define CONFIG_DEFAULT_SERVER_PORT 55555

#define CONFIG_DEFAULT_DATA_DIR "viredata"

#define CONFIG_DEFAULT_MAX_TIME_COMPLEXITY_LIMIT 0 /* Not limited */

#define CONFIG_DEFAULT_SLOWLOG_LOG_SLOWER_THAN 10000
#define CONFIG_DEFAULT_SLOWLOG_MAX_LEN 128

#define CONFIG_AUTHPASS_MAX_LEN 512

#define CONFIG_BINDADDR_MAX 16

#define CONF_UNSET_NUM      -1
#define CONF_UNSET_PTR      NULL
#define CONF_UNSET_GROUP    (group_type_t) -1
#define CONF_UNSET_HASH     (hash_type_t) -1
#define CONF_UNSET_DIST     (dist_type_t) -1

/* Config field data type for conf_option struct */
#define CONF_FIELD_TYPE_INT         0
#define CONF_FIELD_TYPE_LONGLONG    1
#define CONF_FIELD_TYPE_SDS         2
#define CONF_FIELD_TYPE_ARRAYSDS    3

/* Config field flags for conf_option struct */
#define CONF_FIELD_FLAGS_NO_MODIFY  (1<<0)

typedef struct conf_option {
    char    *name;      /* option name */
    int     type;       /* value type */
    int     flags;      /* option flags */
    int     (*set)(void *cf, struct conf_option *opt, void *data);
    int     (*get)(void *cf, struct conf_option *opt, void *data);
    int     offset;     /* offset of this option field in the struct  */
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

typedef struct conf_server {
    dict          *ctable;

    int           databases;
    int           internal_dbs_per_databases;

    /* Limits */
    long long     max_time_complexity_limit;
    long long     maxmemory;            /* Max number of memory bytes to use */
    int           maxmemory_policy;     /* Policy for key eviction */
    int           maxmemory_samples;    /* Pricision of random sampling */
    int           maxclients;           /* Max number of simultaneous clients */

    int           threads;

    struct darray  binds;                /* Type: sds */
    int           port;

    sds           dir;

    long long     slowlog_log_slower_than;  /* SLOWLOG time limit (to get logged) */
    int           slowlog_max_len;      /* SLOWLOG max number of items logged */

    sds           requirepass;          /* Pass for AUTH command, or NULL */
    sds           adminpass;            /* Pass for ADMIN command, or NULL */
    struct darray  commands_need_adminpass;
} conf_server;

typedef struct vr_conf {
    sds           fname;             /* file name , absolute path */

    dict          *organizations;    /* organizations */

    conf_server   cserver;

    unsigned long long version;      /* config version */
    pthread_rwlock_t rwl;            /* config read write lock */
    pthread_mutex_t flock;           /* config file lock */
}vr_conf;

#define CONF_VALUE_TYPE_UNKNOW   0
#define CONF_VALUE_TYPE_STRING   1
#define CONF_VALUE_TYPE_ARRAY    2

typedef struct conf_value{
    int     type;
    void    *value;
}conf_value;

/* Config option used multi times for every loop, 
 * so we cache them here in the cron function. */
typedef struct conf_cache {
    unsigned long long cache_version;

    int maxclients;
    sds requirepass;
    sds adminpass;
    long long maxmemory;
    long long max_time_complexity_limit;
    long long slowlog_log_slower_than;
}conf_cache;

extern vr_conf *conf;
extern conf_server *cserver;

conf_value *conf_value_create(int type);
void conf_value_destroy(conf_value *cv);

vr_conf *conf_create(char *filename);
void conf_destroy(vr_conf *cf);

unsigned long long conf_version_get(void);

int conf_server_get(const char *option_name, void *value);
int conf_server_set(const char *option_name, conf_value *value);

int conf_set_maxmemory(void *obj, conf_option *opt, void *data);
int conf_set_maxmemory_policy(void *obj, conf_option *opt, void *data);
int conf_set_int_non_zero(void *obj, conf_option *opt, void *data);

int conf_get_sds(void *obj, conf_option *opt, void *data);
int conf_get_int(void *obj, conf_option *opt, void *data);
int conf_get_longlong(void *obj, conf_option *opt, void *data);
int conf_get_array_sds(void *obj, conf_option *opt, void *data);

int conf_set_sds(void *obj, conf_option *opt, void *data);
int conf_set_password(void *obj, conf_option *opt, void *data);
int conf_set_int(void *obj, conf_option *opt, void *data);
int conf_set_longlong(void *obj, conf_option *opt, void *data);
int conf_set_yesorno(void *obj, conf_option *opt, void *data);
int conf_set_array_sds(void *obj, conf_option *opt, void *data);
int conf_set_commands_need_adminpass(void *obj, conf_option *opt, void *data);

int CONF_RLOCK(void);
int CONF_WLOCK(void);
int CONF_UNLOCK(void);

int CONFF_LOCK(void);
int CONFF_UNLOCK(void);

const char *get_evictpolicy_strings(int evictpolicy_type);

void configCommand(struct client *c);

int conf_cache_init(conf_cache *cc);
int conf_cache_deinit(conf_cache *cc);
int conf_cache_update(conf_cache *cc);

#endif
