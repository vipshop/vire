#include <vr_core.h>

#define CONF_TOKEN_ORGANIZATION_START   "["
#define CONF_TOKEN_ORGANIZATION_END     "]"
#define CONF_TOKEN_KEY_VALUE_BETWEEN    ":"
#define CONF_TOKEN_ARRAY_START          "-"

#define CONF_ORGANIZATION_NAME_COMMAN   "common"
#define CONF_ORGANIZATION_NAME_SERVER   "server"

#define CONF_VALUE_TRUE                 "true"
#define CONF_VALUE_FALSE                "false"

#define CONF_MAX_LINE                   1024

#define CONF_TAG_DEFAULT_TYPE           GROUP_TYPE_SINGLE
#define CONF_TAG_DEFAULT_HASH           HASH_FNV1_64
#define CONF_TAG_DEFAULT_HASH_TAG       NULL
#define CONF_TAG_DEFAULT_DISTRIBUTION   "ketama"
#define CONF_TAG_DEFAULT_REDIS_AUTH     NULL
#define CONF_TAG_DEFAULT_REDIS_DB       0
#define CONF_TAG_DEFAULT_TIMEOUT        300
#define CONF_TAG_DEFAULT_SERVERS        "127.0.0.1:6379"
#define CONF_TAG_DEFAULT_LISTEN         "127.0.0.1:6380"
#define CONF_TAG_DEFAULT_MAXMEMORY      1073741824   // 1Gb
#define CONF_TAG_DEFAULT_THREADS        sysconf(_SC_NPROCESSORS_ONLN)
#define CONF_TAG_DEFAULT_NOREPLY        "false"
#define CONF_TAG_DEFAULT_RDB_DISKLESS   "false"

#define CONF_VALUE_UNKNOW   0
#define CONF_VALUE_STRING   1
#define CONF_VALUE_ARRAY    2

#define DEFINE_ACTION(_hash, _name) (char*)(#_name),
static char* hash_strings[] = {
    HASH_CODEC( DEFINE_ACTION )
    NULL
};
#undef DEFINE_ACTION

#define DEFINE_ACTION(_dist, _name) (char*)(#_name),
static char* dist_strings[] = {
    DIST_CODEC( DEFINE_ACTION )
    NULL
};
#undef DEFINE_ACTION

#define DEFINE_ACTION(_policy, _name) (char*)(#_name),
static char* evictpolicy_strings[] = {
    EVICTPOLICY_CODEC( DEFINE_ACTION )
    NULL
};
#undef DEFINE_ACTION

static conf_option conf_server_options[] = {
    { (char *)("databases"),
      conf_set_int_non_zero, conf_get_int,
      offsetof(conf_server, databases) },
    { (char *)("internal-dbs-per-databases"),
      conf_set_int_non_zero, conf_get_int,
      offsetof(conf_server, internal_dbs_per_databases) },
    { (char *)("maxmemory"),
      conf_set_maxmemory, conf_get_longlong,
      offsetof(conf_server, maxmemory) },
    { (char *)("maxmemory-policy"),
      conf_set_maxmemory_policy, conf_get_int,
      offsetof(conf_server, maxmemory_policy) },
    { (char *)("maxmemory-samples"),
      conf_set_int_non_zero, conf_get_int,
      offsetof(conf_server, maxmemory_samples) },
    { (char *)("max-time-complexity-limit"),
      conf_set_longlong, conf_get_longlong,
      offsetof(conf_server, max_time_complexity_limit) },
    { (char *)("bind"),
      conf_set_array_string, conf_get_array_string,
      offsetof(conf_server, binds) },
    { (char *)("port"),
      conf_set_int, conf_get_int,
      offsetof(conf_server, port) },
    { (char *)("threads"),
      conf_set_int, conf_get_int,
      offsetof(conf_server, threads) },
    { (char *)("dir"),
      conf_set_string, conf_get_string,
      offsetof(conf_server, dir) },
    { (char *)("maxclients"),
      conf_set_int, conf_get_int,
      offsetof(conf_server, maxclients) },
    { NULL, NULL, 0 }
};

vr_conf *conf = NULL;
conf_server *cserver = NULL;

static void
conf_value_dump(conf_value *cv, int log_level)
{
    uint32_t i;
    conf_value **cv_sub;
    
    if(cv == NULL){
        return;
    }

    if(cv->type == CONF_VALUE_STRING){
        log_debug(log_level, "%.*s", sdslen(cv->value), cv->value);
    }else if(cv->type == CONF_VALUE_ARRAY){
        for(i = 0; i < array_n(cv->value); i++){
            cv_sub = array_get(cv->value, i);
            conf_value_dump(*cv_sub, log_level);
        }
    }else{
        NOT_REACHED();
    }
}

static void
conf_organization_dump(sds name, dict *org, int log_level)
{
    dictIterator *di;
    dictEntry *de;
    sds key;
    conf_value *cv;

    if(name == NULL || org == NULL){
        return;
    }

    log_debug(log_level, "[%.*s]", sdslen(name), name);
    
    di = dictGetIterator(org);

    while((de = dictNext(di)) != NULL){
        key = dictGetKey(de);
        cv = dictGetVal(de);

        if(cv->type == CONF_VALUE_STRING){
            log_debug(log_level, "%.*s: %.*s", 
                sdslen(key), key,
                sdslen(cv->value), cv->value);
        }else if(cv->type == CONF_VALUE_ARRAY){
            log_debug(log_level, "%.*s:",sdslen(key), key);
            conf_value_dump(cv, log_level);
        }else{
            NOT_REACHED();
        }
    }

    dictReleaseIterator(di);
}

static void
conf_organizations_dump(vr_conf *cf)
{
    dict *orgs, *org;
    dictIterator *di;
    dictEntry *de;
    sds name;
    int log_level = LOG_VERB;
    
    if(cf == NULL){
        return;
    }

    orgs = cf->organizations;
    if(orgs == NULL){
        log_debug(log_level, "organization is NULL");
        return;
    }
    
    di = dictGetIterator(orgs);

    while((de = dictNext(di)) != NULL){
        name = dictGetKey(de);
        org = dictGetVal(de);

        conf_organization_dump(name, org, log_level);
        log_debug(log_level, "");
    }

    dictReleaseIterator(di);
}

int
conf_set_maxmemory(void *obj, conf_option *opt, void *data)
{
    uint8_t *p;
    conf_value *cv = data;
    uint64_t value;
    long long *gt;
    int err;

    if(cv->type != CONF_VALUE_STRING){
        log_error("conf pool %s in the conf file error", 
            opt->name);
        return VR_ERROR;
    }

    CONF_WLOCK();
    
    p = obj;
    gt = (long long *)(p + opt->offset);

    value = memtoll(cv->value, &err);
    if(err != 0){
        CONF_ULOCK();
        log_error("value for the key %s in conf file is invalid", 
             opt->name);
        return VR_ERROR;
    }

    *gt = (long long)value;

    CONF_ULOCK();
    return VR_OK;
}

int
conf_set_maxmemory_policy(void *obj, conf_option *opt, void *data)
{
    uint8_t *p;
    conf_value *cv = data;
    int *gt;
    char **policy;

    if(cv->type != CONF_VALUE_STRING){
        log_error("conf server in the conf file is not a string");
        return VR_ERROR;
    }

    CONF_WLOCK();

    p = obj;
    gt = (int*)(p + opt->offset);

    for (policy = evictpolicy_strings; *policy; policy ++) {
        if (strcmp(cv->value, *policy) == 0) {
            *gt = policy - evictpolicy_strings;
            break;
        }
    }

    if (*gt == MAXMEMORY_VOLATILE_LRU || *gt == MAXMEMORY_ALLKEYS_LRU) {
        CONF_ULOCK();
        log_error("ERROR: Conf maxmemory policy now is not support %s and %s", 
            evictpolicy_strings[MAXMEMORY_VOLATILE_LRU], 
            evictpolicy_strings[MAXMEMORY_ALLKEYS_LRU]);
        return VR_ERROR;
    }

    if (*gt == CONF_UNSET_NUM) {
        CONF_ULOCK();
        log_error("ERROR: Conf maxmemory policy in the conf file can not be %s", 
            cv->value);
        return VR_ERROR;
    }

    CONF_ULOCK();
    return VR_OK;
}

int
conf_set_int_non_zero(void *obj, conf_option *opt, void *data)
{
    uint8_t *p;
    conf_value *cv = data;
    int *gt;

    if(cv->type != CONF_VALUE_STRING){
        log_error("conf pool %s in the conf file error", 
            opt->name);
        return VR_ERROR;
    }

    CONF_WLOCK();

    p = obj;
    gt = (int*)(p + opt->offset);

    if(!sdsIsNum(cv->value)){
        CONF_ULOCK();
        log_error("value of the key %s in conf file is not a number", 
            opt->name);
        return VR_ERROR;
    }

    *gt = vr_atoi(cv->value, sdslen(cv->value));

    if (*gt < 0) {
        CONF_ULOCK();
        log_error("value of the key %s in conf file is invalid", 
            opt->name);
        return VR_ERROR;
    } else if (*gt < 1) {
        CONF_ULOCK();
        log_error("value of the key %s in conf file must be 1 or greater", 
            opt->name);
        return VR_ERROR;
    }

    CONF_ULOCK();
    return VR_OK;
}

int
conf_get_string(void *obj, conf_option *opt, void *data)
{
    uint8_t *p;
    sds *str = data;
    sds *gt;

    if (data == NULL)
        return VR_ERROR;

    CONF_RLOCK();
    p = obj;
    gt = (sds*)(p + opt->offset);
    *str = sdsdup(*gt);
    CONF_ULOCK();
    return VR_OK;
}

int
conf_set_string(void *obj, conf_option *opt, void *data)
{
    uint8_t *p;
    conf_value *cv = data;
    sds *gt;

    if(cv->type != CONF_VALUE_STRING){
        log_error("conf pool %s in the conf file is not a string", 
            opt->name);
        return VR_ERROR;
    }

    CONF_WLOCK();
    p = obj;
    gt = (sds*)(p + opt->offset);

    *gt = sdsnewlen(cv->value, sdslen(cv->value));
    CONF_ULOCK();
    return VR_OK;
}

int
conf_get_int(void *obj, conf_option *opt, void *data)
{
    uint8_t *p;
    int *integer = data;
    int *gt;

    if (data == NULL) 
        return VR_ERROR;

    CONF_RLOCK();
    p = obj;
    gt = (int*)(p + opt->offset);
    *integer = *gt;
    CONF_ULOCK();
    return VR_OK;
}

int
conf_set_int(void *obj, conf_option *opt, void *data)
{
    uint8_t *p;
    conf_value *cv = data;
    int *gt;

    if(cv->type != CONF_VALUE_STRING){
        log_error("conf pool %s in the conf file error", 
            opt->name);
        return VR_ERROR;
    }

    CONF_WLOCK();
    
    p = obj;
    gt = (int*)(p + opt->offset);

    if(!sdsIsNum(cv->value)){
        CONF_ULOCK();
        log_error("value of the key %s in conf file is not a number", 
            opt->name);
        return VR_ERROR;
    }

    *gt = vr_atoi(cv->value, sdslen(cv->value));

    if (*gt < 0) {
        CONF_ULOCK();
        log_error("value of the key %s in conf file is invalid", 
            opt->name);
        return VR_ERROR;
    }

    CONF_ULOCK();
    return VR_OK;
}

int
conf_get_longlong(void *obj, conf_option *opt, void *data)
{
    uint8_t *p;
    long long *integer = data;
    long long *gt;

    if (data == NULL)
        return VR_ERROR;

    CONF_RLOCK();
    p = obj;
    gt = (long long*)(p + opt->offset);
    *integer = *gt;
    CONF_ULOCK();
    return VR_OK;
}

int
conf_set_longlong(void *obj, conf_option *opt, void *data)
{
    uint8_t *p;
    conf_value *cv = data;
    long long *gt;

    if(cv->type != CONF_VALUE_STRING){
        log_error("conf pool %s in the conf file error", 
            opt->name);
        return VR_ERROR;
    }

    CONF_WLOCK();
    
    p = obj;
    gt = (long long*)(p + opt->offset);

    if (!string2ll(cv->value, sdslen(cv->value), gt)) {
        CONF_ULOCK();
        log_error("value of the key %s in conf file is invalid", 
            opt->name);
        return VR_ERROR;
    }

    CONF_ULOCK();
    return VR_OK;
}

int
conf_set_bool(void *obj, conf_option *opt, void *data)
{
    uint8_t *p;
    conf_value *cv = data;
    int *gt;

    if(cv->type != CONF_VALUE_STRING){
        log_error("conf pool %s in the conf file error", 
            opt->name);
        return VR_ERROR;
    }

    CONF_WLOCK();
    
    p = obj;
    gt = (int*)(p + opt->offset);

    if(strcmp(cv->value, CONF_VALUE_TRUE) == 0){
        *gt = 1;
    }else if(strcmp(cv->value, CONF_VALUE_FALSE) == 0){
        *gt = 0;
    }else{
        CONF_ULOCK();
        log_error("key %s in conf file must be %s or %s",
            opt->name, CONF_VALUE_TRUE, CONF_VALUE_FALSE);
        return VR_ERROR;
    }

    CONF_ULOCK();
    return VR_OK;
}

int
conf_set_array_string(void *obj, conf_option *opt, void *data)
{
    uint8_t *p;
    uint32_t j;
    conf_value **cv_sub, *cv = data;
    struct array *gt;
    sds *str;

    if(cv->type != CONF_VALUE_STRING && 
        cv->type != CONF_VALUE_ARRAY){
        log_error("conf pool %s in the conf file is not a string or array", 
            opt->name);
        return VR_ERROR;
    } else if (cv->type == CONF_VALUE_ARRAY) {
        cv_sub = array_get(cv->value, j);
        if ((*cv_sub)->type != CONF_VALUE_STRING) {
            log_error("conf pool %s in the conf file is not a string array", 
                opt->name);
            return VR_ERROR;            
        }
    }

    CONF_WLOCK();
    p = obj;
    gt = (struct array*)(p + opt->offset);

    while (array_n(gt) > 0) {
        str = array_pop(gt);
        sdsfree(*str);
    }

    if (cv->type == CONF_VALUE_STRING) {
        str = array_push(gt);
        *str = sdsdup(cv->value);
    } else if (cv->type == CONF_VALUE_ARRAY) {
        for (j = 0; j < array_n(cv->value); j ++) {
            cv_sub = array_get(cv->value, j);
            str = array_push(gt);
            *str = sdsdup((*cv_sub)->value);
        }
    }
    
    CONF_ULOCK();
    return VR_OK;
}

int
conf_get_array_string(void *obj, conf_option *opt, void *data)
{
    uint8_t *p;
    uint32_t j;
    struct array *strs = data;
    struct array *gt;
    sds *str1, *str2;

    if (data == NULL) {
        return VR_ERROR;
    }

    CONF_RLOCK();
    p = obj;
    gt = (struct array*)(p + opt->offset);

    ASSERT(array_n(strs) == 0);

    for (j = 0; j < array_n(gt); j ++) {
        str1 = array_get(gt, j);
        str2 = array_push(strs);
        *str2 = sdsdup(*str1);
    }
    
    CONF_ULOCK();
    return VR_OK;
}

static void dictConfValueDestructor(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);

    conf_value_destroy(val);
}

static void dictDestructor(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);

    dictRelease(val);
}

static dictType OrganizationDictType = {
    dictSdsHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCompare,          /* key compare */
    dictSdsDestructor,          /* key destructor */
    dictDestructor              /* val destructor */
};

static dictType KeyValueDictType = {
    dictSdsHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCompare,          /* key compare */
    dictSdsDestructor,          /* key destructor */
    dictConfValueDestructor     /* val destructor */
};

static dictType ConfTableDictType = {
    dictStrHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictStrKeyCompare,          /* key compare */
    NULL,                       /* key destructor */
    NULL                        /* val destructor */
};

conf_value *conf_value_create(int type)
{
    conf_value *cv;

    cv = vr_alloc(sizeof(*cv));
    if(cv == NULL){
        return NULL;
    }

    cv->type = type;
    cv->value = NULL;

    if(cv->type == CONF_VALUE_ARRAY){
        cv->value = array_create(3, sizeof(conf_value*));
        if(cv->value == NULL){
            vr_free(cv);
            return NULL;
        }
    }

    return cv;
}

void conf_value_destroy(conf_value *cv)
{
    conf_value **cv_sub;
    
    if(cv == NULL){
        return;
    }
    
    if(cv->type == CONF_VALUE_UNKNOW){
        vr_free(cv);
        return;
    }else if(cv->type == CONF_VALUE_STRING){
        if(cv->value != NULL){
            sdsfree(cv->value);
        }
    }else if(cv->type == CONF_VALUE_ARRAY){
        if(cv->value != NULL){
            while(array_n(cv->value) > 0){
                cv_sub = array_pop(cv->value);
                conf_value_destroy(*cv_sub);
            }

            array_destroy(cv->value);
        }
    }else{
        NOT_REACHED();
    }

    vr_free(cv);
}

static int conf_server_init(conf_server *cs)
{
    if(cs == NULL){
        return VR_ERROR;
    }

    cs->ctable = dictCreate(&ConfTableDictType,NULL);

    cs->databases = CONF_UNSET_NUM;
    cs->internal_dbs_per_databases = CONF_UNSET_NUM;
    cs->max_time_complexity_limit = CONF_UNSET_NUM;
    cs->maxmemory = CONF_UNSET_NUM;
    cs->maxmemory_policy = CONF_UNSET_NUM;
    cs->maxmemory_samples = CONF_UNSET_NUM;
    cs->maxclients = CONF_UNSET_NUM;
    cs->threads = CONF_UNSET_NUM;
    array_init(&cs->binds,1,sizeof(sds));
    cs->port = CONF_UNSET_NUM;
    cs->dir = CONF_UNSET_PTR;

    return VR_OK;
}

static int conf_server_set_default(conf_server *cs)
{
    sds *str;
    conf_option *opt;

    if(cs == NULL){
        return VR_ERROR;
    }

    for (opt = conf_server_options; opt&&opt->name; opt++) {
        dictAdd(cs->ctable,opt->name,opt);
    }

    cs->databases = CONFIG_DEFAULT_LOGICAL_DBNUM;
    cs->internal_dbs_per_databases = CONFIG_DEFAULT_INTERNAL_DBNUM;
    cs->max_time_complexity_limit = CONFIG_DEFAULT_MAX_TIME_COMPLEXITY_LIMIT;
    cs->maxmemory = CONFIG_DEFAULT_MAXMEMORY;
    cs->maxmemory_policy = CONFIG_DEFAULT_MAXMEMORY_POLICY;
    cs->maxmemory_samples = CONFIG_DEFAULT_MAXMEMORY_SAMPLES;
    cs->maxclients = CONFIG_DEFAULT_MAX_CLIENTS;
    cs->threads = CONFIG_DEFAULT_THREADS_NUM;

    while (array_n(&cs->binds) > 0) {
        str = array_pop(&cs->binds);
        sdsfree(*str);
    }
    str = array_push(&cs->binds);
    *str = sdsnew(CONFIG_DEFAULT_HOST);
    
    cs->port = CONFIG_DEFAULT_SERVER_PORT;

    if (cs->dir != CONF_UNSET_PTR) {
        sdsfree(cs->dir);
        cs->dir = sdsnew(CONFIG_DEFAULT_DATA_DIR);    
    }

    return VR_OK;
}

static void conf_server_deinit(conf_server *cs)
{
    sds *str;
    
    if(cs == NULL){
        return;
    }

    cs->databases = CONF_UNSET_NUM;
    cs->internal_dbs_per_databases = CONF_UNSET_NUM;
    cs->maxmemory = CONF_UNSET_NUM;
    cs->maxmemory_policy = CONF_UNSET_NUM;
    cs->maxmemory_samples = CONF_UNSET_NUM;
    cs->max_time_complexity_limit = CONF_UNSET_NUM;
    cs->maxclients = CONF_UNSET_NUM;
    cs->threads = CONF_UNSET_NUM;

    while (array_n(&cs->binds) > 0) {
        str = array_pop(&cs->binds);
        sdsfree(*str);
    }
    array_deinit(&cs->binds);

    cs->port = CONF_UNSET_NUM;
    
    if (cs->dir != CONF_UNSET_PTR) {
        sdsfree(cs->dir);
        cs->dir = CONF_UNSET_PTR;    
    }
}

int
conf_server_get(const char *option, void *value)
{
    conf_option *opt;

    opt = dictFetchValue(cserver->ctable, option);
    if (opt == NULL)
        return VR_ERROR;

    return opt->get(cserver, opt, value);
}

static int conf_init(vr_conf *cf)
{
    int ret;

    if(cf == NULL){
        return VR_ERROR;
    }

    cf->fname = NULL;
    cf->organizations = NULL;
    cf->version = 0;
    pthread_rwlock_init(&cf->rwl, NULL);
    pthread_mutex_init(&cf->flock, NULL);

    cf->organizations = dictCreate(&OrganizationDictType, NULL);
    if (cf->organizations == NULL) {
        return VR_ERROR;
    }

    conf_server_init(&cf->cserver);

    conf = cf;
    
    return VR_OK;
}

static int conf_set_default(vr_conf *cf)
{
    CONF_WLOCK();
    conf_server_set_default(&cf->cserver);
    CONF_ULOCK();
    return VR_OK;
}

static void conf_deinit(vr_conf *cf)
{
    if(cf == NULL){
        return;
    }

    if (cf->fname != NULL) {
        sdsfree(cf->fname);
        cf->fname = NULL;
    }

    if(cf->organizations != NULL){
        dictRelease(cf->organizations);
        cf->organizations = NULL;
    }

    conf_server_deinit(&cf->cserver);

    cf->version = 0;
    pthread_rwlock_destroy(&cf->rwl);
    pthread_mutex_destroy(&cf->flock);
}

static void
conf_server_dump(conf_server *cs, int log_level)
{
    if(cs == NULL){
        return;
    }

    log_debug(log_level, "  databases : %d", cs->databases);
    log_debug(log_level, "  internal_dbs_per_databases : %d", cs->internal_dbs_per_databases);
    log_debug(log_level, "  maxmemory : %lld", cs->maxmemory);
    log_debug(log_level, "  maxmemory_policy : %d", cs->maxmemory_policy);    
    log_debug(log_level, "  maxmemory_samples : %d", cs->maxmemory_samples);
    log_debug(log_level, "  max_time_complexity_limit : %lld", cs->max_time_complexity_limit);
}

static void
conf_dump(vr_conf *cf)
{
    int log_level = LOG_VERB;
    conf_server *cs;
    
    if(cf == NULL){
        return;
    }

    cs = &cf->cserver;
    log_debug(log_level, "server in conf file");
    conf_server_dump(cs, log_level);
    log_debug(log_level, "");
}

/* return -1: error
  * return 0: conf value is append
  * return 1: conf value is insert*/
static int
conf_key_value_insert(dict *org, sds key, conf_value *cv)
{
    if (key == NULL) {
        log_error("value in conf file has no key");
        return -1;
    }

    if (cv == NULL) {
        log_error("key %s in conf file has no value", key);
        return -1;
    }

    if (org == NULL) {
        log_error("key %s in conf file has no organization", 
            key);
        return -1;
    }
    
    if (dictAdd(org,key,cv) != DICT_OK) {
        dictEntry *de;
        conf_value *cv_old, *cv_new, **cv_sub;
        de = dictFind(org,key);
        cv_old = dictGetVal(de);
        if (cv_old->type != CONF_VALUE_ARRAY) {
            cv_new = conf_value_create(CONF_VALUE_ARRAY);
            cv_sub = array_push(cv_new->value);
            *cv_sub = cv_old;
            cv_sub = array_push(cv_new->value);
            *cv_sub = cv;
            dictSetVal(org,de,cv_new);
        } else {
            cv_sub = array_push(cv_old->value);
            *cv_sub = cv;
        }
        return 0;
    }

    return 1;
}

static int
conf_pre_load_from_string(vr_conf *cf, char *config)
{
    int ret;
    int linenum = 0, totlines, i, j;
    int slaveof_linenum = 0;
    sds *lines = NULL;
    dict *org = NULL;
    sds org_name = NULL;
    dictEntry *de;
    sds key = NULL;
    conf_value *cv = NULL;

    lines = sdssplitlen(config,strlen(config),"\n",1,&totlines);

    for (i = 0; i < totlines; i++) {
        sds *argv;
        int argc;

        linenum = i+1;
        lines[i] = sdstrim(lines[i]," \t\r\n");

        /* Skip comments and blank lines */
        if (lines[i][0] == '#' || lines[i][0] == '\0') continue;

        if (lines[i][0] == '[') {
            if (sdslen(lines[i]) <= 2 || lines[i][sdslen(lines[i])-1] == ']') {
                log_error("Organization name %s in conf file %s error",
                    lines[i], cf->fname);
                goto loaderr;
            }
            org_name = sdsnewlen(lines[i]+1,sdslen(lines[i])-2);
            de = dictFind(cf->organizations,org_name);
            if (de == NULL) {
                org = dictCreate(&KeyValueDictType, NULL);
                dictAdd(cf->organizations,org_name,org);
            } else {
                org = dictGetVal(de);
                sdsfree(org_name);
            }
            
            continue;
        }

        /* Split into arguments */
        argv = sdssplitargs(lines[i],&argc);
        if (argv == NULL) {
            log_error("Unbalanced quotes in configuration line");
            goto loaderr;
        }

        /* Skip this line if the resulting command vector is empty. */
        if (argc == 0) {
            sdsfreesplitres(argv,argc);
            continue;
        }
        sdstolower(argv[0]);

        if (org == NULL) {
            org_name = sdsnew("server");
            org = dictCreate(&KeyValueDictType, NULL);
            dictAdd(cf->organizations,org_name,org);
        }

        key = argv[0];
        argv[0] = NULL;
        for (j = 1; j < argc; j ++) {
            cv = conf_value_create(CONF_VALUE_STRING);
            cv->value = argv[j];
            argv[j] = NULL;
            ret = conf_key_value_insert(org, key, cv);
            if(ret == -1){
                sdsfreesplitres(argv,argc);
                sdsfree(key);
                conf_value_destroy(cv);
                log_error("key value insert into organization failed");
                goto loaderr;
            } else if (ret == 0) {
                sdsfree(key);
            }
        }

        sdsfreesplitres(argv,argc);
    }

    if (lines) {
        sdsfreesplitres(lines,linenum);
    }
    return VR_OK;
    
loaderr:
    if (lines) {
        sdsfreesplitres(lines,linenum);
    }
    return VR_ERROR;
}

static int
conf_pre_validate(vr_conf *cf)
{
    int ret;
    sds config = sdsempty();
    char buf[CONF_MAX_LINE+1];

    /* Load the file content */
    if (cf->fname) {
        FILE *fp;

        if (cf->fname[0] == '-' && cf->fname[1] == '\0') {
            fp = stdin;
        } else {
            if ((fp = fopen(cf->fname,"r")) == NULL) {
                log_error("Fatal error, can't open config file '%s'", cf->fname);
                sdsfree(config);
                return VR_ERROR;
            }
        }
        while(fgets(buf,CONF_MAX_LINE+1,fp) != NULL)
            config = sdscat(config,buf);
        if (fp != stdin) fclose(fp);
    }

    ret = conf_pre_load_from_string(cf,config);
    if (ret != VR_OK) {
        sdsfree(config);
        return VR_ERROR;
    }
    
    sdsfree(config);
    return VR_OK;
}

static int
conf_parse_conf_server(conf_server *cs, dict *org)
{
    int ret;
    conf_option *opt;
    dictEntry *de;
    sds key;
    
    if(cs == NULL || org == NULL){
        return VR_ERROR;
    }
    
    key = sdsempty();
    for (opt = conf_server_options; opt&&opt->name; opt++) {
        key = sdscpy(key,opt->name);
        de = dictFind(org,key);
        if (de != NULL) {
            ret = opt->set(cs, opt, dictGetVal(de));
            if(ret != VR_OK){
                log_error("parse key %s in conf file error", key);
                sdsfree(key);
                return VR_ERROR;
            }
        }
    }

    sdsfree(key);
    return VR_OK;
}

static int
conf_parse(vr_conf *cf)
{
    int ret;
    dict *orgs, *org;
    dictEntry *de;
    sds key;
    
    if (cf == NULL) {
        return VR_ERROR;
    }

    orgs = cf->organizations;
    if (orgs == NULL) {
        return VR_ERROR;
    }

    /* server */
    key = sdsnew(CONF_ORGANIZATION_NAME_SERVER);
    de = dictFind(orgs, key);
    if (de == NULL) {
        log_error("can not find %s organization in conf file %s", 
            CONF_ORGANIZATION_NAME_SERVER, cf->fname);
        sdsfree(key);
        return VR_ERROR;
    }

    org = dictGetVal(de);
    if (org == NULL) {
        log_error("dict %s entry value is NULL", dictGetKey(de));
        sdsfree(key);
        return VR_ERROR;
    }

    ret = conf_parse_conf_server(&cf->cserver, org);
    if( ret != VR_OK) {
        log_error("common conf parse error");
        sdsfree(key);
        return VR_ERROR;
    }

    sdsfree(key);
    
    return VR_OK;
}

static int
conf_post_validate(vr_conf *cf)
{
    if(cf == NULL){
        return VR_ERROR;
    }

    if(cf->organizations != NULL){
        dictRelease(cf->organizations);
        cf->organizations = NULL;
    }
    
    return VR_OK;
}

static vr_conf *
conf_open(char *filename)
{
    int ret;
    vr_conf *cf = NULL;
    sds path = NULL;

    if (filename == NULL) {
        log_error("configuration file name is NULL.");
        return NULL;
    }

    path = getAbsolutePath(filename);
    if (path == NULL) {
        log_error("configuration file name '%s' is error.", filename);
        goto error;
    }

    cf = vr_alloc(sizeof(*cf));
    if (cf == NULL) {
        goto error;
    }

    ret = conf_init(cf);
    if(ret != VR_OK){
        goto error;
    }

    ret = conf_set_default(cf);
    if (ret != VR_OK) {
        goto error;
    }

    cf->fname = path;

    return cf;

error:

    if (cf != NULL) {
        conf_destroy(cf);
    }

    if (path != NULL) {
        sdsfree(path);
    }
    
    return NULL;
}

vr_conf *
conf_create(char *filename)
{
    int ret;
    vr_conf *cf;

    cf = conf_open(filename);
    if (cf == NULL) {
        return NULL;
    }

    /* validate configuration file before parsing */
    ret = conf_pre_validate(cf);
    if (ret != VR_OK) {
        goto error;
    }

    conf_organizations_dump(cf);

    /* parse the configuration file */
    ret = conf_parse(cf);
    if (ret != VR_OK) {
        goto error;
    }

    /* validate parsed configuration */
    ret = conf_post_validate(cf);
    if (ret != VR_OK) {
        goto error;
    }

    conf_dump(cf);

    cserver = &cf->cserver;

    return cf;

error:
    conf_destroy(cf);
    return NULL;
}

void 
conf_destroy(vr_conf *cf)
{
    if (cf == NULL) {
        return;
    }
    
    conf_deinit(cf);
    
    vr_free(cf);
}

int
CONF_RLOCK(void)
{
    return pthread_rwlock_rdlock(&conf->rwl);
}

int
CONF_WLOCK(void)
{
    return pthread_rwlock_wrlock(&conf->rwl);
}

int
CONF_ULOCK(void)
{
    return pthread_rwlock_unlock(&conf->rwl);
}

const char *
get_evictpolicy_strings(int evictpolicy_type)
{
    return evictpolicy_strings[evictpolicy_type];
}
