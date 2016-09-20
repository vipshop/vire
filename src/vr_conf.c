#include <fcntl.h>

#include <vr_core.h>

typedef const char *(*configEnumGetStrFun)(int type);

#define CONF_TOKEN_ORGANIZATION_START   "["
#define CONF_TOKEN_ORGANIZATION_END     "]"
#define CONF_TOKEN_KEY_VALUE_BETWEEN    ":"
#define CONF_TOKEN_ARRAY_START          "-"

#define CONF_ORGANIZATION_NAME_COMMAN   "common"
#define CONF_ORGANIZATION_NAME_SERVER   "server"

#define CONF_VALUE_YES                  "yes"
#define CONF_VALUE_NO                   "no"

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
    { (char *)CONFIG_SOPN_DATABASES,
      CONF_FIELD_TYPE_INT, 1,
      conf_set_int_non_zero, conf_get_int,
      offsetof(conf_server, databases) },
    { (char *)CONFIG_SOPN_IDPDATABASE,
      CONF_FIELD_TYPE_INT, 1,
      conf_set_int_non_zero, conf_get_int,
      offsetof(conf_server, internal_dbs_per_databases) },
    { (char *)CONFIG_SOPN_MAXMEMORY,
      CONF_FIELD_TYPE_LONGLONG, 0,
      conf_set_maxmemory, conf_get_longlong,
      offsetof(conf_server, maxmemory) },
    { (char *)CONFIG_SOPN_MAXMEMORYP,
      CONF_FIELD_TYPE_INT, 0,
      conf_set_maxmemory_policy, conf_get_int,
      offsetof(conf_server, maxmemory_policy) },
    { (char *)CONFIG_SOPN_MAXMEMORYS,
      CONF_FIELD_TYPE_INT, 0,
      conf_set_int_non_zero, conf_get_int,
      offsetof(conf_server, maxmemory_samples) },
    { (char *)CONFIG_SOPN_MTCLIMIT,
      CONF_FIELD_TYPE_LONGLONG, 0,
      conf_set_longlong, conf_get_longlong,
      offsetof(conf_server, max_time_complexity_limit) },
    { (char *)CONFIG_SOPN_BIND,
      CONF_FIELD_TYPE_ARRAYSDS, 1,
      conf_set_array_sds, conf_get_array_sds,
      offsetof(conf_server, binds) },
    { (char *)CONFIG_SOPN_PORT,
      CONF_FIELD_TYPE_INT, 1,
      conf_set_int, conf_get_int,
      offsetof(conf_server, port) },
    { (char *)CONFIG_SOPN_THREADS,
      CONF_FIELD_TYPE_INT, 1,
      conf_set_int, conf_get_int,
      offsetof(conf_server, threads) },
    { (char *)CONFIG_SOPN_MAXCLIENTS,
      CONF_FIELD_TYPE_INT, 0,
      conf_set_int_non_zero, conf_get_int,
      offsetof(conf_server, maxclients) },
    { (char *)CONFIG_SOPN_SLOWLOGLST,
      CONF_FIELD_TYPE_LONGLONG, 0,
      conf_set_longlong, conf_get_longlong,
      offsetof(conf_server, slowlog_log_slower_than) },
    { (char *)CONFIG_SOPN_SLOWLOGML,
      CONF_FIELD_TYPE_INT, 0,
      conf_set_int, conf_get_int,
      offsetof(conf_server, slowlog_max_len) },
    { (char *)CONFIG_SOPN_REQUIREPASS,
      CONF_FIELD_TYPE_SDS, 0,
      conf_set_password, conf_get_sds,
      offsetof(conf_server, requirepass) },
    { (char *)CONFIG_SOPN_ADMINPASS,
      CONF_FIELD_TYPE_SDS, 0,
      conf_set_password, conf_get_sds,
      offsetof(conf_server, adminpass) },
    { (char *)CONFIG_SOPN_COMMANDSNAP,
      CONF_FIELD_TYPE_ARRAYSDS, 1,
      conf_set_commands_need_adminpass, conf_get_array_sds,
      offsetof(conf_server, commands_need_adminpass) },
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

    if(cv->type == CONF_VALUE_TYPE_STRING){
        log_debug(log_level, "%.*s", sdslen(cv->value), cv->value);
    }else if(cv->type == CONF_VALUE_TYPE_ARRAY){
        for(i = 0; i < darray_n(cv->value); i++){
            cv_sub = darray_get(cv->value, i);
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

        if(cv->type == CONF_VALUE_TYPE_STRING){
            log_debug(log_level, "%.*s: %.*s", 
                sdslen(key), key,
                sdslen(cv->value), cv->value);
        }else if(cv->type == CONF_VALUE_TYPE_ARRAY){
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
    long long value;
    long long *gt;
    int err;

    if(cv->type != CONF_VALUE_TYPE_STRING){
        log_error("conf pool %s in the conf file error", 
            opt->name);
        return VR_ERROR;
    }

    CONF_WLOCK();
    
    p = obj;
    gt = (long long *)(p + opt->offset);

    value = memtoll(cv->value, &err);
    if(err != 0 || value < 0){
        CONF_UNLOCK();
        log_error("value for the key %s in conf file is invalid", 
             opt->name);
        return VR_ERROR;
    }

    *gt = value;
    conf->version ++;
    CONF_UNLOCK();
    return VR_OK;
}

int
conf_set_maxmemory_policy(void *obj, conf_option *opt, void *data)
{
    uint8_t *p;
    conf_value *cv = data;
    int *gt;
    char **policy;

    if(cv->type != CONF_VALUE_TYPE_STRING){
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

    if (*policy == NULL) {
        CONF_UNLOCK();
        log_error("ERROR: Conf maxmemory policy '%s' is invalid", 
            cv->value);
        return VR_ERROR;
    }

    if (*gt == MAXMEMORY_VOLATILE_LRU || *gt == MAXMEMORY_ALLKEYS_LRU) {
        CONF_UNLOCK();
        log_error("ERROR: Conf maxmemory policy now is not support %s and %s", 
            evictpolicy_strings[MAXMEMORY_VOLATILE_LRU], 
            evictpolicy_strings[MAXMEMORY_ALLKEYS_LRU]);
        return VR_ERROR;
    }

    CONF_UNLOCK();
    return VR_OK;
}

int
conf_set_int_non_zero(void *obj, conf_option *opt, void *data)
{
    uint8_t *p;
    conf_value *cv = data;
    int *gt;

    if(cv->type != CONF_VALUE_TYPE_STRING){
        log_error("conf pool %s in the conf file error", 
            opt->name);
        return VR_ERROR;
    }

    CONF_WLOCK();

    p = obj;
    gt = (int*)(p + opt->offset);

    if(!sdsIsNum(cv->value)){
        CONF_UNLOCK();
        log_error("value of the key %s in conf file is not a number", 
            opt->name);
        return VR_ERROR;
    }

    *gt = vr_atoi(cv->value, sdslen(cv->value));

    if (*gt < 0) {
        CONF_UNLOCK();
        log_error("value of the key %s in conf file is invalid", 
            opt->name);
        return VR_ERROR;
    } else if (*gt < 1) {
        CONF_UNLOCK();
        log_error("value of the key %s in conf file must be 1 or greater", 
            opt->name);
        return VR_ERROR;
    }
    conf->version ++;
    CONF_UNLOCK();
    return VR_OK;
}

/* The return data need to free by users. */
int
conf_get_sds(void *obj, conf_option *opt, void *data)
{
    uint8_t *p;
    sds *str = data;
    sds *gt;

    if (data == NULL)
        return VR_ERROR;

    CONF_RLOCK();
    p = obj;
    gt = (sds*)(p + opt->offset);
    if (*gt == NULL) *str = NULL;
    else *str = sdsdup(*gt);
    CONF_UNLOCK();
    return VR_OK;
}

int
conf_set_sds(void *obj, conf_option *opt, void *data)
{
    uint8_t *p;
    conf_value *cv = data;
    sds *gt;

    if(cv->type != CONF_VALUE_TYPE_STRING){
        log_error("conf pool %s in the conf file is not a string", 
            opt->name);
        return VR_ERROR;
    }

    CONF_WLOCK();
    p = obj;
    gt = (sds*)(p + opt->offset);

    *gt = sdsnewlen(cv->value, sdslen(cv->value));
    conf->version ++;
    CONF_UNLOCK();
    return VR_OK;
}

int
conf_set_password(void *obj, conf_option *opt, void *data)
{
    uint8_t *p;
    conf_value *cv = data;
    sds *gt;

    if(cv->type != CONF_VALUE_TYPE_STRING){
        log_error("Conf pool %s in the conf file is not a string", 
            opt->name);
        return VR_ERROR;
    } else if (sdslen(cv->value) > CONFIG_AUTHPASS_MAX_LEN) {
        log_error("Password is longer than CONFIG_AUTHPASS_MAX_LEN");
        return VR_ERROR;
    }

    CONF_WLOCK();
    p = obj;
    gt = (sds*)(p + opt->offset);

    if (*gt != NULL) sdsfree(*gt);
    if (sdslen(cv->value) == 0) *gt = NULL;
    else *gt = sdsnewlen(cv->value, sdslen(cv->value));
    conf->version ++;
    CONF_UNLOCK();
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
    CONF_UNLOCK();
    return VR_OK;
}

int
conf_set_int(void *obj, conf_option *opt, void *data)
{
    uint8_t *p;
    conf_value *cv = data;
    int *gt;

    if(cv->type != CONF_VALUE_TYPE_STRING){
        log_error("conf pool %s in the conf file error", 
            opt->name);
        return VR_ERROR;
    }

    CONF_WLOCK();
    
    p = obj;
    gt = (int*)(p + opt->offset);

    if(!sdsIsNum(cv->value)){
        CONF_UNLOCK();
        log_error("value of the key %s in conf file is not a number", 
            opt->name);
        return VR_ERROR;
    }

    *gt = vr_atoi(cv->value, sdslen(cv->value));

    if (*gt < 0) {
        CONF_UNLOCK();
        log_error("value of the key %s in conf file is invalid", 
            opt->name);
        return VR_ERROR;
    }
    conf->version ++;
    CONF_UNLOCK();
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
    CONF_UNLOCK();
    return VR_OK;
}

int
conf_set_longlong(void *obj, conf_option *opt, void *data)
{
    uint8_t *p;
    conf_value *cv = data;
    long long *gt;

    if(cv->type != CONF_VALUE_TYPE_STRING){
        log_error("conf pool %s in the conf file error", 
            opt->name);
        return VR_ERROR;
    }

    CONF_WLOCK();
    
    p = obj;
    gt = (long long*)(p + opt->offset);

    if (!string2ll(cv->value, sdslen(cv->value), gt)) {
        CONF_UNLOCK();
        log_error("value of the key %s in conf file is invalid", 
            opt->name);
        return VR_ERROR;
    }
    conf->version ++;
    CONF_UNLOCK();
    return VR_OK;
}

int
conf_set_yesorno(void *obj, conf_option *opt, void *data)
{
    uint8_t *p;
    conf_value *cv = data;
    int *gt;

    if(cv->type != CONF_VALUE_TYPE_STRING){
        log_error("conf pool %s in the conf file error", 
            opt->name);
        return VR_ERROR;
    }

    CONF_WLOCK();
    
    p = obj;
    gt = (int*)(p + opt->offset);

    if(!strcasecmp(cv->value, CONF_VALUE_YES)){
        *gt = 1;
    }else if(!strcasecmp(cv->value, CONF_VALUE_NO)){
        *gt = 0;
    }else{
        CONF_UNLOCK();
        log_error("key %s in conf file must be %s or %s",
            opt->name, CONF_VALUE_YES, CONF_VALUE_NO);
        return VR_ERROR;
    }
    conf->version ++;
    CONF_UNLOCK();
    return VR_OK;
}

int
conf_set_array_sds(void *obj, conf_option *opt, void *data)
{
    uint8_t *p;
    uint32_t j;
    conf_value **cv_sub, *cv = data;
    struct darray *gt;
    sds *str;

    if(cv->type != CONF_VALUE_TYPE_STRING && 
        cv->type != CONF_VALUE_TYPE_ARRAY){
        log_error("conf pool %s in the conf file is not a string or array", 
            opt->name);
        return VR_ERROR;
    } else if (cv->type == CONF_VALUE_TYPE_ARRAY) {
        cv_sub = darray_get(cv->value, j);
        if ((*cv_sub)->type != CONF_VALUE_TYPE_STRING) {
            log_error("conf pool %s in the conf file is not a string array", 
                opt->name);
            return VR_ERROR;            
        }
    }

    CONF_WLOCK();
    p = obj;
    gt = (struct darray*)(p + opt->offset);

    while (darray_n(gt) > 0) {
        str = darray_pop(gt);
        sdsfree(*str);
    }

    if (cv->type == CONF_VALUE_TYPE_STRING) {
        str = darray_push(gt);
        *str = sdsdup(cv->value);
    } else if (cv->type == CONF_VALUE_TYPE_ARRAY) {
        for (j = 0; j < darray_n(cv->value); j ++) {
            cv_sub = darray_get(cv->value, j);
            str = darray_push(gt);
            *str = sdsdup((*cv_sub)->value);
        }
    }
    conf->version ++;
    CONF_UNLOCK();
    return VR_OK;
}

int
conf_set_commands_need_adminpass(void *obj, conf_option *opt, void *data)
{
    uint8_t *p;
    uint32_t j;
    conf_value **cv_sub, *cv = data;
    struct darray *gt;
    sds *str;

    if(cv->type != CONF_VALUE_TYPE_STRING && 
        cv->type != CONF_VALUE_TYPE_ARRAY){
        log_error("conf pool %s in the conf file is not a string or array", 
            opt->name);
        return VR_ERROR;
    } else if (cv->type == CONF_VALUE_TYPE_ARRAY) {
        cv_sub = darray_get(cv->value, j);
        if ((*cv_sub)->type != CONF_VALUE_TYPE_STRING) {
            log_error("conf pool %s in the conf file is not a string array", 
                opt->name);
            return VR_ERROR;            
        }
    }

    CONF_WLOCK();
    p = obj;
    gt = (struct darray*)(p + opt->offset);

    while (darray_n(gt) > 0) {
        str = darray_pop(gt);
        sdsfree(*str);
    }

    if (cv->type == CONF_VALUE_TYPE_STRING) {
        str = darray_push(gt);
        *str = sdsdup(cv->value);
    } else if (cv->type == CONF_VALUE_TYPE_ARRAY) {
        for (j = 0; j < darray_n(cv->value); j ++) {
            cv_sub = darray_get(cv->value, j);
            str = darray_push(gt);
            *str = sdsdup((*cv_sub)->value);
        }
    }
    conf->version ++;
    CONF_UNLOCK();
    return VR_OK;
}

int
conf_get_array_sds(void *obj, conf_option *opt, void *data)
{
    uint8_t *p;
    uint32_t j;
    struct darray *strs = data;
    struct array *gt;
    sds *str1, *str2;

    if (data == NULL) {
        return VR_ERROR;
    }

    CONF_RLOCK();
    p = obj;
    gt = (struct darray*)(p + opt->offset);

    ASSERT(darray_n(strs) == 0);

    for (j = 0; j < darray_n(gt); j ++) {
        str1 = darray_get(gt, j);
        str2 = darray_push(strs);
        *str2 = sdsdup(*str1);
    }
    
    CONF_UNLOCK();
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
    dictStrCaseHash,            /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictStrKeyCaseCompare,      /* key compare */
    NULL,                       /* key destructor */
    NULL                        /* val destructor */
};

conf_value *conf_value_create(int type)
{
    conf_value *cv;

    cv = dalloc(sizeof(*cv));
    if(cv == NULL){
        return NULL;
    }

    cv->type = type;
    cv->value = NULL;

    if(cv->type == CONF_VALUE_TYPE_ARRAY){
        cv->value = darray_create(3, sizeof(conf_value*));
        if(cv->value == NULL){
            dfree(cv);
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
    
    if(cv->type == CONF_VALUE_TYPE_UNKNOW){
        dfree(cv);
        return;
    }else if(cv->type == CONF_VALUE_TYPE_STRING){
        if(cv->value != NULL){
            sdsfree(cv->value);
        }
    }else if(cv->type == CONF_VALUE_TYPE_ARRAY){
        if(cv->value != NULL){
            while(darray_n(cv->value) > 0){
                cv_sub = darray_pop(cv->value);
                conf_value_destroy(*cv_sub);
            }

            darray_destroy(cv->value);
        }
    }else{
        NOT_REACHED();
    }

    dfree(cv);
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
    darray_init(&cs->binds,1,sizeof(sds));
    cs->port = CONF_UNSET_NUM;
    cs->requirepass = CONF_UNSET_PTR;
    cs->adminpass = CONF_UNSET_PTR;
    cs->dir = CONF_UNSET_PTR;
    darray_init(&cs->commands_need_adminpass,1,sizeof(sds));

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
    cs->slowlog_log_slower_than = CONFIG_DEFAULT_SLOWLOG_LOG_SLOWER_THAN;
    cs->slowlog_max_len = CONFIG_DEFAULT_SLOWLOG_MAX_LEN;
    cs->requirepass = CONF_UNSET_PTR;
    cs->adminpass = CONF_UNSET_PTR;

    while (darray_n(&cs->binds) > 0) {
        str = darray_pop(&cs->binds);
        sdsfree(*str);
    }
    str = darray_push(&cs->binds);
    *str = sdsnew(CONFIG_DEFAULT_HOST);
    
    cs->port = CONFIG_DEFAULT_SERVER_PORT;

    if (cs->dir != CONF_UNSET_PTR) {
        sdsfree(cs->dir);
    }
    cs->dir = sdsnew(CONFIG_DEFAULT_DATA_DIR);

    while (darray_n(&cs->commands_need_adminpass) > 0) {
        str = darray_pop(&cs->commands_need_adminpass);
        sdsfree(*str);
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

    while (darray_n(&cs->binds) > 0) {
        str = darray_pop(&cs->binds);
        sdsfree(*str);
    }
    darray_deinit(&cs->binds);

    cs->port = CONF_UNSET_NUM;
    
    if (cs->dir != CONF_UNSET_PTR) {
        sdsfree(cs->dir);
        cs->dir = CONF_UNSET_PTR;    
    }

    if (cs->requirepass != CONF_UNSET_PTR) {
        sdsfree(cs->requirepass);
        cs->requirepass = CONF_UNSET_PTR;    
    }
    if (cs->adminpass != CONF_UNSET_PTR) {
        sdsfree(cs->adminpass);
        cs->adminpass = CONF_UNSET_PTR;    
    }

    while (darray_n(&cs->commands_need_adminpass) > 0) {
        str = darray_pop(&cs->commands_need_adminpass);
        sdsfree(*str);
    }
    darray_deinit(&cs->commands_need_adminpass);
}

int
conf_server_get(const char *option_name, void *value)
{
    conf_option *opt;

    opt = dictFetchValue(cserver->ctable, option_name);
    if (opt == NULL)
        return VR_ERROR;

    return opt->get(cserver, opt, value);
}

int
conf_server_set(const char *option_name, conf_value *value)
{
    conf_option *opt;

    opt = dictFetchValue(cserver->ctable, option_name);
    if (opt == NULL || opt->flags&CONF_FIELD_FLAGS_NO_MODIFY)
        return VR_ERROR;

    return opt->set(cserver, opt, value);
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
    CONF_UNLOCK();
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
        if (cv_old->type != CONF_VALUE_TYPE_ARRAY) {
            cv_new = conf_value_create(CONF_VALUE_TYPE_ARRAY);
            cv_sub = darray_push(cv_new->value);
            *cv_sub = cv_old;
            cv_sub = darray_push(cv_new->value);
            *cv_sub = cv;
            dictSetVal(org,de,cv_new);
        } else {
            cv_sub = darray_push(cv_old->value);
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
            cv = conf_value_create(CONF_VALUE_TYPE_STRING);
            cv->value = argv[j];
            argv[j] = NULL;
            ret = conf_key_value_insert(org, key, cv);
            if(ret == -1){
                sdsfreesplitres(argv,argc);
                sdsfree(key);
                conf_value_destroy(cv);
                log_error("key value insert into organization failed");
                goto loaderr;
            } else if (j == 1 && ret == 0) {
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
                log_error("Open config file '%s' failed: %s", cf->fname, strerror(errno));
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

    cf = dalloc(sizeof(*cf));
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
    
    dfree(cf);
}

unsigned long long
conf_version_get(void)
{
    unsigned long long version;
    
    CONF_RLOCK();
    version = conf->version;
    CONF_UNLOCK();

    return version;
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
CONF_UNLOCK(void)
{
    return pthread_rwlock_unlock(&conf->rwl);
}

int
CONFF_LOCK(void)
{
    return pthread_mutex_lock(&conf->flock);
}

int
CONFF_UNLOCK(void)
{
    return pthread_mutex_unlock(&conf->flock);
}

const char *
get_evictpolicy_strings(int evictpolicy_type)
{
    return evictpolicy_strings[evictpolicy_type];
}

/*-----------------------------------------------------------------------------
 * CONFIG SET implementation
 *----------------------------------------------------------------------------*/

static void configSetCommand(client *c) {
    int ret;
    sds value;
    sds *fields;
    int fields_count = 0;
    conf_option *opt;
    conf_value *cv;
    
    serverAssertWithInfo(c,c->argv[2],sdsEncodedObject(c->argv[2]));
    serverAssertWithInfo(c,c->argv[3],sdsEncodedObject(c->argv[3]));

    opt = dictFetchValue(cserver->ctable, c->argv[2]->ptr);

    if (opt == NULL) {
        addReplyErrorFormat(c,"Unsupported CONFIG parameter: %s",
            (char*)c->argv[2]->ptr);
        return;
    } else if (opt->flags&CONF_FIELD_FLAGS_NO_MODIFY) {
        addReplyErrorFormat(c,"Unsupported modify this CONFIG parameter: %s",
            (char*)c->argv[2]->ptr);
        return;
    }

    value = c->argv[3]->ptr;

    /* Handle some special action before setting the config value if needed */
    if (!strcasecmp(c->argv[2]->ptr,CONFIG_SOPN_MAXCLIENTS)) {
        long maxclients;
        int filelimit, threads;
        if (string2l(value,sdslen(value),&maxclients) == 0 || maxclients < 1) goto badfmt;
        conf_server_get(CONFIG_SOPN_THREADS,&threads);
        
        filelimit = adjustOpenFilesLimit((int)maxclients);
        if ((filelimit-threads*2-CONFIG_MIN_RESERVED_FDS) != maxclients) {
            addReplyErrorFormat(c,"The operating system is not able to handle the specified number of clients");
            return;
        }
    } else if (!strcasecmp(c->argv[2]->ptr,CONFIG_SOPN_ADMINPASS)) {
        if (c->vel->cc.adminpass && c->authenticated < 2) {
            addReplyErrorFormat(c,"You need adminpass to set this CONFIG parameter: %s",
                (char*)c->argv[2]->ptr);
            return;
        }
    }

    fields = sdssplitlen(value,sdslen(value)," ",1,&fields_count);
    if (fields == NULL) {
        goto badfmt;
    } else if (fields_count == 0) {
        cv = conf_value_create(CONF_VALUE_TYPE_STRING);
        cv->value = sdsempty();
    } else if (fields_count == 1) {
        cv = conf_value_create(CONF_VALUE_TYPE_STRING);
        cv->value = fields[0];
        fields[0] = NULL;
    } else if (fields_count > 1) {
        conf_value **cv_sub;
        uint32_t i;
    
        cv = conf_value_create(CONF_VALUE_TYPE_ARRAY);
        for (i = 0; i < fields_count; i ++) {
            cv_sub = darray_push(cv->value);
            *cv_sub = conf_value_create(CONF_VALUE_TYPE_STRING);
            (*cv_sub)->value = fields[i];
            fields[i] = NULL;
        }
    } else {
        log_debug(LOG_NOTICE, "fields_count: %d", fields_count);
        serverPanic("Error config set value");
    }
    sdsfreesplitres(fields,fields_count);

    ret = opt->set(cserver, opt, cv);
    conf_value_destroy(cv);
    if (ret != VR_OK) {
        goto badfmt;
    }

    /* Handle some special action after setting the config value if needed */
    if (!strcmp(opt->name,CONFIG_SOPN_MAXMEMORY)) {
        long long maxmemory;
        conf_server_get(CONFIG_SOPN_MAXMEMORY,&maxmemory);
        if (maxmemory) {
            if (maxmemory < dalloc_used_memory()) {
                log_warn("WARNING: the new maxmemory value set via CONFIG SET is smaller than the current memory usage. This will result in keys eviction and/or inability to accept new write commands depending on the maxmemory-policy.");
                freeMemoryIfNeeded(c->vel);
            }
        }
    }

    /* On success we just return a generic OK for all the options. */
    addReply(c,shared.ok);
    return;

badfmt: /* Bad format errors */
    addReplyErrorFormat(c,"Invalid argument '%s' for CONFIG SET '%s'",
        (char*)value,
        (char*)c->argv[2]->ptr);
}

/*-----------------------------------------------------------------------------
 * CONFIG GET implementation
 *----------------------------------------------------------------------------*/

static void addReplyConfOption(client *c,conf_option *cop)
{
    addReplyBulkCString(c,cop->name);
    if (cop->type == CONF_FIELD_TYPE_INT) {
        int value;
        conf_server_get(cop->name,&value);
        
        if (!strcmp(cop->name,CONFIG_SOPN_MAXMEMORYP)) {
            addReplyBulkCString(c,get_evictpolicy_strings(value));
        } else {
            addReplyBulkLongLong(c,value);
        }
    } else if (cop->type == CONF_FIELD_TYPE_LONGLONG) {
        long long value;
        conf_server_get(cop->name,&value);
        addReplyBulkLongLong(c,value);
    } else if (cop->type == CONF_FIELD_TYPE_SDS) {
        sds value;
        conf_server_get(cop->name,&value);
        if (value == NULL) {
            addReplyBulkCString(c,"");
        } else {
            addReplyBulkSds(c,value);
        }
    } else if (cop->type == CONF_FIELD_TYPE_ARRAYSDS) {
        struct darray values;
        sds value = sdsempty();
        sds *elem;

        darray_init(&values,1,sizeof(sds));
        conf_server_get(cop->name,&values);
        while(darray_n(&values) > 0) {
            elem = darray_pop(&values);
            value = sdscatsds(value,*elem);
            value = sdscat(value," ");
            sdsfree(*elem);
        }
        darray_deinit(&values);
        if (sdslen(value) > 0) sdsrange(value,0,-2);
        addReplyBulkSds(c,value);
    } else {
        serverPanic("Error conf field type");
    }
}

static void configGetCommand(client *c) {
    robj *o = c->argv[2];
    char *pattern = o->ptr;
    conf_option *cop;
    serverAssertWithInfo(c,o,sdsEncodedObject(o));

    cop = dictFetchValue(cserver->ctable, pattern);
    if (cop != NULL) {
        /* Don't show adminpass if user has no right. */
        if (!strcmp(cop->name,CONFIG_SOPN_ADMINPASS) && 
            c->vel->cc.adminpass && c->authenticated < 2) {
            addReply(c,shared.noadminerr);
        } else {
            addReplyMultiBulkLen(c,2);
            addReplyConfOption(c,cop);
        }
    } else {
        int matches = 0;
        void * replylen = addDeferredMultiBulkLength(c);
        for (cop = conf_server_options; cop&&cop->name; cop++) {
            if (stringmatch(pattern,cop->name,1)) {
                /* Don't show adminpass if user has no right. */
                if (!strcmp(cop->name,CONFIG_SOPN_ADMINPASS) && 
                    c->vel->cc.adminpass && c->authenticated < 2)
                    continue;
                
                addReplyConfOption(c,cop);
                matches ++;
            }
        }
        setDeferredMultiBulkLength(c,replylen,matches*2);
    }
}

/*-----------------------------------------------------------------------------
 * CONFIG REWRITE implementation
 *----------------------------------------------------------------------------*/

/* The config rewrite state. */
struct rewriteConfigState {
    dict *option_to_line; /* Option -> list of config file lines map */
    dict *rewritten;      /* Dictionary of already processed options */
    int numlines;         /* Number of lines in current config */
    sds *lines;           /* Current lines as an array of sds strings */
    int has_tail;         /* True if we already added directives that were
                             not present in the original config file. */
};

/* Append the new line to the current configuration state. */
static void rewriteConfigAppendLine(struct rewriteConfigState *state, sds line) {
    state->lines = drealloc(state->lines, sizeof(char*) * (state->numlines+1));
    state->lines[state->numlines++] = line;
}

/* Populate the option -> list of line numbers map. */
static void rewriteConfigAddLineNumberToOption(struct rewriteConfigState *state, sds option, int linenum) {
    dlist *l = dictFetchValue(state->option_to_line,option);

    if (l == NULL) {
        l = dlistCreate();
        dictAdd(state->option_to_line,sdsdup(option),l);
    }
    dlistAddNodeTail(l,(void*)(long)linenum);
}

dictType optionToLineDictType = {
    dictSdsCaseHash,            /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCaseCompare,      /* key compare */
    dictSdsDestructor,          /* key destructor */
    dictListDestructor          /* val destructor */
};

dictType optionSetDictType = {
    dictSdsCaseHash,            /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCaseCompare,      /* key compare */
    dictSdsDestructor,          /* key destructor */
    NULL                        /* val destructor */
};

#define CONFIG_MAX_LINE    1024
#define REDIS_CONFIG_REWRITE_SIGNATURE "# Generated by CONFIG REWRITE"
/* Read the old file, split it into lines to populate a newly created
 * config rewrite state, and return it to the caller.
 *
 * If it is impossible to read the old file, NULL is returned.
 * If the old file does not exist at all, an empty state is returned. */
static struct rewriteConfigState *rewriteConfigReadOldFile(char *path) {
    FILE *fp = fopen(path,"r");
    struct rewriteConfigState *state = dalloc(sizeof(*state));
    char buf[CONFIG_MAX_LINE+1];
    int linenum = -1;

    if (fp == NULL && errno != ENOENT) return NULL;

    state->option_to_line = dictCreate(&optionToLineDictType,NULL);
    state->rewritten = dictCreate(&optionSetDictType,NULL);
    state->numlines = 0;
    state->lines = NULL;
    state->has_tail = 0;
    if (fp == NULL) return state;

    /* Read the old file line by line, populate the state. */
    while(fgets(buf,CONFIG_MAX_LINE+1,fp) != NULL) {
        int argc;
        sds *argv;
        sds line = sdstrim(sdsnew(buf),"\r\n\t ");

        linenum++; /* Zero based, so we init at -1 */

        /* Handle comments and empty lines. */
        if (line[0] == '#' || line[0] == '\0') {
            if (!state->has_tail && !strcmp(line,REDIS_CONFIG_REWRITE_SIGNATURE))
                state->has_tail = 1;
            rewriteConfigAppendLine(state,line);
            continue;
        }

        /* Not a comment, split into arguments. */
        argv = sdssplitargs(line,&argc);
        if (argv == NULL) {
            /* Apparently the line is unparsable for some reason, for
             * instance it may have unbalanced quotes. Load it as a
             * comment. */
            sds aux = sdsnew("# ??? ");
            aux = sdscatsds(aux,line);
            sdsfree(line);
            rewriteConfigAppendLine(state,aux);
            continue;
        }

        sdstolower(argv[0]); /* We only want lowercase config directives. */

        /* Now we populate the state according to the content of this line.
         * Append the line and populate the option -> line numbers map. */
        rewriteConfigAppendLine(state,line);
        rewriteConfigAddLineNumberToOption(state,argv[0],linenum);

        sdsfreesplitres(argv,argc);
    }
    fclose(fp);
    return state;
}

/* Add the specified option to the set of processed options.
 * This is useful as only unused lines of processed options will be blanked
 * in the config file, while options the rewrite process does not understand
 * remain untouched. */
static void rewriteConfigMarkAsProcessed(struct rewriteConfigState *state, const char *option) {
    sds opt = sdsnew(option);

    if (dictAdd(state->rewritten,opt,NULL) != DICT_OK) sdsfree(opt);
}

/* Rewrite the specified configuration option with the new "line".
 * It progressively uses lines of the file that were already used for the same
 * configuration option in the old version of the file, removing that line from
 * the map of options -> line numbers.
 *
 * If there are lines associated with a given configuration option and
 * "force" is non-zero, the line is appended to the configuration file.
 * Usually "force" is true when an option has not its default value, so it
 * must be rewritten even if not present previously.
 *
 * The first time a line is appended into a configuration file, a comment
 * is added to show that starting from that point the config file was generated
 * by CONFIG REWRITE.
 *
 * "line" is either used, or freed, so the caller does not need to free it
 * in any way. */
static void rewriteConfigRewriteLine(struct rewriteConfigState *state, const char *option, sds line, int force) {
    sds o = sdsnew(option);
    dlist *l = dictFetchValue(state->option_to_line,o);

    rewriteConfigMarkAsProcessed(state,option);

    if (!l && !force) {
        /* Option not used previously, and we are not forced to use it. */
        sdsfree(line);
        sdsfree(o);
        return;
    }

    if (l) {
        dlistNode *ln = dlistFirst(l);
        int linenum = (long) ln->value;

        /* There are still lines in the old configuration file we can reuse
         * for this option. Replace the line with the new one. */
        dlistDelNode(l,ln);
        if (dlistLength(l) == 0) dictDelete(state->option_to_line,o);
        sdsfree(state->lines[linenum]);
        state->lines[linenum] = line;
    } else {
        /* Append a new line. */
        if (!state->has_tail) {
            rewriteConfigAppendLine(state,
                sdsnew(REDIS_CONFIG_REWRITE_SIGNATURE));
            state->has_tail = 1;
        }
        rewriteConfigAppendLine(state,line);
    }
    sdsfree(o);
}

/* Free the configuration rewrite state. */
static void rewriteConfigReleaseState(struct rewriteConfigState *state) {
    sdsfreesplitres(state->lines,state->numlines);
    dictRelease(state->option_to_line);
    dictRelease(state->rewritten);
    dfree(state);
}

/* At the end of the rewrite process the state contains the remaining
 * map between "option name" => "lines in the original config file".
 * Lines used by the rewrite process were removed by the function
 * rewriteConfigRewriteLine(), all the other lines are "orphaned" and
 * should be replaced by empty lines.
 *
 * This function does just this, iterating all the option names and
 * blanking all the lines still associated. */
static void rewriteConfigRemoveOrphaned(struct rewriteConfigState *state) {
    dictIterator *di = dictGetIterator(state->option_to_line);
    dictEntry *de;

    while((de = dictNext(di)) != NULL) {
        dlist *l = dictGetVal(de);
        sds option = dictGetKey(de);

        /* Don't blank lines about options the rewrite process
         * don't understand. */
        if (dictFind(state->rewritten,option) == NULL) {
            log_debug(LOG_DEBUG,"Not rewritten option: %s", option);
            continue;
        }

        while(dlistLength(l)) {
            dlistNode *ln = dlistFirst(l);
            int linenum = (long) ln->value;

            sdsfree(state->lines[linenum]);
            state->lines[linenum] = sdsempty();
            dlistDelNode(l,ln);
        }
    }
    dictReleaseIterator(di);
}

/* Glue together the configuration lines in the current configuration
 * rewrite state into a single string, stripping multiple empty lines. */
static sds rewriteConfigGetContentFromState(struct rewriteConfigState *state) {
    sds content = sdsempty();
    int j, was_empty = 0;

    for (j = 0; j < state->numlines; j++) {
        /* Every cluster of empty lines is turned into a single empty line. */
        if (sdslen(state->lines[j]) == 0) {
            if (was_empty) continue;
            was_empty = 1;
        } else {
            was_empty = 0;
        }
        content = sdscatsds(content,state->lines[j]);
        content = sdscatlen(content,"\n",1);
    }
    return content;
}

/* This function overwrites the old configuration file with the new content.
 *
 * 1) The old file length is obtained.
 * 2) If the new content is smaller, padding is added.
 * 3) A single write(2) call is used to replace the content of the file.
 * 4) Later the file is truncated to the length of the new content.
 *
 * This way we are sure the file is left in a consistent state even if the
 * process is stopped between any of the four operations.
 *
 * The function returns 0 on success, otherwise -1 is returned and errno
 * set accordingly. */
static int rewriteConfigOverwriteFile(char *configfile, sds content) {
    int retval = 0;
    int fd = open(configfile,O_RDWR|O_CREAT,0644);
    int content_size = sdslen(content), padding = 0;
    struct stat sb;
    sds content_padded;

    /* 1) Open the old file (or create a new one if it does not
     *    exist), get the size. */
    if (fd == -1) return -1; /* errno set by open(). */
    if (fstat(fd,&sb) == -1) {
        close(fd);
        return -1; /* errno set by fstat(). */
    }

    /* 2) Pad the content at least match the old file size. */
    content_padded = sdsdup(content);
    if (content_size < sb.st_size) {
        /* If the old file was bigger, pad the content with
         * a newline plus as many "#" chars as required. */
        padding = sb.st_size - content_size;
        content_padded = sdsgrowzero(content_padded,sb.st_size);
        content_padded[content_size] = '\n';
        memset(content_padded+content_size+1,'#',padding-1);
    }

    /* 3) Write the new content using a single write(2). */
    if (write(fd,content_padded,strlen(content_padded)) == -1) {
        retval = -1;
        goto cleanup;
    }

    /* 4) Truncate the file to the right length if we used padding. */
    if (padding) {
        if (ftruncate(fd,content_size) == -1) {
            /* Non critical error... */
        }
    }

cleanup:
    sdsfree(content_padded);
    close(fd);
    return retval;
}

/* Rewrite a numerical (int range) option. */
static void rewriteConfigIntOption(struct rewriteConfigState *state, char *option, int defvalue) {
    int value;
    int force;
    sds line;

    conf_server_get(option,&value);
    line = sdscatprintf(sdsempty(),"%s %d",option,value);
    force = value != defvalue;

    rewriteConfigRewriteLine(state,option,line,force);
}

/* Rewrite a numerical (int range) option. */
static void rewriteConfigSdsOption(struct rewriteConfigState *state, char *option, sds defvalue) {
    sds value;
    int force;
    sds line;

    conf_server_get(option,&value);
    if (defvalue == NULL && value == NULL) {
        force = 0;
    } else if (defvalue != NULL && value != NULL && !sdscmp(value,defvalue)) {
        force = 0;
    } else {
        force = 1;
    }

    if (value == NULL) {
        line = sdscatprintf(sdsempty(),"%s \"\"",option);
    } else {
        line = sdscatprintf(sdsempty(),"%s %s",option,value);
        sdsfree(value);
    }
    
    rewriteConfigRewriteLine(state,option,line,force);
}

/* Rewrite a numerical (long long range) option. */
static void rewriteConfigLongLongOption(struct rewriteConfigState *state, char *option, long long defvalue) {
    long long value;
    int force;
    sds line;

    conf_server_get(option,&value);
    line = sdscatprintf(sdsempty(),"%s %lld",option,value);
    force = value != defvalue;

    rewriteConfigRewriteLine(state,option,line,force);
}

/* Write the long long 'bytes' value as a string in a way that is parsable
 * inside redis.conf. If possible uses the GB, MB, KB notation. */
static int rewriteConfigFormatMemory(char *buf, size_t len, long long bytes) {
    int gb = 1024*1024*1024;
    int mb = 1024*1024;
    int kb = 1024;

    if (bytes && (bytes % gb) == 0) {
        return snprintf(buf,len,"%lldgb",bytes/gb);
    } else if (bytes && (bytes % mb) == 0) {
        return snprintf(buf,len,"%lldmb",bytes/mb);
    } else if (bytes && (bytes % kb) == 0) {
        return snprintf(buf,len,"%lldkb",bytes/kb);
    } else {
        return snprintf(buf,len,"%lld",bytes);
    }
}

/* Rewrite a simple "option-name <bytes>" configuration option. */
static void rewriteConfigBytesOption(struct rewriteConfigState *state, char *option, long long defvalue) {
     long long value;
    char buf[64];
    int force;
    sds line;

    conf_server_get(option,&value);
    force = value != defvalue;

    rewriteConfigFormatMemory(buf,sizeof(buf),value);
    line = sdscatprintf(sdsempty(),"%s %s",option,buf);
    rewriteConfigRewriteLine(state,option,line,force);
}

/* Rewrite an enumeration option. It takes as usually state and option name,
 * and in addition the enumeration array and the default value for the
 * option. */
static void rewriteConfigEnumOption(struct rewriteConfigState *state, char *option, configEnumGetStrFun fun, int defval) {
    int value;
    sds line;
    const char *name;
    int force;

    conf_server_get(option,&value);
    force = value != defval;
    name = fun(value);
    line = sdscatprintf(sdsempty(),"%s %s",option,name);
    rewriteConfigRewriteLine(state,option,line,force);
}

/* Rewrite the bind option. */
static void rewriteConfigBindOption(struct rewriteConfigState *state) {
    struct darray values;
    sds *value, line;
    int force = 1;
    char *option = CONFIG_SOPN_BIND;

    darray_init(&values,1,sizeof(sds));
    conf_server_get(option,&values);
    /* Nothing to rewrite if we don't have bind addresses. */
    if (darray_n(&values) == 0) {
        darray_deinit(&values);
        rewriteConfigMarkAsProcessed(state,option);
        return;
    }

    /* Rewrite as bind <addr1> <addr2> ... <addrN> */
    line = sdsnew(option);
    while(darray_n(&values) > 0) {
        line = sdscat(line," ");
        value = darray_pop(&values);
        line = sdscatsds(line,*value);
        sdsfree(*value);
    }
    darray_deinit(&values);

    rewriteConfigRewriteLine(state,option,line,force);
}

/* Rewrite the save option. */
void rewriteConfigCommandsNAPOption(struct rewriteConfigState *state) {
    struct darray values;
    sds *value, line;
    int force = 1;
    char *option = CONFIG_SOPN_COMMANDSNAP;

    darray_init(&values,1,sizeof(sds));
    conf_server_get(option,&values);
    /* Nothing to rewrite if we don't have commands that need adminpass. */
    if (darray_n(&values) == 0) {
        darray_deinit(&values);
        rewriteConfigMarkAsProcessed(state,option);
        return;
    }

    while(darray_n(&values) > 0) {
        value = darray_pop(&values);
        line = sdscatprintf(sdsempty(),"%s %s",option,*value);
        rewriteConfigRewriteLine(state,option,line,force);
        sdsfree(*value);
    }
    darray_deinit(&values);
    rewriteConfigMarkAsProcessed(state,option);
}

/* Rewrite the configuration file at "path".
 * If the configuration file already exists, we try at best to retain comments
 * and overall structure.
 *
 * Configuration parameters that are at their default value, unless already
 * explicitly included in the old configuration file, are not rewritten.
 *
 * On error -1 is returned and errno is set accordingly, otherwise 0. */
static int rewriteConfig(char *path) {
    struct rewriteConfigState *state;
    sds newcontent;
    int retval;
    conf_option *cop;

    CONFF_LOCK();
    /* Step 1: read the old config into our rewrite state. */
    if ((state = rewriteConfigReadOldFile(path)) == NULL) {
        CONFF_UNLOCK();
        return -1;
    }

    /* Step 2: rewrite every single option, replacing or appending it inside
     * the rewrite state. */
    rewriteConfigIntOption(state,CONFIG_SOPN_DATABASES,CONFIG_DEFAULT_LOGICAL_DBNUM);
    rewriteConfigIntOption(state,CONFIG_SOPN_IDPDATABASE,CONFIG_DEFAULT_INTERNAL_DBNUM);
    rewriteConfigBytesOption(state,CONFIG_SOPN_MAXMEMORY,CONFIG_DEFAULT_MAXMEMORY);
    rewriteConfigEnumOption(state,CONFIG_SOPN_MAXMEMORYP,get_evictpolicy_strings,CONFIG_DEFAULT_MAXMEMORY_POLICY);
    rewriteConfigIntOption(state,CONFIG_SOPN_MAXMEMORYS,CONFIG_DEFAULT_MAXMEMORY_SAMPLES);
    rewriteConfigLongLongOption(state,CONFIG_SOPN_MTCLIMIT,CONFIG_DEFAULT_MAX_TIME_COMPLEXITY_LIMIT);
    rewriteConfigBindOption(state);
    rewriteConfigIntOption(state,CONFIG_SOPN_PORT,CONFIG_DEFAULT_SERVER_PORT);
    rewriteConfigIntOption(state,CONFIG_SOPN_THREADS,CONFIG_DEFAULT_THREADS_NUM);
    rewriteConfigLongLongOption(state,CONFIG_SOPN_SLOWLOGLST,CONFIG_DEFAULT_SLOWLOG_LOG_SLOWER_THAN);
    rewriteConfigIntOption(state,CONFIG_SOPN_SLOWLOGML,CONFIG_DEFAULT_SLOWLOG_MAX_LEN);
    rewriteConfigIntOption(state,CONFIG_SOPN_MAXCLIENTS,CONFIG_DEFAULT_MAX_CLIENTS);
    rewriteConfigSdsOption(state,CONFIG_SOPN_REQUIREPASS,NULL);
    rewriteConfigSdsOption(state,CONFIG_SOPN_ADMINPASS,NULL);
    rewriteConfigCommandsNAPOption(state);
    
    /* Step 3: remove all the orphaned lines in the old file, that is, lines
     * that were used by a config option and are no longer used, like in case
     * of multiple "save" options or duplicated options. */
    rewriteConfigRemoveOrphaned(state);

    /* Step 4: generate a new configuration file from the modified state
     * and write it into the original file. */
    newcontent = rewriteConfigGetContentFromState(state);
    retval = rewriteConfigOverwriteFile(server.configfile,newcontent);
    CONFF_UNLOCK();

    sdsfree(newcontent);
    rewriteConfigReleaseState(state);
    return retval;
}

/*-----------------------------------------------------------------------------
 * CONFIG command entry point
 *----------------------------------------------------------------------------*/

void configCommand(client *c) {
    /* Only allow CONFIG GET while loading. */
    if (server.loading && strcasecmp(c->argv[1]->ptr,"get")) {
        addReplyError(c,"Only CONFIG GET is allowed during loading");
        return;
    }

    if (!strcasecmp(c->argv[1]->ptr,"set")) {
        if (c->argc != 4) goto badarity;
        configSetCommand(c);
    } else if (!strcasecmp(c->argv[1]->ptr,"get")) {
        if (c->argc != 3) goto badarity;
        configGetCommand(c);
    } /*else if (!strcasecmp(c->argv[1]->ptr,"resetstat")) {
        if (c->argc != 2) goto badarity;
        resetServerStats();
        resetCommandTableStats();
        addReply(c,shared.ok);
    }*/ else if (!strcasecmp(c->argv[1]->ptr,"rewrite")) {
        if (c->argc != 2) goto badarity;
        if (server.configfile == NULL) {
            addReplyError(c,"The server is running without a config file");
            return;
        }
        if (rewriteConfig(server.configfile) == -1) {
            log_warn("CONFIG REWRITE failed: %s", strerror(errno));
            addReplyErrorFormat(c,"Rewriting config file: %s", strerror(errno));
        } else {
            log_warn("CONFIG REWRITE executed with success.");
            addReply(c,shared.ok);
        }
    } else {
        addReplyError(c,
            //"CONFIG subcommand must be one of GET, SET, RESETSTAT, REWRITE");
            "CONFIG subcommand must be GET, SET, REWRITE");
    }
    return;

badarity:
    addReplyErrorFormat(c,"Wrong number of arguments for CONFIG %s",
        (char*) c->argv[1]->ptr);
}

int
conf_cache_init(conf_cache *cc)
{
    cc->cache_version = 0;
    conf_server_get(CONFIG_SOPN_MAXCLIENTS,&cc->maxclients);
    conf_server_get(CONFIG_SOPN_REQUIREPASS,&cc->requirepass);
    conf_server_get(CONFIG_SOPN_ADMINPASS,&cc->adminpass);
    conf_server_get(CONFIG_SOPN_MAXMEMORY,&cc->maxmemory);
    conf_server_get(CONFIG_SOPN_MTCLIMIT,&cc->max_time_complexity_limit);
    conf_server_get(CONFIG_SOPN_SLOWLOGLST,&cc->slowlog_log_slower_than);

    return VR_OK;
}

int
conf_cache_deinit(conf_cache *cc)
{
    cc->cache_version = 0;
    if (cc->requirepass != NULL) {
        sdsfree(cc->requirepass);
        cc->requirepass = NULL;
    }
    if (cc->adminpass != NULL) {
        sdsfree(cc->adminpass);
        cc->adminpass = NULL;
    }

    return VR_OK;
}

int
conf_cache_update(conf_cache *cc)
{
    unsigned long long cversion = conf_version_get();

    /* Not need update conf cache. */
    if (cversion <= cc->cache_version) {
        return;
    }

    if (cc->requirepass != NULL) {
        sdsfree(cc->requirepass);
        cc->requirepass = NULL;
    }
    if (cc->adminpass != NULL) {
        sdsfree(cc->adminpass);
        cc->adminpass = NULL;
    }

    conf_server_get(CONFIG_SOPN_MAXCLIENTS,&cc->maxclients);
    conf_server_get(CONFIG_SOPN_REQUIREPASS,&cc->requirepass);
    conf_server_get(CONFIG_SOPN_ADMINPASS,&cc->adminpass);
    conf_server_get(CONFIG_SOPN_MAXMEMORY,&cc->maxmemory);
    conf_server_get(CONFIG_SOPN_MTCLIMIT,&cc->max_time_complexity_limit);
    conf_server_get(CONFIG_SOPN_SLOWLOGLST,&cc->slowlog_log_slower_than);

    cc->cache_version = cversion;

    return VR_OK;
}
