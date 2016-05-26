#include <vr_core.h>

#define CONF_TOKEN_ORGANIZATION_START   "["
#define CONF_TOKEN_ORGANIZATION_END     "]"
#define CONF_TOKEN_KEY_VALUE_BETWEEN    ":"
#define CONF_TOKEN_ARRAY_START          "-"

#define CONF_ORGANIZATION_NAME_COMMAN   "common"
#define CONF_ORGANIZATION_NAME_SOURCE   "source"
#define CONF_ORGANIZATION_NAME_TARGET   "target"

#define CONF_VALUE_TRUE                 "true"
#define CONF_VALUE_FALSE                "false"

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

#define CONF_VALUE_UNKNOW 0
#define CONF_VALUE_STRING 1
#define CONF_VALUE_ARRAY  2

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

#define DEFINE_ACTION(_group, _name) (char*)(#_name),
static char* group_strings[] = {
    GROUP_CODEC( DEFINE_ACTION )
    NULL
};
#undef DEFINE_ACTION

static conf_option conf_pool_options[] = {
    { (char*)"type",
      conf_pool_set_type,
      offsetof(conf_pool, type) },
    { (char*)"hash",
      conf_pool_set_hash,
      offsetof(conf_pool, hash) },
    { (char*)"hash_tag",
      conf_pool_set_hash_tag,
      offsetof(conf_pool, hash_tag) },
    { (char*)"distribution",
      conf_pool_set_distribution,
      offsetof(conf_pool, distribution) },
    { (char*)"redis_auth",
      conf_set_string,
      offsetof(conf_pool, redis_auth) },
    { (char*)"redis_db",
      conf_set_num,
      offsetof(conf_pool, redis_db) },
    { (char*)"timeout",
      conf_set_num,
      offsetof(conf_pool, timeout) },
    { (char*)"servers",
      conf_pool_set_servers,
      offsetof(conf_pool, servers) },
    { NULL, NULL, 0 }
};

static conf_option conf_common_options[] = {
    { (char*)"listen",
      conf_set_string,
      offsetof(vr_conf, listen) },
    { (char*)"maxmemory",
      conf_common_set_maxmemory,
      offsetof(vr_conf, maxmemory) },
    { (char*)"threads",
      conf_set_num,
      offsetof(vr_conf, threads) },
    { (char*)"dir",
      conf_set_string,
      offsetof(vr_conf, dir) },
    { (char*)"max_clients",
      conf_set_num,
      offsetof(vr_conf, max_clients) },
    { NULL, NULL, 0 }
};

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
conf_pool_set_type(void *obj, conf_option *opt, void *data)
{
    uint8_t *p;
    conf_value *cv = data;
    group_type_t *gt;
    char **type;

    if(cv->type != CONF_VALUE_STRING){
        log_error("conf pool %s in the conf file error",
            opt->name);
        return VR_ERROR;
    }

    p = obj;
    gt = (group_type_t *)(p + opt->offset);

    for (type = group_strings; *type; type++) {
        if(strcmp(cv->value, *type) == 0){
            *gt = type - group_strings;
            break;
        }
    }    

    if(*gt == CONF_UNSET_GROUP){
        log_error("conf pool type in the conf file can not be %s", 
            cv->value);
        return VR_ERROR;
    }

    return VR_OK;
}

int
conf_pool_set_hash(void *obj, conf_option *opt, void *data)
{
    uint8_t *p;
    conf_value *cv = data;
    hash_type_t *gt;
    char **hash;

    if(cv->type != CONF_VALUE_STRING){
        log_error("conf pool %s in the conf file error", 
            opt->name);
        return VR_ERROR;
    }

    p = obj;
    gt = (hash_type_t *)(p + opt->offset);

    for (hash = hash_strings; *hash; hash++) {
        if(strcmp(cv->value, *hash) == 0){
            *gt = hash - hash_strings;
            break;
        }
    }    

    if(*gt == CONF_UNSET_HASH){
        log_error("conf pool hash in the conf file can not be %s", 
            cv->value);
        return VR_ERROR;
    }

    return VR_OK;
}

int
conf_pool_set_hash_tag(void *obj, conf_option *opt, void *data)
{
    uint8_t *p;
    conf_value *cv = data;
    sds *gt, str;
    
    if(cv->type != CONF_VALUE_STRING){
        log_error("conf pool %s in the conf file error", 
            opt->name);
        return VR_ERROR;
    }

    str = cv->value;

    if(sdslen(str) < 2){
        log_error("%s is not a valid hash tag string with two characters", 
            cv->value);
        return VR_ERROR;
    }

    if(*(str) == '"'){
        if(*(str+sdslen(str)-1) != '"'){
            log_error("%s is not a valid hash tag string with two characters", 
                cv->value);
            return VR_ERROR;
        }

        sdsrange(str,1,-2);
    }

    if (sdslen(str) != 2) {
        log_error("%s is not a valid hash tag string with two characters", 
            cv->value);
        return VR_ERROR;
    }

    p = obj;
    gt = (sds*)(p + opt->offset);

    *gt = sdsdup(str);

    return VR_OK;
}

int
conf_pool_set_distribution(void *obj, conf_option *opt, void *data)
{
    uint8_t *p;
    conf_value *cv = data;
    dist_type_t *gt;
    char **dist;

    if(cv->type != CONF_VALUE_STRING){
        log_error("conf pool %s in the conf file error",
            opt->name);
        return VR_ERROR;
    }

    p = obj;
    gt = (dist_type_t *)(p + opt->offset);

    for (dist = dist_strings; *dist; dist++) {
        if(strcmp(cv->value, *dist) == 0){
            *gt = dist - dist_strings;
            break;
        }
    }    

    if(*gt == CONF_UNSET_DIST){
        log_error("conf pool distribution in the conf file can not be %s", 
            cv->value);
        return VR_ERROR;
    }

    return VR_OK;
}

int
conf_pool_set_servers(void *obj, conf_option *opt, void *data)
{
    uint8_t *p;
    conf_value *cv = data, **cv_sub;
    struct array **gt;
    sds *str;
    uint32_t i, count;

    if(cv->type != CONF_VALUE_ARRAY){
        log_error("conf pool %s in the conf file is not a string", 
            opt->name);
        return VR_ERROR;
    }

    count = array_n(cv->value);

    if(count == 0){
        log_error("conf pool %s in the conf file has no elements",
            opt->name);
        return VR_ERROR;
    }

    p = obj;
    gt = (struct array **)(p + opt->offset);
    
    if(*gt == NULL){
        *gt = array_create(count, sizeof(sds));
        if(*gt == NULL){
            log_error("out of memory");
            return VR_ENOMEM;
        }
    }

    for(i = 0; i < count; i++){
        cv_sub = array_get(cv->value, i);
        
        if((*cv_sub)->type != CONF_VALUE_STRING){
            log_error("conf pool %s in the conf file is array," 
                "but element is not string", opt->name);
            return VR_ERROR;
        }

        ASSERT((*cv_sub)->value != NULL);

        str = array_push(*gt);
        *str = sdsdup((*cv_sub)->value);
    }

    return VR_OK;
}

int
conf_common_set_maxmemory(void *obj, conf_option *opt, void *data)
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

    p = obj;
    gt = (long long *)(p + opt->offset);

    value = memtoll(cv->value, &err);
    if(err != 0){
        log_error("value for the key %s in conf file is invalid", 
             opt->name);
        return VR_ERROR;
    }

    *gt = (long long)value;

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

    p = obj;
    gt = (sds*)(p + opt->offset);

    *gt = sdsnewlen(cv->value, sdslen(cv->value));

    return VR_OK;
}

int
conf_set_num(void *obj, conf_option *opt, void *data)
{
    uint8_t *p;
    conf_value *cv = data;
    int *gt;

    if(cv->type != CONF_VALUE_STRING){
        log_error("conf pool %s in the conf file error", 
            opt->name);
        return VR_ERROR;
    }

    p = obj;
    gt = (int*)(p + opt->offset);

    if(!sdsIsNum(cv->value)){
        log_error("value of the key %s in conf file is not a number", 
            opt->name);
        return VR_ERROR;
    }

    *gt = vr_atoi(cv->value, sdslen(cv->value));

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

    p = obj;
    gt = (int*)(p + opt->offset);

    if(strcmp(cv->value, CONF_VALUE_TRUE) == 0){
        *gt = 1;
    }else if(strcmp(cv->value, CONF_VALUE_FALSE) == 0){
        *gt = 0;
    }else{
        log_error("key %s in conf file must be %s or %s",
            opt->name, CONF_VALUE_TRUE, CONF_VALUE_FALSE);
        return VR_ERROR;
    }

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
    dictConfValueDestructor         /* val destructor */
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

static int conf_pool_init(conf_pool *cp)
{
    if(cp == NULL){
        return VR_ERROR;
    }

    cp->type = CONF_UNSET_GROUP;
    cp->servers = CONF_UNSET_PTR;
    cp->hash = CONF_UNSET_HASH;
    cp->distribution = CONF_UNSET_DIST;
    cp->hash_tag = CONF_UNSET_PTR;
    cp->redis_auth = CONF_UNSET_PTR;
    cp->redis_db = CONF_UNSET_NUM;
    cp->timeout = CONF_UNSET_NUM;
    cp->backlog = CONF_UNSET_NUM;

    cp->servers = array_create(3, sizeof(sds));
    if(cp->servers == NULL){
        log_error("out of memory");
        return VR_ENOMEM;
    }

    return VR_OK;
}

static void conf_pool_deinit(conf_pool *cp)
{
    sds *str;

    if(cp == NULL){
        return;
    }

    cp->type = CONF_UNSET_GROUP;

    if(cp->servers != NULL){
        while(array_n(cp->servers) > 0){
            str = array_pop(cp->servers);
            sdsfree(*str);
        }

        array_destroy(cp->servers);
        cp->servers = CONF_UNSET_PTR;
    }
    
    cp->hash = CONF_UNSET_HASH;
    cp->distribution = CONF_UNSET_DIST;

    if(cp->hash_tag != NULL){
        sdsfree(cp->hash_tag);
        cp->hash_tag = CONF_UNSET_PTR;
    }

    if(cp->redis_auth != NULL){
        sdsfree(cp->redis_auth);
        cp->redis_auth = CONF_UNSET_PTR;
    }
    
    cp->redis_db = CONF_UNSET_NUM;
    cp->timeout = CONF_UNSET_NUM;
    cp->backlog = CONF_UNSET_NUM;
}

static int conf_init(vr_conf *cf)
{
    int ret;

    if(cf == NULL){
        return VR_ERROR;
    }

    cf->fname = NULL;
    cf->fh = NULL;
    cf->organizations = NULL;

    cf->organizations = dictCreate(&OrganizationDictType, NULL);
    if (cf->organizations == NULL) {
        return VR_ERROR;
    }

    cf->listen = CONF_UNSET_PTR;
    cf->maxmemory = CONF_UNSET_NUM;
    cf->threads = CONF_UNSET_NUM;
    cf->dir = CONF_UNSET_PTR;

    cf->max_clients = CONF_UNSET_NUM;
    
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

    if(cf->fh != NULL){
        fclose(cf->fh);
        cf->fh = NULL;
    }

    if(cf->organizations != NULL){
        dictRelease(cf->organizations);
        cf->organizations = NULL;
    }

    if(cf->listen != NULL){
        sdsfree(cf->listen);
        cf->listen = CONF_UNSET_PTR;
    }

    if(cf->dir != NULL){
        sdsfree(cf->dir);
        cf->dir = CONF_UNSET_PTR;
    }
    
    cf->maxmemory = CONF_UNSET_NUM;
    cf->threads = CONF_UNSET_NUM;
}

static void
conf_pool_dump(conf_pool *cp, int log_level)
{    
    uint32_t j, nserver;
    sds *s;
    if(cp == NULL){
        return;
    }

    log_debug(log_level, "  type : %d", cp->type);
    log_debug(log_level, "  hash : %d", cp->hash);    
    log_debug(log_level, "  hash_tag : %s", cp->hash_tag);
    log_debug(log_level, "  distribution : %d", cp->distribution);
    log_debug(log_level, "  redis_auth : %s", cp->redis_auth);
    log_debug(log_level, "  redis_db : %d", cp->redis_db);
    log_debug(log_level, "  timeout : %d", cp->timeout);
    log_debug(log_level, "  backlog : %d", cp->backlog);


    nserver = array_n(cp->servers);
    log_debug(log_level, "  servers: %"PRIu32"", nserver);
    for (j = 0; j < nserver; j++) {
        s = array_get(cp->servers, j);
        log_debug(log_level, "    %.*s", sdslen(*s), *s);
    }
}

static void
conf_dump(vr_conf *cf)
{
    int log_level = LOG_VERB;
    conf_pool *source_pool, *target_pool;
    
    if(cf == NULL){
        return;
    }

    log_debug(log_level, "common in conf file");
    log_debug(log_level, "  listen: %s", cf->listen);
    log_debug(log_level, "  maxmemory: %lld", cf->maxmemory);
    log_debug(log_level, "  threads: %d", cf->threads);
    log_debug(log_level, "  dir: %s", cf->dir);
    log_debug(log_level, "  max_clients: %s", cf->max_clients);
    log_debug(log_level, "");
}

static int
conf_key_value_insert(dict *org, sds key, conf_value *cv)
{
    if(key == NULL){
        log_error("value in conf file has no key");
        return VR_ERROR;
    }

    if(cv == NULL){
        log_error("key %s in conf file has no value", key);
        return VR_ERROR;
    }

    if(org == NULL){
        log_error("key %s in conf file has no organization", 
            key);
        return VR_ERROR;
    }
    
    if(dictAdd(org,key,cv) != DICT_OK){
        log_error("key %s in organization of conf file is duplicate", key);
        return VR_ERROR;
    }

    return VR_OK;
}

static int
conf_pre_validate(vr_conf *cf)
{
    int ret;
    FILE *fh;
    char line[256];
    dict *organization = NULL;
    sds str = NULL;
    sds org_name = NULL;
    sds *key_value = NULL;
    int key_value_count = 0;
    sds key = NULL;
    conf_value *cv = NULL, **cv_sub;

    if (cf == NULL) {
        return VR_ERROR;
    }

    fh = cf->fh;
    if (fh == NULL) {
        return VR_ERROR;
    }

    while (!feof(fh)) {
        if (fgets(line,256,fh) == NULL) {
            if(feof(fh)) break;
            
            log_error("read a line from conf file %s failed: %s", 
                cf->fname, strerror(errno));
            goto error;
        }

        if(str != NULL){
            sdsfree(str);
            str = NULL;
        }

        if(key_value!= NULL){
            sdsfreesplitres(key_value, key_value_count);
            key_value = NULL;
            key_value_count = 0;
        }
        
        ASSERT(str == NULL);
        str = sdsnew(line);
        if(str == NULL){
            log_error("out of memory");
            goto error;
        }

        sdstrim(str, " \n");
        if(strchr(str, '#') != NULL){
            if(str[0] == '#'){
                log_debug(LOG_VVERB, "This line is comment");
                sdsfree(str);
                str = NULL;
                continue;
            }

            sds *content_comment = NULL;
            int content_comment_count = 0;

            content_comment = sdssplitlenonce(str, sdslen(str), 
                "#", 1, &content_comment_count);
            if(content_comment == NULL || content_comment_count != 2){
                log_error("split content and comment error.");
                goto error;
            }

            sdsfree(str);
            str = content_comment[0];
            content_comment[0] = NULL;
            sdsfreesplitres(content_comment, content_comment_count);
        }
        
        log_debug(LOG_VVERB, "%s", str);

        if(sdslen(str) == 0){
            log_debug(LOG_VVERB, "This line is space");
            sdsfree(str);
            str = NULL;
            continue;
        }else if(*str == '['){
            if(sdslen(str) <= 2 || *(str+sdslen(str)-1) != ']') {
                log_error("organization name %s in conf file %s error",
                    str, cf->fname);
                goto error;
            }

            if(key != NULL || cv != NULL){
                ret = conf_key_value_insert(organization, key, cv);
                if(ret != VR_OK){
                    log_error("key value insert into organization failed");
                    goto error;
                }
                organization = NULL;
                key = NULL;
                cv = NULL;
            }

            ASSERT(org_name == NULL);
            sdsrange(str,1,-2);
            org_name = sdstrim(str, " ");
            str = NULL;

            if(sdslen(org_name) == 0){
                log_error("organization name %s in conf file %s error",
                    str, cf->fname);
                goto error;
            }

            organization = dictCreate(&KeyValueDictType, NULL);
            if(organization == NULL){
                log_error("create dict organization %s failed", 
                    org_name);
                goto error;
            }

            ret = dictAdd(cf->organizations, org_name, organization);
            if(ret != DICT_OK){
                log_error("organization %s in conf file is duplicate", 
                    org_name);
                goto error;
            }
            org_name = NULL;
        }else if(*str == '-'){
            
            if(cv == NULL || cv->type != CONF_VALUE_ARRAY){
                log_error("array %s in conf file %s has wrong conf value",
                    str, cf->fname);
                goto error;
            }

            sdsrange(str,1,-1);
            sdstrim(str, " ");
            
            cv_sub = array_push(cv->value);
            *cv_sub = conf_value_create(CONF_VALUE_STRING);
            if(*cv_sub == NULL){
                log_error("conf value create failed");
                goto error;
            }
            
            (*cv_sub)->value = str;
            str = NULL;
        }else{        
            key_value = sdssplitlenonce(str,sdslen(str),":",1,&key_value_count);
            log_debug(LOG_VVERB, "key_value_count: %d", key_value_count);
            if(key_value == NULL || key_value_count == 0){
                log_error("line %s split by : failed", str);
                goto error;
            }else if(key_value_count == 1){
                log_error("line %s in conf file %s has no :", 
                    str, cf->fname);
                goto error;
            }else if(key_value_count == 2){           

                if(key != NULL || cv != NULL){
                    ret = conf_key_value_insert(organization, key, cv);
                    if(ret != VR_OK){
                        log_error("key value insert into organization failed");
                        goto error;
                    }
                    key = NULL;
                    cv = NULL;
                }

                sdstrim(key_value[0]," ");
                sdstrim(key_value[1]," ");
                
                if(sdslen(key_value[1]) == 0){
                    ASSERT(cv == NULL);
                    cv = conf_value_create(CONF_VALUE_ARRAY);
                    if(cv == NULL){
                        log_error("conf value create failed");
                        goto error;
                    }

                    ASSERT(key == NULL);
                    key = key_value[0];
                    key_value[0] = NULL;
                    
                    continue;
                }

                ASSERT(cv == NULL);
                cv = conf_value_create(CONF_VALUE_STRING);
                if(cv == NULL){
                    log_error("conf value create failed");
                    goto error;
                }

                cv->value = key_value[1];
                key_value[1] = NULL;

                if(organization == NULL){
                    log_error("line %s in conf file %s has no organization", 
                        str, cf->fname);
                    goto error;
                }
                ret = dictAdd(organization,key_value[0],cv);
                if(ret != DICT_OK){
                    log_error("key %s in organization of conf file is duplicate", 
                        key_value[0]);
                    goto error;
                }
                key_value[0] = NULL;
                cv = NULL;     
            }else{
                NOT_REACHED();
            }
        }
        
    }

    if(key != NULL || cv != NULL){
        ret = conf_key_value_insert(organization, key, cv);
        if(ret != VR_OK){
            log_error("key value insert into organization failed");
            goto error;
        }
        key = NULL;
        cv = NULL;
    }

    if(str != NULL){
        sdsfree(str);
        str = NULL;
    }

    if(key_value!= NULL){
        sdsfreesplitres(key_value, key_value_count);
        key_value = NULL;
        key_value_count = 0;
    }

    return VR_OK;

error:

    if(str != NULL){
        sdsfree(str);
    }

    if(key != NULL){
        sdsfree(key);
    }

    if(org_name != NULL){
        sdsfree(org_name);
    }

    if(key_value != NULL){
        sdsfreesplitres(key_value, key_value_count);
    }

    if(cv != NULL){
        conf_value_destroy(cv);
    }

    return VR_ERROR;
}

static int
conf_parse_conf_pool(conf_pool *cp, dict *org)
{
    int ret;
    conf_option *opt;
    dictEntry *de;
    sds key;
    
    if(cp == NULL || org == NULL){
        return VR_ERROR;
    }

    key = sdsempty();

    for(opt = conf_pool_options; opt&&opt->name; opt++){
        key = sdscpy(key, opt->name);
        de = dictFind(org, key);
        if(de != NULL){
            ret = opt->set(cp, opt, dictGetVal(de));
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
conf_parse_conf_common(vr_conf *cf, dict *org)
{
    int ret;
    conf_option *opt;
    dictEntry *de;
    sds key;
    
    if(cf == NULL || org == NULL){
        return VR_ERROR;
    }

    key = sdsempty();

    for(opt = conf_common_options; opt&&opt->name; opt++){
        key = sdscpy(key, opt->name);
        de = dictFind(org, key);
        if(de != NULL){
            ret = opt->set(cf, opt, dictGetVal(de));
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
    conf_pool *cp;
    dict *orgs, *org;
    dictEntry *de;
    sds key;
    
    if(cf == NULL){
        return VR_ERROR;
    }

    orgs = cf->organizations;
    if(orgs == NULL){
        return VR_ERROR;
    }

    //common
    key = sdsnew(CONF_ORGANIZATION_NAME_COMMAN);
    de = dictFind(orgs, key);
    if(de == NULL){
        log_error("can not find %s organization in conf file %s", 
            CONF_ORGANIZATION_NAME_COMMAN, cf->fname);
        sdsfree(key);
        return VR_ERROR;
    }

    org = dictGetVal(de);
    if(org == NULL){
        log_error("dict %s entry value is NULL", dictGetKey(de));
        sdsfree(key);
        return VR_ERROR;
    }

    ret = conf_parse_conf_common(cf, org);
    if(ret != VR_OK){
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
    FILE *fh = NULL;
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

    fh = fopen(path, "r");
    if (fh == NULL) {
        log_error("failed to open configuration '%s': %s", path,
                  strerror(errno));
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

    cf->fname = path;
    cf->fh = fh;

    return cf;

error:

    if(fh != NULL) {
        fclose(fh);
    }

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

    fclose(cf->fh);
    cf->fh = NULL;

    return cf;

error:
    fclose(cf->fh);
    cf->fh = NULL;
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