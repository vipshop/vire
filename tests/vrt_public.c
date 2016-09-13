#include <stdio.h>
#include <stdlib.h>
#include <getopt.h>
#include <fcntl.h>
#include <errno.h>
#include <signal.h>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/utsname.h>

#include <hiredis.h>

#include <darray.h>
#include <dlog.h>

#include <vrt_util.h>
#include <vrt_public.h>

/* GCC version >= 4.7 */
#if defined(__ATOMIC_RELAXED)
/* GCC version >= 4.1 */
#elif defined(HAVE_ATOMIC)
#else
pthread_mutex_t state_locker = PTHREAD_MUTEX_INITIALIZER;
#endif

#define VIRE_TEST_CONFIG_DEFAULT_EXECUTE_FILE "src/vire"

static char *execute_file = VIRE_TEST_CONFIG_DEFAULT_EXECUTE_FILE;

static sds workdir = NULL;

static int vireport = 55556; /* The available port for vire to start */

void set_execute_file(char *file)
{
    execute_file = file;
}

static sds vire_conf_create(char *dir, int port)
{
    sds conf_file;
    int fd;
    sds line;
    
    conf_file = sdscatfmt(sdsempty(),"%s\/vire.conf",dir);

    fd = open(conf_file,O_WRONLY|O_CREAT|O_TRUNC,0644);
    if (fd < 0) {
        test_log_error("Open conf file %s failed: %s", conf_file, strerror(errno));
        sdsfree(conf_file);
        return NULL;
    }

    line = sdsempty();

    line = sdscatfmt(line,"port %i\n",port);
    write(fd, line, sdslen(line));

    sdsclear(line);
    line = sdscatfmt(line,"\n");
    write(fd, line, sdslen(line));
    
    close(fd);
    sdsfree(line);
    return conf_file;
}

vire_instance *vire_instance_create(int port)
{
    vire_instance *vi;

    vi = malloc(sizeof(vire_instance));
    vi->host = NULL;
    vi->port = 0;
    vi->dir = NULL;
    vi->conf_file = NULL;
    vi->pid_file = NULL;
    vi->log_file = NULL;
    vi->running = 0;
    vi->pid = -1;
    vi->ctx = NULL;

    vi->host = sdsnew("127.0.0.1");
    vi->port = port;
    vi->dir = sdscatfmt(sdsempty(),"%s\/%i",workdir,port);

    if (mkdir(vi->dir,0755) < 0) {
        vire_instance_destroy(vi);
        return NULL;
    }

    vi->conf_file = vire_conf_create(vi->dir, port);
    if (vi->conf_file == NULL) {
        vire_instance_destroy(vi);
        return NULL;
    }

    vi->pid_file = sdscatfmt(sdsempty(),"%s\/vire.pid",vi->dir);
    vi->log_file = sdscatfmt(sdsempty(),"%s\/vire.log",vi->dir);

    test_log_debug("vire host: %s", vi->host);
    test_log_debug("vire port: %d", vi->port);
    test_log_debug("vire dir: %s", vi->dir);
    test_log_debug("vire conf_file: %s", vi->conf_file);
    test_log_debug("vire pid_file: %s", vi->pid_file);
    test_log_debug("vire log_file: %s", vi->log_file);

    return vi;
}

void vire_instance_destroy(vire_instance *vi)
{
    if (vi->running) {
        vire_server_stop(vi);
    }

    if (vi->dir) {
        destroy_dir(vi->dir);
        sdsfree(vi->dir);
    }

    if (vi->conf_file) {
        sdsfree(vi->conf_file);
    }

    if (vi->pid_file) {
        sdsfree(vi->pid_file);
    }

    if (vi->log_file) {
        sdsfree(vi->log_file);
    }

    if (vi->ctx) {
        redisFree(vi->ctx);        
    }

    if (vi->host) {
        sdsfree(vi->host);
    }

    free(vi);
}

int vire_server_run(vire_instance *vi)
{
    int ret;
    pid_t pid;
    int status;
    struct timeval timeout = { 3, 500000 }; // 3.5 seconds
    
    if ((pid = fork()) < 0) {
        test_log_error("Fork a chind failed: %s", strerror(errno));
        return VRT_ERROR;
    } else if (pid == 0) {
        ret = execl(execute_file,"vire","-c",vi->conf_file,
            "-p",vi->pid_file,"-o",vi->log_file,"-v","8",NULL);
        if (ret < 0) {
            test_log_error("Execl the vire server failed: %s", strerror(errno));
            return VRT_ERROR;
        }
        return;
    }    

    sleep(1);

    ret = waitpid(pid,NULL,WNOHANG);
    if (ret != 0) {
        test_log_debug("Run vire server(port %d) failed",vi->port);
        return VRT_ERROR;
    }

    vi->ctx = redisConnectWithTimeout(vi->host,vi->port,timeout);
    if (vi->ctx == NULL || vi->ctx->err) {
        test_log_error("Connect to %s:%d failed: %s", 
            vi->host, vi->port, vi->ctx?vi->ctx->errstr:"out of memory");
        if (vi->ctx) {
            redisFree(vi->ctx);
            vi->ctx = NULL;
        }
        return VRT_ERROR;
    }

    vi->pid = get_pid_from_reply(vi->ctx,vi->host,vi->port);
    if (vi->pid < 0) {
        test_log_error("Get pid from %s:%d reply error", vi->host, vi->port);
        return VRT_ERROR;
    } else if (vi->pid != pid) {
        test_log_error("Get wrong pid from %s:%d reply", vi->host, vi->port);
        return VRT_ERROR;
    }

    test_log_debug("Run vire server(port %d) success",vi->port);

    vi->running = 1;

    return VRT_OK;
}

void vire_server_stop(vire_instance *vi)
{
    long pid;

    if (!vi->running) return;

    if (vi->pid > 0) {
        pid = vi->pid;
    } else if (vi->pid_file) {
        int fd;
        char pid_str[20];
        size_t nread;
        fd = open(vi->pid_file, O_RDONLY);
        if (fd < 0) {
            test_log_error("Open pid file %s failed", vi->pid_file);
            return;
        }
        nread = read(fd,pid_str,20);
        if (string2l(pid_str,nread,&pid) == 0) {
            test_log_error("Convert pid string %.*s to long failed",nread,pid_str);
            return;
        }
    } else {
        pid = get_pid_from_reply(vi->ctx, vi->host, vi->port);
    }

    if (pid < 0) {
        test_log_error("Get pid failed");
        return;
    }

    kill(pid,9);

    vi->running = 0;
    vi->pid = -1;
    if (vi->ctx) {
        redisFree(vi->ctx);
        vi->ctx = NULL;
    }
}

int create_work_dir(void)
{
    sds dirname;
    dirname = sdscatfmt(sdsempty(), "tmp_test_%I", vrt_usec_now());
    workdir = getAbsolutePath(dirname);
    sdsfree(dirname);

    if (create_dir(workdir) != VRT_OK) {
        test_log_error("Create workdir %s failed",workdir);
        return VRT_ERROR;
    }

    test_log_debug("Create workdir: %s",workdir);
    
    return VRT_OK;
}

int destroy_work_dir(void)
{
    if (workdir == NULL) return VRT_OK;

    if (destroy_dir(workdir) != VRT_OK) {
        test_log_error("Delete the workdir %s failed",workdir);
    } else {
        test_log_debug("Delete the workdir: %s",workdir);
    }
    
    sdsfree(workdir);
    workdir = NULL;
    
    return VRT_OK;
}

static int get_next_port(void)
{
    int port = vireport;
    vireport += 11;

    return port;
}

vire_instance *start_one_vire_instance(void)
{
    int ret;
    int retry = 0;
    vire_instance *vi;
    
    vi = vire_instance_create(get_next_port());
    if (vi == NULL) {
        return NULL;
    }
    
    ret = vire_server_run(vi);
    while (ret != VRT_OK && retry++ < 10) {
        vire_instance_destroy(vi);
        vi = vire_instance_create(get_next_port());
        if (vi == NULL) {
            return NULL;
        }
        ret = vire_server_run(vi);
    }

    if (ret != VRT_OK) {
        vire_instance_destroy(vi);
        return NULL;
    }

    return vi;
}

void show_test_result(int result,char *test_content,char *errmsg)
{
    if (result == VRT_TEST_OK) {
        test_log_out("[\033[32mOK\033[0m]: %s", test_content);
    } else if (result == VRT_TEST_ERR) {
        test_log_out("[\033[31mERR\033[0m]: %s, \033[33mfail cause: %s\033[0m", test_content, 
            (errmsg==NULL||strlen(errmsg)==0)?"unknown":errmsg);
    }
}

/************** Key cache pool implement start *************/
key_cache_array *key_cache_array_create(long long max_pool_size)
{
    long long idx;
    key_cache_array *kca;

    /* It is too small */
    if (max_pool_size < 10) return NULL;

    kca = malloc(sizeof(*kca));
    if (kca == NULL) return NULL;

    kca->cached_keys_count = 0;
    kca->ckeys_write_idx = 0;
    kca->max_pool_size = max_pool_size;
    kca->ckeys = NULL;
    pthread_mutex_init(&kca->pmutex,NULL);

    kca->ckeys = malloc(max_pool_size*sizeof(sds));
    for (idx = 0; idx < max_pool_size; idx ++) {
        kca->ckeys[idx] = sdsempty();
    }

    return kca;
}

void key_cache_array_destroy(key_cache_array *kca)
{
    long long idx;
    
    if (kca == NULL) return;

    pthread_mutex_destroy(&kca->pmutex);
    
    if (kca->ckeys) {
        for (idx = 0; idx < kca->max_pool_size; idx ++) {
            sdsfree(kca->ckeys[idx]);
        }
        free(kca->ckeys);
    }

    free(kca);
}

int key_cache_array_input(key_cache_array *kca, char *key, size_t keylen)
{
    if (kca == NULL || key == NULL || keylen == 0) return VRT_ERROR;

    pthread_mutex_lock(&kca->pmutex);
    kca->ckeys[kca->ckeys_write_idx]=sdscpylen(kca->ckeys[kca->ckeys_write_idx],key,keylen);
    kca->ckeys_write_idx++;
    if (kca->ckeys_write_idx >= kca->max_pool_size) {
        kca->ckeys_write_idx = 0;
    }
    
    if (kca->cached_keys_count < kca->max_pool_size) {
        kca->cached_keys_count++;
    }
    pthread_mutex_unlock(&kca->pmutex);
    
    return VRT_OK;
}

sds key_cache_array_random(key_cache_array *kca)
{
    unsigned int idx, randomval;
    sds key;

    if (kca == NULL) {
        return NULL;
    }

    randomval = (unsigned int)rand();
    
    pthread_mutex_lock(&kca->pmutex);
    if (kca->cached_keys_count == 0) {
        pthread_mutex_unlock(&kca->pmutex);
        return NULL;
    }

    idx = randomval%(unsigned int)kca->cached_keys_count;

    key = sdsdup(kca->ckeys[idx]);
    pthread_mutex_unlock(&kca->pmutex);
    
    return key;
}

/************** Key cache pool implement end *************/

long long get_longlong_from_info_reply(redisReply *reply, char *name)
{
    sds *lines;
    size_t line_len, len;
    int count, j;
    long long value = -1;

    len = strlen(name);
    
    if (reply->type != REDIS_REPLY_STRING) {
        test_log_error("Reply for 'info' command from vire type %d is error",
            reply->type);
        return -1;
    }

    lines = sdssplitlen(reply->str,reply->len,"\r\n",2,&count);
    if (lines == NULL) {
        test_log_error("Reply for 'info server' command from vire is error");
        return -1;
    }

    for (j = 0; j < count; j ++) {
        line_len = sdslen(lines[j]);
        if (line_len > len+1 && !strncmp(name, lines[j], len)) {
            if (string2ll(lines[j]+len+1,line_len-len-1,&value) == 0) {
                test_log_error("Convert pid string %.*s to long failed",
                    line_len-len-1,lines[j]+len+1);
                sdsfreesplitres(lines,count);
                return -1;
            }
            break;
        }
    }

    sdsfreesplitres(lines,count);
    return value;
}

redisReply *steal_hiredis_redisreply(redisReply *r)
{
    redisReply *reply;

    reply = calloc(1,sizeof(*reply));
    if (reply == NULL) {
        return NULL;
    }

    reply->type = r->type;
    reply->integer = r->integer;
    reply->len = r->len;
    reply->str = r->str;
    reply->elements = r->elements;
    reply->element = r->element;

    r->len = 0;
    r->str = NULL;
    r->elements = 0;
    r->element = NULL;

    return reply;
}

int check_two_replys_if_same(redisReply *reply1, redisReply *reply2)
{
    if (reply1 == NULL || reply2 == NULL) {
        return 1;
    }
    
    if (reply1->type != reply2->type) {
        return 1;
    }

    if (reply1->type == REDIS_REPLY_STRING || 
        reply1->type == REDIS_REPLY_STATUS ||
        reply1->type == REDIS_REPLY_ERROR) {
        if (reply1->len != reply2->len) {
            return reply1->len-reply2->len;
        }
        
        return memcmp(reply1->str, reply2->str, reply1->len);
    } else if (reply1->type == REDIS_REPLY_ARRAY) {
        size_t j;
        if (reply1->elements != reply2->elements) {
            return (reply1->elements-reply2->elements);
        }

        for (j = 0; j < reply1->elements; j ++) {
            int ret = check_two_replys_if_same(reply1->element[j], reply2->element[j]);
            if (ret != 0) return ret;
        }
        return 0;
    } else if (reply1->type == REDIS_REPLY_INTEGER) {
        return (reply1->integer-reply2->integer);
    } else if (reply1->type == REDIS_REPLY_NIL) {
        return 0;
    } else {
        test_log_error("reply type %d is error", reply1->type);
    }

    return 0;
}

struct sort_unit {
    size_t nfield;
    void **fields;
    unsigned int idx_cmp;
    int (*fcmp)(const void *,const void *);
};

static int element_cmp_multi_step(const void *ele1,const void *ele2)
{
    struct sort_unit *su1 = (struct sort_unit *)ele1, *su2 = (struct sort_unit *)ele2;

    ASSERT(su1->fcmp == su2->fcmp);
    ASSERT(su1->nfield == su2->nfield);
    ASSERT(su1->idx_cmp == su2->idx_cmp);
    ASSERT(su1->idx_cmp < su1->nfield);

    return su1->fcmp(&(su1->fields[su1->idx_cmp]),&(su2->fields[su2->idx_cmp]));
}

/* The element in the array must a pointer. */
int sort_array_by_step(void **element, size_t elements, 
    int step, int idx_cmp, int (*fcmp)(const void *,const void *))
{
    struct sort_unit *sus;
    size_t count, j, k;

    if (elements <= 1)
        return VRT_OK;

    if (step <= 0)
        return VRT_ERROR;
    
    if (step == 1) {
        qsort(element, elements, sizeof(void *), fcmp);
        return VRT_OK;
    }

    if (elements%step != 0)
        return VRT_ERROR;

    count = elements/step;
    if (count == 0)
        return VRT_ERROR;
    sus = calloc(count,sizeof(struct sort_unit));
    for (j = 0; j < count; j ++) {
        sus[j].nfield = step;
        sus[j].idx_cmp = idx_cmp;
        sus[j].fcmp = fcmp;
        sus[j].fields = malloc(step*sizeof(void*));
        for (k = 0; k < step; k ++) {
            sus[j].fields[k] = element[j*step+k];
        }
    }

    qsort(sus, count, sizeof(struct sort_unit), element_cmp_multi_step);

    for (j = 0; j < count; j ++) {
        for (k = 0; k < step; k ++) {
            element[j*step+k] = sus[j].fields[k];
        }
        free(sus[j].fields);
    }
    free(sus);
    return VRT_OK;
}

/* The reply type must be string */
int reply_string_binary_compare(const void *r1,const void *r2)
{
    redisReply *reply1 = *(redisReply **)r1, *reply2 = *(redisReply **)r2;
    int minlen;
    int cmp;

    minlen = (reply1->len < reply2->len) ? reply1->len:reply2->len;
    cmp = memcmp(reply1->str,reply2->str,minlen);
    if (cmp == 0) return reply1->len - reply2->len;
    return cmp;
}

/* command types string is like 'string,list,set,zset,hash,server,key,expire' */
int parse_command_types(char *command_types_str)
{
    int types = 0;
    sds *types_strs;
    int types_count, j;

    types_strs = sdssplitlen(command_types_str,strlen(command_types_str),",",1,&types_count);
    if (types_strs == NULL) {
        return -1;
    } else if (types_count <= 0) {
        sdsfreesplitres(types_strs,types_count);
        return -1;
    }
    
    for (j = 0; j < types_count; j ++) {
        if (!strcasecmp(types_strs[j],"string")) {
            types |= TEST_CMD_TYPE_STRING;
        } else if (!strcasecmp(types_strs[j],"list")) {
            types |= TEST_CMD_TYPE_LIST;
        } else if (!strcasecmp(types_strs[j],"set")) {
            types |= TEST_CMD_TYPE_SET;
        } else if (!strcasecmp(types_strs[j],"zset")) {
            types |= TEST_CMD_TYPE_ZSET;
        } else if (!strcasecmp(types_strs[j],"hash")) {
            types |= TEST_CMD_TYPE_HASH;
        } else if (!strcasecmp(types_strs[j],"server")) {
            types |= TEST_CMD_TYPE_SERVER;
        } else if (!strcasecmp(types_strs[j],"key")) {
            types |= TEST_CMD_TYPE_KEY;
        } else if (!strcasecmp(types_strs[j],"expire")) {
            types |= TEST_CMD_TYPE_EXPIRE;
        } else {
            sdsfreesplitres(types_strs,types_count);
            return -1;
        } 
    }

    sdsfreesplitres(types_strs,types_count);

    return types;
}

/* command list string is like 'get,set,lrange,zrange' */
darray *parse_command_list(char *command_list_str)
{
    darray *commands;
    sds *command_elem;
    sds *command_strs;
    int command_count, j;

    command_strs = sdssplitlen(command_list_str,strlen(command_list_str),",",1,&command_count);
    if (command_strs == NULL) {
        return -1;
    } else if (command_count <= 0) {
        sdsfreesplitres(command_strs,command_count);
        return -1;
    }

    commands = darray_create(command_count, sizeof(sds));
    for (j = 0; j < command_count; j ++) {
        command_elem = darray_push(commands);
        *command_elem = command_strs[j];
        command_strs[j] = NULL;
    }

    sdsfreesplitres(command_strs,command_count);

    return commands;
}

char *
get_key_type_string(int keytype)
{
    switch (keytype) {
    case REDIS_STRING:
        return "string";
        break;
    case REDIS_LIST:
        return "list";
        break;
    case REDIS_SET:
        return "set";
        break;
    case REDIS_ZSET:
        return "zset";
        break;
    case REDIS_HASH:
        return "hash";
        break;
    default:
        return "unknow";
        break;
    }

    return "unknow";
}

