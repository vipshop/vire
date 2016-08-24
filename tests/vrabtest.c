#include <stdio.h>
#include <stdlib.h>
#include <getopt.h>
#include <fcntl.h>
#include <errno.h>
#include <signal.h>
#include <assert.h>
#include <sys/stat.h>
#include <sys/utsname.h>

#include <hiredis.h>
#include <darray.h>
#include <dlog.h>

#include <vrt_util.h>
#include <vrt_public.h>
#include <vrt_produce_data.h>
#include <vrt_dispatch_data.h>
#include <vrt_check_data.h>
#include <vrt_backend.h>
#include <vrabtest.h>

#define CONFIG_DEFAULT_PIDFILE                      NULL
#define CONFIG_DEFAULT_CHECKER                      "myself"
#define CONFIG_DEFAULT_TEST_INTERVAL                3600
#define CONFIG_DEFAULT_KEY_LENGTH_RANGE_BEGIN       0
#define CONFIG_DEFAULT_KEY_LENGTH_RANGE_END         100
#define CONFIG_DEFAULT_STRING_MAX_LENGTH            512
#define CONFIG_DEFAULT_FIELDS_MAX_COUNT             16
#define CONFIG_DEFAULT_TEST_TARGET                  ""
#define CONFIG_DEFAULT_PRODUCE_THREADS_COUNT        1
#define CONFIG_DEFAULT_CACHED_KEYS_COUNT            10000
#define CONFIG_DEFAULT_HIT_RATIO                    75
#define CONFIG_DEFAULT_DISPATCH_THREADS_COUNT       1
#define CONFIG_DEFAULT_CLIENTS_PER_DISPATCH_THREAD  10
#define CONFIG_DEFAULT_LOGFILE                      NULL

#define VRABTEST_GROUP_TYPE_REDIS   0
#define VRABTEST_GROUP_TYPE_VIRE    1

struct config {
    char *checker;
    long long test_interval;
    int key_length_range_begin;
    int key_length_range_end;
    int string_max_length;
    int fields_max_count;
    int cmd_type;
    darray *cmd_blacklist;
    darray *cmd_whitelist;
    char *test_targets; 
    int produce_data_threads;
    long long cached_keys_per_produce_thread;
    int hit_ratio;
    int dispatch_data_threads;
    int clients_per_dispatch_thread;
    char *pid_filename;
    char *log_filename;
};

static struct config config;

static int show_help;
static int show_version;
static int daemonize;

/* 0 or 1
 * 1: used the expire,expireat,pexpire and pexpireat commands
 * 0 is the opposite. */
int expire_enabled;

/* Interval time for test data dispatched to test targets. 
 * Unit is second */
long long test_interval;

/* Last begin time to test the data.
 * Unit is second */
long long last_test_begin_time;

static struct option long_options[] = {
    { "help",                   no_argument,        NULL,   'h' },
    { "version",                no_argument,        NULL,   'V' },
    { "daemonize",              no_argument,        NULL,   'D' },
    { "enable-expire",          no_argument,        NULL,   'E' },
    { "pid-file",               required_argument,  NULL,   'P' },
    { "checker",                required_argument,  NULL,   'C' },
    { "test-interval",          required_argument,  NULL,   'i' },
    { "key-length-range",       required_argument,  NULL,   'k' },
    { "string-max-length",      required_argument,  NULL,   's' },
    { "fields-max-count",       required_argument,  NULL,   'f' },
    { "command-types",          required_argument,  NULL,   'T' },
    { "command-black-list",     required_argument,  NULL,   'B' },
    { "command-white-list",     required_argument,  NULL,   'W' },
    { "test-targets",           required_argument,  NULL,   't' },
    { "produce-data-threads",   required_argument,  NULL,   'p' },
    { "cached-keys",            required_argument,  NULL,   'K' },
    { "hit-ratio",              required_argument,  NULL,   'H' },
    { "dispatch-data-threads",  required_argument,  NULL,   'd' },
    { "clients",                required_argument,  NULL,   'c' },
    { "log-file",               required_argument,  NULL,   'o' },
    { NULL,                     0,                  NULL,    0  }
};

static char short_options[] = "hVDEP:C:i:k:s:f:T:B:W:t:p:K:H:d:c:o:";

static void
vrt_show_usage(void)
{
    printf(
        "Usage: vireabtest [-?hVDE]" CRLF
        "" CRLF);
    printf(
        "Options:" CRLF
        "  -h, --help                   : this help" CRLF
        "  -V, --version                : show version and exit" CRLF
        "  -D, --daemonize              : run as a daemon" CRLF
        "  -E, --enable-expire          : enable the expire" CRLF);
    printf(
        "  -P, --pid-file               : pid file" CRLF
        "  -C, --checker                : the checker to check data consistency" CRLF
        "  -i, --test-interval          : the interval for checking data consistency, unit is second" CRLF
        "  -k, --key-length-range       : the key length range to generate for test, like 0-100" CRLF
        "  -s, --string-max-length      : the max string length to generate for test, string is for STRING/LIST... value element" CRLF
        "  -f, --fields-max-count       : the max fields count to generate for test, field is the LIST/HASH...'s element" CRLF
        "  -T, --command-types          : the command types to generate for test, like string,hash,key" CRLF
        "  -B, --command-black-list     : the commands not want to test, like del,lrange,mget" CRLF
        "  -W, --command-white-list     : the commands only allows to test, like del,lrange,mget" CRLF
        "  -t, --test-targets           : the test targets for test, like vire[127.0.0.1:12301]-redis[127.0.0.1:12311]" CRLF
        "  -p, --produce-data-threads   : the threads count to produce test data" CRLF
        "  -K, --cached-keys            : the cached keys count for every produce data thread" CRLF
        "  -H, --hit-ratio              : the hit ratio for readonly commands, between 0 and 100" CRLF
        "  -d, --dispatch-data-threads  : the threads count to dispatch test data to target groups" CRLF
        "  -c, --clients                : the clients count for every dispatch data thread" CRLF
        "  -o, --log-file               : set logging file (default: %s)" CRLF
        "", 
        CONFIG_DEFAULT_LOGFILE != NULL ? CONFIG_DEFAULT_LOGFILE : "stderr");
}

static void
vrt_set_default_options(void)
{
    config.pid_filename = CONFIG_DEFAULT_PIDFILE;
    config.checker = CONFIG_DEFAULT_CHECKER;
    config.test_interval = CONFIG_DEFAULT_TEST_INTERVAL;
    config.key_length_range_begin = CONFIG_DEFAULT_KEY_LENGTH_RANGE_BEGIN;
    config.key_length_range_end = CONFIG_DEFAULT_KEY_LENGTH_RANGE_END;
    config.string_max_length = CONFIG_DEFAULT_STRING_MAX_LENGTH;
    config.fields_max_count = CONFIG_DEFAULT_FIELDS_MAX_COUNT;
    config.cmd_type = TEST_CMD_TYPE_STRING|TEST_CMD_TYPE_LIST|
        TEST_CMD_TYPE_SET|TEST_CMD_TYPE_ZSET|TEST_CMD_TYPE_HASH|
        TEST_CMD_TYPE_SERVER|TEST_CMD_TYPE_KEY;
    config.cmd_blacklist = NULL;
    config.cmd_whitelist = NULL;
    config.test_targets = CONFIG_DEFAULT_TEST_TARGET; 
    config.produce_data_threads = CONFIG_DEFAULT_PRODUCE_THREADS_COUNT;
    config.cached_keys_per_produce_thread = CONFIG_DEFAULT_CACHED_KEYS_COUNT;
    config.hit_ratio = CONFIG_DEFAULT_HIT_RATIO;
    config.dispatch_data_threads = CONFIG_DEFAULT_DISPATCH_THREADS_COUNT;
    config.clients_per_dispatch_thread = CONFIG_DEFAULT_CLIENTS_PER_DISPATCH_THREAD;
    config.log_filename = CONFIG_DEFAULT_LOGFILE;
    
    expire_enabled = 0;
}

static void
vrt_clean_options(void)
{
    if (config.cmd_blacklist != NULL) {
        sds *command;
        while (darray_n(config.cmd_blacklist) > 0) {
            command = darray_pop(config.cmd_blacklist);
            sdsfree(command);
        }
        darray_destroy(config.cmd_blacklist);
        config.cmd_blacklist = NULL;
    }

    if (config.cmd_whitelist != NULL) {
        sds *command;
        while (darray_n(config.cmd_whitelist) > 0) {
            command = darray_pop(config.cmd_whitelist);
            sdsfree(command);
        }
        darray_destroy(config.cmd_whitelist);
        config.cmd_whitelist = NULL;
    }
}

static int
vrt_get_options(int argc, char **argv)
{
    int c;
    long lvalue;
    long long llvalue;
    long long *range;
    int range_count;

    opterr = 0;

    for (;;) {
        c = getopt_long(argc, argv, short_options, long_options, NULL);
        if (c == -1) {
            /* no more options */
            break;
        }

        switch (c) {
        case 'h':
            show_version = 1;
            show_help = 1;
            break;

        case 'V':
            show_version = 1;
            break;
            
        case 'D':
            daemonize = 1;
            break;

        case 'E':
            expire_enabled = 1;
            break;
            
        case 'C':
            config.checker = optarg;
            break;

        case 'i':
            if (string2ll(optarg,strlen(optarg),&llvalue) != 1) {
                log_stderr("vireabtest: option -i requires a number");
                return VRT_ERROR;
            }
            config.test_interval = llvalue;
            break;
            
        case 'k':
            range = get_range_from_string(optarg,strlen(optarg),&range_count);
            if (range == NULL) {
                log_stderr("vireabtest: option -k is invalid, you need input a range like 0-100");
                return VRT_ERROR;
            }
            config.key_length_range_begin = (int)range[0];
            if (range_count == 1) config.key_length_range_end = (int)range[0];
            else if (range_count == 2) config.key_length_range_end = (int)range[1];
            else assert(0);

            free(range);
            
            break;

        case 's':
            if (string2l(optarg,strlen(optarg),&lvalue) != 1) {
                log_stderr("vireabtest: option -s requires a number");
                return VRT_ERROR;
            }
            config.string_max_length = (int)lvalue;
            break;

        case 'f':
            if (string2l(optarg,strlen(optarg),&lvalue) != 1) {
                log_stderr("vireabtest: option -f requires a number");
                return VRT_ERROR;
            }
            config.fields_max_count = (int)lvalue;
            break;
            
        case 'T':
            config.cmd_type = parse_command_types(optarg);
            if (config.cmd_type <= 0) {
                log_stderr("vireabtest: option -T requires the correct command types");
                return VRT_ERROR;
            }
            break;

        case 'B':
            config.cmd_blacklist = parse_command_list(optarg);
            if (config.cmd_blacklist == NULL) {
                log_stderr("vireabtest: option -B requires the correct command list");
                return VRT_ERROR;
            }
            break;

        case 'W':
            config.cmd_whitelist = parse_command_list(optarg);
            if (config.cmd_whitelist == NULL) {
                log_stderr("vireabtest: option -W requires the correct command list");
                return VRT_ERROR;
            }
            break;
            
        case 't':
            config.test_targets = optarg;
            break;

        case 'p':
            if (string2l(optarg,strlen(optarg),&lvalue) != 1) {
                log_stderr("vireabtest: option -p requires a number");
                return VRT_ERROR;
            }
            config.produce_data_threads = (int)lvalue;
            break;

        case 'K':
            if (string2ll(optarg,strlen(optarg),&llvalue) != 1) {
                log_stderr("vireabtest: option -K requires a number");
                return VRT_ERROR;
            }
            if (llvalue < 1000) {
                log_stderr("vireabtest: option -K requires a number that must bigger than 1000");
                return VRT_ERROR;
            }
            
            config.cached_keys_per_produce_thread = llvalue;
            break;

        case 'H':
            if (string2l(optarg,strlen(optarg),&lvalue) != 1) {
                log_stderr("vireabtest: option -H requires a number");
                return VRT_ERROR;
            }
            if (lvalue < 0 || lvalue > 100) {
                log_stderr("vireabtest: option hit-ratio need between 0 and 100");
                return VRT_ERROR;
            }
            config.hit_ratio = (int)lvalue;
            break;

        case 'd':
            if (string2l(optarg,strlen(optarg),&lvalue) != 1) {
                log_stderr("vireabtest: option -d requires a number");
                return VRT_ERROR;
            }
            config.dispatch_data_threads = (int)lvalue;
            break;

        case 'c':
            if (string2l(optarg,strlen(optarg),&lvalue) != 1) {
                log_stderr("vireabtest: option -c requires a number");
                return VRT_ERROR;
            }
            config.clients_per_dispatch_thread = (int)lvalue;
            break;
            
        case 'P':
            config.pid_filename = optarg;
            break;

        case 'o':
            config.log_filename = optarg;
            break;
            
        case '?':
            switch (optopt) {
            case 'C':
            case 'k':
            case 'T':
            case 'B':
            case 'W':
            case 't':
            case 'P':
            case 'o':
                log_stderr("vire: option -%c requires string",
                           optopt);
                break;

            case 'i':
            case 'p':
            case 'd':
            case 'c':
            case 's':
                log_stderr("vire: option -%c requires number",
                           optopt);
                break;
                
            default:
                log_stderr("vire: invalid option -- '%c'", optopt);
                break;
            }
            return VRT_ERROR;

        default:
            log_stderr("vire: invalid option -- '%c'", optopt);
            return VRT_ERROR;

        }
    }

    return VRT_OK;
}

static int vrt_daemonize(int dump_core)
{
    int ret;
    pid_t pid, sid;
    int fd;

    pid = fork();
    switch (pid) {
    case -1:
        log_error("fork() failed: %s", strerror(errno));
        return VRT_ERROR;

    case 0:
        break;

    default:
        /* parent terminates */
        _exit(0);
    }

    /* 1st child continues and becomes the session leader */

    sid = setsid();
    if (sid < 0) {
        log_error("setsid() failed: %s", strerror(errno));
        return VRT_ERROR;
    }

    if (signal(SIGHUP, SIG_IGN) == SIG_ERR) {
        log_error("signal(SIGHUP, SIG_IGN) failed: %s", strerror(errno));
        return VRT_ERROR;
    }

    pid = fork();
    switch (pid) {
    case -1:
        log_error("fork() failed: %s", strerror(errno));
        return VRT_ERROR;

    case 0:
        break;

    default:
        /* 1st child terminates */
        _exit(0);
    }

    /* 2nd child continues */

    /* change working directory */
    if (dump_core == 0) {
        ret = chdir("/");
        if (ret < 0) {
            log_error("chdir(\"/\") failed: %s", strerror(errno));
            return VRT_ERROR;
        }
    }

    /* clear file mode creation mask */
    umask(0);

    /* redirect stdin, stdout and stderr to "/dev/null" */

    fd = open("/dev/null", O_RDWR);
    if (fd < 0) {
        log_error("open(\"/dev/null\") failed: %s", strerror(errno));
        return VRT_ERROR;
    }

    ret = dup2(fd, STDIN_FILENO);
    if (ret < 0) {
        log_error("dup2(%d, STDIN) failed: %s", fd, strerror(errno));
        close(fd);
        return VRT_ERROR;
    }

    ret = dup2(fd, STDOUT_FILENO);
    if (ret < 0) {
        log_error("dup2(%d, STDOUT) failed: %s", fd, strerror(errno));
        close(fd);
        return VRT_ERROR;
    }

    ret = dup2(fd, STDERR_FILENO);
    if (ret < 0) {
        log_error("dup2(%d, STDERR) failed: %s", fd, strerror(errno));
        close(fd);
        return VRT_ERROR;
    }

    if (fd > STDERR_FILENO) {
        ret = close(fd);
        if (ret < 0) {
            log_error("close(%d) failed: %s", fd, strerror(errno));
            return VRT_ERROR;
        }
    }

    return VRT_OK;
}

static int abtest_server_init(abtest_server *abs, char *address)
{
    sds *host_port;
    int count;
    long value;

    abs->host = NULL;
    abs->port = 0;
    abs->conn_contexts = NULL;
    abs->data = NULL;

    host_port = sdssplitlen(address,strlen(address),":",1,&count);
    if (host_port == NULL) {
        return VRT_ERROR;
    } else if (count != 2) {
        sdsfreesplitres(host_port,count);
        return VRT_ERROR;
    }

    abs->host = host_port[0];
    host_port[0] = NULL;

    if (string2l(host_port[1],sdslen(host_port[1]),&value) != 1) {
        sdsfreesplitres(host_port,count);
        return VRT_ERROR;
    }

    abs->port = (int)value;
    sdsfreesplitres(host_port,count);
    
    return VRT_OK;
}

static void abtest_server_deinit(abtest_server *abs)
{
    if (abs->host) {
        sdsfree(abs->host);
        abs->host = NULL;
    }

    if (abs->port > 0) abs->port = 0;

    if (abs->conn_contexts) {
        ASSERT(darray_n(abs->conn_contexts) == 0);
        darray_destroy(abs->conn_contexts);
        abs->conn_contexts = NULL;
    }
}

unsigned int get_backend_server_idx(abtest_group *abg, char *key, size_t keylen)
{
    unsigned int hashvalue, servers_count;

    servers_count = darray_n(&abg->abtest_servers);
    if (servers_count == 1) {
        return 0;
    }

    hashvalue = (unsigned int)hash_crc32a(key, keylen);
    
    return hashvalue%servers_count;
}

abtest_server *get_backend_server(abtest_group *abg, char *key, size_t keylen)
{
    abtest_server *abs;
    unsigned int idx;

    idx = abg->get_backend_server_idx(abg,key,keylen);
    abs = darray_get(&abg->abtest_servers, idx);

    return abs;
}

static int abtest_group_init(abtest_group *abg, char *group_string)
{
    sds *type_addrs, *addrs;
    int type_addrs_count, addrs_count;
    int j;

    abg->type = 0;
    darray_init(&abg->abtest_servers, 1, sizeof(abtest_server));

    type_addrs = sdssplitlen(group_string,sdslen(group_string),"[",1,&type_addrs_count);
    if (type_addrs == NULL) {
        return VRT_ERROR;
    } else if (type_addrs_count != 2) {
        sdsfreesplitres(type_addrs,type_addrs_count);
        return VRT_ERROR;
    }

    if (!strcasecmp(type_addrs[0],"vire")) {
        abg->type = VRABTEST_GROUP_TYPE_VIRE;
    } else if (!strcasecmp(type_addrs[0],"redis")) {
        abg->type = VRABTEST_GROUP_TYPE_REDIS;
    } else {
        sdsfreesplitres(type_addrs,type_addrs_count);
        return VRT_ERROR;
    }

    if (sdslen(type_addrs[1]) <= 1 || 
        type_addrs[1][sdslen(type_addrs[1])-1] != ']') {
        sdsfreesplitres(type_addrs,type_addrs_count);
        return VRT_ERROR;
    }

    sdsrange(type_addrs[1],0,-2);

    addrs = sdssplitlen(type_addrs[1],sdslen(type_addrs[1]),",",1,&addrs_count);
    if (addrs == NULL) {
        sdsfreesplitres(type_addrs,type_addrs_count);
        return VRT_ERROR;
    } else if (addrs_count < 1) {
        sdsfreesplitres(addrs,addrs_count);
        sdsfreesplitres(type_addrs,type_addrs_count);
        return VRT_ERROR;
    }

    for (j = 0; j < addrs_count; j ++) {
        abtest_server *abs = darray_push(&abg->abtest_servers);
        if (abtest_server_init(abs,addrs[j]) != VRT_OK) {
            sdsfreesplitres(addrs,addrs_count);
            sdsfreesplitres(type_addrs,type_addrs_count);
            return VRT_ERROR;
        }
    }

    sdsfreesplitres(addrs,addrs_count);
    sdsfreesplitres(type_addrs,type_addrs_count);

    abg->get_backend_server_idx = get_backend_server_idx;
    abg->get_backend_server = get_backend_server;

    return VRT_OK;
}

static void abtest_group_deinit(abtest_group *abg)
{
    abtest_server *abs;
    
    abg->type = 0;
    
    while (darray_n(&abg->abtest_servers) > 0) {
        abs = darray_pop(&abg->abtest_servers);
        abtest_server_deinit(abs);
    }
    darray_deinit(&abg->abtest_servers);
}

/* groups_string is like "vire[127.0.0.1:12301,127.0.0.1:12302]-redis[127.0.0.1:12311,127.0.0.1:12312]" */
darray *abtest_groups_create(char *groups_string)
{
    darray *abgs;
    sds *group_strings;
    int group_count, j;

    group_strings = sdssplitlen(groups_string,strlen(groups_string),"-",1,&group_count);
    if (group_strings == NULL) {
        return NULL;
    } else if (group_count < 1) {
        sdsfreesplitres(group_strings,group_count);
        return NULL;
    }

    abgs = darray_create(2, sizeof(abtest_group));
    if (abgs == NULL) {
        sdsfreesplitres(group_strings,group_count);
        return NULL;
    }
    
    for (j = 0; j < group_count; j ++) {
        abtest_group *abg;
        sds group_string = group_strings[j];
        sds *type_addrs;
        int elem_count;

        abg = darray_push(abgs);
        if (abtest_group_init(abg,group_string) != VRT_OK) {
            sdsfreesplitres(group_strings,group_count);
            abtest_groups_destroy(abgs);
            return NULL;
        }
    }

    return abgs;
}

void abtest_groups_destroy(darray *abgs)
{
    while (darray_n(abgs) > 0) {
        abtest_group *abg = darray_pop(abgs);
        abtest_group_deinit(abg);
    }
    
    darray_destroy(abgs);
}

int
main(int argc, char **argv)
{
    int ret;

    vrt_set_default_options();

    ret = vrt_get_options(argc, argv);
    if (ret != VRT_OK) {
        vrt_show_usage();
        exit(1);
    }

    if (show_version) {
        log_stdout("This is vireabtest-%s", VR_VERSION_STRING);
        if (show_help) {
            vrt_show_usage();
        }
        exit(0);
    }

    ret = log_init(LOG_INFO, config.log_filename);
    if (ret < 0) {
        exit(1);
    }

    if (daemonize) {
        ret = vrt_daemonize(1);
        if (ret != VRT_OK) {
            exit(1);
        }
    }

    test_interval = config.test_interval;
    
    ret = vrt_produce_data_init(config.key_length_range_begin,
        config.key_length_range_end,
        config.string_max_length,config.fields_max_count,
        config.cmd_type,config.cmd_blacklist,config.cmd_whitelist,
        config.produce_data_threads,
        config.cached_keys_per_produce_thread, 
        config.hit_ratio);
    if (ret != VRT_OK) {
        log_error("Init data producer failed");
        exit(1);
    }
    ret = vrt_dispatch_data_init(config.dispatch_data_threads, 
        config.test_targets, config.clients_per_dispatch_thread);
    if (ret != VRT_OK) {
        log_error("Init data dispatcher failed");
        exit(1);
    }
    ret = vrt_backend_init(config.dispatch_data_threads, 
        config.test_targets);
    if (ret != VRT_OK) {
        log_error("Init backend thread failed");
        exit(1);
    }
    ret = vrt_data_checker_init(config.checker, config.test_targets);
    if (ret != VRT_OK) {
        log_error("Init check data thread failed");
        exit(1);
    }

    log_debug(LOG_INFO,"State lock type: %s", TEST_STATE_LOCK_TYPE);

    vrt_start_produce_data();
    vrt_start_dispatch_data();
    vrt_start_backend();
    vrt_start_data_checker();

    vrt_wait_produce_data();
    vrt_wait_dispatch_data();
    vrt_wait_backend();
    vrt_wait_data_checker();

    vrt_data_checker_deinit();
    vrt_backend_deinit();
    vrt_dispatch_data_deinit();
    vrt_produce_data_deinit();

    log_deinit();
    vrt_clean_options();
    
    return VRT_OK;
}
