#include <stdio.h>
#include <stdlib.h>
#include <getopt.h>
#include <fcntl.h>
#include <errno.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/utsname.h>

#include <hiredis.h>
#include <darray.h>

#include <vrt_util.h>
#include <vrt_public.h>
#include <vrt_produce_data.h>
#include <vrt_dispatch_data.h>
#include <vrt_check_data.h>
#include <vrabtest.h>

#define CONFIG_DEFAULT_PIDFILE                      NULL
#define CONFIG_DEFAULT_CHECKER                      "myself"
#define CONFIG_DEFAULT_CHECK_INTERVAL               3600
#define CONFIG_DEFAULT_KEY_LENGTH_RANGE_BEGIN       0
#define CONFIG_DEFAULT_KEY_LENGTH_RANGE_END         100
#define CONFIG_DEFAULT_TEST_TARGET                  ""
#define CONFIG_DEFAULT_PRODUCE_THREADS_COUNT        1
#define CONFIG_DEFAULT_CACHED_KEYS_COUNT            10000
#define CONFIG_DEFAULT_HIT_RATIO                    75
#define CONFIG_DEFAULT_DISPATCH_THREADS_COUNT       1
#define CONFIG_DEFAULT_CLIENTS_PER_DISPATCH_THREAD  10

#define VRABTEST_GROUP_TYPE_REDIS   0
#define VRABTEST_GROUP_TYPE_VIRE    1

struct config {
    char *checker;
    long long check_interval;
    int key_length_range_begin;
    int key_length_range_end;
    int cmd_type;
    char *test_targets; 
    int produce_data_threads;
    long long cached_keys_per_produce_thread;
    int hit_ratio;
    int dispatch_data_threads;
    int clients_per_dispatch_thread;
    char *pid_filename;
};

static struct config config;

static int show_help;
static int show_version;
static int daemonize;

static struct option long_options[] = {
    { "help",                   no_argument,        NULL,   'h' },
    { "version",                no_argument,        NULL,   'V' },
    { "daemonize",              no_argument,        NULL,   'D' },
    { "checker",                required_argument,  NULL,   'C' },
    { "check-interval",         required_argument,  NULL,   'i' },
    { "key-length-range",       required_argument,  NULL,   'k' },
    { "command-types",          required_argument,  NULL,   'T' },
    { "test-targets",           required_argument,  NULL,   't' },
    { "produce-data-threads",   required_argument,  NULL,   'p' },
    { "cached-keys",            required_argument,  NULL,   'K' },
    { "hit-ratio",              required_argument,  NULL,   'H' },
    { "dispatch-data-threads",  required_argument,  NULL,   'd' },
    { "clients",                required_argument,  NULL,   'c' },
    { "pid-file",               required_argument,  NULL,   'P' },
    { NULL,                     0,                  NULL,    0  }
};

static char short_options[] = "hVDP:C:i:k:T:t:p:K:H:d:c:";

static void
vrt_show_usage(void)
{
    printf(
        "Usage: vireabtest [-?hVD]" CRLF
        "" CRLF);
    printf(
        "Options:" CRLF
        "  -h, --help                   : this help" CRLF
        "  -V, --version                : show version and exit" CRLF
        "  -D, --daemonize              : run as a daemon" CRLF);
    printf(
        "  -P, --pid-file               : pid file" CRLF
        "  -C, --checker                : the checker to check data consistency" CRLF
        "  -i, --check-interval         : the interval for checking data consistency" CRLF
        "  -k, --key-length-range       : the key length to generate for test" CRLF
        "  -T, --command-types          : the command types to generate for test" CRLF
        "  -t, --test-targets           : the test targets for test, like vire[127.0.0.1:12301]-redis[127.0.0.1:12311]" CRLF
        "  -p, --produce-data-threads   : the threads count to produce test data" CRLF
        "  -K, --cached-keys            : the cached keys count for every produce data thread" CRLF
        "  -H, --hit-ratio              : the hit ratio for readonly commands, between 0 and 100" CRLF
        "  -d, --dispatch-data-threads  : the threads count to dispatch test data to target groups" CRLF
        "  -c, --clients                : the clients count for every dispatch data thread" CRLF
        "" CRLF);
}

static void
vrt_set_default_options(void)
{
    config.pid_filename = CONFIG_DEFAULT_PIDFILE;
    config.checker = CONFIG_DEFAULT_CHECKER;
    config.check_interval = CONFIG_DEFAULT_CHECK_INTERVAL;
    config.key_length_range_begin = CONFIG_DEFAULT_KEY_LENGTH_RANGE_BEGIN;
    config.key_length_range_end = CONFIG_DEFAULT_KEY_LENGTH_RANGE_END;
    config.cmd_type = TEST_CMD_TYPE_STRING|TEST_CMD_TYPE_LIST|
        TEST_CMD_TYPE_SET|TEST_CMD_TYPE_ZSET|TEST_CMD_TYPE_HASH|
        TEST_CMD_TYPE_SERVER|TEST_CMD_TYPE_KEY;
    config.test_targets = CONFIG_DEFAULT_TEST_TARGET; 
    config.produce_data_threads = CONFIG_DEFAULT_PRODUCE_THREADS_COUNT;
    config.cached_keys_per_produce_thread = CONFIG_DEFAULT_CACHED_KEYS_COUNT;
    config.hit_ratio = CONFIG_DEFAULT_HIT_RATIO;
    config.dispatch_data_threads = CONFIG_DEFAULT_DISPATCH_THREADS_COUNT;
    config.clients_per_dispatch_thread = CONFIG_DEFAULT_CLIENTS_PER_DISPATCH_THREAD;
}

static int
vrt_get_options(int argc, char **argv)
{
    int c;
    long lvalue;
    long long llvalue;

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

        case 'C':
            config.checker = optarg;
            break;

        case 'i':
            if (string2ll(optarg,strlen(optarg),&llvalue) != 1) {
                test_log_error("vireabtest: option -i requires a number");
                return VRT_ERROR;
            }
            config.check_interval = llvalue;
            break;
            
        case 'k':
            config.pid_filename = optarg;
            break;

        case 'T':
            //config.cmd_type = optarg;
            break;

        case 't':
            config.test_targets = optarg;
            break;

        case 'p':
            if (string2l(optarg,strlen(optarg),&lvalue) != 1) {
                test_log_error("vireabtest: option -p requires a number");
                return VRT_ERROR;
            }
            config.produce_data_threads = (int)lvalue;
            break;

        case 'K':
            if (string2ll(optarg,strlen(optarg),&llvalue) != 1) {
                test_log_error("vireabtest: option -K requires a number");
                return VRT_ERROR;
            }
            config.cached_keys_per_produce_thread = llvalue;
            break;

        case 'H':
            if (string2l(optarg,strlen(optarg),&lvalue) != 1) {
                test_log_error("vireabtest: option -H requires a number");
                return VRT_ERROR;
            }
            if (lvalue < 0 || lvalue > 100) {
                test_log_error("vireabtest: option hit-ratio need between 0 and 100");
                return VRT_ERROR;
            }
            config.hit_ratio = (int)lvalue;
            break;

        case 'd':
            if (string2l(optarg,strlen(optarg),&lvalue) != 1) {
                test_log_error("vireabtest: option -d requires a number");
                return VRT_ERROR;
            }
            config.dispatch_data_threads = (int)lvalue;
            break;

        case 'c':
            if (string2l(optarg,strlen(optarg),&lvalue) != 1) {
                test_log_error("vireabtest: option -c requires a number");
                return VRT_ERROR;
            }
            config.clients_per_dispatch_thread = (int)lvalue;
            break;
            
        case 'P':
            config.pid_filename = optarg;
            break;
            
        case '?':
            switch (optopt) {
            case 'C':
            case 'k':
            case 'T':
            case 't':
            case 'P':
                test_log_error("vire: option -%c requires string",
                           optopt);
                break;

            case 'i':
            case 'p':
            case 'd':
            case 'c':
                test_log_error("vire: option -%c requires string",
                           optopt);
                break;
                
            default:
                test_log_error("vire: invalid option -- '%c'", optopt);
                break;
            }
            return VRT_ERROR;

        default:
            test_log_error("vire: invalid option -- '%c'", optopt);
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
        test_log_error("fork() failed: %s", strerror(errno));
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
        test_log_error("setsid() failed: %s", strerror(errno));
        return VRT_ERROR;
    }

    if (signal(SIGHUP, SIG_IGN) == SIG_ERR) {
        test_log_error("signal(SIGHUP, SIG_IGN) failed: %s", strerror(errno));
        return VRT_ERROR;
    }

    pid = fork();
    switch (pid) {
    case -1:
        test_log_error("fork() failed: %s", strerror(errno));
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
            test_log_error("chdir(\"/\") failed: %s", strerror(errno));
            return VRT_ERROR;
        }
    }

    /* clear file mode creation mask */
    umask(0);

    /* redirect stdin, stdout and stderr to "/dev/null" */

    fd = open("/dev/null", O_RDWR);
    if (fd < 0) {
        test_log_error("open(\"/dev/null\") failed: %s", strerror(errno));
        return VRT_ERROR;
    }

    ret = dup2(fd, STDIN_FILENO);
    if (ret < 0) {
        test_log_error("dup2(%d, STDIN) failed: %s", fd, strerror(errno));
        close(fd);
        return VRT_ERROR;
    }

    ret = dup2(fd, STDOUT_FILENO);
    if (ret < 0) {
        test_log_error("dup2(%d, STDOUT) failed: %s", fd, strerror(errno));
        close(fd);
        return VRT_ERROR;
    }

    ret = dup2(fd, STDERR_FILENO);
    if (ret < 0) {
        test_log_error("dup2(%d, STDERR) failed: %s", fd, strerror(errno));
        close(fd);
        return VRT_ERROR;
    }

    if (fd > STDERR_FILENO) {
        ret = close(fd);
        if (ret < 0) {
            test_log_error("close(%d) failed: %s", fd, strerror(errno));
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

    addrs = sdssplitlen(type_addrs[1],sdslen(type_addrs[1]),"|",1,&addrs_count);
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

/* groups_string is like "vire[127.0.0.1:12301|127.0.0.1:12302]-redis[127.0.0.1:12311|127.0.0.1:12312]" */
darray *abtest_groups_create(char *groups_string)
{
    darray *abgs;
    sds *group_strings;
    int group_count, j;

    group_strings = sdssplitlen(groups_string,strlen(groups_string),"-",1,&group_count);
    if (group_strings == NULL) {
        return NULL;
    } else if (group_count <= 1) {
        sdsfreesplitres(groups_string,group_count);
        return NULL;
    }

    abgs = darray_create(2, sizeof(abtest_group));
    if (abgs == NULL) {
        sdsfreesplitres(groups_string,group_count);
        return NULL;
    }
    
    for (j = 0; j < group_count; j ++) {
        abtest_group *abg;
        sds group_string = group_strings[j];
        sds *type_addrs;
        int elem_count;

        abg = darray_push(abgs);
        if (abtest_group_init(abg,group_string) != VRT_OK) {
            sdsfreesplitres(groups_string,group_count);
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
        test_log_out("This is vireabtest-%s", VR_VERSION_STRING);
        if (show_help) {
            vrt_show_usage();
        }
        exit(0);
    }

    if (daemonize) {
        ret = vrt_daemonize(1);
        if (ret != VRT_OK) {
            return VRT_ERROR;
        }
    }
    
    ret = vrt_produce_data_init(config.key_length_range_begin,
        config.key_length_range_end,config.cmd_type, 
        config.produce_data_threads, config.cached_keys_per_produce_thread, 
        config.hit_ratio);
    if (ret != VRT_OK) {
        test_log_error("Init data producer failed");
        return VRT_ERROR;
    }
    ret = vrt_dispatch_data_init(config.dispatch_data_threads, 
        config.test_targets, config.clients_per_dispatch_thread);
    if (ret != VRT_OK) {
        test_log_error("Init data dispatcher failed");
        return VRT_ERROR;
    }

    vrt_start_produce_data();
    vrt_start_dispatch_data();

    vrt_wait_produce_data();
    vrt_wait_dispatch_data();

    vrt_dispatch_data_deinit();
    vrt_produce_data_deinit();
    return VRT_OK;
}
