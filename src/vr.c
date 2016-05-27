#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <signal.h>
#include <getopt.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/utsname.h>

#include <vr_core.h>
#include <vr_conf.h>
#include <vr_signal.h>

#define VR_CONF_PATH        "conf/vire.conf"

#define VR_LOG_DEFAULT      LOG_NOTICE
#define VR_LOG_MIN          LOG_EMERG
#define VR_LOG_MAX          LOG_PVERB
#define VR_LOG_PATH         NULL

#define VR_PORT             8889
#define VR_ADDR             "0.0.0.0"
#define VR_INTERVAL         (30 * 1000) /* in msec */

#define VR_PID_FILE         NULL

#define VR_THREAD_NUM_DEFAULT	(sysconf(_SC_NPROCESSORS_ONLN)>6?6:sysconf(_SC_NPROCESSORS_ONLN))

static int show_help;
static int show_version;
static int test_conf;
static int daemonize;

static struct option long_options[] = {
    { "help",           no_argument,        NULL,   'h' },
    { "version",        no_argument,        NULL,   'V' },
    { "test-conf",      no_argument,        NULL,   't' },
    { "daemonize",      no_argument,        NULL,   'd' },
    { "describe-stats", no_argument,        NULL,   'D' },
    { "verbose",        required_argument,  NULL,   'v' },
    { "output",         required_argument,  NULL,   'o' },
    { "conf-file",      required_argument,  NULL,   'c' },
    { "port",           required_argument,  NULL,   's' },
    { "interval",       required_argument,  NULL,   'i' },
    { "addr",           required_argument,  NULL,   'a' },
    { "pid-file",       required_argument,  NULL,   'p' },
    { "thread-num",     required_argument,  NULL,   'T' },
    { NULL,             0,                  NULL,    0  }
};

static char short_options[] = "hVtdDv:o:c:s:i:a:p:m:T:";

static rstatus_t
vr_daemonize(int dump_core)
{
    rstatus_t status;
    pid_t pid, sid;
    int fd;

    pid = fork();
    switch (pid) {
    case -1:
        log_error("fork() failed: %s", strerror(errno));
        return VR_ERROR;

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
        return VR_ERROR;
    }

    if (signal(SIGHUP, SIG_IGN) == SIG_ERR) {
        log_error("signal(SIGHUP, SIG_IGN) failed: %s", strerror(errno));
        return VR_ERROR;
    }

    pid = fork();
    switch (pid) {
    case -1:
        log_error("fork() failed: %s", strerror(errno));
        return VR_ERROR;

    case 0:
        break;

    default:
        /* 1st child terminates */
        _exit(0);
    }

    /* 2nd child continues */

    /* change working directory */
    if (dump_core == 0) {
        status = chdir("/");
        if (status < 0) {
            log_error("chdir(\"/\") failed: %s", strerror(errno));
            return VR_ERROR;
        }
    }

    /* clear file mode creation mask */
    umask(0);

    /* redirect stdin, stdout and stderr to "/dev/null" */

    fd = open("/dev/null", O_RDWR);
    if (fd < 0) {
        log_error("open(\"/dev/null\") failed: %s", strerror(errno));
        return VR_ERROR;
    }

    status = dup2(fd, STDIN_FILENO);
    if (status < 0) {
        log_error("dup2(%d, STDIN) failed: %s", fd, strerror(errno));
        close(fd);
        return VR_ERROR;
    }

    status = dup2(fd, STDOUT_FILENO);
    if (status < 0) {
        log_error("dup2(%d, STDOUT) failed: %s", fd, strerror(errno));
        close(fd);
        return VR_ERROR;
    }

    status = dup2(fd, STDERR_FILENO);
    if (status < 0) {
        log_error("dup2(%d, STDERR) failed: %s", fd, strerror(errno));
        close(fd);
        return VR_ERROR;
    }

    if (fd > STDERR_FILENO) {
        status = close(fd);
        if (status < 0) {
            log_error("close(%d) failed: %s", fd, strerror(errno));
            return VR_ERROR;
        }
    }

    return VR_OK;
}

static void
vr_print_run(struct instance *nci)
{
    int status;
    struct utsname name;

    status = uname(&name);

    if (nci->log_filename) {
        char *ascii_logo =
"                _._                                                  \n"
"           _.-``__ ''-._                                             \n"
"      _.-``    `.  *_.  ''-._           Vire %s %s bit\n"
"  .-`` .-```.  ```\-/    _.,_ ''-._                                   \n"
" (    |      |       .-`    `,    )     Running in %s mode\n"
" |`-._`-...-` __...-.``-._;'` _.-'|     Port: %d\n"
" |    `-._   `._    /     _.-'    |     PID: %ld\n"
"  `-._    `-._  `-./  _.-'    _.-'      OS: %s %s %s\n"
" |`-._`-._    `-.__.-'    _.-'_.-'|                                  \n"
" |    `-._`-._        _.-'_.-'    |     https://github.com/vipshop/vire\n"
"  `-._    `-._`-.__.-'_.-'    _.-'                                   \n"
" |`-._`-._    `-.__.-'    _.-'_.-'|                                  \n"
" |    `-._`-._        _.-'_.-'    |                                  \n"
"  `-._    `-._`-.__.-'_.-'    _.-'                                   \n"
"      `-._    `-.__.-'    _.-'                                       \n"
"          `-._        _.-'                                           \n"
"              `-.__.-'                                               \n\n";
        char *buf = vr_alloc(1024*16);
        snprintf(buf,1024*16,ascii_logo,
            VR_VERSION_STRING,
            (sizeof(long) == 8) ? "64" : "32",
            "standalone", server.port,
            (long) nci->pid,
            status < 0 ? " ":name.sysname,
            status < 0 ? " ":name.release,
            status < 0 ? " ":name.machine);
        write_to_log(buf, strlen(buf));
        vr_free(buf);
    }else {
        char buf[256];
        snprintf(buf,256,"Vire %s, %s bit, %s mode, port %d, pid %ld, built for %s %s %s ready to run.\n",
            VR_VERSION_STRING, (sizeof(long) == 8) ? "64" : "32",
            "standalone", server.port, (long) nci->pid,
            status < 0 ? " ":name.sysname,
            status < 0 ? " ":name.release,
            status < 0 ? " ":name.machine);
        write_to_log(buf, strlen(buf));
    }
}

static void
vr_print_done(void)
{
    loga("done, rabbit done");
}

static void
vr_show_usage(void)
{
    log_stderr(
        "Usage: vire [-?hVdDt] [-v verbosity level] [-o output file]" CRLF
        "            [-c conf file] [-s manage port] [-a manage addr]" CRLF
        "            [-i interval] [-p pid file] [-T worker threads number]" CRLF
        "");
    log_stderr(
        "Options:" CRLF
        "  -h, --help             : this help" CRLF
        "  -V, --version          : show version and exit" CRLF
        "  -t, --test-conf        : test configuration for syntax errors and exit" CRLF
        "  -d, --daemonize        : run as a daemon" CRLF
        "  -D, --describe-stats   : print stats description and exit");
    log_stderr(
        "  -v, --verbose=N        : set logging level (default: %d, min: %d, max: %d)" CRLF
        "  -o, --output=S         : set logging file (default: %s)" CRLF
        "  -c, --conf-file=S      : set configuration file (default: %s)" CRLF
        "  -s, --port=N           : set manage port (default: %d)" CRLF
        "  -a, --addr=S           : set manage ip (default: %s)" CRLF
        "  -i, --interval=N       : set interval in msec (default: %d msec)" CRLF
        "  -p, --pid-file=S       : set pid file (default: %s)" CRLF
        "  -T, --thread_num=N     : set the worker threads number (default: %d)" CRLF
        "",
        VR_LOG_DEFAULT, VR_LOG_MIN, VR_LOG_MAX,
        VR_LOG_PATH != NULL ? VR_LOG_PATH : "stderr",
        VR_CONF_PATH,
        VR_PORT, VR_ADDR, VR_INTERVAL,
        VR_PID_FILE != NULL ? VR_PID_FILE : "off",
        VR_THREAD_NUM_DEFAULT);
}

static rstatus_t
vr_create_pidfile(struct instance *nci)
{
    char pid[VR_UINTMAX_MAXLEN];
    int fd, pid_len;
    ssize_t n;

    fd = open(nci->pid_filename, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        log_error("opening pid file '%s' failed: %s", nci->pid_filename,
                  strerror(errno));
        return VR_ERROR;
    }
    nci->pidfile = 1;

    pid_len = vr_snprintf(pid, VR_UINTMAX_MAXLEN, "%d", nci->pid);

    n = vr_write(fd, pid, pid_len);
    if (n < 0) {
        log_error("write to pid file '%s' failed: %s", nci->pid_filename,
                  strerror(errno));
        return VR_ERROR;
    }

    close(fd);

    return VR_OK;
}

static void
vr_remove_pidfile(struct instance *nci)
{
    int status;

    status = unlink(nci->pid_filename);
    if (status < 0) {
        log_error("unlink of pid file '%s' failed, ignored: %s",
                  nci->pid_filename, strerror(errno));
    }
}

static void
vr_set_default_options(struct instance *nci)
{
    int status;

    nci->log_level = VR_LOG_DEFAULT;
    nci->log_filename = VR_LOG_PATH;

    nci->conf_filename = VR_CONF_PATH;

    nci->port = VR_PORT;
    nci->addr = VR_ADDR;
    nci->interval = VR_INTERVAL;

    status = vr_gethostname(nci->hostname, VR_MAXHOSTNAMELEN);
    if (status < 0) {
        log_warn("gethostname failed, ignored: %s", strerror(errno));
        vr_snprintf(nci->hostname, VR_MAXHOSTNAMELEN, "unknown");
    }
    nci->hostname[VR_MAXHOSTNAMELEN - 1] = '\0';

    nci->pid = (pid_t)-1;
    nci->pid_filename = NULL;
    nci->pidfile = 0;

    nci->thread_num = (int)VR_THREAD_NUM_DEFAULT;
}

static rstatus_t
vr_get_options(int argc, char **argv, struct instance *nci)
{
    int c, value;

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

        case 't':
            test_conf = 1;
            break;

        case 'd':
            daemonize = 1;
            break;

        case 'D':
            show_version = 1;
            break;

        case 'v':
            value = vr_atoi(optarg, strlen(optarg));
            if (value < 0) {
                log_stderr("vire: option -v requires a number");
                return VR_ERROR;
            }
            nci->log_level = value;
            break;

        case 'o':
            nci->log_filename = optarg;
            break;

        case 'c':
            nci->conf_filename = optarg;
            break;

        case 's':
            value = vr_atoi(optarg, strlen(optarg));
            if (value < 0) {
                log_stderr("vire: option -s requires a number");
                return VR_ERROR;
            }
            if (!vr_valid_port(value)) {
                log_stderr("vire: option -s value %d is not a valid "
                           "port", value);
                return VR_ERROR;
            }

            nci->port = (uint16_t)value;
            break;

        case 'i':
            value = vr_atoi(optarg, strlen(optarg));
            if (value < 0) {
                log_stderr("vire: option -i requires a number");
                return VR_ERROR;
            }

            nci->interval = value;
            break;

        case 'a':
            nci->addr = optarg;
            break;

        case 'p':
            nci->pid_filename = optarg;
            break;
            
        case 'T':
            value = vr_atoi(optarg, strlen(optarg));
            if (value < 0) {
                log_stderr("vire: option -T requires a number");
                return VR_ERROR;
            }

            nci->thread_num = value;
            break;

        case '?':
            switch (optopt) {
            case 'o':
            case 'c':
            case 'p':
                log_stderr("vire: option -%c requires a file name",
                           optopt);
                break;

            case 'm':
            case 'v':
            case 's':
            case 'i':
            case 'T':
                log_stderr("vire: option -%c requires a number", optopt);
                break;

            case 'a':
                log_stderr("vire: option -%c requires a string", optopt);
                break;

            default:
                log_stderr("vire: invalid option -- '%c'", optopt);
                break;
            }
            return VR_ERROR;

        default:
            log_stderr("vire: invalid option -- '%c'", optopt);
            return VR_ERROR;

        }
    }

    return VR_OK;
}

/*
 * Returns true if configuration file has a valid syntax, otherwise
 * returns false
 */
static bool
vr_test_conf(struct instance *nci, int test)
{
    vr_conf *cf;

    cf = conf_create(nci->conf_filename);
    if (cf == NULL) {
        if (test)
            log_stderr("vire: configuration file '%s' syntax is invalid",
                nci->conf_filename);
        return false;
    }

    conf_destroy(cf);

    if (test)
        log_stderr("vire: configuration file '%s' syntax is ok",
            nci->conf_filename);
    return true;
}

static rstatus_t
vr_pre_run(struct instance *nci)
{
    rstatus_t status;

    status = log_init(nci->log_level, nci->log_filename);
    if (status != VR_OK) {
        return status;
    }

    if (!vr_test_conf(nci, false)) {
        log_error("conf file %s is error", nci->conf_filename);
        return VR_ERROR;
    }

    if (daemonize) {
        status = vr_daemonize(1);
        if (status != VR_OK) {
            return status;
        }
    }

    nci->pid = getpid();

    status = signal_init();
    if (status != VR_OK) {
        return status;
    }

    if (nci->pid_filename) {
        status = vr_create_pidfile(nci);
        if (status != VR_OK) {
            return status;
        }
    }

    status = init_server(nci);
    if (status != VR_OK) {
        return status;
    }

    vr_print_run(nci);

    return VR_OK;
}

static void
vr_post_run(struct instance *nci)
{
    if (nci->pidfile) {
        vr_remove_pidfile(nci);
    }

    signal_deinit();

    vr_print_done();

    log_deinit();
}

static void
vr_run(struct instance *nci)
{
    if (nci->thread_num <= 0) {
        log_error("number of work threads must be greater than 0");
        return;
    } else if (nci->thread_num > 64) {
        log_warn("WARNING: Setting a high number of worker threads is not recommended."
            " Set this value to the number of cores in your machine or less.");
    }

    /* run the threads */
    master_run();
    workers_run();

    /* wait for the worker finish */
	workers_wait();

    /* deinit the workers */
	workers_deinit();
}

int
main(int argc, char **argv)
{
    rstatus_t status;
    struct instance nci;

    vr_set_default_options(&nci);

    status = vr_get_options(argc, argv, &nci);
    if (status != VR_OK) {
        vr_show_usage();
        exit(1);
    }

    if (show_version) {
        log_stderr("This is vire-%s" CRLF, VR_VERSION_STRING);
        if (show_help) {
            vr_show_usage();
        }
        exit(0);
    }

    if (test_conf) {
        if (!vr_test_conf(&nci, true)) {
            exit(1);
        }
        exit(0);
    }

    status = vr_pre_run(&nci);
    if (status != VR_OK) {
        vr_post_run(&nci);
        exit(1);
    }

    server.executable = getAbsolutePath(argv[0]);

    vr_run(&nci);

    vr_post_run(&nci);

    return VR_OK;
}
