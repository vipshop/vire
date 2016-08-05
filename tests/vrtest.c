#include <stdio.h>
#include <stdlib.h>
#include <getopt.h>
#include <fcntl.h>
#include <errno.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/utsname.h>

#include <hiredis.h>

#include <vrt_util.h>
#include <vrt_public.h>
#include <vrt_simple.h>

struct config {
    char *pid_filename;

    int pidfile;
    int pid;
};

static struct config config;

static int show_help;
static int show_version;

static struct option long_options[] = {
    { "help",           no_argument,        NULL,   'h' },
    { "version",        no_argument,        NULL,   'V' },
    { "execute-file",   required_argument,  NULL,   'e' },
    { "pid-file",       required_argument,  NULL,   'p' },
    { NULL,             0,                  NULL,    0  }
};

static char short_options[] = "hVe:p:";

static void
vr_show_usage(void)
{
    printf(
        "Usage: viretest [-?hV] [-e execute-file] [-p pid-file]" CRLF
        "" CRLF);
    printf(
        "Options:" CRLF
        "  -h, --help             : this help" CRLF
        "  -V, --version          : show version and exit" CRLF
        "  -e, --execute-file     : vire execute file, default is src/vire" CRLF
        "  -p, --pid-file         : pid file" CRLF
        "" CRLF);
}

static void
vr_set_default_options(void)
{
    config.pid_filename = NULL;
}

static int
vr_get_options(int argc, char **argv)
{
    int c;

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

        case 'e':
            set_execute_file(optarg);
            break;

        case 'p':
            config.pid_filename = optarg;
            break;

        case '?':
            switch (optopt) {
            case 'e':
            case 'p':
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

int
main(int argc, char **argv)
{
    int ret;
    int ok_count = 0, all_count = 0;

    vr_set_default_options();

    ret = vr_get_options(argc, argv);
    if (ret != VRT_OK) {
        vr_show_usage();
        exit(1);
    }

    if (show_version) {
        test_log_out("This is viretest-%s", VR_VERSION_STRING);
        if (show_help) {
            vr_show_usage();
        }
        exit(0);
    }

    create_work_dir();

    test_log_out("Testing Vire version %s \n", VR_VERSION_STRING);
    
    ok_count+=simple_test(); all_count++;
    
clean:
    destroy_work_dir();

    if (ok_count == all_count)
        test_log_out("\n\\o/ \033[32;1mAll tests passed without errors!\033[0m\n");
    return VRT_OK;
}
