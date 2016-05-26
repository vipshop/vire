#include <stdlib.h>
#include <signal.h>

#include <vr_core.h>
#include <vr_signal.h>

static struct signal signals[] = {
    { SIGUSR1, "SIGUSR1", 0,                 signal_handler },
    { SIGUSR2, "SIGUSR2", 0,                 signal_handler },
    { SIGTTIN, "SIGTTIN", 0,                 signal_handler },
    { SIGTTOU, "SIGTTOU", 0,                 signal_handler },
    { SIGHUP,  "SIGHUP",  0,                 signal_handler },
    { SIGINT,  "SIGINT",  0,                 signal_handler },
    { SIGSEGV, "SIGSEGV", (int)SA_RESETHAND, signal_handler },
    { SIGPIPE, "SIGPIPE", 0,                 SIG_IGN },
    { 0,        NULL,     0,                 NULL }
};

rstatus_t
signal_init(void)
{
    struct signal *sig;

    for (sig = signals; sig->signo != 0; sig++) {
        rstatus_t status;
        struct sigaction sa;

        memset(&sa, 0, sizeof(sa));
        sa.sa_handler = sig->handler;
        sa.sa_flags = sig->flags;
        sigemptyset(&sa.sa_mask);

        status = sigaction(sig->signo, &sa, NULL);
        if (status < 0) {
            log_error("sigaction(%s) failed: %s", sig->signame,
                      strerror(errno));
            return VR_ERROR;
        }
    }

    return VR_OK;
}

void
signal_deinit(void)
{
}

void
signal_handler(int signo)
{
    struct signal *sig;
    void (*action)(void);
    char *actionstr;
    bool done;

    for (sig = signals; sig->signo != 0; sig++) {
        if (sig->signo == signo) {
            break;
        }
    }
    ASSERT(sig->signo != 0);

    actionstr = "";
    action = NULL;
    done = false;

    switch (signo) {
    case SIGUSR1:
        break;

    case SIGUSR2:
        break;

    case SIGTTIN:
        actionstr = ", up logging level";
        action = log_level_up;
        break;

    case SIGTTOU:
        actionstr = ", down logging level";
        action = log_level_down;
        break;

    case SIGHUP:
        actionstr = ", reopening log file";
        action = log_reopen;
        break;

    case SIGINT:
        done = true;
        actionstr = ", exiting";
        break;

    case SIGSEGV:
        log_stacktrace();
        actionstr = ", core dumping";
        raise(SIGSEGV);
        break;

    default:
        NOT_REACHED();
    }

    log_safe("signal %d (%s) received%s", signo, sig->signame, actionstr);

    if (action != NULL) {
        action();
    }

    if (done) {
        exit(1);
    }
}
