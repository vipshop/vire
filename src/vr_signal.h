#ifndef _VR_SIGNAL_H_
#define _VR_SIGNAL_H_

struct signal {
    int  signo;
    char *signame;
    int  flags;
    void (*handler)(int signo);
};

rstatus_t signal_init(void);
void signal_deinit(void);
void signal_handler(int signo);

#endif
