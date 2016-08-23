#ifndef _DLOCKQUEUE_H_
#define _DLOCKQUEUE_H_

struct dlist;

typedef struct dlockqueue{
    struct dlist *l;
    long long maxlen;
    int maxlen_policy;
    pthread_mutex_t lmutex;
} dlockqueue;

dlockqueue *dlockqueue_create(void);
long long dlockqueue_push(void *q, void *value);
void *dlockqueue_pop(void *q);
void dlockqueue_destroy(void *q);
long long dlockqueue_length(void *q);

#endif
