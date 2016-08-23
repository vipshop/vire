#ifndef _DMTQUEUE_H_
#define _DMTQUEUE_H_

#define MAX_LENGTH_POLICY_REJECT        0
#define MAX_LENGTH_POLICY_EVICT_HEAD    1
#define MAX_LENGTH_POLICY_EVICT_END     2

/* Multi-thread safe queue */
typedef struct dmtqueue{
    void *l;
    long long (*lock_push)(void *q, void *value);
    void *(*lock_pop)(void *q);
    void (*destroy)(void *q);
    long long (*length)(void *q);
} dmtqueue;

#define dmtqueueSetMaxlength(q,l)        ((q)->l->maxlen = (l))
#define dmtqueueSetMaxlengthPolicy(q,p)  ((q)->l->maxlen = (p))

typedef int (*dmtqueue_init)(dmtqueue *);

/******** multi-thread safe list interface ********/

dmtqueue *dmtqueue_create(void);
void dmtqueue_destroy(dmtqueue *q);
long long dmtqueue_push(dmtqueue *q, void *value);
void *dmtqueue_pop(dmtqueue *q);
int dmtqueue_empty(dmtqueue *q);
long long dmtqueue_length(dmtqueue *q);

/******** multi-thread safe list implement ********/

typedef void (*dlockqueue_freefunc)(void *);
int dmtqueue_init_with_lockqueue(dmtqueue *l, dlockqueue_freefunc freefunc);

#endif
