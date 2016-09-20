#include <stdlib.h>

#include <dmalloc.h>

#include <dlist.h>
#include <dmtqueue.h>
#include <dlockqueue.h>

/******** multi-thread safe queue interface ********/
dmtqueue *dmtqueue_create(void)
{
    dmtqueue *q;

    q = dalloc(sizeof(*q));
    if (q == NULL) {
        return NULL;
    }

    q->l = NULL;
    q->lock_push = NULL;
    q->lock_pop = NULL;
    q->destroy = NULL;
    q->length = NULL;
    
    return q;
}

void dmtqueue_destroy(dmtqueue *q)
{
    if (q == NULL) {
        return;
    }

    if (q->destroy) {
        q->destroy(q->l);
    }

    dfree(q);
}

long long dmtqueue_push(dmtqueue *q, void *value)
{
    if(q == NULL || q->l == NULL
        || q->lock_push == NULL)
    {
        return -1;
    }

    return q->lock_push(q->l, value);
}

void *dmtqueue_pop(dmtqueue *q)
{
    if(q == NULL || q->l == NULL
        || q->lock_pop == NULL)
    {
        return NULL;
    }
    
    return q->lock_pop(q->l);
}

int dmtqueue_empty(dmtqueue *q)
{
    if(q == NULL || q->l == NULL
        || q->length == NULL)
    {
        return -1;
    }

    if(q->length(q->l) > 0)
    {
        return 0;
    }

    return 1;
}

long long dmtqueue_length(dmtqueue *q)
{
    if(q == NULL || q->l == NULL
        || q->length == NULL)
    {
        return -1;
    }

    return q->length(q->l);
}

/******** multi-thread safe queue implement ********/

/**
* This is multi-thread safe queue.
* This lock list's performance is not good, but it is safe.
*/
int dmtqueue_init_with_lockqueue(dmtqueue *q, dlockqueue_freefunc freefunc)
{
    dlockqueue *lq;
    
    if (q == NULL) {
        return -1;
    }

    lq = dlockqueue_create();
    if (lq == NULL) {
        return -1;
    }

    lq->l->free = freefunc;

    q->l = lq;
    q->lock_push = dlockqueue_push;
    q->lock_pop = dlockqueue_pop;
    q->destroy = dlockqueue_destroy;
    q->length = dlockqueue_length;
    
    return 0;
}
