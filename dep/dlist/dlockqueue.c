#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>

#include <dmalloc.h>

#include <dlist.h>
#include <dmtqueue.h>
#include <dlockqueue.h>

dlockqueue *dlockqueue_create(void)
{
    dlockqueue *lqueue;

    lqueue = dalloc(sizeof(*lqueue));
    if (lqueue == NULL) {
        return NULL;
    }

    lqueue->maxlen = -1;
    lqueue->maxlen_policy = MAX_LENGTH_POLICY_REJECT;
    pthread_mutex_init(&lqueue->lmutex,NULL);
    
    lqueue->l = dlistCreate();
    if (lqueue->l == NULL) {
        dlockqueue_destroy(lqueue);
        return NULL;
    }

    return lqueue;
}

long long dlockqueue_push(void *q, void *value)
{
    dlockqueue *lqueue = q;
    dlist *list;
    long long length;
    
    pthread_mutex_lock(&lqueue->lmutex);
    length = (long long)dlistLength(lqueue->l);
    if (lqueue->maxlen >0 && length >= lqueue->maxlen) {
        if (lqueue->maxlen_policy == MAX_LENGTH_POLICY_REJECT) {
            length = -1;
        } else if (lqueue->maxlen_policy == MAX_LENGTH_POLICY_EVICT_HEAD) {
            while (length >= lqueue->maxlen) {
                dlistNode *ln = dlistFirst(lqueue->l);
                dlistDelNode(lqueue->l,ln);
                length = (long long)dlistLength(lqueue->l);
            }
            list = dlistAddNodeTail(lqueue->l, value);
            length ++;
        } else if (lqueue->maxlen_policy == MAX_LENGTH_POLICY_EVICT_END) {
            while (length >= lqueue->maxlen) {
                dlistNode *ln = dlistLast(lqueue->l);
                dlistDelNode(lqueue->l,ln);
                length = (long long)dlistLength(lqueue->l);
            }
            list = dlistAddNodeTail(lqueue->l, value);
            length ++;
        }
    } else {
        list = dlistAddNodeTail(lqueue->l, value);
        length ++;
    }
    pthread_mutex_unlock(&lqueue->lmutex);

    if (list == NULL) {
        return -1;
    }

    return length;
}

void *dlockqueue_pop(void *q)
{
    dlockqueue *lqueue = q;
    dlistNode *node;
    void *value;
        
    if (lqueue == NULL || lqueue->l == NULL) {
        return NULL;
    }
    
    pthread_mutex_lock(&lqueue->lmutex);
    
    node = dlistFirst(lqueue->l);
    if (node == NULL) {
        pthread_mutex_unlock(&lqueue->lmutex);
        return NULL;
    }

    value = dlistNodeValue(node);

    dlistDelNode(lqueue->l, node);

    pthread_mutex_unlock(&lqueue->lmutex);

    return value;
}

void dlockqueue_destroy(void *q)
{
    dlockqueue *lqueue = q;
    if (lqueue == NULL) {
        return;
    }

    if (lqueue->l != NULL) {
        dlistRelease(lqueue->l);
    }

    pthread_mutex_destroy(&lqueue->lmutex);

    dfree(lqueue);
}

long long dlockqueue_length(void *q)
{
    dlockqueue *lqueue = q;
    long long length;
    
    if (lqueue == NULL || lqueue->l == NULL) {
        return -1;
    }

    pthread_mutex_lock(&lqueue->lmutex);
    length = dlistLength(lqueue->l);
    pthread_mutex_unlock(&lqueue->lmutex);
    
    return length;
}
