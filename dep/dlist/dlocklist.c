#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>

#include <dlist.h>
#include <dlocklist.h>

dlocklist *dlocklist_create(void)
{
    dlocklist *llist;

    llist = malloc(sizeof(*llist));
    if (llist == NULL) {
        return NULL;
    }

    pthread_mutex_init(&llist->lmutex,NULL);
    
    llist->l = dlistCreate();
    if (llist->l == NULL) {
        dlocklist_free(llist);
        return NULL;
    }

    return llist;
}

long long dlocklist_push(void *l, void *value)
{
    dlocklist *llist = l;
    dlist *list;
    long long length;
    
    pthread_mutex_lock(&llist->lmutex);    
    list = dlistAddNodeTail(llist->l, value);
    length = dlistLength(llist->l);
    pthread_mutex_unlock(&llist->lmutex);

    if (list == NULL) {
        return -1;
    }

    return length;
}

void *dlocklist_pop(void *l)
{
    dlocklist *llist = l;
    dlistNode *node;
    void *value;
        
    if (llist == NULL || llist->l == NULL) {
        return NULL;
    }
    
    pthread_mutex_lock(&llist->lmutex);
    
    node = dlistFirst(llist->l);
    if (node == NULL) {
        pthread_mutex_unlock(&llist->lmutex);
        return NULL;
    }

    value = dlistNodeValue(node);

    dlistDelNode(llist->l, node);

    pthread_mutex_unlock(&llist->lmutex);

    return value;
}

void dlocklist_free(void *l)
{
    dlocklist *llist = l;
    if (llist == NULL) {
        return;
    }

    if (llist->l != NULL) {
        dlistRelease(llist->l);
    }

    pthread_mutex_destroy(&llist->lmutex);

    free(llist);
}

long long dlocklist_length(void *l)
{
    dlocklist *llist = l;
    long long length;
    
    if (llist == NULL || llist->l == NULL) {
        return -1;
    }

    pthread_mutex_lock(&llist->lmutex);
    length = dlistLength(llist->l);
    pthread_mutex_unlock(&llist->lmutex);
    
    return length;
}
