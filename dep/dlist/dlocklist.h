#ifndef _DLOCKLIST_H_
#define _DLOCKLIST_H_

struct dlist;

typedef struct dlocklist{
    struct dlist *l;
    pthread_mutex_t lmutex;
} dlocklist;

dlocklist *dlocklist_create(void);
long long dlocklist_push(void *l, void *value);
void *dlocklist_pop(void *l);
void dlocklist_free(void *l);
long long dlocklist_length(void *l);

#endif
