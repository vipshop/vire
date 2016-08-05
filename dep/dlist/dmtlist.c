#include <stdlib.h>

#include <dmtlist.h>
#include <dlocklist.h>

/******** multi-thread safe list interface ********/
dmtlist *dmtlist_create(void)
{
    dmtlist *l;

    l = malloc(sizeof(*l));
    if (l == NULL) {
        return NULL;
    }

    l->l = NULL;
    l->lock_push = NULL;
    l->lock_pop = NULL;
    l->free = NULL;
    l->length = NULL;
    
    return l;
}

void dmtlist_destroy(dmtlist *l)
{
    if (l == NULL) {
        return;
    }

    if (l->free) {
        l->free(l->l);
    }

    free(l);
}

long long dmtlist_push(dmtlist *l, void *value)
{
    if(l == NULL || l->l == NULL
        || l->lock_push == NULL)
    {
        return -1;
    }

    return l->lock_push(l->l, value);
}

void *dmtlist_pop(dmtlist *l)
{
    if(l == NULL || l->l == NULL
        || l->lock_pop == NULL)
    {
        return NULL;
    }
    
    return l->lock_pop(l->l);
}

int dmtlist_empty(dmtlist *l)
{
    if(l == NULL || l->l == NULL
        || l->length == NULL)
    {
        return -1;
    }

    if(l->length(l->l) > 0)
    {
        return 0;
    }

    return 1;
}

long long dmtlist_length(dmtlist *l)
{
    if(l == NULL || l->l == NULL
        || l->length == NULL)
    {
        return -1;
    }

    return l->length(l->l);
}

/******** multi-thread safe list implement ********/

/**
* This is multi-thread safe list.
* This lock list's performance is not good, but it is safe.
*/
int dmtlist_init_with_locklist(dmtlist *l)
{
    if (l == NULL) {
        return -1;
    }

    l->l = dlocklist_create();
    if (l->l == NULL) {
        return -1;
    }
    
    l->lock_push = dlocklist_push;
    l->lock_pop = dlocklist_pop;
    l->free = dlocklist_free;
    l->length = dlocklist_length;

    return 0;
}
