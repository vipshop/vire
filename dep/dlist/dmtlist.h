#ifndef _DMTLIST_H_
#define _DMTLIST_H_

//multi-thread safe list
typedef struct dmtlist{
    void *l;
    long long (*lock_push)(void *l, void *value);
    void *(*lock_pop)(void *l);
    void (*free)(void *l);
    long long (*length)(void *l);
} dmtlist;

typedef int (*dmtlist_init)(dmtlist *);

/******** multi-thread safe list interface ********/

dmtlist *dmtlist_create(void);
void dmtlist_destroy(dmtlist *l);
long long dmtlist_push(dmtlist *l, void *value);
void *dmtlist_pop(dmtlist *l);
int dmtlist_empty(dmtlist *l);
long long dmtlist_length(dmtlist *l);

/******** multi-thread safe list implement ********/

int dmtlist_init_with_locklist(dmtlist *l);

#endif
