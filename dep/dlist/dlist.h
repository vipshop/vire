#ifndef _DLIST_H__
#define _DLIST_H__

/* Node, List, and Iterator are the only data structures used currently. */

typedef struct dlistNode {
    struct dlistNode *prev;
    struct dlistNode *next;
    void *value;
} dlistNode;

typedef struct dlistIter {
    dlistNode *next;
    int direction;
} dlistIter;

typedef struct dlist {
    dlistNode *head;
    dlistNode *tail;
    void *(*dup)(void *ptr);
    void (*free)(void *ptr);
    int (*match)(void *ptr, void *key);
    unsigned long len;
} dlist;

/* Functions implemented as macros */
#define dlistLength(l) ((l)->len)
#define dlistFirst(l) ((l)->head)
#define dlistLast(l) ((l)->tail)
#define dlistPrevNode(n) ((n)->prev)
#define dlistNextNode(n) ((n)->next)
#define dlistNodeValue(n) ((n)->value)

#define dlistSetDupMethod(l,m) ((l)->dup = (m))
#define dlistSetFreeMethod(l,m) ((l)->free = (m))
#define dlistSetMatchMethod(l,m) ((l)->match = (m))

#define dlistGetDupMethod(l) ((l)->dup)
#define dlistGetFree(l) ((l)->free)
#define dlistGetMatchMethod(l) ((l)->match)

/* Prototypes */
dlist *dlistCreate(void);
void dlistRelease(dlist *list);
dlist *dlistAddNodeHead(dlist *list, void *value);
dlist *dlistAddNodeTail(dlist *list, void *value);
dlist *dlistInsertNode(dlist *list, dlistNode *old_node, void *value, int after);
void dlistDelNode(dlist *list, dlistNode *node);
dlistIter *dlistGetIterator(dlist *list, int direction);
dlistNode *dlistNext(dlistIter *iter);
void dlistReleaseIterator(dlistIter *iter);
dlist *dlistDup(dlist *orig);
dlistNode *dlistSearchKey(dlist *list, void *key);
dlistNode *dlistIndex(dlist *list, long index);
void dlistRewind(dlist *list, dlistIter *li);
void dlistRewindTail(dlist *list, dlistIter *li);
void dlistRotate(dlist *list);
dlist *dlistPush(dlist *list, void *value);
void *dlistPop(dlist *list);

/* Directions for iterators */
#define AL_START_HEAD 0
#define AL_START_TAIL 1

#endif /* __ADLIST_H__ */
