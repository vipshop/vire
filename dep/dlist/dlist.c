#include <stdlib.h>

#include <dmalloc.h>

#include <dlist.h>

/* Create a new list. The created list can be freed with
 * AlFreeList(), but private value of every node need to be freed
 * by the user before to call AlFreeList().
 *
 * On error, NULL is returned. Otherwise the pointer to the new list. */
dlist *dlistCreate(void)
{
    struct dlist *list;

    if ((list = dalloc(sizeof(*list))) == NULL)
        return NULL;
    list->head = list->tail = NULL;
    list->len = 0;
    list->dup = NULL;
    list->free = NULL;
    list->match = NULL;
    return list;
}

/* Free the whole list.
 *
 * This function can't fail. */
void dlistRelease(dlist *list)
{
    unsigned long len;
    dlistNode *current, *next;

    current = list->head;
    len = list->len;
    while(len--) {
        next = current->next;
        if (list->free) list->free(current->value);
        dfree(current);
        current = next;
    }
    dfree(list);
}

/* Add a new node to the list, to head, containing the specified 'value'
 * pointer as value.
 *
 * On error, NULL is returned and no operation is performed (i.e. the
 * list remains unaltered).
 * On success the 'list' pointer you pass to the function is returned. */
dlist *dlistAddNodeHead(dlist *list, void *value)
{
    dlistNode *node;

    if ((node = dalloc(sizeof(*node))) == NULL)
        return NULL;
    node->value = value;
    if (list->len == 0) {
        list->head = list->tail = node;
        node->prev = node->next = NULL;
    } else {
        node->prev = NULL;
        node->next = list->head;
        list->head->prev = node;
        list->head = node;
    }
    list->len++;
    return list;
}

/* Add a new node to the list, to tail, containing the specified 'value'
 * pointer as value.
 *
 * On error, NULL is returned and no operation is performed (i.e. the
 * list remains unaltered).
 * On success the 'list' pointer you pass to the function is returned. */
dlist *dlistAddNodeTail(dlist *list, void *value)
{
    dlistNode *node;

    if ((node = dalloc(sizeof(*node))) == NULL)
        return NULL;
    node->value = value;
    if (list->len == 0) {
        list->head = list->tail = node;
        node->prev = node->next = NULL;
    } else {
        node->prev = list->tail;
        node->next = NULL;
        list->tail->next = node;
        list->tail = node;
    }
    list->len++;
    return list;
}

dlist *dlistInsertNode(dlist *list, dlistNode *old_node, void *value, int after) {
    dlistNode *node;

    if ((node = dalloc(sizeof(*node))) == NULL)
        return NULL;
    node->value = value;
    if (after) {
        node->prev = old_node;
        node->next = old_node->next;
        if (list->tail == old_node) {
            list->tail = node;
        }
    } else {
        node->next = old_node;
        node->prev = old_node->prev;
        if (list->head == old_node) {
            list->head = node;
        }
    }
    if (node->prev != NULL) {
        node->prev->next = node;
    }
    if (node->next != NULL) {
        node->next->prev = node;
    }
    list->len++;
    return list;
}

/* Remove the specified node from the specified list.
 * It's up to the caller to free the private value of the node.
 *
 * This function can't fail. */
void dlistDelNode(dlist *list, dlistNode *node)
{
    if (node->prev)
        node->prev->next = node->next;
    else
        list->head = node->next;
    if (node->next)
        node->next->prev = node->prev;
    else
        list->tail = node->prev;
    if (list->free) list->free(node->value);
    dfree(node);
    list->len--;
}

/* Returns a list iterator 'iter'. After the initialization every
 * call to dlistNext() will return the next element of the list.
 *
 * This function can't fail. */
dlistIter *dlistGetIterator(dlist *list, int direction)
{
    dlistIter *iter;

    if ((iter = dalloc(sizeof(*iter))) == NULL) return NULL;
    if (direction == AL_START_HEAD)
        iter->next = list->head;
    else
        iter->next = list->tail;
    iter->direction = direction;
    return iter;
}

/* Release the iterator memory */
void dlistReleaseIterator(dlistIter *iter) {
    dfree(iter);
}

/* Create an iterator in the list private iterator structure */
void dlistRewind(dlist *list, dlistIter *li) {
    li->next = list->head;
    li->direction = AL_START_HEAD;
}

void dlistRewindTail(dlist *list, dlistIter *li) {
    li->next = list->tail;
    li->direction = AL_START_TAIL;
}

/* Return the next element of an iterator.
 * It's valid to remove the currently returned element using
 * dlistDelNode(), but not to remove other elements.
 *
 * The function returns a pointer to the next element of the list,
 * or NULL if there are no more elements, so the classical usage patter
 * is:
 *
 * iter = dlistGetIterator(list,<direction>);
 * while ((node = dlistNext(iter)) != NULL) {
 *     doSomethingWith(dlistNodeValue(node));
 * }
 *
 * */
dlistNode *dlistNext(dlistIter *iter)
{
    dlistNode *current = iter->next;

    if (current != NULL) {
        if (iter->direction == AL_START_HEAD)
            iter->next = current->next;
        else
            iter->next = current->prev;
    }
    return current;
}

/* Duplicate the whole list. On out of memory NULL is returned.
 * On success a copy of the original list is returned.
 *
 * The 'Dup' method set with listSetDupMethod() function is used
 * to copy the node value. Otherwise the same pointer value of
 * the original node is used as value of the copied node.
 *
 * The original list both on success or error is never modified. */
dlist *dlistDup(dlist *orig)
{
    dlist *copy;
    dlistIter iter;
    dlistNode *node;

    if ((copy = dlistCreate()) == NULL)
        return NULL;
    copy->dup = orig->dup;
    copy->free = orig->free;
    copy->match = orig->match;
    dlistRewind(orig, &iter);
    while((node = dlistNext(&iter)) != NULL) {
        void *value;

        if (copy->dup) {
            value = copy->dup(node->value);
            if (value == NULL) {
                dlistRelease(copy);
                return NULL;
            }
        } else
            value = node->value;
        if (dlistAddNodeTail(copy, value) == NULL) {
            dlistRelease(copy);
            return NULL;
        }
    }
    return copy;
}

/* Search the list for a node matching a given key.
 * The match is performed using the 'match' method
 * set with listSetMatchMethod(). If no 'match' method
 * is set, the 'value' pointer of every node is directly
 * compared with the 'key' pointer.
 *
 * On success the first matching node pointer is returned
 * (search starts from head). If no matching node exists
 * NULL is returned. */
dlistNode *dlistSearchKey(dlist *list, void *key)
{
    dlistIter iter;
    dlistNode *node;

    dlistRewind(list, &iter);
    while((node = dlistNext(&iter)) != NULL) {
        if (list->match) {
            if (list->match(node->value, key)) {
                return node;
            }
        } else {
            if (key == node->value) {
                return node;
            }
        }
    }
    return NULL;
}

/* Return the element at the specified zero-based index
 * where 0 is the head, 1 is the element next to head
 * and so on. Negative integers are used in order to count
 * from the tail, -1 is the last element, -2 the penultimate
 * and so on. If the index is out of range NULL is returned. */
dlistNode *dlistIndex(dlist *list, long index) {
    dlistNode *n;

    if (index < 0) {
        index = (-index)-1;
        n = list->tail;
        while(index-- && n) n = n->prev;
    } else {
        n = list->head;
        while(index-- && n) n = n->next;
    }
    return n;
}

/* Rotate the list removing the tail node and inserting it to the head. */
void dlistRotate(dlist *list) {
    dlistNode *tail = list->tail;

    if (dlistLength(list) <= 1) return;

    /* Detach current tail */
    list->tail = tail->prev;
    list->tail->next = NULL;
    /* Move it as head */
    list->head->prev = tail;
    tail->prev = NULL;
    tail->next = list->head;
    list->head = tail;
}

dlist *dlistPush(dlist *list, void *value) {
    dlistAddNodeTail(list, value);
    return list;
}

void *dlistPop(dlist *list) {
    dlistNode *node;
    void *value;
    
    node = dlistFirst(list);
    if (node == NULL) {
        return NULL;
    }

    value = dlistNodeValue(node);
    dlistDelNode(list, node);

    if (list->free) return NULL;
    
    return value;
}
