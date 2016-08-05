#ifndef _DARRAY_H_
#define _DARRAY_H_

typedef int (*darray_compare_t)(const void *, const void *);
typedef int (*darray_each_t)(void *, void *);

typedef struct darray {
    unsigned long long   nelem;  /* # element */
    void                 *elem;  /* element */
    size_t               size;   /* element size */
    unsigned long long   nalloc; /* # allocated element */
} darray;

#define null_darray { 0, NULL, 0, 0 }

static inline void
darray_null(darray *a)
{
    a->nelem = 0;
    a->elem = NULL;
    a->size = 0;
    a->nalloc = 0;
}

static inline void
darray_set(darray *a, void *elem, size_t size, unsigned long long nalloc)
{
    a->nelem = 0;
    a->elem = elem;
    a->size = size;
    a->nalloc = nalloc;
}

static inline unsigned long long
darray_n(const darray *a)
{
    return a->nelem;
}

darray *darray_create(unsigned long long n, size_t size);
void darray_destroy(darray *a);
int darray_init(darray *a, unsigned long long n, size_t size);
void darray_deinit(darray *a);

unsigned long long darray_idx(darray *a, void *elem);
void *darray_push(darray *a);
void *darray_pop(darray *a);
void *darray_get(darray *a, unsigned long long idx);
void *darray_top(darray *a);
void darray_swap(darray *a, darray *b);
void darray_sort(darray *a, darray_compare_t compare);
int darray_each(darray *a, darray_each_t func, void *data);

#endif
