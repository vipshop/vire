#ifndef _DMALLOC_H_
#define _DMALLOC_H_

#ifdef HAVE_CONFIG_H
# include <config.h>
#endif

#include <dspecialconfig.h>

#ifdef HAVE_JEMALLOC
# define DUSE_JEMALLOC 1
#endif

/*
 * Memory allocation and free wrappers.
 *
 * These wrappers enables us to loosely detect double free, dangling
 * pointer access and zero-byte alloc.
 */
#if defined(DUSE_JEMALLOC)
#define DMALLOC_LIB ("jemalloc-" __xstr(JEMALLOC_VERSION_MAJOR) "." __xstr(JEMALLOC_VERSION_MINOR) "." __xstr(JEMALLOC_VERSION_BUGFIX))
#include <jemalloc/jemalloc.h>
#if (JEMALLOC_VERSION_MAJOR == 2 && JEMALLOC_VERSION_MINOR >= 1) || (JEMALLOC_VERSION_MAJOR > 2)
#define HAVE_MALLOC_SIZE 1
#define dmalloc_size(p) je_malloc_usable_size(p)
#else
#error "Newer version of jemalloc required"
#endif
#elif defined(__APPLE__)
#include <malloc/malloc.h>
#define HAVE_MALLOC_SIZE 1
#define dmalloc_size(p) malloc_size(p)
#endif
    
#ifndef DMALLOC_LIB
#define DMALLOC_LIB "libc"
#endif

#define dalloc(_s)                    \
    _dalloc((size_t)(_s), __FILE__, __LINE__)

#define dzalloc(_s)                   \
    _dzalloc((size_t)(_s), __FILE__, __LINE__)

#define dcalloc(_n, _s)               \
    _dcalloc((size_t)(_n), (size_t)(_s), __FILE__, __LINE__)

#define drealloc(_p, _s)              \
    _drealloc(_p, (size_t)(_s), __FILE__, __LINE__)

#define dfree(_p) do {                \
    _dfree(_p, __FILE__, __LINE__);   \
} while (0)

char *dmalloc_lock_type(void);

#ifndef HAVE_MALLOC_SIZE
size_t dmalloc_size(void *ptr);
#endif

void *_dalloc(size_t size, const char *name, int line);
void *_dzalloc(size_t size, const char *name, int line);
void *_dcalloc(size_t nmemb, size_t size, const char *name, int line);
void *_drealloc(void *ptr, size_t size, const char *name, int line);
void _dfree(void *ptr, const char *name, int line);

size_t dalloc_used_memory(void);

size_t dalloc_get_memory_size(void);

size_t dalloc_get_rss(void);
float dalloc_get_fragmentation_ratio(size_t rss);

#endif
