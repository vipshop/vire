#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>

#include <unistd.h>

#include <dutil.h>
#include <dlog.h>

#include <dmalloc.h>

/*memory api*/
static size_t used_memory = 0;
pthread_mutex_t used_memory_mutex = PTHREAD_MUTEX_INITIALIZER;

#if defined(__ATOMIC_RELAXED)
#define update_used_mem_stat_add(__n) __atomic_add_fetch(&used_memory, (__n), __ATOMIC_RELAXED)
#define update_used_mem_stat_sub(__n) __atomic_sub_fetch(&used_memory, (__n), __ATOMIC_RELAXED)
char *malloc_lock_type(void) {return "__ATOMIC_RELAXED";}
#elif defined(HAVE_ATOMIC)
#define update_used_mem_stat_add(__n) __sync_add_and_fetch(&used_memory, (__n))
#define update_used_mem_stat_sub(__n) __sync_sub_and_fetch(&used_memory, (__n))
char *malloc_lock_type(void) {return "HAVE_ATOMIC";}
#else
#define update_used_mem_stat_add(__n) do {      \
    pthread_mutex_lock(&used_memory_mutex);     \
    used_memory += (__n); \
    pthread_mutex_unlock(&used_memory_mutex);   \
} while(0)

#define update_used_mem_stat_sub(__n) do {      \
    pthread_mutex_lock(&used_memory_mutex);     \
    used_memory -= (__n); \
    pthread_mutex_unlock(&used_memory_mutex);   \
} while(0)

char *malloc_lock_type(void) {return "pthread_mutex_t";}
#endif

#define update_dmalloc_stat_alloc(__n) do {                                 \
    size_t _n = (__n);                                                      \
    if (_n&(sizeof(long)-1)) _n += sizeof(long)-(_n&(sizeof(long)-1));    \
    update_used_mem_stat_add(_n);                                           \
} while(0)

#define update_dmalloc_stat_free(__n) do {                                  \
    size_t _n = (__n);                                                      \
    if (_n&(sizeof(long)-1)) _n += sizeof(long)-(_n&(sizeof(long)-1));    \
    update_used_mem_stat_sub(_n);                                           \
} while(0)

#ifdef HAVE_MALLOC_SIZE
#define PREFIX_SIZE (0)
#else
#if defined(__sun) || defined(__sparc) || defined(__sparc__)
#define PREFIX_SIZE (sizeof(long long))
#else
#define PREFIX_SIZE (sizeof(size_t))
#endif
#endif

/* Provide dmalloc_size() for systems where this function is not provided by
 * malloc itself, given that in that case we store a header with this
 * information as the first bytes of every allocation. */
#ifndef HAVE_MALLOC_SIZE
size_t dmalloc_size(void *ptr) {
    void *realptr = (char*)ptr-PREFIX_SIZE;
    size_t size = *((size_t*)realptr);
    /* Assume at least that all the allocations are padded at sizeof(long) by
     * the underlying allocator. */
    if (size&(sizeof(long)-1)) size += sizeof(long)-(size&(sizeof(long)-1));
    return size+PREFIX_SIZE;
}
#endif

void *
_dalloc(size_t size, const char *name, int line)
{
    void *p;

    ASSERT(size != 0);

#ifdef DUSE_JEMALLOC
    p = je_malloc(size+PREFIX_SIZE);
#else
    p = malloc(size+PREFIX_SIZE);
#endif
    if (p == NULL) {
        log_error("malloc(%zu) failed @ %s:%d", size, name, line);
    } else {
#ifdef HAVE_MALLOC_SIZE
        update_dmalloc_stat_alloc(dmalloc_size(p));
        return p;
#else
        *((size_t*)p) = size;
        update_dmalloc_stat_alloc(size+PREFIX_SIZE);
        return (char*)p+PREFIX_SIZE;
#endif
        log_debug(LOG_VVERB, "malloc(%zu) at %p @ %s:%d", size, p, name, line);
    }

    return p;
}

void *
_dzalloc(size_t size, const char *name, int line)
{
    void *p;

    p = _dalloc(size, name, line);
    if (p != NULL) {
        memset(p, 0, size);
    }

    return p;
}

void *
_dcalloc(size_t nmemb, size_t size, const char *name, int line)
{
    return _dzalloc(nmemb * size, name, line);
}

void *
_drealloc(void *ptr, size_t size, const char *name, int line)
{
#ifndef HAVE_MALLOC_SIZE
    void *realp;
#endif
    void *p;
    size_t oldsize;

    ASSERT(size != 0);

    if (ptr == NULL) return _dalloc(size, name, line);

#ifdef HAVE_MALLOC_SIZE
    oldsize = dmalloc_size(ptr);
#ifdef DUSE_JEMALLOC
    p = je_realloc(ptr, size);
#else
    p = realloc(ptr, size);
#endif
#else
    realp = (char*)ptr-PREFIX_SIZE;
    oldsize = *((size_t*)realp);
#ifdef DUSE_JEMALLOC
    p = je_realloc(ptr, size+PREFIX_SIZE);
#else
    p = realloc(ptr, size+PREFIX_SIZE);
#endif
#endif
    if (p == NULL) {
        log_error("realloc(%zu) failed @ %s:%d", size, name, line);
        return NULL;
    } else {
        log_debug(LOG_VVERB, "realloc(%zu) at %p @ %s:%d", size, p, name, line);
#ifdef HAVE_MALLOC_SIZE
        update_dmalloc_stat_free(oldsize);
        update_dmalloc_stat_alloc(dmalloc_size(p));
        return p;
#else
        *((size_t*)p) = size;
        update_dmalloc_stat_free(oldsize);
        update_dmalloc_stat_alloc(size);
        return p+PREFIX_SIZE;
#endif
    }

    return NULL;
}

void
_dfree(void *ptr, const char *name, int line)
{
#ifndef HAVE_MALLOC_SIZE
    void *realp;
    size_t oldsize;
#endif

    ASSERT(ptr != NULL);
    log_debug(LOG_VVERB, "free(%p) @ %s:%d", ptr, name, line);

#ifdef HAVE_MALLOC_SIZE
    update_dmalloc_stat_free(dmalloc_size(ptr));
#ifdef DUSE_JEMALLOC
    je_free(ptr);
#else
    free(ptr);
#endif
#else
    realp = (char*)ptr-PREFIX_SIZE;
    oldsize = *((size_t*)realp);
    update_dmalloc_stat_free(oldsize+PREFIX_SIZE);
    free(realp);
#ifdef DUSE_JEMALLOC
    je_free(realp);
#else
    free(realp);
#endif
#endif
}

size_t
dalloc_used_memory(void)
{
    size_t um;

#if defined(__ATOMIC_RELAXED) || defined(HAVE_ATOMIC)
    um = update_used_mem_stat_add(0);
#else
    pthread_mutex_lock(&used_memory_mutex);
    um = used_memory;
    pthread_mutex_unlock(&used_memory_mutex);
#endif

    return um;
}

/* Returns the size of physical memory (RAM) in bytes.
 * It looks ugly, but this is the cleanest way to achive cross platform results.
 * Cleaned up from:
 *
 * http://nadeausoftware.com/articles/2012/09/c_c_tip_how_get_physical_memory_size_system
 *
 * Note that this function:
 * 1) Was released under the following CC attribution license:
 *    http://creativecommons.org/licenses/by/3.0/deed.en_US.
 * 2) Was originally implemented by David Robert Nadeau.
 * 3) Was modified for Redis by Matt Stancliff.
 * 4) This note exists in order to comply with the original license.
 */
size_t dalloc_get_memory_size(void) {
#if defined(__unix__) || defined(__unix) || defined(unix) || \
    (defined(__APPLE__) && defined(__MACH__))
#if defined(CTL_HW) && (defined(HW_MEMSIZE) || defined(HW_PHYSMEM64))
    int mib[2];
    mib[0] = CTL_HW;
#if defined(HW_MEMSIZE)
    mib[1] = HW_MEMSIZE;            /* OSX. --------------------- */
#elif defined(HW_PHYSMEM64)
    mib[1] = HW_PHYSMEM64;          /* NetBSD, OpenBSD. --------- */
#endif
    int64_t size = 0;               /* 64-bit */
    size_t len = sizeof(size);
    if (sysctl( mib, 2, &size, &len, NULL, 0) == 0)
        return (size_t)size;
    return 0L;          /* Failed? */

#elif defined(_SC_PHYS_PAGES) && defined(_SC_PAGESIZE)
    /* FreeBSD, Linux, OpenBSD, and Solaris. -------------------- */
    return (size_t)sysconf(_SC_PHYS_PAGES) * (size_t)sysconf(_SC_PAGESIZE);

#elif defined(CTL_HW) && (defined(HW_PHYSMEM) || defined(HW_REALMEM))
    /* DragonFly BSD, FreeBSD, NetBSD, OpenBSD, and OSX. -------- */
    int mib[2];
    mib[0] = CTL_HW;
#if defined(HW_REALMEM)
    mib[1] = HW_REALMEM;        /* FreeBSD. ----------------- */
#elif defined(HW_PYSMEM)
    mib[1] = HW_PHYSMEM;        /* Others. ------------------ */
#endif
    unsigned int size = 0;      /* 32-bit */
    size_t len = sizeof(size);
    if (sysctl(mib, 2, &size, &len, NULL, 0) == 0)
        return (size_t)size;
    return 0L;          /* Failed? */
#endif /* sysctl and sysconf variants */

#else
    return 0L;          /* Unknown OS. */
#endif
}

/* Get the RSS information in an OS-specific way.
 *
 * WARNING: the function zmalloc_get_rss() is not designed to be fast
 * and may not be called in the busy loops where Redis tries to release
 * memory expiring or swapping out objects.
 *
 * For this kind of "fast RSS reporting" usages use instead the
 * function RedisEstimateRSS() that is a much faster (and less precise)
 * version of the function. */

#if defined(HAVE_PROC_STAT)
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

size_t dalloc_get_rss(void) {
    int page = sysconf(_SC_PAGESIZE);
    size_t rss;
    char buf[4096];
    char filename[256];
    int fd, count;
    char *p, *x;

    snprintf(filename,256,"/proc/%d/stat",getpid());
    if ((fd = open(filename,O_RDONLY)) == -1) return 0;
    if (read(fd,buf,4096) <= 0) {
        close(fd);
        return 0;
    }
    close(fd);

    p = buf;
    count = 23; /* RSS is the 24th field in /proc/<pid>/stat */
    while(p && count--) {
        p = strchr(p,' ');
        if (p) p++;
    }
    if (!p) return 0;
    x = strchr(p,' ');
    if (!x) return 0;
    *x = '\0';

    rss = strtoll(p,NULL,10);
    rss *= page;
    return rss;
}
#elif defined(HAVE_TASKINFO)
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/sysctl.h>
#include <mach/task.h>
#include <mach/mach_init.h>

size_t dalloc_get_rss(void) {
    task_t task = MACH_PORT_NULL;
    struct task_basic_info t_info;
    mach_msg_type_number_t t_info_count = TASK_BASIC_INFO_COUNT;

    if (task_for_pid(current_task(), getpid(), &task) != KERN_SUCCESS)
        return 0;
    task_info(task, TASK_BASIC_INFO, (task_info_t)&t_info, &t_info_count);

    return t_info.resident_size;
}
#else
size_t dalloc_get_rss(void) {
    /* If we can't get the RSS in an OS-specific way for this system just
     * return the memory usage we estimated in dalloc()..
     *
     * Fragmentation will appear to be always 1 (no fragmentation)
     * of course... */
    return dalloc_used_memory();
}
#endif

/* Fragmentation = RSS / allocated-bytes */
float dalloc_get_fragmentation_ratio(size_t rss) {
    return (float)rss/dalloc_used_memory();
}
