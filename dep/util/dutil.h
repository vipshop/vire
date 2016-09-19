#ifndef _DUTIL_H_
#define _DUTIL_H_

#include <stdarg.h>

#include <dspecialconfig.h>

#define UNUSED(x) (void)(x)

#define LF                  (uint8_t) 10
#define CR                  (uint8_t) 13
#define CRLF                "\x0d\x0a"
#define CRLF_LEN            (sizeof("\x0d\x0a") - 1)

#define NELEMS(a)           ((sizeof(a)) / sizeof((a)[0]))

#define MIN(a, b)           ((a) < (b) ? (a) : (b))
#define MAX(a, b)           ((a) > (b) ? (a) : (b))

#define SQUARE(d)           ((d) * (d))
#define VAR(s, s2, n)       (((n) < 2) ? 0.0 : ((s2) - SQUARE(s)/(n)) / ((n) - 1))
#define STDDEV(s, s2, n)    (((n) < 2) ? 0.0 : sqrt(VAR((s), (s2), (n))))

/*
 * Wrappers for defining custom assert based on whether macro
 * RMT_ASSERT_PANIC or RMT_ASSERT_LOG was defined at the moment
 * ASSERT was called.
 */
#ifdef HAVE_ASSERT_PANIC

#define ASSERT(_x) do {                         \
    if (!(_x)) {                                \
        dassert(#_x, __FILE__, __LINE__, 1);  \
    }                                           \
} while (0)

#define NOT_REACHED() ASSERT(0)

#elif HAVE_ASSERT_LOG

#define ASSERT(_x) do {                         \
    if (!(_x)) {                                \
        dassert(#_x, __FILE__, __LINE__, 0);  \
    }                                           \
} while (0)

#define NOT_REACHED() ASSERT(0)

#else

#define ASSERT(_x)

#define NOT_REACHED()

#endif

void dassert(const char *cond, const char *file, int line, int panic);
void dstacktrace(int skip_count);
void dstacktrace_fd(int fd);

int _dscnprintf(char *buf, size_t size, const char *fmt, ...);
int _dvscnprintf(char *buf, size_t size, const char *fmt, va_list args);
long long dusec_now(void);
long long dmsec_now(void);
long long dsec_now(void);

/*
 * A (very) limited version of snprintf
 * @param   to   Destination buffer
 * @param   n    Size of destination buffer
 * @param   fmt  printf() style format string
 * @returns Number of bytes written, including terminating '\0'
 * Supports 'd' 'i' 'u' 'x' 'p' 's' conversion
 * Supports 'l' and 'll' modifiers for integral types
 * Does not support any width/precision
 * Implemented with simplicity, and async-signal-safety in mind
 */
int _safe_vsnprintf(char *to, size_t size, const char *format, va_list ap);
int _safe_snprintf(char *to, size_t n, const char *fmt, ...);

#define dsafe_snprintf(_s, _n, ...)       \
    _safe_snprintf((char *)(_s), (size_t)(_n), __VA_ARGS__)

#define dsafe_vsnprintf(_s, _n, _f, _a)   \
    _safe_vsnprintf((char *)(_s), (size_t)(_n), _f, _a)

/*
 * snprintf(s, n, ...) will write at most n - 1 of the characters printed into
 * the output string; the nth character then gets the terminating `\0'; if
 * the return value is greater than or equal to the n argument, the string
 * was too short and some of the printed characters were discarded; the output
 * is always null-terminated.
 *
 * Note that, the return value of snprintf() is always the number of characters
 * that would be printed into the output string, assuming n were limited not
 * including the trailing `\0' used to end output.
 *
 * scnprintf(s, n, ...) is same as snprintf() except, it returns the number
 * of characters printed into the output string not including the trailing '\0'
 */
#define dsnprintf(_s, _n, ...)        \
    snprintf((char *)(_s), (size_t)(_n), __VA_ARGS__)

#define dscnprintf(_s, _n, ...)       \
    _dscnprintf((char *)(_s), (size_t)(_n), __VA_ARGS__)

#define dvsnprintf(_s, _n, _f, _a)    \
    vsnprintf((char *)(_s), (size_t)(_n), _f, _a)

#define dvscnprintf(_s, _n, _f, _a)   \
    _dvscnprintf((char *)(_s), (size_t)(_n), _f, _a)

#define dstrftime(_s, _n, fmt, tm)        \
    (int)strftime((char *)(_s), (size_t)(_n), fmt, tm)

int string_match_len(const char *pattern, int patternLen, const char *string, int stringLen, int nocase);
int string_match(const char *pattern, const char *string, int nocase);


/* Atomic API */
/* GCC version >= 4.7 */
#if defined(__ATOMIC_RELAXED)
#define atomic_add(_value, _n) __atomic_add_fetch(&_value, (_n), __ATOMIC_RELAXED)
#define atomic_sub(_value, _n) __atomic_sub_fetch(&_value, (_n), __ATOMIC_RELAXED)
#define atomic_set(_value, _n) __atomic_store_n(&_value, (_n), __ATOMIC_RELAXED)
#define atomic_get(_value, _v) do {                 \
    __atomic_load(&_value, _v, __ATOMIC_RELAXED);   \
} while(0)

#define ATOMIC_LOCK_TYPE "__ATOMIC_RELAXED"
/* GCC version >= 4.1 */
#elif defined(HAVE_ATOMIC)
#define atomic_add(_value, _n) __sync_add_and_fetch(&_value, (_n))
#define atomic_sub(_value, _n) __sync_sub_and_fetch(&_value, (_n))
#define atomic_set(_value, _n) __sync_lock_test_and_set(&_value, (_n))
#define atomic_get(_value, _v) do {                 \
    (*_v) = __sync_add_and_fetch(&_value, 0);       \
} while(0)

#define ATOMIC_LOCK_TYPE "HAVE_ATOMIC"
#else
extern pthread_mutex_t atomic_locker;

#define atomic_add(_value, _n) do {         \
    pthread_mutex_lock(&atomic_locker);     \
    _value += (_n);                         \
    pthread_mutex_unlock(&atomic_locker);   \
} while(0)

#define atomic_sub(_value, _n) do {         \
    pthread_mutex_lock(&atomic_locker);     \
    _value -= (_n);                         \
    pthread_mutex_unlock(&atomic_locker);   \
} while(0)

#define atomic_set(_value, _n) do {         \
    pthread_mutex_lock(&atomic_locker);     \
    _value = (_n);                          \
    pthread_mutex_unlock(&atomic_locker);   \
} while(0)

#define atomic_get(_value, _v) do {         \
    pthread_mutex_lock(&atomic_locker);     \
    (*_v) = _value;                         \
    pthread_mutex_unlock(&atomic_locker);   \
} while(0)

#define ATOMIC_LOCK_TYPE "pthread_mutex_lock"
#endif

#endif
