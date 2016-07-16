#ifndef _VR_UTIL_H_
#define _VR_UTIL_H_

#include <stdarg.h>
#include <stdint.h>
#include <stdbool.h>

#include <netinet/in.h>
#include <sys/un.h>

#include <vr_sds.h>

/* Double expansion needed for stringification of macro values. */
#define __xstr(s) __str(s)
#define __str(s) #s

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

#define VR_INET4_ADDRSTRLEN (sizeof("255.255.255.255") - 1)
#define VR_INET6_ADDRSTRLEN \
    (sizeof("ffff:ffff:ffff:ffff:ffff:ffff:255.255.255.255") - 1)
#define VR_INET_ADDRSTRLEN  MAX(VR_INET4_ADDRSTRLEN, VR_INET6_ADDRSTRLEN)
#define VR_UNIX_ADDRSTRLEN  \
    (sizeof(struct sockaddr_un) - offsetof(struct sockaddr_un, sun_path))
    
#define VR_INET_PEER_ID_LEN (VR_INET_ADDRSTRLEN+32) /* Must be enough for ip:port */

#define VR_MAXHOSTNAMELEN   256

/*
 * Length of 1 byte, 2 bytes, 4 bytes, 8 bytes and largest integral
 * type (uintmax_t) in ascii, including the null terminator '\0'
 *
 * From stdint.h, we have:
 * # define UINT8_MAX	(255)
 * # define UINT16_MAX	(65535)
 * # define UINT32_MAX	(4294967295U)
 * # define UINT64_MAX	(__UINT64_C(18446744073709551615))
 */
#define VR_UINT8_MAXLEN     (3 + 1)
#define VR_UINT16_MAXLEN    (5 + 1)
#define VR_UINT32_MAXLEN    (10 + 1)
#define VR_UINT64_MAXLEN    (20 + 1)
#define VR_UINTMAX_MAXLEN   VR_UINT64_MAXLEN

#define LONG_STR_SIZE       21  /* Bytes needed for long -> str */

/*
 * Make data 'd' or pointer 'p', n-byte aligned, where n is a power of 2
 * of 2.
 */
#define VR_ALIGNMENT        sizeof(unsigned long) /* platform word */
#define VR_ALIGN(d, n)      (((d) + (n - 1)) & ~(n - 1))
#define VR_ALIGN_PTR(p, n)  \
    (void *) (((uintptr_t) (p) + ((uintptr_t) n - 1)) & ~((uintptr_t) n - 1))

/*
 * Wrapper to workaround well known, safe, implicit type conversion when
 * invoking system calls.
 */
#define vr_gethostname(_name, _len) \
    gethostname((char *)_name, (size_t)_len)

#define vr_atoi(_line, _n)          \
    _vr_atoi((uint8_t *)_line, (size_t)_n)

int vr_set_blocking(int sd);
int vr_set_nonblocking(int sd);
int vr_set_reuseaddr(int sd);
int vr_set_tcpnodelay(int sd);
int vr_set_linger(int sd, int timeout);
int vr_set_sndbuf(int sd, int size);
int vr_set_rcvbuf(int sd, int size);
int vr_get_soerror(int sd);
int vr_get_sndbuf(int sd);
int vr_get_rcvbuf(int sd);
int vr_set_tcpkeepalive(int sd, int keepidle, int keepinterval, int keepcount);

int _vr_atoi(uint8_t *line, size_t n);
uint32_t digits10(uint64_t v);
uint32_t sdigits10(int64_t v);
int ll2string(char* dst, size_t dstlen, long long svalue);
int string2ll(const char *s, size_t slen, long long *value);
int string2l(const char *s, size_t slen, long *lval);
int d2string(char *buf, size_t len, double value);

bool vr_valid_port(int n);

/*
 * Memory allocation and free wrappers.
 *
 * These wrappers enables us to loosely detect double free, dangling
 * pointer access and zero-byte alloc.
 */
#if defined(VR_USE_JEMALLOC)
#define VR_MALLOC_LIB ("jemalloc-" __xstr(JEMALLOC_VERSION_MAJOR) "." __xstr(JEMALLOC_VERSION_MINOR) "." __xstr(JEMALLOC_VERSION_BUGFIX))
#include <jemalloc/jemalloc.h>
#if (JEMALLOC_VERSION_MAJOR == 2 && JEMALLOC_VERSION_MINOR >= 1) || (JEMALLOC_VERSION_MAJOR > 2)
#define HAVE_MALLOC_SIZE 1
#define vr_malloc_size(p) je_malloc_usable_size(p)
#else
#error "Newer version of jemalloc required"
#endif
#elif defined(__APPLE__)
#include <malloc/malloc.h>
#define HAVE_MALLOC_SIZE 1
#define vr_malloc_size(p) malloc_size(p)
#endif
    
#ifndef VR_MALLOC_LIB
#define VR_MALLOC_LIB "libc"
#endif

#define vr_alloc(_s)                    \
    _vr_alloc((size_t)(_s), __FILE__, __LINE__)

#define vr_zalloc(_s)                   \
    _vr_zalloc((size_t)(_s), __FILE__, __LINE__)

#define vr_calloc(_n, _s)               \
    _vr_calloc((size_t)(_n), (size_t)(_s), __FILE__, __LINE__)

#define vr_realloc(_p, _s)              \
    _vr_realloc(_p, (size_t)(_s), __FILE__, __LINE__)

#define vr_free(_p) do {                \
    _vr_free(_p, __FILE__, __LINE__);   \
    (_p) = NULL;                        \
} while (0)

char *malloc_lock_type(void);

#ifndef HAVE_MALLOC_SIZE
size_t vr_malloc_size(void *ptr);
#endif

void *_vr_alloc(size_t size, const char *name, int line);
void *_vr_zalloc(size_t size, const char *name, int line);
void *_vr_calloc(size_t nmemb, size_t size, const char *name, int line);
void *_vr_realloc(void *ptr, size_t size, const char *name, int line);
void _vr_free(void *ptr, const char *name, int line);

size_t vr_alloc_used_memory(void);

/*
 * Wrappers to send or receive n byte message on a blocking
 * socket descriptor.
 */
#define vr_sendn(_s, _b, _n)    \
    _vr_sendn(_s, _b, (size_t)(_n))

#define vr_recvn(_s, _b, _n)    \
    _vr_recvn(_s, _b, (size_t)(_n))

/*
 * Wrappers to read or write data to/from (multiple) buffers
 * to a file or socket descriptor.
 */
#define vr_read(_d, _b, _n)     \
    read(_d, _b, (size_t)(_n))

#define vr_readv(_d, _b, _n)    \
    readv(_d, _b, (int)(_n))

#define vr_write(_d, _b, _n)    \
    write(_d, _b, (size_t)(_n))

#define vr_writev(_d, _b, _n)   \
    writev(_d, _b, (int)(_n))

ssize_t _vr_sendn(int sd, const void *vptr, size_t n);
ssize_t _vr_recvn(int sd, void *vptr, size_t n);

/*
 * Wrappers for defining custom assert based on whether macro
 * VR_ASSERT_PANIC or VR_ASSERT_LOG was defined at the moment
 * ASSERT was called.
 */
#ifdef VR_ASSERT_PANIC

#define ASSERT(_x) do {                         \
    if (!(_x)) {                                \
        vr_assert(#_x, __FILE__, __LINE__, 1);  \
    }                                           \
} while (0)

#define NOT_REACHED() ASSERT(0)

#elif VR_ASSERT_LOG

#define ASSERT(_x) do {                         \
    if (!(_x)) {                                \
        vr_assert(#_x, __FILE__, __LINE__, 0);  \
    }                                           \
} while (0)

#define NOT_REACHED() ASSERT(0)

#else

#define ASSERT(_x)

#define NOT_REACHED()

#endif

void vr_assert(const char *cond, const char *file, int line, int panic);
void vr_stacktrace(int skip_count);
void vr_stacktrace_fd(int fd);

int _scnprintf(char *buf, size_t size, const char *fmt, ...);
int _vscnprintf(char *buf, size_t size, const char *fmt, va_list args);
int64_t vr_usec_now(void);
int64_t vr_msec_now(void);

/*
 * Address resolution for internet (ipv4 and ipv6) and unix domain
 * socket address.
 */

struct sockinfo {
    int       family;              /* socket address family */
    socklen_t addrlen;             /* socket address length */
    union {
        struct sockaddr_in  in;    /* ipv4 socket address */
        struct sockaddr_in6 in6;   /* ipv6 socket address */
        struct sockaddr_un  un;    /* unix domain address */
    } addr;
};

int vr_resolve(sds name, int port, struct sockinfo *si);
int vr_net_format_peer(int fd, char *buf, size_t buf_len);

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

#define vr_safe_snprintf(_s, _n, ...)       \
    _safe_snprintf((char *)(_s), (size_t)(_n), __VA_ARGS__)

#define vr_safe_vsnprintf(_s, _n, _f, _a)   \
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
#define vr_snprintf(_s, _n, ...)        \
    snprintf((char *)(_s), (size_t)(_n), __VA_ARGS__)

#define vr_scnprintf(_s, _n, ...)       \
    _scnprintf((char *)(_s), (size_t)(_n), __VA_ARGS__)

#define vr_vsnprintf(_s, _n, _f, _a)    \
    vsnprintf((char *)(_s), (size_t)(_n), _f, _a)

#define vr_vscnprintf(_s, _n, _f, _a)   \
    _vscnprintf((char *)(_s), (size_t)(_n), _f, _a)

#define vr_strftime(_s, _n, fmt, tm)        \
    (int)strftime((char *)(_s), (size_t)(_n), fmt, tm)

void get_random_hex_chars(char *p, unsigned int len);

int stringmatchlen(const char *pattern, int patternLen, const char *string, int stringLen, int nocase);
int stringmatch(const char *pattern, const char *string, int nocase);

/*
 * Wrapper around common routines for manipulating C character
 * strings
 */
#define vr_memcpy(_d, _c, _n)           \
    memcpy(_d, _c, (size_t)(_n))

#define vr_memmove(_d, _c, _n)          \
    memmove(_d, _c, (size_t)(_n))

#define vr_memchr(_d, _c, _n)           \
    memchr(_d, _c, (size_t)(_n))

#define vr_strlen(_s)                   \
    strlen((char *)(_s))

#define vr_strncmp(_s1, _s2, _n)        \
    strncmp((char *)(_s1), (char *)(_s2), (size_t)(_n))

#define vr_strchr(_p, _l, _c)           \
    _vr_strchr((uint8_t *)(_p), (uint8_t *)(_l), (uint8_t)(_c))

#define vr_strrchr(_p, _s, _c)          \
    _vr_strrchr((uint8_t *)(_p),(uint8_t *)(_s), (uint8_t)(_c))

#define vr_strndup(_s, _n)              \
    (uint8_t *)strndup((char *)(_s), (size_t)(_n));

static inline uint8_t *
_vr_strchr(uint8_t *p, uint8_t *last, uint8_t c)
{
    while (p < last) {
        if (*p == c) {
            return p;
        }
        p++;
    }

    return NULL;
}

static inline uint8_t *
_vr_strrchr(uint8_t *p, uint8_t *start, uint8_t c)
{
    while (p >= start) {
        if (*p == c) {
            return p;
        }
        p--;
    }

    return NULL;
}

void memrev16(void *p);
void memrev32(void *p);
void memrev64(void *p);
uint16_t intrev16(uint16_t v);
uint32_t intrev32(uint32_t v);
uint64_t intrev64(uint64_t v);

/* variants of the function doing the actual convertion only if the target
 * host is big endian */
#ifdef VR_LITTLE_ENDIAN
#define memrev16ifbe(p)
#define memrev32ifbe(p)
#define memrev64ifbe(p)
#define intrev16ifbe(v) (v)
#define intrev32ifbe(v) (v)
#define intrev64ifbe(v) (v)
#else
#define memrev16ifbe(p) memrev16(p)
#define memrev32ifbe(p) memrev32(p)
#define memrev64ifbe(p) memrev64(p)
#define intrev16ifbe(v) intrev16(v)
#define intrev32ifbe(v) intrev32(v)
#define intrev64ifbe(v) intrev64(v)
#endif

long long memtoll(const char *p, int *err);
void bytesToHuman(char *s, unsigned long long n);

sds getAbsolutePath(char *filename);

size_t vr_alloc_get_memory_size(void);

size_t vr_alloc_get_rss(void);
float vr_alloc_get_fragmentation_ratio(size_t rss);

#endif
