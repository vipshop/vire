#ifndef _VR_UTIL_H_
#define _VR_UTIL_H_

#include <stdarg.h>
#include <stdint.h>
#include <stdbool.h>

#include <netinet/in.h>
#include <sys/un.h>

/* Double expansion needed for stringification of macro values. */
#define __xstr(s) __str(s)
#define __str(s) #s

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
    _vr_atoi((char *)_line, (size_t)_n)

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

int _vr_atoi(char *line, size_t n);
uint32_t digits10(uint64_t v);
uint32_t sdigits10(int64_t v);
int ll2string(char* dst, size_t dstlen, long long svalue);
int string2ll(const char *s, size_t slen, long long *value);
int string2l(const char *s, size_t slen, long *lval);
int d2string(char *buf, size_t len, double value);

bool vr_valid_port(int n);

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

#endif
