#ifndef _VRT_UTIL_H_
#define _VRT_UTIL_H_

#ifdef HAVE_CONFIG_H
# include <config.h>
#endif

#ifdef HAVE_DEBUG_LOG
# define VRT_DEBUG_LOG 1
#endif

#include <dspecialconfig.h>

#include <sds.h>

#define VRT_OK        0
#define VRT_ERROR    -1

#define VRT_UINT8_MAXLEN     (3 + 1)
#define VRT_UINT16_MAXLEN    (5 + 1)
#define VRT_UINT32_MAXLEN    (10 + 1)
#define VRT_UINT64_MAXLEN    (20 + 1)
#define VRT_UINTMAX_MAXLEN   VRT_UINT64_MAXLEN

#define VRT_MAXHOSTNAMELEN   256

#define LF                  (uint8_t) 10
#define CR                  (uint8_t) 13
#define CRLF                "\x0d\x0a"
#define CRLF_LEN            (sizeof("\x0d\x0a") - 1)

#define LOG_MAX_LEN 256 /* max length of log message */

void _test_log_error(const char *file, int line, const char *fmt, ...);
void _test_log_out(const char *fmt, ...);
#if defined(VRT_DEBUG_LOG)
#define test_log_debug(...) do {                              \
    _test_log_error(__FILE__, __LINE__, __VA_ARGS__);         \
} while (0)
#else
#define test_log_debug(...)
#endif
#define test_log_error(...) do {                              \
    _test_log_error(__FILE__, __LINE__, __VA_ARGS__);         \
} while (0)
#define test_log_out(...) do {                                \
    _test_log_out(__VA_ARGS__);                               \
} while (0)

void vrt_assert(const char *cond, const char *file, int line, int panic);

#define ASSERT(_x) do {                         \
    if (!(_x)) {                                \
        vrt_assert(#_x, __FILE__, __LINE__, 1);  \
    }                                           \
} while (0)

int vrt_scnprintf(char *buf, size_t size, const char *fmt, ...);

int64_t vrt_usec_now(void);
int64_t vrt_msec_now(void);
int64_t vrt_sec_now(void);

sds getAbsolutePath(char *filename);

int ll2string(char* dst, size_t dstlen, long long svalue);
int string2ll(const char *s, size_t slen, long long *value);
int string2l(const char *s, size_t slen, long *lval);
int d2string(char *buf, size_t len, double value);

int create_dir(char *path);
int destroy_dir(char *dir);

int get_pid_from_reply(struct redisContext *redisctx, char *host, int port);

long long *get_range_from_string(char *str, size_t len, int *count);

sds get_host_port_from_address_string(char *address, int *port);

#endif
