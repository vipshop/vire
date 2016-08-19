#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>
#include <string.h>
#include <ctype.h>
#include <limits.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <hiredis.h>

#include <vrt_util.h>

void
vrt_assert(const char *cond, const char *file, int line, int panic)
{
    test_log_error("assert '%s' failed @ (%s, %d)", cond, file, line);
    if (panic) {
        abort();
    }
}

int
vrt_vscnprintf(char *buf, size_t size, const char *fmt, va_list args)
{
    int n;

    n = vsnprintf(buf, size, fmt, args);

    /*
     * The return value is the number of characters which would be written
     * into buf not including the trailing '\0'. If size is == 0 the
     * function returns 0.
     *
     * On error, the function also returns 0. This is to allow idiom such
     * as len += _vscnprintf(...)
     *
     * See: http://lwn.net/Articles/69419/
     */
    if (n <= 0) {
        return 0;
    }

    if (n < (int) size) {
        return n;
    }

    return (int)(size - 1);
}

int
vrt_scnprintf(char *buf, size_t size, const char *fmt, ...)
{
    va_list args;
    int n;

    va_start(args, fmt);
    n = vrt_vscnprintf(buf, size, fmt, args);
    va_end(args);

    return n;
}

void
_test_log_error(const char *file, int line, const char *fmt, ...)
{
    int len, size, errno_save;
    char buf[LOG_MAX_LEN];
    va_list args;
    
    errno_save = errno;
    len = 0;            /* length of output buffer */
    size = LOG_MAX_LEN; /* size of output buffer */
    
    len += vrt_scnprintf(buf + len, size - len, "%s:%d ", file, line);

    va_start(args, fmt);
    len += vsnprintf(buf + len, size - len, fmt, args);
    va_end(args);

    buf[len++] = '\n';

    write(STDERR_FILENO, buf, len);
    
    errno = errno_save;
}

void
_test_log_out(const char *fmt, ...)
{
    int len, size, errno_save;
    char buf[LOG_MAX_LEN];
    va_list args;
    
    errno_save = errno;
    len = 0;            /* length of output buffer */
    size = LOG_MAX_LEN; /* size of output buffer */

    va_start(args, fmt);
    len += vsnprintf(buf + len, size - len, fmt, args);
    va_end(args);

    buf[len++] = '\n';

    write(STDOUT_FILENO, buf, len);
    
    errno = errno_save;
}

/*
 * Return the current time in microseconds since Epoch
 */
int64_t
vrt_usec_now(void)
{
    struct timeval now;
    int64_t usec;
    int status;

    status = gettimeofday(&now, NULL);
    if (status < 0) {
        return -1;
    }

    usec = (int64_t)now.tv_sec * 1000000LL + (int64_t)now.tv_usec;

    return usec;
}

/*
 * Return the current time in milliseconds since Epoch
 */
int64_t
vrt_msec_now(void)
{
    return vrt_usec_now() / 1000LL;
}

/*
 * Return the current time in seconds since Epoch
 */
int64_t
vrt_sec_now(void)
{
    return vrt_usec_now() / 1000000LL;
}

/* Given the filename, return the absolute path as an SDS string, or NULL
 * if it fails for some reason. Note that "filename" may be an absolute path
 * already, this will be detected and handled correctly.
 *
 * The function does not try to normalize everything, but only the obvious
 * case of one or more "../" appearning at the start of "filename"
 * relative path. */
sds getAbsolutePath(char *filename) {
    char cwd[1024];
    sds abspath;
    sds relpath = sdsnew(filename);

    sdstrim(relpath," \r\n\t");
    if (relpath[0] == '/') return relpath; /* Path is already absolute. */

    /* If path is relative, join cwd and relative path. */
    if (getcwd(cwd,sizeof(cwd)) == NULL) {
        sdsfree(relpath);
        return NULL;
    }
    abspath = sdsnew(cwd);
    if (sdslen(abspath) && abspath[sdslen(abspath)-1] != '/')
        abspath = sdscat(abspath,"/");

    /* At this point we have the current path always ending with "/", and
     * the trimmed relative path. Try to normalize the obvious case of
     * trailing ../ elements at the start of the path.
     *
     * For every "../" we find in the filename, we remove it and also remove
     * the last element of the cwd, unless the current cwd is "/". */
    while (sdslen(relpath) >= 3 &&
           relpath[0] == '.' && relpath[1] == '.' && relpath[2] == '/')
    {
        sdsrange(relpath,3,-1);
        if (sdslen(abspath) > 1) {
            char *p = abspath + sdslen(abspath)-2;
            int trimlen = 1;

            while(*p != '/') {
                p--;
                trimlen++;
            }
            sdsrange(abspath,0,-(trimlen+1));
        }
    }

    /* Finally glue the two parts together. */
    abspath = sdscatsds(abspath,relpath);
    sdsfree(relpath);
    return abspath;
}

/* Return the number of digits of 'v' when converted to string in radix 10.
 * See ll2string() for more information. */
uint32_t digits10(uint64_t v) {
    if (v < 10) return 1;
    if (v < 100) return 2;
    if (v < 1000) return 3;
    if (v < 1000000000000UL) {
        if (v < 100000000UL) {
            if (v < 1000000) {
                if (v < 10000) return 4;
                return 5 + (v >= 100000);
            }
            return 7 + (v >= 10000000UL);
        }
        if (v < 10000000000UL) {
            return 9 + (v >= 1000000000UL);
        }
        return 11 + (v >= 100000000000UL);
    }
    return 12 + digits10(v / 1000000000000UL);
}

/* Like digits10() but for signed values. */
uint32_t sdigits10(int64_t v) {
    if (v < 0) {
        /* Abs value of LLONG_MIN requires special handling. */
        uint64_t uv = (v != LLONG_MIN) ?
                      (uint64_t)-v : ((uint64_t) LLONG_MAX)+1;
        return digits10(uv)+1; /* +1 for the minus. */
    } else {
        return digits10(v);
    }
}

/* Convert a long long into a string. Returns the number of
 * characters needed to represent the number.
 * If the buffer is not big enough to store the string, 0 is returned.
 *
 * Based on the following article (that apparently does not provide a
 * novel approach but only publicizes an already used technique):
 *
 * https://www.facebook.com/notes/facebook-engineering/three-optimization-tips-for-c/10151361643253920
 *
 * Modified in order to handle signed integers since the original code was
 * designed for unsigned integers. */
int ll2string(char* dst, size_t dstlen, long long svalue) {
    static const char digits[201] =
        "0001020304050607080910111213141516171819"
        "2021222324252627282930313233343536373839"
        "4041424344454647484950515253545556575859"
        "6061626364656667686970717273747576777879"
        "8081828384858687888990919293949596979899";
    int negative;
    unsigned long long value;

    /* The main loop works with 64bit unsigned integers for simplicity, so
     * we convert the number here and remember if it is negative. */
    if (svalue < 0) {
        if (svalue != LLONG_MIN) {
            value = -svalue;
        } else {
            value = ((unsigned long long) LLONG_MAX)+1;
        }
        negative = 1;
    } else {
        value = svalue;
        negative = 0;
    }

    /* Check length. */
    uint32_t const length = digits10(value)+negative;
    if (length >= dstlen) return 0;

    /* Null term. */
    uint32_t next = length;
    dst[next] = '\0';
    next--;
    while (value >= 100) {
        int const i = (value % 100) * 2;
        value /= 100;
        dst[next] = digits[i + 1];
        dst[next - 1] = digits[i];
        next -= 2;
    }

    /* Handle last 1-2 digits. */
    if (value < 10) {
        dst[next] = '0' + (uint32_t) value;
    } else {
        int i = (uint32_t) value * 2;
        dst[next] = digits[i + 1];
        dst[next - 1] = digits[i];
    }

    /* Add sign. */
    if (negative) dst[0] = '-';
    return length;
}

/* Convert a string into a long long. Returns 1 if the string could be parsed
 * into a (non-overflowing) long long, 0 otherwise. The value will be set to
 * the parsed value when appropriate. */
int string2ll(const char *s, size_t slen, long long *value) {
    const char *p = s;
    size_t plen = 0;
    int negative = 0;
    unsigned long long v;

    if (plen == slen)
        return 0;

    /* Special case: first and only digit is 0. */
    if (slen == 1 && p[0] == '0') {
        if (value != NULL) *value = 0;
        return 1;
    }

    if (p[0] == '-') {
        negative = 1;
        p++; plen++;

        /* Abort on only a negative sign. */
        if (plen == slen)
            return 0;
    }

    /* First digit should be 1-9, otherwise the string should just be 0. */
    if (p[0] >= '1' && p[0] <= '9') {
        v = p[0]-'0';
        p++; plen++;
    } else if (p[0] == '0' && slen == 1) {
        *value = 0;
        return 1;
    } else {
        return 0;
    }

    while (plen < slen && p[0] >= '0' && p[0] <= '9') {
        if (v > (ULLONG_MAX / 10)) /* Overflow. */
            return 0;
        v *= 10;

        if (v > (ULLONG_MAX - (p[0]-'0'))) /* Overflow. */
            return 0;
        v += p[0]-'0';

        p++; plen++;
    }

    /* Return if not all bytes were used. */
    if (plen < slen)
        return 0;

    if (negative) {
        if (v > ((unsigned long long)(-(LLONG_MIN+1))+1)) /* Overflow. */
            return 0;
        if (value != NULL) *value = -v;
    } else {
        if (v > LLONG_MAX) /* Overflow. */
            return 0;
        if (value != NULL) *value = v;
    }
    return 1;
}

/* Convert a string into a long. Returns 1 if the string could be parsed into a
 * (non-overflowing) long, 0 otherwise. The value will be set to the parsed
 * value when appropriate. */
int string2l(const char *s, size_t slen, long *lval) {
    long long llval;

    if (!string2ll(s,slen,&llval))
        return 0;

    if (llval < LONG_MIN || llval > LONG_MAX)
        return 0;

    *lval = (long)llval;
    return 1;
}

/* Convert a double to a string representation. Returns the number of bytes
 * required. The representation should always be parsable by strtod(3). */
int d2string(char *buf, size_t len, double value) {
    if (isnan(value)) {
        len = snprintf(buf,len,"nan");
    } else if (isinf(value)) {
        if (value < 0)
            len = snprintf(buf,len,"-inf");
        else
            len = snprintf(buf,len,"inf");
    } else if (value == 0) {
        /* See: http://en.wikipedia.org/wiki/Signed_zero, "Comparisons". */
        if (1.0/value < 0)
            len = snprintf(buf,len,"-0");
        else
            len = snprintf(buf,len,"0");
    } else {
#if (DBL_MANT_DIG >= 52) && (LLONG_MAX == 0x7fffffffffffffffLL)
        /* Check if the float is in a safe range to be casted into a
         * long long. We are assuming that long long is 64 bit here.
         * Also we are assuming that there are no implementations around where
         * double has precision < 52 bit.
         *
         * Under this assumptions we test if a double is inside an interval
         * where casting to long long is safe. Then using two castings we
         * make sure the decimal part is zero. If all this is true we use
         * integer printing function that is much faster. */
        double min = -4503599627370495; /* (2^52)-1 */
        double max = 4503599627370496; /* -(2^52) */
        if (value > min && value < max && value == ((double)((long long)value)))
            len = ll2string(buf,len,(long long)value);
        else
#endif
            len = snprintf(buf,len,"%.17g",value);
    }

    return len;
}

int create_dir(char *path)
{
    if (mkdir(path,0755) < 0) {
        return VRT_ERROR;
    }

    return VRT_OK;
}

int destroy_dir(char *path)
{  
    DIR *dp;
    struct dirent *entry;
    struct stat statbuf;
    char cwd[1024];

    if (getcwd(cwd,sizeof(cwd)) == NULL) {
        return VRT_ERROR;
    }
    
    if ((dp = opendir(path)) == NULL) {  
        test_log_error("Can't open dir: %s", path);  
        return VRT_ERROR;  
    }
    
    chdir (path);
    while ((entry = readdir(dp)) != NULL) {
        lstat(entry->d_name, &statbuf);
        if (S_IFDIR & statbuf.st_mode) {
            if (strcmp(".", entry->d_name) == 0 || strcmp("..", entry->d_name) == 0)
                continue;

            destroy_dir(entry->d_name);
        } else {
            remove(entry->d_name);
        }
    }
    
    chdir(cwd);
    closedir(dp);

    remove(path);
    return VRT_OK;
}

int get_pid_from_reply(redisContext *redisctx, char *host, int port)
{
    redisContext *ctx = redisctx;
    redisReply * reply;
    sds *lines;
    size_t line_len;
    int count, j;
    int pid = -1;
    
    if (ctx == NULL) {
        ctx = redisConnect(host,port);
    }

    if (ctx == NULL) {
        test_log_error("Get pid from instance failed: can't connect to %s:%d",host,port);
        return -1;
    }

    reply = redisCommand(ctx, "info server");
    if (reply == NULL) {
        test_log_error("Execute 'info server' command on vire failed: %s\n",
            ctx->err?ctx->errstr:"");
        if (redisctx == NULL) redisFree(ctx);
        return -1;
    }

    if (reply->type != REDIS_REPLY_STRING) {
        test_log_error("Reply for 'info server' command from vire type %d is error",
            reply->type);
        if (redisctx == NULL) redisFree(ctx);
        freeReplyObject(reply);
        return -1;
    }

    lines = sdssplitlen(reply->str,reply->len,"\r\n",2,&count);
    if (lines == NULL) {
        test_log_error("Reply for 'info server' command from vire is error");
        if (redisctx == NULL) redisFree(ctx);
        freeReplyObject(reply);
        return -1;
    }

    for (j = 0; j < count; j ++) {
        line_len = sdslen(lines[j]);
        if (line_len > 11 && !strncmp("process_id", lines[j], 10)) {
            if (string2l(lines[j]+11,line_len-11,&pid) == 0) {
                test_log_error("Convert pid string %.*s to long failed",
                    line_len-11,lines[j]+11);
                sdsfreesplitres(lines,count);
                if (redisctx == NULL) redisFree(ctx);
                freeReplyObject(reply);
                return -1;
            }
            break;
        }
    }

    sdsfreesplitres(lines,count);
    if (redisctx == NULL) redisFree(ctx);
    freeReplyObject(reply);

    return pid;
}

/* Range is like 0-100 or just 10. 
  * So the count must be 1 or 2. */
long long *get_range_from_string(char *str, size_t len, int *count)
{
    int elem_count;
    sds *elems;
    long long value;
    long long *range;
    
    elems = sdssplitlen(optarg,strlen(optarg),"-",1,&elem_count);
    if (elems == NULL) {
        goto error;
    } else if (elem_count <= 0 || elem_count >= 3) {
        sdsfreesplitres(elems,elem_count);
        goto error;
    }

    if (elem_count == 1) {
        if (string2ll(elems[0],sdslen(elems[0]),&value) != 1) {
            sdsfreesplitres(elems,elem_count);
            goto error;
        }

        range = malloc(1*sizeof(*range));
        range[0] = value;
        *count = 1;
    } else if (elem_count == 2) {
        if (string2ll(elems[0],sdslen(elems[0]),&value) != 1) {
            sdsfreesplitres(elems,elem_count);
            goto error;
        }
        
        range = malloc(2*sizeof(*range));
        range[0] = value;

        if (string2ll(elems[1],sdslen(elems[1]),&value) != 1) {
            sdsfreesplitres(elems,elem_count);
            free(range);
            goto error;
        }

        range[1] = value;
        *count = 2;

        if (range[0] > range[1]) {
            sdsfreesplitres(elems,elem_count);
            free(range);
            goto error;
        }
    }

    sdsfreesplitres(elems,elem_count);

    return range;

error:

    *count = -1;
    return NULL;
}

sds get_host_port_from_address_string(char *address, int *port)
{
    sds *host_port;
    int count = 0;
    sds host;
    long value;

    *port = 0;
    
    host_port = sdssplitlen(address,strlen(address),":",1,&count);
    if (host_port == NULL) {
        return NULL;
    } else if (count != 2) {
        sdsfreesplitres(host_port,count);
        return NULL;
    }

    if (string2l(host_port[1],sdslen(host_port[1]),&value) != 1 || 
        value <= 0 || value >= 65535) {
        sdsfreesplitres(host_port,count);
        return NULL;
    }

    *port = (int)value;
    host = host_port[0];
    host_port[0] = NULL;
    sdsfreesplitres(host_port,count);
    
    return host;
}
