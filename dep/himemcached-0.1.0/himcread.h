#ifndef _HIMC_READ_H_
#define _HIMC_READ_H_
#include <stdio.h> /* for size_t */

#include <himcread.h>

#define MC_ERR   -1
#define MC_OK    0

/* When an error occurs, the err flag in a context is set to hold the type of
 * error that occured. REDIS_ERR_IO means there was an I/O error and you
 * should use the "errno" variable to find out what is wrong.
 * For other values, the "errstr" field will hold a description. */
#define MC_ERR_IO 1 /* Error in read or write */
#define MC_ERR_EOF 3 /* End of file */
#define MC_ERR_PROTOCOL 4 /* Protocol error */
#define MC_ERR_OOM 5 /* Out of memory */
#define MC_ERR_OTHER 2 /* Everything else... */

#define MC_REPLY_STRING     1
#define MC_REPLY_ARRAY      2
#define MC_REPLY_INTEGER    3
#define MC_REPLY_NIL        4
#define MC_REPLY_STATUS     5
#define MC_REPLY_ERROR      6

#define MC_READER_MAX_BUF (1024*16)  /* Default max unused reader buffer. */

#ifdef __cplusplus
extern "C" {
#endif

typedef struct mcReplyObjectFunctions {
    void *(*createString)(int, char*, size_t, char*, size_t, int, long long);
    void *(*createArray)(size_t, void **);
    void *(*createInteger)(long long);
    void *(*createNil)(void);
    void (*freeObject)(void*);
} mcReplyObjectFunctions;

typedef struct mcReader {
    int err; /* Error flags, 0 when there is no error */
    char errstr[128]; /* String representation of error when applicable */

    char *buf; /* Read buffer */
    size_t pos; /* Buffer cursor */
    size_t len; /* Buffer length */
    size_t maxbuf; /* Max length of unused buffer */

    void *subreply; /* Temporary reply for array type */
    size_t alloc_len; /* Temporary reply array alloc length */
    size_t elements; /* Temporary reply array length */
    void **element; /* Temporary reply array */

    char *str;
    size_t strlen;
    int kflags;  /* Used for key flags (get/gets command reply) */
    long long kversion;  /* Used for key version (gets command reply) */

    int state;  /* Current parser state */
    char *token;    /* Token marker */

    long long integer; /* Cache the integer if need */

    int type;   /* Response type */
    int result; /* Parsing result */

    mcReplyObjectFunctions *fn;
    void *privdata;
} mcReader;

/* Public API for the protocol parser. */
mcReader *memcachedReaderCreateWithFunctions(mcReplyObjectFunctions *fn);
void memcachedReaderFree(mcReader *r);
int memcachedReaderFeed(mcReader *r, const char *buf, size_t len);
int memcachedReaderGetReply(mcReader *r, void **reply);

#ifdef __cplusplus
}
#endif

#endif
