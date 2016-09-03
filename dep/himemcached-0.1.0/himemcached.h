#ifndef _HIMEMCACHED_H_
#define _HIMEMCACHED_H_

#include "himcread.h"
#include "himcdep/sds.h"

#define HIMC_MAJOR 0
#define HIMC_MINOR 13
#define HIMC_PATCH 1

/* Connection type can be blocking or non-blocking and is set in the
 * least significant bit of the flags field in redisContext. */
#define MC_BLOCK 0x1

/* Connection may be disconnected before being free'd. The second bit
 * in the flags field is set when the context is connected. */
#define MC_CONNECTED 0x2

/* The async API might try to disconnect cleanly and flush the output
 * buffer and read all subsequent replies before disconnecting.
 * This flag means no new commands can come in and the connection
 * should be terminated once all replies have been read. */
#define MC_DISCONNECTING 0x4

/* Flag specific to the async API which means that the context should be clean
 * up as soon as possible. */
#define MC_FREEING 0x8

/* Flag that is set when an async callback is executed. */
#define MC_IN_CALLBACK 0x10

/* Flag that is set when the async context has one or more subscriptions. */
#define MC_SUBSCRIBED 0x20

/* Flag that is set when monitor mode is active */
#define MC_MONITORING 0x40

/* Flag that is set when we should set SO_REUSEADDR before calling bind() */
#define MC_REUSEADDR 0x80

#define MC_KEEPALIVE_INTERVAL 15 /* seconds */

/* number of times we retry to connect in the case of EADDRNOTAVAIL and
 * SO_REUSEADDR is being used. */
#define MC_CONNECT_RETRIES  10

/* This is the reply object returned by memcachedCommand() */
typedef struct mcReply {
    int type; /* MC_REPLY_* */
    long long integer; /* The integer when type is MC_REPLY_INTEGER */
    int keylen; /* Length of key */
    char *key;  /* Key string */
    int len; /* Length of string */
    char *str; /* Used for both REDIS_REPLY_ERROR and MC_REPLY_STRING */
    int flags;
    long long version;
    size_t elements; /* number of elements, for MC_REPLY_ARRAY */
    struct mcReply **element; /* elements vector for MC_REPLY_ARRAY */
} mcReply;

mcReader *memcachedReaderCreate(void);

/* Function to free the reply objects hiredis returns by default. */
void freeMcReplyObject(void *reply);

enum mcConnectionType {
    MC_CONN_TCP,
    MC_CONN_UNIX,
};

/* Context for a connection to Memcached */
typedef struct mcContext {
    int err; /* Error flags, 0 when there is no error */
    char errstr[128]; /* String representation of error when applicable */
    int fd;
    int flags;
    char *obuf; /* Write buffer */
    mcReader *reader; /* Protocol reader */

    enum mcConnectionType connection_type;
    struct timeval *timeout;

    struct {
        char *host;
        char *source_addr;
        int port;
    } tcp;

    struct {
        char *path;
    } unix_sock;
} mcContext;

int memcachedBufferWrite(mcContext *c, int *done);
int memcachedBufferRead(mcContext *c);

int memcachedGetReplyFromReader(mcContext *c, void **reply);
int memcachedGetReply(mcContext *c, void **reply);

mcContext *memcachedContextInit(void);
void memcachedFree(mcContext *c) ;

int memcachedFormatCommandSdsArgv(char **target, int argc, const sds *argv);
int memcachedvFormatCommand(char **target, const char *format, va_list ap);
int memcachedFormatCommand(char **target, const char *format, ...);
int memcachedFormatCommandArgv(char **target, int argc, const char **argv, const size_t *argvlen);

#endif /* _HIMEMCACHED_H_ */
