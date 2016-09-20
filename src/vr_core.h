#ifndef _VR_CORE_H_
#define _VR_CORE_H_

#ifdef HAVE_CONFIG_H
# include <config.h>
#endif

#include <dspecialconfig.h>

#ifdef HAVE_STATS
# define VR_STATS 1
#else
# define VR_STATS 0
#endif

#ifdef HAVE_LITTLE_ENDIAN
# define VR_LITTLE_ENDIAN 1
#endif

#ifdef HAVE_BACKTRACE
# define VR_HAVE_BACKTRACE 1
#endif

#ifdef HAVE_SPINLOCK
# define VR_USE_SPINLOCK 1
#endif

#define VR_OK        0
#define VR_ERROR    -1
#define VR_EAGAIN   -2
#define VR_ENOMEM   -3

/* reserved fds for std streams, log, stats fd, epoll etc. */
#define RESERVED_FDS 32

typedef int rstatus_t; /* return type */
typedef int err_t;      /* error type */

typedef long long mstime_t; /* millisecond time type. */

struct instance;
struct darray;
struct conn;
struct client;
struct clientBufferLimitsConfig;
struct redisCommand;
struct vr_worker;

#include <stddef.h>
#include <stdint.h>
#include <inttypes.h>
#include <string.h>
#include <stdio.h>
#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <netinet/in.h>

#include <ae.h>
#include <sds.h>
#include <dutil.h>
#include <dlog.h>
#include <dhashkit.h>
#include <dmalloc.h>
#include <darray.h>
#include <dlist.h>

#include <vr_util.h>
#include <vr_signal.h>

#include <vr_ziplist.h>
#include <vr_zipmap.h>
#include <vr_dict.h>
#include <vr_rbtree.h>
#include <vr_intset.h>
#include <vr_quicklist.h>

#include <vr_lzf.h>
#include <vr_lzfP.h>

#include <vr_object.h>

#include <vr_listen.h>
#include <vr_connection.h>

#include <vr_stats.h>
#include <vr_conf.h>

#include <vr_thread.h>
#include <vr_eventloop.h>
#include <vr_master.h>
#include <vr_worker.h>
#include <vr_backend.h>

#include <vr_db.h>
#include <vr_multi.h>

#include <vr_command.h>
#include <vr_block.h>
#include <vr_client.h>
#include <vr_server.h>

#include <vr_notify.h>
#include <vr_pubsub.h>

#include <vr_rdb.h>
#include <vr_aof.h>
#include <vr_replication.h>
#include <vr_scripting.h>

#include <vr_t_hash.h>
#include <vr_t_list.h>
#include <vr_t_set.h>
#include <vr_t_string.h>
#include <vr_t_zset.h>

#include <vr_bitops.h>

#include <vr_hyperloglog.h>

#include <vr_slowlog.h>

struct instance {
    int             log_level;                   /* log level */
    char            *log_filename;               /* log filename */
    char            *conf_filename;              /* configuration filename */
    char            hostname[VR_MAXHOSTNAMELEN]; /* hostname */
    size_t          mbuf_chunk_size;             /* mbuf chunk size */
    pid_t           pid;                         /* process id */
    char            *pid_filename;               /* pid filename */
    unsigned        pidfile:1;                   /* pid file created? */
    int             thread_num;                  /* the thread number */
};

#endif
