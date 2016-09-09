#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <fcntl.h>
#include <errno.h>
#include <signal.h>
#include <assert.h>
#include <sys/stat.h>
#include <sys/utsname.h>

#include <ae.h>

#include <hiredis.h>
#include <sds.h>

#include <darray.h>
#include <dlist.h>
#include <dutil.h>
#include <dlog.h>

#include <vrt_util.h>
#include <vrt_public.h>
#include <himemcached.h>

#define TEST_CMD_PROTOCOL_REDIS     0
#define TEST_CMD_PROTOCOL_MEMCACHE  1

#define RANDPTR_INITIAL_SIZE 8

static struct config {
    const char *hostip;
    int hostport;
    const char *hostsocket;
    int numclients;
    int liveclients;
    int requests;
    int requests_issued;
    int requests_finished;
    int keysize;
    int datasize;
    int randomkeys;
    int randomkeys_keyspacelen;
    int keepalive;
    int pipeline;
    int showerrors;
    long long start;
    long long totlatency;
    long long *latency;
    const char *title;
    int quiet;
    int csv;
    int loop;
    int idlemode;
    int dbnum;
    sds dbnumstr;
    char *tests;
    char *auth;
    int threads_count;
    int protocol;
    int noinline;
} config;

typedef struct benchmark_thread {
    int id;
    pthread_t thread_id;
    
    struct aeEventLoop *el;
    int hz;
    int cronloops;          /* Number of times the cron function run */

    dlist *clients;
    int numclients;
    int liveclients;

    int requests;
    int requests_issued;
    int requests_finished;

    long long start;
    long long totlatency;
    long long *latency;
} benchmark_thread;

typedef struct _benchmark_client {
    benchmark_thread *bt;
    
    redisContext *rc;
    mcContext *mc;
    sds obuf;
    char **randptr;         /* Pointers to :rand: strings inside the command buf */
    size_t randlen;         /* Number of pointers in client->randptr */
    size_t randfree;        /* Number of unused pointers in client->randptr */
    size_t written;         /* Bytes of 'obuf' already written */
    long long start;        /* Start time of a request */
    long long latency;      /* Request latency */
    int pending;            /* Number of pending requests (replies to consume) */
    int prefix_pending;     /* If non-zero, number of pending prefix commands. Commands
                               such as auth and select are prefixed to the pipeline of
                               benchmark commands and discarded after the first send. */
    int prefixlen;          /* Size in bytes of the pending prefix commands */
} *benchmark_client;

static darray *bts; /* Benchmark threads */

/* Prototypes */
static void writeHandler(aeEventLoop *el, int fd, void *privdata, int mask);
static benchmark_client createClient(char *cmd, size_t len, benchmark_client from, benchmark_thread *thread);
static void createMissingClients(benchmark_client c);
static int showThroughput(struct aeEventLoop *eventLoop, long long id, void *clientData);

static void freeClient(benchmark_client c) {
    benchmark_thread *bt = c->bt;
    dlistNode *ln;

    if (bt->el) {
        aeDeleteFileEvent(bt->el,c->rc->fd,AE_WRITABLE);
        aeDeleteFileEvent(bt->el,c->rc->fd,AE_READABLE);
    }
    redisFree(c->rc);
    if (c->mc) {
        c->mc->fd = -1;
        memcachedFree(c->mc);
    }
    sdsfree(c->obuf);
    free(c->randptr);
    free(c);
    update_state_sub(bt->liveclients,1);
    ln = dlistSearchKey(bt->clients,c);
    ASSERT(ln != NULL);
    dlistDelNode(bt->clients,ln);
}

static void freeAllClients(dlist *clients) {
    dlistNode *ln = clients->head, *next;

    while(ln) {
        next = ln->next;
        freeClient(ln->value);
        ln = next;
    }
}

static void resetClient(benchmark_client c) {
    benchmark_thread *bt = c->bt;
    
    aeDeleteFileEvent(bt->el,c->rc->fd,AE_WRITABLE);
    aeDeleteFileEvent(bt->el,c->rc->fd,AE_READABLE);
    aeCreateFileEvent(bt->el,c->rc->fd,AE_WRITABLE,writeHandler,c);
    c->written = 0;
    c->pending = config.pipeline;
}

static void randomizeClientKey(benchmark_client c) {
    size_t i;

    for (i = 0; i < c->randlen; i++) {
        char *p = c->randptr[i]+11;
        size_t r = random() % config.randomkeys_keyspacelen;
        size_t j;

        for (j = 0; j < 12; j++) {
            *p = '0'+r%10;
            r/=10;
            p--;
        }
    }
}

static void clientDone(benchmark_client c) {
    benchmark_thread *bt = c->bt;
    int requests_finished;

    update_state_get(bt->requests_finished,&requests_finished);
    if (requests_finished == bt->requests) {
        freeClient(c);
        aeStop(bt->el);
        return;
    }
    if (config.keepalive) {
        resetClient(c);
    } else {
        update_state_sub(bt->liveclients,1);
        createMissingClients(c);
        update_state_add(bt->liveclients,1);
        freeClient(c);
    }
}

static int benchmark_thread_init(benchmark_thread *bt, int requests, int numclients, char *cmd, size_t len)
{    
    benchmark_client c;
    
    bt->thread_id = 0;
    bt->el = NULL;
    bt->hz = 10;
    bt->cronloops = 0;
    bt->clients = NULL;
    bt->numclients = numclients;
    bt->liveclients = 0;
    bt->requests = requests;
    bt->requests_issued = 0;
    bt->requests_finished = 0;
    bt->start = 0;
    bt->totlatency = 0;
    bt->latency = NULL;

    bt->el = aeCreateEventLoop(1024*10);
    if (bt->el == NULL) {
        return VRT_ERROR;
    }

    bt->clients = dlistCreate();
    if (bt->clients == NULL) {
        return VRT_ERROR;
    }

    bt->latency = malloc(sizeof(long long)*bt->requests);

    c = createClient(cmd,len,NULL,bt);
    createMissingClients(c);

    if (bt->id == 0) {
        aeCreateTimeEvent(bt->el,1,showThroughput,NULL,NULL);
    }
    
    return VRT_OK;
}

static void benchmark_thread_deinit(benchmark_thread *bt)
{
    if (bt->clients) {
        freeAllClients(bt->clients);
        dlistRelease(bt->clients);
        bt->clients = NULL;
    }

    if (bt->el) {
        aeDeleteEventLoop(bt->el);
        bt->el = NULL;
    }
    
    if (bt->latency) {
        free(bt->latency);
        bt->latency = NULL;
    }
}

static void *benchmark_thread_run(void *args)
{
    benchmark_thread *bt = args;
    srand(vrt_usec_now()^(int)pthread_self());

    aeMain(bt->el);
    
    return NULL;
}

static int start_benchmark_threads_until_finish(void)
{
    int i;
    benchmark_thread *bt;
    
    for (i = 0; i < config.threads_count; i ++) {
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        bt = darray_get(bts, i);
        pthread_create(&bt->thread_id, 
            &attr, benchmark_thread_run, bt);
    }

    for (i = 0; i < config.threads_count; i ++) {
        bt = darray_get(bts, i);
        pthread_join(bt->thread_id, NULL);
    }
    
    return VRT_OK;
}

static void readHandlerMC(aeEventLoop *el, int fd, void *privdata, int mask) {
    benchmark_client c = privdata;
    benchmark_thread *bt = c->bt;
    mcContext *mc = c->mc;
    int requests_finished;
    void *reply = NULL;
    UNUSED(el);
    UNUSED(fd);
    UNUSED(mask);

    /* Calculate latency only for the first read event. This means that the
     * server already sent the reply and we need to parse it. Parsing overhead
     * is not part of the latency, so calculate it only once, here. */
    if (c->latency < 0) c->latency = dusec_now()-(c->start);

    if (memcachedBufferRead(mc) != MC_OK) {
        fprintf(stderr,"Error: %s\n",mc->errstr);
        exit(1);
    } else {
        while(c->pending) {
            if (memcachedGetReply(mc,&reply) != MC_OK) {
                fprintf(stderr,"Error: %s\n",mc->errstr);
                exit(1);
            }
            
            if (reply != NULL) {
                if (reply == (void*)MC_REPLY_ERROR) {
                    fprintf(stderr,"Unexpected error reply, exiting...\n");
                    exit(1);
                }

                if (config.showerrors) {
                    static time_t lasterr_time = 0;
                    time_t now = time(NULL);
                    mcReply *r = reply;
                    if (r->type == MC_REPLY_ERROR && lasterr_time != now) {
                        lasterr_time = now;
                        printf("Error from server: %s\n", r->str);
                    }
                }

                freeMcReplyObject(reply);
                /* This is an OK for prefix commands such as auth and select.*/
                if (c->prefix_pending > 0) {
                    c->prefix_pending--;
                    c->pending--;
                    /* Discard prefix commands on first response.*/
                    if (c->prefixlen > 0) {
                        size_t j;
                        sdsrange(c->obuf, c->prefixlen, -1);
                        /* We also need to fix the pointers to the strings
                        * we need to randomize. */
                        for (j = 0; j < c->randlen; j++)
                            c->randptr[j] -= c->prefixlen;
                        c->prefixlen = 0;
                    }
                    continue;
                }

                update_state_get(bt->requests_finished,&requests_finished);
                if (requests_finished < bt->requests) {
                    bt->latency[requests_finished] = c->latency;
                    update_state_add(bt->requests_finished,1);
                }
                c->pending--;
                if (c->pending == 0) {
                    clientDone(c);
                    break;
                }
            } else {
                break;
            }
        }
    }
}

static void readHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    benchmark_client c = privdata;
    benchmark_thread *bt = c->bt;
    int requests_finished;
    void *reply = NULL;
    UNUSED(el);
    UNUSED(fd);
    UNUSED(mask);

    /* Calculate latency only for the first read event. This means that the
     * server already sent the reply and we need to parse it. Parsing overhead
     * is not part of the latency, so calculate it only once, here. */
    if (c->latency < 0) c->latency = dusec_now()-(c->start);

    if (redisBufferRead(c->rc) != REDIS_OK) {
        fprintf(stderr,"Error: %s\n",c->rc->errstr);
        exit(1);
    } else {
        while(c->pending) {
            if (redisGetReply(c->rc,&reply) != REDIS_OK) {
                fprintf(stderr,"Error: %s\n",c->rc->errstr);
                exit(1);
            }
            if (reply != NULL) {
                if (reply == (void*)REDIS_REPLY_ERROR) {
                    fprintf(stderr,"Unexpected error reply, exiting...\n");
                    exit(1);
                }

                if (config.showerrors) {
                    static time_t lasterr_time = 0;
                    time_t now = time(NULL);
                    redisReply *r = reply;
                    if (r->type == REDIS_REPLY_ERROR && lasterr_time != now) {
                        lasterr_time = now;
                        printf("Error from server: %s\n", r->str);
                    }
                }

                freeReplyObject(reply);
                /* This is an OK for prefix commands such as auth and select.*/
                if (c->prefix_pending > 0) {
                    c->prefix_pending--;
                    c->pending--;
                    /* Discard prefix commands on first response.*/
                    if (c->prefixlen > 0) {
                        size_t j;
                        sdsrange(c->obuf, c->prefixlen, -1);
                        /* We also need to fix the pointers to the strings
                        * we need to randomize. */
                        for (j = 0; j < c->randlen; j++)
                            c->randptr[j] -= c->prefixlen;
                        c->prefixlen = 0;
                    }
                    continue;
                }

                update_state_get(bt->requests_finished,&requests_finished);
                if (requests_finished < bt->requests) {
                    bt->latency[requests_finished] = c->latency;
                    update_state_add(bt->requests_finished,1);
                }
                c->pending--;
                if (c->pending == 0) {
                    clientDone(c);
                    break;
                }
            } else {
                break;
            }
        }
    }
}

static void writeHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    benchmark_client c = privdata;
    benchmark_thread *bt = c->bt;
    UNUSED(el);
    UNUSED(fd);
    UNUSED(mask);

    /* Initialize request when nothing was written. */
    if (c->written == 0) {
        /* Enforce upper bound to number of requests. */
        if (bt->requests_issued++ >= bt->requests) {
            freeClient(c);
            return;
        }

        /* Really initialize: randomize keys and set start time. */
        if (config.randomkeys) randomizeClientKey(c);
        c->start = dusec_now();
        c->latency = -1;
    }

    if (sdslen(c->obuf) > c->written) {
        void *ptr = c->obuf+c->written;
        ssize_t nwritten = write(c->rc->fd,ptr,sdslen(c->obuf)-c->written);
        
        if (nwritten == -1) {
            if (errno != EPIPE)
                fprintf(stderr, "Writing to socket: %s\n", strerror(errno));
            freeClient(c);
            return;
        }
        c->written += nwritten;
        if (sdslen(c->obuf) == c->written) {
            aeDeleteFileEvent(bt->el,c->rc->fd,AE_WRITABLE);
            if (config.protocol == TEST_CMD_PROTOCOL_REDIS) {
                aeCreateFileEvent(bt->el,c->rc->fd,AE_READABLE,readHandler,c);
            } else if (config.protocol == TEST_CMD_PROTOCOL_MEMCACHE) {
                aeCreateFileEvent(bt->el,c->rc->fd,AE_READABLE,readHandlerMC,c);
            } else {
                NOT_REACHED();
            }
        }
    }
}

/* Create a benchmark client, configured to send the command passed as 'cmd' of
 * 'len' bytes.
 *
 * The command is copied N times in the client output buffer (that is reused
 * again and again to send the request to the server) accordingly to the configured
 * pipeline size.
 *
 * Also an initial SELECT command is prepended in order to make sure the right
 * database is selected, if needed. The initial SELECT will be discarded as soon
 * as the first reply is received.
 *
 * To create a client from scratch, the 'from' pointer is set to NULL. If instead
 * we want to create a client using another client as reference, the 'from' pointer
 * points to the client to use as reference. In such a case the following
 * information is take from the 'from' client:
 *
 * 1) The command line to use.
 * 2) The offsets of the __rand_int__ elements inside the command line, used
 *    for arguments randomization.
 *
 * Even when cloning another client, prefix commands are applied if needed.*/
static benchmark_client createClient(char *cmd, size_t len, benchmark_client from, benchmark_thread *thread) {
    int j;
    benchmark_thread *bt;
    benchmark_client c = malloc(sizeof(struct _benchmark_client));

    c->bt = NULL;
    c->rc = NULL;
    c->mc = NULL;
    c->obuf = NULL;
    c->randptr = NULL;
    c->randlen = 0;
    c->randfree = 0;
    c->written = 0;
    c->start = 0;
    c->latency = 0;
    c->pending = 0;
    c->prefix_pending = 0;
    c->prefixlen = 0;
    
    if (from == NULL) {
        ASSERT(thread != NULL);
        bt = thread;
    } else {
        bt = from->bt;
    }

    c->bt = bt;

    if (config.hostsocket == NULL) {
        c->rc = redisConnectNonBlock(config.hostip,config.hostport);
    } else {
        c->rc = redisConnectUnixNonBlock(config.hostsocket);
    }
    if (c->rc->err) {
        fprintf(stderr,"Could not connect to Redis at ");
        if (config.hostsocket == NULL)
            fprintf(stderr,"%s:%d: %s\n",config.hostip,config.hostport,c->rc->errstr);
        else
            fprintf(stderr,"%s: %s\n",config.hostsocket,c->rc->errstr);
        exit(1);
    }
    /* Suppress hiredis cleanup of unused buffers for max speed. */
    c->rc->reader->maxbuf = 0;

    /* Build the request buffer:
     * Queue N requests accordingly to the pipeline size, or simply clone
     * the example client buffer. */
    c->obuf = sdsempty();
    /* Prefix the request buffer with AUTH and/or SELECT commands, if applicable.
     * These commands are discarded after the first response, so if the client is
     * reused the commands will not be used again. */
    c->prefix_pending = 0;
    if (config.auth) {
        char *buf = NULL;
        int len = redisFormatCommand(&buf, "AUTH %s", config.auth);
        c->obuf = sdscatlen(c->obuf, buf, len);
        free(buf);
        c->prefix_pending++;
    }

    /* If a DB number different than zero is selected, prefix our request
     * buffer with the SELECT command, that will be discarded the first
     * time the replies are received, so if the client is reused the
     * SELECT command will not be used again. */
    if (config.dbnum != 0) {
        c->obuf = sdscatprintf(c->obuf,"*2\r\n$6\r\nSELECT\r\n$%d\r\n%s\r\n",
            (int)sdslen(config.dbnumstr),config.dbnumstr);
        c->prefix_pending++;
    }
    c->prefixlen = sdslen(c->obuf);
    /* Append the request itself. */
    if (from) {
        c->obuf = sdscatlen(c->obuf,
            from->obuf+from->prefixlen,
            sdslen(from->obuf)-from->prefixlen);
    } else {
        for (j = 0; j < config.pipeline; j++)
            c->obuf = sdscatlen(c->obuf,cmd,len);
    }

    c->written = 0;
    c->pending = config.pipeline+c->prefix_pending;
    c->randptr = NULL;
    c->randlen = 0;

    /* Find substrings in the output buffer that need to be randomized. */
    if (config.randomkeys) {
        if (from) {
            c->randlen = from->randlen;
            c->randfree = 0;
            c->randptr = malloc(sizeof(char*)*c->randlen);
            /* copy the offsets. */
            for (j = 0; j < (int)c->randlen; j++) {
                c->randptr[j] = c->obuf + (from->randptr[j]-from->obuf);
                /* Adjust for the different select prefix length. */
                c->randptr[j] += c->prefixlen - from->prefixlen;
            }
        } else {
            char *p = c->obuf;

            c->randlen = 0;
            c->randfree = RANDPTR_INITIAL_SIZE;
            c->randptr = malloc(sizeof(char*)*c->randfree);
            while ((p = strstr(p,"__rand_int__")) != NULL) {
                if (c->randfree == 0) {
                    c->randptr = realloc(c->randptr,sizeof(char*)*c->randlen*2);
                    c->randfree += c->randlen;
                }
                c->randptr[c->randlen++] = p;
                c->randfree--;
                p += 12; /* 12 is strlen("__rand_int__). */
            }
        }
    }
    if (config.idlemode == 0)
        aeCreateFileEvent(bt->el,c->rc->fd,AE_WRITABLE,writeHandler,c);

    /* Attach the redis fd to memcached fd */
    if (config.protocol == TEST_CMD_PROTOCOL_MEMCACHE) {
        c->mc = memcachedContextInit();
        c->mc->fd = c->rc->fd;
        c->mc->flags &= ~MC_BLOCK;
    }
    
    dlistAddNodeTail(bt->clients,c);
    update_state_add(bt->liveclients,1);

    return c;
}

static void createMissingClients(benchmark_client c) {
    int n = 0;
    benchmark_thread *bt = c->bt;
    int liveclients;

    update_state_get(bt->liveclients,&liveclients);
    
    while(liveclients < bt->numclients) {
        createClient(NULL,0,c,NULL);

        /* Listen backlog is quite limited on most systems */
        if (++n > 64) {
            usleep(50000);
            n = 0;
        }
        update_state_get(bt->liveclients,&liveclients);
    }
}

static int compareLatency(const void *a, const void *b) {
    return (*(long long*)a)-(*(long long*)b);
}

static void updateBenchmarkStats(void)
{
    int i;
    int count;

    config.liveclients = 0;
    config.requests_finished = 0;

    for (i = 0; i < config.threads_count; i ++) {
        benchmark_thread *bt = darray_get(bts, i);
        update_state_get(bt->liveclients,&count);
        config.liveclients += count;
        update_state_get(bt->requests_finished,&count);
        config.requests_finished += count;
    }
}

static void showLatencyReport(void) {
    int i, j, curlat = 0;
    int n = 0;
    float perc, reqpersec;

    updateBenchmarkStats();

    reqpersec = (float)config.requests_finished/((float)config.totlatency/1000);
    if (!config.quiet && !config.csv) {
        printf("====== %s ======\n", config.title);
        printf("  %d requests completed in %.2f seconds\n", config.requests_finished,
            (float)config.totlatency/1000);
        printf("  %d parallel clients\n", config.numclients);
        printf("  %d bytes payload\n", config.datasize);
        printf("  keep alive: %d\n", config.keepalive);
        printf("\n");

        for (i = 0; i < config.threads_count; i++) {
            benchmark_thread *bt = darray_get(bts, i);
            for (j = 0; j < bt->requests; j ++) {
                config.latency[n++] = bt->latency[j];
            }
        }
        
        qsort(config.latency,config.requests,sizeof(long long),compareLatency);
        for (i = 0; i < config.requests; i++) {
            if (config.latency[i]/1000 != curlat || i == (config.requests-1)) {
                curlat = config.latency[i]/1000;
                perc = ((float)(i+1)*100)/config.requests;
                printf("%.2f%% <= %d milliseconds\n", perc, curlat);
            }
        }
        printf("%.2f requests per second\n\n", reqpersec);
    } else if (config.csv) {
        printf("\"%s\",\"%.2f\"\n", config.title, reqpersec);
    } else {
        printf("%s: %.2f requests per second\n", config.title, reqpersec);
    }
}

static void benchmark(char *title, char *cmd, int len) {
    int i;
    int requests_per_thread, requests_remainder;
    int clients_per_thread, clients_remainder;
    benchmark_client c;

    config.title = title;
    config.requests_issued = 0;
    config.requests_finished = 0;

    /* Init the benchmark threads */
    if (config.threads_count <= 0) {
        printf("ERROR: threads count need bigger than zero\n");
    }
    bts = darray_create(config.threads_count, sizeof(benchmark_thread));
    requests_per_thread = config.requests/config.threads_count;
    requests_remainder = config.requests%config.threads_count;
    clients_per_thread = config.numclients/config.threads_count;
    clients_remainder = config.numclients%config.threads_count;
    for (i = 0; i < config.threads_count; i ++) {
        benchmark_thread *bt = darray_push(bts);
        bt->id = i;
        benchmark_thread_init(bt,
            requests_remainder-->0?requests_per_thread+1:requests_per_thread,
            clients_remainder-->0?clients_per_thread+1:clients_per_thread,
            cmd,len);
    }

    config.start = dmsec_now();
    start_benchmark_threads_until_finish();
    config.totlatency = dmsec_now()-config.start;

    showLatencyReport();

    while (darray_n(bts) > 0) {
        benchmark_thread *bt = darray_pop(bts);
        benchmark_thread_deinit(bt);
    }
    darray_destroy(bts);
    bts = NULL;
}

/* Returns number of consumed options. */
int parseOptions(int argc, const char **argv) {
    int i;
    int lastarg;
    int exit_status = 1;

    for (i = 1; i < argc; i++) {
        lastarg = (i == (argc-1));

        if (!strcmp(argv[i],"-c")) {
            if (lastarg) goto invalid;
            config.numclients = atoi(argv[++i]);
        } else if (!strcmp(argv[i],"-n")) {
            if (lastarg) goto invalid;
            config.requests = atoi(argv[++i]);
        } else if (!strcmp(argv[i],"-k")) {
            if (lastarg) goto invalid;
            config.keepalive = atoi(argv[++i]);
        } else if (!strcmp(argv[i],"-h")) {
            if (lastarg) goto invalid;
            config.hostip = strdup(argv[++i]);
        } else if (!strcmp(argv[i],"-p")) {
            if (lastarg) goto invalid;
            config.hostport = atoi(argv[++i]);
        } else if (!strcmp(argv[i],"-s")) {
            if (lastarg) goto invalid;
            config.hostsocket = strdup(argv[++i]);
        } else if (!strcmp(argv[i],"-a") ) {
            if (lastarg) goto invalid;
            config.auth = strdup(argv[++i]);
        } else if (!strcmp(argv[i],"-d")) {
            if (lastarg) goto invalid;
            config.datasize = atoi(argv[++i]);
            if (config.datasize < 1) config.datasize=1;
            if (config.datasize > 1024*1024*1024) config.datasize = 1024*1024*1024;
        } else if (!strcmp(argv[i],"-P")) {
            if (lastarg) goto invalid;
            config.pipeline = atoi(argv[++i]);
            if (config.pipeline <= 0) config.pipeline=1;
        } else if (!strcmp(argv[i],"-r")) {
            if (lastarg) goto invalid;
            config.randomkeys = 1;
            config.randomkeys_keyspacelen = atoi(argv[++i]);
            if (config.randomkeys_keyspacelen < 0)
                config.randomkeys_keyspacelen = 0;
        } else if (!strcmp(argv[i],"-q")) {
            config.quiet = 1;
        } else if (!strcmp(argv[i],"--csv")) {
            config.csv = 1;
        } else if (!strcmp(argv[i],"-l")) {
            config.loop = 1;
        } else if (!strcmp(argv[i],"-I")) {
            config.idlemode = 1;
        } else if (!strcmp(argv[i],"-e")) {
            config.showerrors = 1;
        } else if (!strcmp(argv[i],"-t")) {
            if (lastarg) goto invalid;
            /* We get the list of tests to run as a string in the form
             * get,set,lrange,...,test_N. Then we add a comma before and
             * after the string in order to make sure that searching
             * for ",testname," will always get a match if the test is
             * enabled. */
            config.tests = sdsnew(",");
            config.tests = sdscat(config.tests,(char*)argv[++i]);
            config.tests = sdscat(config.tests,",");
            sdstolower(config.tests);
        } else if (!strcmp(argv[i],"--dbnum")) {
            if (lastarg) goto invalid;
            config.dbnum = atoi(argv[++i]);
            config.dbnumstr = sdsfromlonglong(config.dbnum);
        } else if (!strcmp(argv[i],"-T")) {
            if (lastarg) goto invalid;
            config.threads_count = atoi(argv[++i]);
        } else if (!strcmp(argv[i],"-m")) {
            config.protocol = TEST_CMD_PROTOCOL_MEMCACHE;
        } else if (!strcmp(argv[i],"--noinline")) {
            config.noinline = 1;
        } else if (!strcmp(argv[i],"--help")) {
            exit_status = 0;
            goto usage;
        } else {
            /* Assume the user meant to provide an option when the arg starts
             * with a dash. We're done otherwise and should use the remainder
             * as the command and arguments for running the benchmark. */
            if (argv[i][0] == '-') goto invalid;
            return i;
        }
    }

    return i;

invalid:
    printf("Invalid option \"%s\" or option argument missing\n\n",argv[i]);

usage:
    printf(
"Usage: vire-benchmark [-h <host>] [-p <port>] [-c <clients>] [-n <requests]> [-k <boolean>]\n\n"
" -h <hostname>      Server hostname (default 127.0.0.1)\n"
" -p <port>          Server port (default 6379)\n"
" -s <socket>        Server socket (overrides host and port)\n"
" -a <password>      Password for Redis Auth\n"
" -c <clients>       Number of parallel connections (default 50)\n"
" -n <requests>      Total number of requests (default 100000)\n"
" -T <threads>       Threads count to run (default 1)\n"
" -d <size>          Data size of SET/GET value in bytes (default 2)\n"
" -dbnum <db>        SELECT the specified db number (default 0)\n"
" -k <boolean>       1=keep alive 0=reconnect (default 1)\n"
" -r <keyspacelen>   Use random keys for SET/GET/INCR, random values for SADD\n"
"  Using this option the benchmark will expand the string __rand_int__\n"
"  inside an argument with a 12 digits number in the specified range\n"
"  from 0 to keyspacelen-1. The substitution changes every time a command\n"
"  is executed. Default tests use this to hit random keys in the\n"
"  specified range.\n"
" -P <numreq>        Pipeline <numreq> requests. Default 1 (no pipeline).\n"
" -e                 If server replies with errors, show them on stdout.\n"
"                    (no more than 1 error per second is displayed)\n"
" -q                 Quiet. Just show query/sec values\n"
" --csv              Output in CSV format\n"
" -l                 Loop. Run the tests forever\n"
" -t <tests>         Only run the comma separated list of tests. The test\n"
"                    names are the same as the ones produced as output.\n"
" -I                 Idle mode. Just open N idle connections and wait.\n"
" -m                 Use memcached protocol. This option is used for testing memcached.\n"
" --noinline         Not test redis inline commands.\n\n"
"Examples:\n\n"
" Run the benchmark with the default configuration against 127.0.0.1:6379:\n"
"   $ vire-benchmark\n\n"
" Use 20 parallel clients, for a total of 100k requests, against 192.168.1.1:\n"
"   $ vire-benchmark -h 192.168.1.1 -p 6379 -n 100000 -c 20\n\n"
" Fill 127.0.0.1:6379 with about 1 million keys only using the SET test:\n"
"   $ vire-benchmark -t set -n 1000000 -r 100000000\n\n"
" Benchmark 127.0.0.1:6379 for a few commands producing CSV output:\n"
"   $ vire-benchmark -t ping,set,get -n 100000 --csv\n\n"
" Benchmark a specific command line:\n"
"   $ vire-benchmark -r 10000 -n 10000 eval 'return redis.call(\"ping\")' 0\n\n"
" Fill a list with 10000 random elements:\n"
"   $ vire-benchmark -r 10000 -n 10000 lpush mylist __rand_int__\n\n"
" On user specified command lines __rand_int__ is replaced with a random integer\n"
" with a range of values selected by the -r option.\n"
    );
    exit(exit_status);
}

static int showThroughput(struct aeEventLoop *eventLoop, long long id, void *clientData) {
    UNUSED(eventLoop);
    UNUSED(id);
    UNUSED(clientData);

    updateBenchmarkStats();

    if (config.liveclients == 0) {
        fprintf(stderr,"All clients disconnected... aborting.\n");
        exit(1);
    }
    if (config.csv) return 250;
    if (config.idlemode == 1) {
        printf("clients: %d\r", config.liveclients);
        fflush(stdout);
	    return 250;
    }
    float dt = (float)(dmsec_now()-config.start)/1000.0;
    float rps = (float)config.requests_finished/dt;
    printf("%s: %.2f\r", config.title, rps);
    fflush(stdout);
    return 250; /* every 250ms */
}

/* Return true if the named test was selected using the -t command line
 * switch, or if all the tests are selected (no -t passed by user). */
int test_is_selected(char *name) {
    char buf[256];
    int l = strlen(name);

    if (config.tests == NULL) return 1;
    buf[0] = ',';
    memcpy(buf+1,name,l);
    buf[l+1] = ',';
    buf[l+2] = '\0';
    return strstr(config.tests,buf) != NULL;
}

static int test_redis(int argc, const char **argv)
{
    int i;
    char *data, *cmd;
    int len;

    /* Run benchmark with command in the remainder of the arguments. */
    if (argc) {
        sds title = sdsnew(argv[0]);
        for (i = 1; i < argc; i++) {
            title = sdscatlen(title, " ", 1);
            title = sdscatlen(title, (char*)argv[i], strlen(argv[i]));
        }

        do {
            len = redisFormatCommandArgv(&cmd,argc,argv,NULL);
            benchmark(title,cmd,len);
            free(cmd);
        } while(config.loop);

        return 0;
    }

    /* Run default benchmark suite. */
    data = malloc(config.datasize+1);
    do {
        memset(data,'x',config.datasize);
        data[config.datasize] = '\0';

        if (!config.noinline && (test_is_selected("ping_inline") || test_is_selected("ping")))
            benchmark("PING_INLINE","PING\r\n",6);

        if (test_is_selected("ping_mbulk") || test_is_selected("ping")) {
            len = redisFormatCommand(&cmd,"PING");
            benchmark("PING_BULK",cmd,len);
            free(cmd);
        }

        if (test_is_selected("set")) {
            len = redisFormatCommand(&cmd,"SET key:__rand_int__ %s",data);
            benchmark("SET",cmd,len);
            free(cmd);
        }

        if (test_is_selected("get")) {
            len = redisFormatCommand(&cmd,"GET key:__rand_int__");
            benchmark("GET",cmd,len);
            free(cmd);
        }

        if (test_is_selected("incr")) {
            len = redisFormatCommand(&cmd,"INCR counter:__rand_int__");
            benchmark("INCR",cmd,len);
            free(cmd);
        }

        if (test_is_selected("lpush")) {
            len = redisFormatCommand(&cmd,"LPUSH mylist %s",data);
            benchmark("LPUSH",cmd,len);
            free(cmd);
        }

        if (test_is_selected("rpush")) {
            len = redisFormatCommand(&cmd,"RPUSH mylist %s",data);
            benchmark("RPUSH",cmd,len);
            free(cmd);
        }

        if (test_is_selected("lpop")) {
            len = redisFormatCommand(&cmd,"LPOP mylist");
            benchmark("LPOP",cmd,len);
            free(cmd);
        }

        if (test_is_selected("rpop")) {
            len = redisFormatCommand(&cmd,"RPOP mylist");
            benchmark("RPOP",cmd,len);
            free(cmd);
        }

        if (test_is_selected("sadd")) {
            len = redisFormatCommand(&cmd,
                "SADD myset element:__rand_int__");
            benchmark("SADD",cmd,len);
            free(cmd);
        }

        if (test_is_selected("spop")) {
            len = redisFormatCommand(&cmd,"SPOP myset");
            benchmark("SPOP",cmd,len);
            free(cmd);
        }

        if (test_is_selected("lrange") ||
            test_is_selected("lrange_100") ||
            test_is_selected("lrange_300") ||
            test_is_selected("lrange_500") ||
            test_is_selected("lrange_600"))
        {
            len = redisFormatCommand(&cmd,"LPUSH mylist %s",data);
            benchmark("LPUSH (needed to benchmark LRANGE)",cmd,len);
            free(cmd);
        }

        if (test_is_selected("lrange") || test_is_selected("lrange_100")) {
            len = redisFormatCommand(&cmd,"LRANGE mylist 0 99");
            benchmark("LRANGE_100 (first 100 elements)",cmd,len);
            free(cmd);
        }

        if (test_is_selected("lrange") || test_is_selected("lrange_300")) {
            len = redisFormatCommand(&cmd,"LRANGE mylist 0 299");
            benchmark("LRANGE_300 (first 300 elements)",cmd,len);
            free(cmd);
        }

        if (test_is_selected("lrange") || test_is_selected("lrange_500")) {
            len = redisFormatCommand(&cmd,"LRANGE mylist 0 449");
            benchmark("LRANGE_500 (first 450 elements)",cmd,len);
            free(cmd);
        }

        if (test_is_selected("lrange") || test_is_selected("lrange_600")) {
            len = redisFormatCommand(&cmd,"LRANGE mylist 0 599");
            benchmark("LRANGE_600 (first 600 elements)",cmd,len);
            free(cmd);
        }

        if (test_is_selected("mset")) {
            const char *argv[21];
            argv[0] = "MSET";
            for (i = 1; i < 21; i += 2) {
                argv[i] = "key:__rand_int__";
                argv[i+1] = data;
            }
            len = redisFormatCommandArgv(&cmd,21,argv,NULL);
            benchmark("MSET (10 keys)",cmd,len);
            free(cmd);
        }

        if (!config.csv) printf("\n");
    } while(config.loop);

    return VRT_OK;
}

static int test_memcached(int argc, const char **argv)
{
    int i;
    char *data, *cmd;
    int len;

    /* Run benchmark with command in the remainder of the arguments. */
    if (argc) {
        sds title = sdsnew(argv[0]);
        for (i = 1; i < argc; i++) {
            title = sdscatlen(title, " ", 1);
            title = sdscatlen(title, (char*)argv[i], strlen(argv[i]));
        }

        do {
            len = memcachedFormatCommandArgv(&cmd,argc,argv,NULL);
            if (len < 0) {
                return 0;
            }
            
            benchmark(title,cmd,len);
            free(cmd);
        } while(config.loop);

        return 0;
    }

    /* Run default benchmark suite. */
    data = malloc(config.datasize+1);
    do {
        memset(data,'x',config.datasize);
        data[config.datasize] = '\0';

        if (test_is_selected("set")) {
            len = memcachedFormatCommand(&cmd,"set key:__rand_int__ 0 0 %d %s", config.datasize, data);
            
            benchmark("SET",cmd,len);
            free(cmd);
        }

        if (test_is_selected("get")) {
            len = memcachedFormatCommand(&cmd,"get key:__rand_int__");
            benchmark("GET",cmd,len);
            free(cmd);
        }
        
        if (!config.csv) printf("\n");
    } while(config.loop);

    return VRT_OK;
}

int main(int argc, const char **argv) {
    int i;

    benchmark_client c;

    srandom(time(NULL));
    signal(SIGHUP, SIG_IGN);
    signal(SIGPIPE, SIG_IGN);

    config.numclients = 50;
    config.requests = 100000;
    config.liveclients = 0;
    config.keepalive = 1;
    config.datasize = 3;
    config.pipeline = 1;
    config.showerrors = 0;
    config.randomkeys = 0;
    config.randomkeys_keyspacelen = 0;
    config.quiet = 0;
    config.csv = 0;
    config.loop = 0;
    config.idlemode = 0;
    config.latency = NULL;
    config.hostip = "127.0.0.1";
    config.hostport = 6379;
    config.hostsocket = NULL;
    config.tests = NULL;
    config.dbnum = 0;
    config.auth = NULL;
    config.threads_count = 1;
    config.protocol = TEST_CMD_PROTOCOL_REDIS;
    config.noinline = 0;

    i = parseOptions(argc,argv);
    argc -= i;
    argv += i;

    config.latency = malloc(sizeof(long long)*config.requests);

    if (config.keepalive == 0) {
        printf("WARNING: keepalive disabled, you probably need 'echo 1 > /proc/sys/net/ipv4/tcp_tw_reuse' for Linux and 'sudo sysctl -w net.inet.tcp.msl=1000' for Mac OS X in order to use a lot of clients/requests\n");
    }

    //if (config.idlemode) {
    //    printf("Creating %d idle connections and waiting forever (Ctrl+C when done)\n", config.numclients);
    //    c = createClient("",0,NULL); /* will never receive a reply */
    //    createMissingClients(c);
    //    aeMain(config.el);
        /* and will wait for every */
    //}

    if (config.protocol == TEST_CMD_PROTOCOL_REDIS) {
        test_redis(argc, argv);
    } else if (config.protocol == TEST_CMD_PROTOCOL_MEMCACHE) {
        test_memcached(argc, argv);
    } else {
        NOT_REACHED();
    }

    return 0;
}
