#ifndef _VR_STATS_H_
#define _VR_STATS_H_

#if 1
#define STATS_ATOMIC_FIRST 1
#endif

/* Instantaneous metrics tracking. */
#define STATS_METRIC_SAMPLES 16     /* Number of samples per metric. */
#define STATS_METRIC_COMMAND 0      /* Number of commands executed. */
#define STATS_METRIC_NET_INPUT 1    /* Bytes read to network .*/
#define STATS_METRIC_NET_OUTPUT 2   /* Bytes written to network. */
#define STATS_METRIC_COUNT 3

typedef struct vr_stats {
    /* Fields used only for stats */
    time_t starttime;          /* Server start time */
    long long numcommands;     /* Number of processed commands */
    long long numconnections;  /* Number of connections received */
    long long expiredkeys;     /* Number of expired keys */
    long long evictedkeys;     /* Number of evicted keys (maxmemory) */
    long long keyspace_hits;   /* Number of successful lookups of keys */
    long long keyspace_misses; /* Number of failed lookups of keys */
    long long rejected_conn;   /* Clients rejected because of maxclients */
    long long sync_full;       /* Number of full resyncs with slaves. */
    long long sync_partial_ok; /* Number of accepted PSYNC requests. */
    long long sync_partial_err;/* Number of unaccepted PSYNC requests. */
    long long net_input_bytes; /* Bytes read from network. */
    long long net_output_bytes; /* Bytes written to network. */
    size_t    peak_memory;     /* Max used memory record */
    
    /* The following two are used to track instantaneous metrics, like
     * number of operations per second, network traffic. */
    struct {
        long long last_sample_time; /* Timestamp of last sample in ms */
        long long last_sample_count;/* Count in last sample */
        long long samples[STATS_METRIC_SAMPLES];
        int idx;
    } inst_metric[STATS_METRIC_COUNT];

#if !defined(STATS_ATOMIC_FIRST) || (!defined(__ATOMIC_RELAXED) && !defined(HAVE_ATOMIC))
    pthread_spinlock_t statslock;
#endif
}vr_stats;

/* GCC version >= 4.7 */
#if defined(__ATOMIC_RELAXED) && defined(STATS_ATOMIC_FIRST)
#define update_stats_add(_stats, _field, _n) __atomic_add_fetch(&(_stats)->_field, (_n), __ATOMIC_RELAXED)
#define update_stats_sub(_stats, _field, _n) __atomic_sub_fetch(&(_stats)->_field, (_n), __ATOMIC_RELAXED)
#define update_stats_set(_stats, _field, _n) __atomic_store_n(&(_stats)->_field, (_n), __ATOMIC_RELAXED)
#define update_stats_get(_stats, _field, _v) do {           \
    __atomic_load(&(_stats)->_field, _v, __ATOMIC_RELAXED); \
} while(0)

#define STATS_LOCK_TYPE "__ATOMIC_RELAXED"
/* GCC version >= 4.1 */
#elif defined(HAVE_ATOMIC) && defined(STATS_ATOMIC_FIRST)
#define update_stats_add(_stats, _field, _n) __sync_add_and_fetch(&(_stats)->_field, (_n))
#define update_stats_sub(_stats, _field, _n) __sync_sub_and_fetch(&(_stats)->_field, (_n))
#define update_stats_set(_stats, _field, _n) __sync_lock_test_and_set(&(_stats)->_field, (_n))
#define update_stats_get(_stats, _field, _v) do {           \
    (*_v) = __sync_add_and_fetch(&(_stats)->_field, 0);     \
} while(0)

#define STATS_LOCK_TYPE "HAVE_ATOMIC"
#else
#define update_stats_add(_stats, _field, _n) do {   \
    pthread_spin_lock(&(_stats)->statslock);        \
    (_stats)->_field += (_n);                       \
    pthread_spin_unlock(&(_stats)->statslock);      \
} while(0)

#define update_stats_sub(_stats, _field, _n) do {   \
    pthread_spin_lock(&(_stats)->statslock);        \
    (_stats)->_field -= (_n);                       \
    pthread_spin_unlock(&(_stats)->statslock);      \
} while(0)

#define update_stats_set(_stats, _field, _n) do {   \
    pthread_spin_lock(&(_stats)->statslock);        \
    (_stats)->_field = (_n);                        \
    pthread_spin_unlock(&(_stats)->statslock);      \
} while(0)

#define update_stats_get(_stats, _field, _v) do {   \
    pthread_spin_lock(&(_stats)->statslock);        \
    (*_v) = (_stats)->_field;                       \
    pthread_spin_unlock(&(_stats)->statslock);      \
} while(0)

#define STATS_LOCK_TYPE "pthread_spin_lock"
#endif

int vr_stats_init(vr_stats *stats);
void vr_stats_deinit(vr_stats *stats);

void trackInstantaneousMetric(vr_stats *stats, int metric, long long current_reading);
long long getInstantaneousMetric(vr_stats *stats, int metric);

#endif
