#ifndef _VR_STATS_H_
#define _VR_STATS_H_

#define STATS_ATOMIC_FIRST 1

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
#if !defined(STATS_ATOMIC_FIRST) || (!defined(__ATOMIC_RELAXED) && !defined(HAVE_ATOMIC))
    pthread_spinlock_t statslock;
#endif
}vr_stats;

#if defined(__ATOMIC_RELAXED) && defined(STATS_ATOMIC_FIRST)
#define update_stats_add(_stats, _field, _n) __atomic_add_fetch(&(_stats)->_field, (_n), __ATOMIC_RELAXED)
#define update_stats_sub(_stats, _field, _n) __atomic_sub_fetch(&(_stats)->_field, (_n), __ATOMIC_RELAXED)
#elif defined(HAVE_ATOMIC) && defined(STATS_ATOMIC_FIRST)
#define update_stats_add(_stats, _field, _n) __sync_add_and_fetch(&(_stats)->_field, (_n))
#define update_stats_sub(_stats, _field, _n) __sync_sub_and_fetch(&(_stats)->_field, (_n))
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
#endif

int vr_stats_init(vr_stats *stats);
void vr_stats_deinit(vr_stats *stats);

#endif
