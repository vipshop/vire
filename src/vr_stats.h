#ifndef _VR_STATS_H_
#define _VR_STATS_H_

typedef struct vr_stats {
    /* Fields used only for stats */
    time_t starttime;          /* Server start time */
    long long numcommands;     /* Number of processed commands */
    long long numconnections;  /* Number of connections received */
    long long expiredkeys;     /* Number of expired keys */
    long long evictedkeys;     /* Number of evicted keys (maxmemory) */
    long long keyspace_hits;   /* Number of successful lookups of keys */
    long long keyspace_misses; /* Number of failed lookups of keys */
    size_t peak_memory;        /* Max used memory record */
    long long rejected_conn;   /* Clients rejected because of maxclients */
    long long sync_full;       /* Number of full resyncs with slaves. */
    long long sync_partial_ok; /* Number of accepted PSYNC requests. */
    long long sync_partial_err;/* Number of unaccepted PSYNC requests. */
    long long net_input_bytes; /* Bytes read from network. */
    long long net_output_bytes; /* Bytes written to network. */
}vr_stats;

#endif
