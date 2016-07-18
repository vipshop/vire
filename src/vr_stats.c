#include <vr_core.h>

int
vr_stats_init(vr_stats *stats)
{
    rstatus_t ret;

    if (stats == NULL) {
        return VR_ERROR;
    }

    stats->starttime = 0;
    stats->numcommands = 0;
    stats->numconnections = 0;
    stats->expiredkeys = 0;
    stats->evictedkeys = 0;
    stats->keyspace_hits = 0;
    stats->keyspace_misses = 0;
    stats->rejected_conn = 0;
    stats->sync_full = 0;
    stats->sync_partial_ok = 0;
    stats->sync_partial_err = 0;
    stats->net_input_bytes = 0;
    stats->net_output_bytes = 0;
    stats->peak_memory = 0;
    
#if !defined(STATS_ATOMIC_FIRST) || (!defined(__ATOMIC_RELAXED) && !defined(HAVE_ATOMIC))
    ret = pthread_spin_init(&stats->statslock, 0);
    if (ret != 0) {
        return VR_ERROR;
    }
#endif

    stats->starttime = time(NULL);

    return VR_OK;
}

void
vr_stats_deinit(vr_stats *stats)
{
    if (stats == NULL) {
        return;
    }

    stats->starttime = 0;
    stats->numcommands = 0;
    stats->numconnections = 0;
    stats->expiredkeys = 0;
    stats->evictedkeys = 0;
    stats->keyspace_hits = 0;
    stats->keyspace_misses = 0;
    stats->rejected_conn = 0;
    stats->sync_full = 0;
    stats->sync_partial_ok = 0;
    stats->sync_partial_err = 0;
    stats->net_input_bytes = 0;
    stats->net_output_bytes = 0;
    
#if !defined(STATS_ATOMIC_FIRST) || (!defined(__ATOMIC_RELAXED) && !defined(HAVE_ATOMIC))
    pthread_spin_destroy(&stats->statslock);
#endif
}

/* Add a sample to the operations per second array of samples. */
void trackInstantaneousMetric(vr_stats *stats, int metric, long long current_reading) {
    long long t = vr_msec_now() - stats->inst_metric[metric].last_sample_time;
    long long ops = current_reading -
                    stats->inst_metric[metric].last_sample_count;
    long long ops_sec;

    ops_sec = t > 0 ? (ops*1000/t) : 0;
    
    update_stats_set(stats,inst_metric[metric].samples[stats->inst_metric[metric].idx],ops_sec);
    stats->inst_metric[metric].idx++;
    stats->inst_metric[metric].idx %= STATS_METRIC_SAMPLES;
    stats->inst_metric[metric].last_sample_time = vr_msec_now();
    stats->inst_metric[metric].last_sample_count = current_reading;
}

/* Return the mean of all the samples. */
long long getInstantaneousMetric(vr_stats *stats, int metric) {
    int j;
    long long sum = 0;

    for (j = 0; j < STATS_METRIC_SAMPLES; j++) {
        long long value;
        update_stats_get(stats, inst_metric[metric].samples[j], &value);
        sum += value;
    }
    return sum / STATS_METRIC_SAMPLES;
}
