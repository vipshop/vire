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

    ret = pthread_spin_init(&stats->statslock, 0);
    if (ret != 0) {
        return VR_ERROR;
    }

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

    pthread_spin_destroy(&stats->statslock);
}
