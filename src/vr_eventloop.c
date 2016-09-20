#include <vr_core.h>

int
vr_eventloop_init(vr_eventloop *vel, int filelimit)
{    
    rstatus_t status;
    int maxclients, threads_num;

    if (vel == NULL || filelimit <= 0) {
        return VR_ERROR;
    }

    vr_thread_init(&vel->thread);
    vel->el = NULL;
    vel->hz = 10;
    vel->cronloops = 0;
    vel->unixtime = time(NULL);
    vel->mstime = vr_msec_now();
    vel->lruclock = getLRUClock();
    vel->cb = NULL;
    vel->next_client_id = 1;    /* Client IDs, start from 1 .*/
    vel->current_client = NULL;
    vel->clients = NULL;
    vel->clients_pending_write = NULL;
    vel->clients_to_close = NULL;
    vel->clients_paused = 0;
    vel->clients_pause_end_time = 0;
    vel->stats = NULL;
    vel->resident_set_size = 0;
    vel->dirty = 0;
    vel->bpop_blocked_clients = 0;
    vel->unblocked_clients = NULL;
    vel->clients_waiting_acks = NULL;
    vel->pubsub_channels = NULL;
    vel->pubsub_patterns = NULL;
    vel->notify_keyspace_events = 0;
    vel->cstable = NULL;

    vel->el = aeCreateEventLoop(filelimit);
    if (vel->el == NULL) {
        log_error("create eventloop failed.");
        return VR_ERROR;
    }

    vel->cb = dalloc(sizeof(conn_base));
    if (vel->cb == NULL) {
        log_error("create conn_base failed: out of memory");
        return VR_ENOMEM;
    }
    status = conn_init(vel->cb);
    if (status != VR_OK) {
        log_error("init conn_base failed");
        return VR_ERROR;
    }

    vel->clients = dlistCreate();
    if (vel->clients == NULL) {
        log_error("create list failed: out of memory");
        return VR_ENOMEM;
    }

    vel->clients_pending_write = dlistCreate();
    if (vel->clients_pending_write == NULL) {
        log_error("create list failed: out of memory");
        return VR_ENOMEM;
    }

    vel->clients_to_close = dlistCreate();
    if (vel->clients_to_close == NULL) {
        log_error("create list failed: out of memory");
        return VR_ENOMEM;
    }

    vel->unblocked_clients = dlistCreate();
    if (vel->unblocked_clients == NULL) {
        log_error("create list failed: out of memory");
        return VR_ENOMEM;
    }

    vel->stats = dalloc(sizeof(vr_stats));
    if (vel->stats == NULL) {
        log_error("out of memory");
        return VR_ENOMEM;
    }

    vr_stats_init(vel->stats);

    conf_cache_init(&vel->cc);

    return VR_OK;
}

void
vr_eventloop_deinit(vr_eventloop *vel)
{
    if (vel == NULL) {
        return;
    }

    vr_thread_deinit(&vel->thread);

    if (vel->el != NULL) {
        aeDeleteEventLoop(vel->el);
        vel->el = NULL;
    }

    if (vel->clients != NULL) {
        client *c;
        while (c = dlistPop(vel->clients)) {
            freeClient(c);
        }
        dlistRelease(vel->clients);
        vel->clients = NULL;
    }

    if (vel->clients_pending_write != NULL) {
        client *c;
        while (c = dlistPop(vel->clients_pending_write)) {}
        dlistRelease(vel->clients_pending_write);
        vel->clients_pending_write = NULL;
    }

    if (vel->clients_to_close != NULL) {
        client *c;
        while (c = dlistPop(vel->clients_to_close)) {
            freeClient(c);
        }
        dlistRelease(vel->clients_to_close);
        vel->clients_to_close = NULL;
    }

    if (vel->unblocked_clients != NULL) {
        client *c;
        while (c = dlistPop(vel->unblocked_clients)) {}
        dlistRelease(vel->unblocked_clients);
        vel->unblocked_clients = NULL;
    }

    if (vel->cb != NULL) {
        conn_deinit(vel->cb);
        dfree(vel->cb);
        vel->cb = NULL;
    }

    if (vel->stats != NULL) {
        vr_stats_deinit(vel->stats);
        dfree(vel->stats);
        vel->stats = NULL;
    }

    if (vel->cstable != NULL) {
        commandStatsTableDestroy(vel->cstable);
        vel->cstable = NULL;
    }

    conf_cache_deinit(&vel->cc);
}

