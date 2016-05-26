#include <vr_core.h>

int
vr_eventloop_init(vr_eventloop *vel)
{    
    rstatus_t status;

    if (vel == NULL) {
        return VR_ERROR;
    }

    vr_thread_init(&vel->thread);
    vel->el = NULL;
    vel->cronloops = 0;
    vel->unixtime = 0;
    vel->mstime = 0;
    vel->cb = NULL;
    vel->current_client = NULL;
    vel->clients = NULL;
    vel->clients_pending_write = NULL;
    vel->clients_to_close = NULL;
    vel->clients_paused = 0;
    vel->clients_pause_end_time = 0;
    vel->stats = NULL;
    vel->bpop_blocked_clients = 0;
    vel->unblocked_clients = NULL;
    vel->clients_waiting_acks = NULL;
    vel->pubsub_channels = NULL;
    vel->pubsub_patterns = NULL;
    vel->notify_keyspace_events = 0;

    vel->el = aeCreateEventLoop(1024);
    if (vel->el == NULL) {
        log_error("create eventloop failed.");
        return VR_ERROR;
    }

    vel->cb = vr_alloc(sizeof(conn_base));
    if (vel->cb == NULL) {
        log_error("create conn_base failed: out of memory");
        return VR_ENOMEM;
    }
    status = conn_init(vel->cb);
    if (status != VR_OK) {
        log_error("init conn_base failed");
        return VR_ERROR;
    }

    vel->clients = listCreate();
    if (vel->clients == NULL) {
        log_error("create list failed: out of memory");
        return VR_ENOMEM;
    }

    vel->clients_pending_write = listCreate();
    if (vel->clients_pending_write == NULL) {
        log_error("create list failed: out of memory");
        return VR_ENOMEM;
    }

    vel->clients_to_close = listCreate();
    if (vel->clients_to_close == NULL) {
        log_error("create list failed: out of memory");
        return VR_ENOMEM;
    }

    vel->unblocked_clients = listCreate();
    if (vel->unblocked_clients == NULL) {
        log_error("create list failed: out of memory");
        return VR_ENOMEM;
    }

    vel->stats = vr_alloc(sizeof(vr_stats));
    if (vel->stats == NULL) {
        log_error("out of memory");
        return VR_ENOMEM;
    }

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
        while (c = listPop(vel->clients)) {
            freeClient(c);
        }
        listRelease(vel->clients);
        vel->clients = NULL;
    }

    if (vel->clients_pending_write != NULL) {
        client *c;
        while (c = listPop(vel->clients_pending_write)) {}
        listRelease(vel->clients_pending_write);
        vel->clients_pending_write = NULL;
    }

    if (vel->clients_to_close != NULL) {
        client *c;
        while (c = listPop(vel->clients_to_close)) {
            freeClient(c);
        }
        listRelease(vel->clients_to_close);
        vel->clients_to_close = NULL;
    }

    if (vel->unblocked_clients != NULL) {
        client *c;
        while (c = listPop(vel->unblocked_clients)) {}
        listRelease(vel->unblocked_clients);
        vel->unblocked_clients = NULL;
    }

    if (vel->cb != NULL) {
        conn_deinit(vel->cb);
        vr_free(vel->cb);
        vel->cb = NULL;
    }

    if (vel->stats != NULL) {
        vr_free(vel->stats);
        vel->stats = NULL;
    }
}

