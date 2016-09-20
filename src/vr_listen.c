#include <sys/stat.h>
#include <sys/un.h>

#include <vr_core.h>

vr_listen *
vr_listen_create(sds listen_str)
{
    rstatus_t status;
    vr_listen *vlisten;
    uint8_t *p, *name;
    uint32_t namelen;

    if (listen_str == NULL) {
        return NULL;
    }

    vlisten = dalloc(sizeof(struct vr_listen));
    if (vlisten == NULL) {
        return NULL;
    }

    vlisten->name = NULL;
    vlisten->port = 0;
    memset(&vlisten->info, 0, sizeof(vlisten->info));
    vlisten->sd = -1;
    
    if (listen_str == '/') {
        uint8_t *q, *start, *perm;
        uint32_t permlen;

        /* parse "socket_path permissions" from the end */
        p = listen_str + sdslen(listen_str) - 1;
        start = listen_str;
        q = vr_strrchr(p, start, ' ');
        if (q == NULL) {
            /* no permissions field, so use defaults */
            name = listen_str;
            namelen = sdslen(listen_str);
        } else {
            perm = q + 1;
            permlen = (uint32_t)(p - perm + 1);

            p = q - 1;
            name = start;
            namelen = (uint32_t)(p - start + 1);

            errno = 0;
            vlisten->perm = (mode_t)strtol((char *)perm, NULL, 8);
            if (errno || vlisten->perm > 0777) {
                log_error("config file has an invalid file permission in \"socket_path permission\" format string");
                vr_listen_destroy(vlisten);
                return NULL;
            }
        }
    } else {
        uint8_t *q, *start, *port;
        uint32_t portlen;

        /* parse "hostname:port" from the end */
        p = listen_str + sdslen(listen_str) - 1;
        start = listen_str;
        q = vr_strrchr(p, start, ':');
        if (q == NULL) {
            log_error("config file has an invalid \"hostname:port\" format string");
            vr_listen_destroy(vlisten);
            return NULL;
        }

        port = q + 1;
        portlen = (uint32_t)(p - port + 1);

        p = q - 1;

        name = start;
        namelen = (uint32_t)(p - start + 1);

        vlisten->port = vr_atoi(port, portlen);
        if (vlisten->port < 0 || !vr_valid_port(vlisten->port)) {
            log_error("config file has an invalid port in \"hostname:port\" format string");
            vr_listen_destroy(vlisten);
            return NULL;
        }
    }

    vlisten->name = sdsnewlen(name, namelen);
    if (vlisten->name == NULL) {
        log_error("create a sds string failed: out of memory.");
        vr_listen_destroy(vlisten);
        return NULL;
    }

    status = vr_resolve(vlisten->name, vlisten->port, &vlisten->info);
    if (status != VR_OK) {
        vr_listen_destroy(vlisten);
        return NULL;
    }

    return vlisten;
}

void
vr_listen_destroy(vr_listen *vliston)
{
    if (vliston == NULL) {
        return;
    }

    if (vliston->name) {
        sdsfree(vliston->name);
        vliston->name = NULL;
    }

    if (vliston->sd > 0) {
        close(vliston->sd);
        vliston->sd = -1;
    }
    
    dfree(vliston);
}

static rstatus_t
vr_listen_reuse(vr_listen *p)
{
    rstatus_t status;
    struct sockaddr_un *un;

    switch (p->info.family) {
    case AF_INET:
    case AF_INET6:
        status = vr_set_reuseaddr(p->sd);
        break;

    case AF_UNIX:
        /*
         * bind() will fail if the pathname already exist. So, we call unlink()
         * to delete the pathname, in case it already exists. If it does not
         * exist, unlink() returns error, which we ignore
         */
        un = (struct sockaddr_un *) &p->info.addr;
        unlink(un->sun_path);
        status = VR_OK;
        break;

    default:
        NOT_REACHED();
        status = VR_ERROR;
    }

    return status;
}

rstatus_t
vr_listen_begin(vr_listen *vlisten)
{
    rstatus_t status;

    vlisten->sd = socket(vlisten->info.family, SOCK_STREAM, 0);
    if (vlisten->sd < 0) {
        log_error("socket failed: %s", strerror(errno));
        return VR_ERROR;
    }

    status = vr_listen_reuse(vlisten);
    if (status < 0) {
        log_error("reuse of addr %s for listening on p %d failed: %s",
                  vlisten->name, vlisten->sd, strerror(errno));
        return VR_ERROR;
    }

    status = bind(vlisten->sd, (struct sockaddr *)&vlisten->info.addr, vlisten->info.addrlen);
    if (status < 0) {
        log_error("bind on p %d to addr %s failed: %s", vlisten->sd,
                  vlisten->name, strerror(errno));
        return VR_ERROR;
    }

    if (vlisten->info.family == AF_UNIX && vlisten->perm) {
        struct sockaddr_un *un = (struct sockaddr_un *)&vlisten->info.addr;
        status = chmod(un->sun_path, vlisten->perm);
        if (status < 0) {
            log_error("chmod on p %d on addr %s failed: %s", vlisten->sd,
                      vlisten->name, strerror(errno));
            return VR_ERROR;
        }
    }

    status = listen(vlisten->sd, 512);
    if (status < 0) {
        log_error("listen on p %d on addr %s failed: %s", vlisten->sd,
                  vlisten->name, strerror(errno));
        return VR_ERROR;
    }

    status = vr_set_nonblocking(vlisten->sd);
    if (status < 0) {
        log_error("set nonblock on p %d on addr %s failed: %s", vlisten->sd,
                  vlisten->name, strerror(errno));
        return VR_ERROR;
    }

    return VR_OK;
}

int
vr_listen_accept(vr_listen *vlisten)
{
    rstatus_t status;
    int sd;
    int maxclients;
    
    ASSERT(vlisten->sd > 0);
    
    log_debug(LOG_DEBUG,"client_accept");

    conf_server_get(CONFIG_SOPN_MAXCLIENTS,&maxclients);
    for (;;) {
        sd = accept(vlisten->sd, NULL, NULL);
        if (sd < 0) {
            if (errno == EINTR) {
                log_debug(LOG_VERB, "accept on p %d not ready - eintr", vlisten->sd);
                continue;
            }

            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == ECONNABORTED) {
                log_debug(LOG_VERB, "accept on p %d not ready - eagain", vlisten->sd);
                return -1;
            }
            
            if (errno == EMFILE || errno == ENFILE) {
                log_debug(LOG_CRIT, "accept on p %d "
                          "max client connections %d "
                          "curr client connections %d failed: %s",
                          vlisten->sd, maxclients, 
                          current_clients(), strerror(errno));
                return -1;
            }

            log_warn("accept on p %d failed: %s", vlisten->sd, strerror(errno));

            return -1;
        }

        break;
    }

    if (current_clients() >= maxclients) {
        log_debug(LOG_CRIT, "client connections %d exceed limit %d",
                  current_clients(), maxclients);
        status = close(sd);
        if (status < 0) {
            log_error("close c %d failed, ignored: %s", sd, strerror(errno));
        }

        update_stats_add(master.vel.stats, rejected_conn, 1);
        
        return -1;
    }

    return sd;
}
