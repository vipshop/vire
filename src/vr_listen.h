#ifndef _VR_LISTEN_H_
#define _VR_LISTEN_H_

typedef struct vr_listen {
    sds name;               /* hostname:port */
    int port;               /* port */
    mode_t perm;            /* socket permissions */
    struct sockinfo info;   /* listen socket info */
    int sd;                 /* socket descriptor */
}vr_listen;

vr_listen *vr_listen_create(sds linten_str);
void vr_listen_destroy(vr_listen *vliston);
rstatus_t vr_listen_begin(struct vr_listen *vlisten);
int vr_listen_accept(vr_listen *vlisten);

#endif
