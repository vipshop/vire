#include <vr_core.h>

/* Save the DB on disk. Return C_ERR on error, C_OK on success. */
int rdbSave(char *filename) {
    return VR_OK;
}

void rdbRemoveTempFile(pid_t childpid) {
    char tmpfile[256];

    snprintf(tmpfile,sizeof(tmpfile),"temp-%d.rdb", (int) childpid);
    unlink(tmpfile);
}
