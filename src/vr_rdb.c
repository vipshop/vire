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

static int rdbSaveBackground(char *filename) {
    int idx;
    redisDb *db;
    dictIterator *di;
    dictEntry *de;
    unsigned long size;

    for (idx = 0; idx < server.dbnum; idx ++) {
        db = array_get(&server.dbs, (uint32_t)idx);
        lockDbRead(db);
        size = dictSize(db->dict);
        if (size == 0) {
            unlockDb(db);
            continue;
        }

        if (db->snapshot_keys == NULL) {
            db->snapshot_keys = dhashtableCreate(size);
        } else {
            dhashtableExpandIfNeeded(db->snapshot_keys, size);
            dhashtableReset(db->snapshot_keys);
        }
        
        di = dictGetIterator(db->dict);
        while ((de = dictNext(di)) != NULL) {
            dhashtableAdd(db->snapshot_keys, dictGetKey(de));
        }
        dictReleaseIterator(di);
        unlockDb(db);
    }    
    
    return VR_OK;
}

void bgsaveCommand(client *c) {
    int bgsave_state;
    atomic_get(server.rdb_generating,&bgsave_state);
    
    if (bgsave_state == 1) {
        addReplyError(c,"Background save already in progress");
    } else if (rdbSaveBackground(server.rdb_filename) == VR_OK) {
        addReplyStatus(c,"Background saving started");
    } else {
        addReply(c,shared.err);
    }
}
