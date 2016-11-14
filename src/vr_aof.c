#include <fcntl.h>
#include <unistd.h>

#include <vr_core.h>

/* Write the append only file buffer on disk.
 *
 * Since we are required to write the AOF before replying to the client,
 * and the only way the client socket can get a write is entering when the
 * the event loop, we accumulate all the AOF writes in a memory
 * buffer and write it on disk using this function just before entering
 * the event loop again.
 *
 * About the 'force' argument:
 *
 * When the fsync policy is set to 'everysec' we may delay the flush if there
 * is still an fsync() going on in the background thread, since for instance
 * on Linux write(2) will be blocked by the background fsync anyway.
 * When this happens we remember that there is some aof buffer to be
 * flushed ASAP, and will try to do that in the serverCron() function.
 *
 * However if force is set to 1 we'll write regardless of the background
 * fsync. */
#define AOF_WRITE_LOG_ERROR_RATE 30 /* Seconds between errors logging. */
void writeAppendOnlyFile(redisDb *db)
{
    ssize_t nwritten;
    long long now = dsec_now();
    
    /* We want to perform a single write. This should be guaranteed atomic
     * at least if the filesystem we are writing is a real physical one.
     * While this will save us against the server being killed I don't think
     * there is much to do about the whole server stopping for power problems
     * or alike */
    nwritten = write(db->aof_fd,db->aof_buf,sdslen(db->aof_buf));

    if (nwritten != (signed)sdslen(db->aof_buf)) {
        int can_log = 0;

        /* Limit logging rate to 1 line per AOF_WRITE_LOG_ERROR_RATE seconds. */
        if ((now - db->aof_last_write_error_log) > AOF_WRITE_LOG_ERROR_RATE) {
            can_log = 1;
            db->aof_last_write_error_log = now;
        }

        /* Log the AOF write error and record the error code. */
        if (nwritten == -1) {
            if (can_log) {
                log_warn("Error writing to the AOF file: %s",
                    strerror(errno));
                db->aof_last_write_errno = errno;
            }
        } else {
            if (can_log) {
                log_warn("Short write while writing to "
                                       "the AOF file: (nwritten=%lld, "
                                       "expected=%lld)",
                                       (long long)nwritten,
                                       (long long)sdslen(db->aof_buf));
            }

            if (ftruncate(db->aof_fd, db->aof_current_size) == -1) {
                if (can_log) {
                    log_warn("Could not remove short write "
                             "from the append-only file.  Redis may refuse "
                             "to load the AOF the next time it starts.  "
                             "ftruncate: %s", strerror(errno));
                }
            } else {
                /* If the ftruncate() succeeded we can set nwritten to
                 * -1 since there is no longer partial data into the AOF. */
                nwritten = -1;
            }
            db->aof_last_write_errno = ENOSPC;
        }

        /* Handle the AOF write error. */
        if (server.aof_fsync_policy == AOF_FSYNC_ALWAYS) {
            /* We can't recover when the fsync policy is ALWAYS since the
             * reply for the client is already in the output buffers, and we
             * have the contract with the user that on acknowledged write data
             * is synced on disk. */
            log_warn("Can't recover from AOF write error when the AOF fsync policy is 'always'. Exiting...");
            exit(1);
        } else {
            /* Recover from failed write leaving data into the buffer. However
             * set an error to stop accepting writes as long as the error
             * condition is not cleared. */
            db->aof_last_write_status = VR_ERROR;

            /* Trim the sds buffer if there was a partial write, and there
             * was no way to undo it with ftruncate(2). */
            if (nwritten > 0) {
                db->aof_current_size += nwritten;
                sdsrange(db->aof_buf,nwritten,-1);
            }
            return; /* We'll try again on the next call... */
        }
    } else {
        /* Successful write(2). If AOF was in error state, restore the
         * OK state and log the event. */
        if (db->aof_last_write_status == VR_ERROR) {
            log_warn("AOF write error looks solved, Redis can write again.");
            db->aof_last_write_status = VR_OK;
        }
    }
    db->aof_current_size += nwritten;

    /* Re-use AOF buffer when it is small enough. The maximum comes from the
     * arena size of 4k minus some overhead (but is otherwise arbitrary). */
    if ((sdslen(db->aof_buf)+sdsavail(db->aof_buf)) < 4000) {
        sdsclear(db->aof_buf);
    } else {
        sdsfree(db->aof_buf);
        db->aof_buf = sdsempty();
    }
}

void flushAppendOnlyFile(redisDb *db, int force) {
    int sync_in_progress = 0;
    long long now;

    if (sdslen(db->aof_buf) == 0) return;

    if (db->aof_fd < 0) {
        struct stat sb;
        if (sdslen(db->aof_filename) == 0)
            generateAofFilename(db,dmsec_now());
        db->aof_fd = open(db->aof_filename,
            O_WRONLY|O_APPEND|O_CREAT,0644);
        if (db->aof_fd == -1) {
            log_error("Can't open the append-only file %s: %s",
                db->aof_filename, strerror(errno));
            return;
        }

        /* Write the select db command at the front of this aof file. */
        fstat(db->aof_fd, &sb);
        if (sb.st_size == 0) {
            sds buf;
            char seldb[64];
            
            snprintf(seldb,sizeof(seldb),"%d",db->id/server.dbinum);
            buf = sdscatprintf(sdsempty(),"*2\r\n$6\r\nSELECT\r\n$%lu\r\n%s\r\n",
            (unsigned long)strlen(seldb),seldb);
            buf = sdscatsds(buf,db->aof_buf);
            sdsfree(db->aof_buf);
            db->aof_buf = buf;
        }
    }

    if (server.aof_fsync_policy == AOF_FSYNC_EVERYSEC)
        sync_in_progress = !dmtqueue_empty(fsync_jobs);

    now = dsec_now();
    if (server.aof_fsync_policy == AOF_FSYNC_EVERYSEC && !force) {
        /* With this append fsync policy we do background fsyncing.
         * If the fsync is still in progress we can try to delay
         * the write for a couple of seconds. */
        if (sync_in_progress) {
            if (db->aof_flush_postponed_start == 0) {
                /* No previous write postponing, remember that we are
                 * postponing the flush and return. */
                db->aof_flush_postponed_start = now;
                return;
            } else if (now - db->aof_flush_postponed_start < 2) {
                /* We were already waiting for fsync to finish, but for less
                 * than two seconds this is still ok. Postpone again. */
                return;
            }
            /* Otherwise fall trough, and go write since we can't wait
             * over two seconds. */
            db->aof_delayed_fsync++;
            log_notice("Asynchronous AOF fsync is taking too long (disk is busy?). Writing the AOF buffer without waiting for fsync to complete, this may slow down Redis.");
        }
    }

    /* We performed the write so reset the postponed flush sentinel to zero. */
    db->aof_flush_postponed_start = 0;

    writeAppendOnlyFile(db);

    /* Perform the fsync if needed. */
    if (server.aof_fsync_policy == AOF_FSYNC_ALWAYS) {
        /* aof_fsync is defined as fdatasync() for Linux in order to avoid
         * flushing metadata. */
        aof_fsync(db->aof_fd); /* Let's try to get this data on the disk */
        db->aof_last_fsync = now;
    } else if ((server.aof_fsync_policy == AOF_FSYNC_EVERYSEC &&
                now > db->aof_last_fsync)) {
        if (!sync_in_progress) dmtqueue_push(fsync_jobs,(void *)db->aof_fd);
        db->aof_last_fsync = now;
    }
}

/* Create the sds representation of an PEXPIREAT command, using
 * 'seconds' as time to live and 'cmd' to understand what command
 * we are translating into a PEXPIREAT.
 *
 * This command is used in order to translate EXPIRE and PEXPIRE commands
 * into PEXPIREAT command so that we retain precision in the append only
 * file, and the time is always absolute and not relative. */
sds catAppendOnlyExpireAtCommand(sds buf, struct redisCommand *cmd, robj *key, robj *seconds) {
    long long when;
    robj *argv[3], *seconds_new;

    /* Make sure we can use strtoll */
    seconds_new = getDecodedObject(seconds);
    when = strtoll(seconds_new->ptr,NULL,10);
    /* Convert argument into milliseconds for EXPIRE, SETEX, EXPIREAT */
    if (cmd->proc == expireCommand || cmd->proc == setexCommand ||
        cmd->proc == expireatCommand)
    {
        when *= 1000;
    }
    /* Convert into absolute time for EXPIRE, PEXPIRE, SETEX, PSETEX */
    if (cmd->proc == expireCommand || cmd->proc == pexpireCommand ||
        cmd->proc == setexCommand || cmd->proc == psetexCommand)
    {
        when += vr_msec_now();
    }
    if (seconds_new != seconds) freeObject(seconds_new);

    argv[0] = createStringObject("PEXPIREAT",9);
    argv[1] = key;
    argv[2] = createStringObjectFromLongLong(when);
    buf = catAppendOnlyGenericCommand(buf, 3, argv);
    freeObject(argv[0]);
    freeObject(argv[2]);
    return buf;
}

sds catAppendOnlyGenericCommand(sds dst, int argc, robj **argv) {
    char buf[32];
    int len, j;
    robj *o;

    buf[0] = '*';
    len = 1+ll2string(buf+1,sizeof(buf)-1,argc);
    buf[len++] = '\r';
    buf[len++] = '\n';
    dst = sdscatlen(dst,buf,len);

    for (j = 0; j < argc; j++) {
        o = getDecodedObject(argv[j]);
        buf[0] = '$';
        len = 1+ll2string(buf+1,sizeof(buf)-1,sdslen(o->ptr));
        buf[len++] = '\r';
        buf[len++] = '\n';
        dst = sdscatlen(dst,buf,len);
        dst = sdscatlen(dst,o->ptr,sdslen(o->ptr));
        dst = sdscatlen(dst,"\r\n",2);
        if (o != argv[j]) freeObject(o);
    }
    return dst;
}

void feedAppendOnlyFileIfNeeded(struct redisCommand *cmd, redisDb *db, robj **argv, int argc) {
    sds buf = sdsempty();
    robj *tmpargv[3];

    if (!db->aof_enabled)
        return;

    if (cmd->proc == expireCommand || cmd->proc == pexpireCommand ||
        cmd->proc == expireatCommand) {
        /* Translate EXPIRE/PEXPIRE/EXPIREAT into PEXPIREAT */
        buf = catAppendOnlyExpireAtCommand(buf,cmd,argv[1],argv[2]);
    } else if (cmd->proc == setexCommand || cmd->proc == psetexCommand) {
        /* Translate SETEX/PSETEX to SET and PEXPIREAT */
        tmpargv[0] = createStringObject("SET",3);
        tmpargv[1] = argv[1];
        tmpargv[2] = argv[3];
        buf = catAppendOnlyGenericCommand(buf,3,tmpargv);
        freeObject(tmpargv[0]);
        buf = catAppendOnlyExpireAtCommand(buf,cmd,argv[1],argv[2]);
    } else {
        /* All the other commands don't need translation or need the
         * same translation already operated in the command vector
         * for the replication itself. */
        buf = catAppendOnlyGenericCommand(buf,argc,argv);
    }

    /* Append to the AOF buffer. This will be flushed on disk just before
     * of re-entering the event loop, so before the client will get a
     * positive reply about the operation performed. */
    db->aof_buf = sdscatlen(db->aof_buf,buf,sdslen(buf));

    sdsfree(buf);
}

int loadAppendOnlyFile(char *filename)
{
    return VR_OK;
}
