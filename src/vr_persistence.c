#include <math.h>
#include <dirent.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <sys/param.h>

#include <vr_core.h>

#define RDB_SAVE_OPERATION_COUNT_PER_TIME 100

#define DATA_LOAD_THREADS_COUNT_DEFAULT 1

typedef struct quicklistDumpHelper {
    quicklistNode *nodes;
    unsigned int len;   /* Total number of quicklistNodes need to be added. */
    unsigned int count; /* Count of quicklistNodes had be added(bikeyDump step) or had be writed(bikeyWrite step). */
    unsigned long values_count; /* total count of all entries in all ziplists. */
    long long expire;   /* expire time for this quicklist. */
} quicklistDumpHelper;

pthread_mutex_t persistence_lock;

/* Persistence information */
volatile int loading;           /* We are loading data from disk if true. */
off_t loading_total_bytes;
off_t loading_loaded_bytes;
long long loading_start_time;   /* In milliseconds */

volatile int rdb_is_generating; /* If the rdb is generating? */
int rdb_generated_count;        /* Count of rdb file have generated */
long long rdb_save_time_start;  /* Current RDB save start time in milliseconds. */
long long rdb_save_time_last;   /* Time in secends used by last RDB save run. */
long long last_rdb_saved_time;  /* Time in milliseconds of last successful saved. */

dmtqueue *fsync_jobs;           /* Persistence file sync jobs, stored file descriptor. */

static char *rdb_filename_prefix = "rdb";
static char *aof_filename_prefix = "aof";
static char *rdb_temp_filename_prefix = "rdbtmpfile";

int persistenceInit(void)
{
    int ret;
    ret = pthread_mutex_init(&persistence_lock, NULL);
    if(ret != 0)
        return VR_ERROR;

    loading = 0;
    loading_total_bytes = 0;
    loading_loaded_bytes = 0;
    loading_start_time = 0;

    rdb_is_generating = 0;
    rdb_generated_count = 0;
    rdb_save_time_last = 0;
    last_rdb_saved_time = 0;

    fsync_jobs = dmtqueue_create();
    dmtqueue_init_with_lockqueue(fsync_jobs, NULL);

    return VR_OK;
}

void persistenceDeinit(void)
{
    pthread_mutex_destroy(&persistence_lock);
    dmtqueue_destroy(fsync_jobs);
    fsync_jobs = NULL;
}

bigkeyDumper *bigkeyDumperCreate(void)
{
    bigkeyDumper *bkdumper;

    bkdumper = dalloc(sizeof(bigkeyDumper));
    bkdumper->key = NULL;
    bkdumper->type = 0;
    bkdumper->encoding = 0;
    bkdumper->write_type = BIGKEY_DUMP_WRITE_TYPE_NONE;
    bkdumper->state = BIGKEY_DUMP_STATE_GENERATING;
    bkdumper->data = NULL;
    bkdumper->cursor_field = NULL;
    bkdumper->written = 0;

#ifdef HAVE_DEBUG_LOG
    bkdumper->elements_count = 0;
    bkdumper->elements_dumped_count = 0;
#endif
    
    return bkdumper;
}

void bigkeyDumperDestroy(bigkeyDumper *bkdumper)
{
    if (bkdumper->key)
        sdsfree(bkdumper->key);
    if (bkdumper->data) {
        if (bkdumper->write_type == BIGKEY_DUMP_WRITE_TYPE_QUICKLIST) {
            quicklistDumpHelper *qldhelper = bkdumper->data;

            ASSERT(bkdumper->type == OBJ_LIST);
            ASSERT(bkdumper->encoding == OBJ_ENCODING_QUICKLIST);
            
            if (qldhelper) {
                if (qldhelper->nodes) {
                    int idx;
                    for (idx = 0; idx < qldhelper->len; idx ++) {
                        if (qldhelper->nodes[idx].zl)
                            dfree(qldhelper->nodes[idx].zl);
                    }
                    dfree(qldhelper->nodes);
                }

                dfree(qldhelper);
            }
        } else if (bkdumper->write_type == BIGKEY_DUMP_WRITE_TYPE_SDS) {
            sdsfree(bkdumper->data);
        } else {
            NOT_REACHED();
        }
    }

    if (bkdumper->cursor_field) {
        if (bkdumper->encoding != OBJ_ENCODING_QUICKLIST) {
            dictReleaseIterator(bkdumper->cursor_field);
        }
    }
    
    dfree(bkdumper);
}

bigkeyDumpHelper *bigkeyDumpHelperCreate(void)
{
    bigkeyDumpHelper *bkdhelper;
    
    bkdhelper = dalloc(sizeof(bigkeyDumpHelper));
    bkdhelper->bigkeys_to_generate = NULL;
    bkdhelper->dump_to_buffer_helper = NULL;
    bkdhelper->bigkeys_to_write = NULL;
	bkdhelper->finished_bigkeys = 0;
    
    bkdhelper->bigkeys_to_generate = dictCreate(&keyptrDictType,NULL);
    bkdhelper->dump_to_buffer_helper = dalloc(sizeof(rio));
    rioInitWithBuffer(bkdhelper->dump_to_buffer_helper,NULL);
    
    bkdhelper->bigkeys_to_write = dlistCreate();

    return bkdhelper;
}

void bigkeyDumpHelperDestroy(bigkeyDumpHelper *bkdhelper)
{
    if (bkdhelper == NULL)
        return;

    if (bkdhelper->bigkeys_to_generate) {
        dictIterator *it = dictGetIterator(bkdhelper->bigkeys_to_generate);
        dictEntry *de;
        while ((de=dictNext(it)) != NULL) {
            bigkeyDumper *bkdumper = dictGetVal(de);
            bigkeyDumperDestroy(bkdumper);
        }
        dictReleaseIterator(it);
        dictRelease(bkdhelper->bigkeys_to_generate);
    }

    if (bkdhelper->bigkeys_to_write) {
        bigkeyDumper *bkdumper;
        while ((bkdumper=dlistPop(bkdhelper->bigkeys_to_write)) != NULL) {
            bigkeyDumperDestroy(bkdumper);
        }
        dlistRelease(bkdhelper->bigkeys_to_write);
    }

    dfree(bkdhelper);
}

int bigkeyDumpHelperIsBusy(bigkeyDumpHelper *bkdhelper)
{
    if (dictSize(bkdhelper->bigkeys_to_generate) > 0)
        return 1;

    if (dlistLength(bkdhelper->bigkeys_to_write) > 0)
        return 1;

    return 0;
}

int bigkeyRdbGenerate(redisDb *db, sds key, robj *val, bigkeyDumper *bkdumper, int dump_complete)
{
    int count = 0;
    int dump_finished = 0;
    bigkeyDumpHelper *bkdhelper = db->bkdhelper;
    
    if (!val)
        val = dictFetchValue(db->dict, key);

    if (!bkdumper)
        bkdumper = dictFetchValue(bkdhelper->bigkeys_to_generate, key);

    if (!val) {
        if (bkdumper) {
            dictDelete(bkdhelper->bigkeys_to_generate, key);
            bigkeyDumperDestroy(bkdumper);
        }
        return 0;
    }

    if (!bkdumper) {
        robj keyobj;
        long long expiretime;

        initStaticStringObject(keyobj,key);
        
        expiretime = getExpire(db,&keyobj);
        
        bkdumper = bigkeyDumperCreate();
        bkdumper->key = sdsdup(key);
        bkdumper->type = val->type;
        bkdumper->encoding = val->encoding;
        bkdumper->state = BIGKEY_DUMP_STATE_GENERATING;
        dictAdd(bkdhelper->bigkeys_to_generate, bkdumper->key, bkdumper);

        /* If big key encoding is quicklist, just save the quicklist 
         * node in bigkeyDumper, and generate the rdb string in the
         * bigkeyWrite step. */
        if (bkdumper->encoding == OBJ_ENCODING_QUICKLIST) {
            unsigned int idx = 0;
            quicklist *ql = val->ptr;
            quicklistNode *node = ql->head;
            quicklistDumpHelper *qldhelper = dalloc(sizeof(quicklistDumpHelper));
            qldhelper->len = ql->len;
            qldhelper->count = 0;
            qldhelper->nodes = dzalloc(qldhelper->len*sizeof(quicklistNode));
            
            bkdumper->data = qldhelper;
            bkdumper->write_type = BIGKEY_DUMP_WRITE_TYPE_QUICKLIST;
#ifdef HAVE_DEBUG_LOG
            bkdumper->elements_count = ql->len;
#endif
            
            do {
                if (node->version < db->version)
                	node->index = idx++;
            } while ((node = node->next));

            qldhelper->values_count = ql->count;
            qldhelper->expire = expiretime;
        } else { /* Other encoding big key saved as rdb string. */
            long long now = dmsec_now();
            rio *rdb = bkdhelper->dump_to_buffer_helper;
            
            bkdumper->data = sdsempty();
            rioSetBuffer(rdb,
                bkdumper->data,sdslen(bkdumper->data));
            bkdumper->write_type = BIGKEY_DUMP_WRITE_TYPE_SDS;
            
            /* Save the expire time */
            if (expiretime != -1) {
                /* If this key is already expired skip it */
                if (expiretime < now) return 0;
                if (rdbSaveType(rdb,RDB_OPCODE_EXPIRETIME_MS) == -1) return -1;
                if (rdbSaveMillisecondTime(rdb,expiretime) == -1) return -1;
            }

            /* Save type, key */
            if (rdbSaveObjectType(rdb,val) == -1) return -1;
            if (rdbSaveStringObject(rdb,&keyobj) == -1) return -1;

            switch (bkdumper->encoding) {
            case OBJ_ENCODING_HT:
            {
                ASSERT(bkdumper->type == OBJ_SET || bkdumper->type == OBJ_HASH);
                dict *ht = val->ptr;
                rdbSaveLen(rdb,dictSize(ht));

#ifdef HAVE_DEBUG_LOG
                bkdumper->elements_count = dictSize(ht);
#endif
                break;
            }
            case OBJ_ENCODING_SKIPLIST:
            {
                ASSERT(bkdumper->type == OBJ_ZSET);
                zset *zs = val->ptr;
                rdbSaveLen(rdb,dictSize(zs->dict));

#ifdef HAVE_DEBUG_LOG
                bkdumper->elements_count = dictSize(zs->dict);
#endif
                break;
            }
            default:
                NOT_REACHED();
                break;
            }

            /* Update the rdb string in the bigkeyDumper. */
            bkdumper->data = rdb->io.buffer.ptr;
        }
    }

    switch (bkdumper->encoding) {
    case OBJ_ENCODING_HT:
    {
        dict *ht = val->ptr;
        dictIterator *di;
        dictEntry *de;
        rio *rdb = bkdhelper->dump_to_buffer_helper;

        ASSERT(bkdumper->type == OBJ_SET || 
            bkdumper->type == OBJ_HASH);

        if (bkdumper->cursor_field == NULL) {
            bkdumper->cursor_field = dictGetSafeIterator(ht);
        }

        ASSERT(bkdumper->write_type == BIGKEY_DUMP_WRITE_TYPE_SDS);
        rioSetBuffer(rdb,bkdumper->data,sdslen(bkdumper->data));
        di = bkdumper->cursor_field;
        while((de = dictNext(di)) != NULL) {
            robj *fieldobj = dictGetKey(de);
            if (fieldobj->version >= db->version)
                continue;
            rdbSaveStringObject(rdb,fieldobj);
            fieldobj->version = db->version;
            if (bkdumper->type == OBJ_HASH) {
                robj *valueobj = dictGetVal(de);
                ASSERT(valueobj->version < db->version);
                rdbSaveStringObject(rdb,valueobj);
                valueobj->version = db->version;
            }

#ifdef HAVE_DEBUG_LOG
            bkdumper->elements_dumped_count ++;
#endif

            if (++count > RDB_SAVE_OPERATION_COUNT_PER_TIME && !dump_complete) {
                break;
            }
        }

        /* Update the rdb string in the bigkeyDumper. */
        bkdumper->data = rdb->io.buffer.ptr;
        
        if (de == NULL) {
            dictReleaseIterator(di);
            dump_finished = 1;
        }
        
        break;
    }
    case OBJ_ENCODING_SKIPLIST:
    {
        zset *zs = val->ptr;
        rio *rdb = bkdhelper->dump_to_buffer_helper;
        dictIterator *di;
        dictEntry *de;

        if (bkdumper->cursor_field == NULL) {
            bkdumper->cursor_field = dictGetSafeIterator(zs->dict);
        }

        ASSERT(bkdumper->write_type == BIGKEY_DUMP_WRITE_TYPE_SDS);
        rioSetBuffer(rdb,bkdumper->data,sdslen(bkdumper->data));
        di = bkdumper->cursor_field;
        while ((de = dictNext(di)) != NULL) {
            robj *eleobj = dictGetKey(de);
            double *score = dictGetVal(de);
            
            if (eleobj->version >= db->version)
                continue;

            rdbSaveStringObject(rdb,eleobj);
            rdbSaveDoubleValue(rdb,*score);
            eleobj->version = db->version;

#ifdef HAVE_DEBUG_LOG
            bkdumper->elements_dumped_count ++;
#endif
            
            if (++count > RDB_SAVE_OPERATION_COUNT_PER_TIME && !dump_complete) {
                break;
            }
        }

        /* Update the rdb string in the bigkeyDumper. */
        bkdumper->data = rdb->io.buffer.ptr;

        if (de == NULL) {
            dictReleaseIterator(di);
            dump_finished = 1;
        }
        break;
    }
    case OBJ_ENCODING_QUICKLIST:
    {
        quicklist *ql = val->ptr;
        quicklistNode *node;
        quicklistDumpHelper *qldhelper;

        ASSERT(bkdumper->write_type == BIGKEY_DUMP_WRITE_TYPE_QUICKLIST);
        qldhelper = bkdumper->data;
        
        if (bkdumper->cursor_field == NULL) {
            bkdumper->cursor_field = ql->head;
        }

        node = bkdumper->cursor_field;
        
        while (node != NULL) {
            if (node->version >= db->version) {
                node = node->next;
                continue;
            }
            
            ASSERT(node->index < qldhelper->len);
			ASSERT(qldhelper->nodes[node->index].index == 0);
            qldhelper->nodes[node->index] = *node;
            if (quicklistNodeIsCompressed(node)) {
                quicklistLZF *lzf = (quicklistLZF *)node->zl;
                size_t lzf_sz = sizeof(*lzf) + lzf->sz;
                qldhelper->nodes[node->index].zl = dalloc(lzf_sz);
                memcpy(qldhelper->nodes[node->index].zl, node->zl, lzf_sz);
            } else if (node->encoding == QUICKLIST_NODE_ENCODING_RAW) {
                qldhelper->nodes[node->index].zl = dalloc(node->sz);
                memcpy(qldhelper->nodes[node->index].zl, node->zl, node->sz);
            }
            qldhelper->count ++;
            
            node->version = db->version;
            node->index = 0;

#ifdef HAVE_DEBUG_LOG
            bkdumper->elements_dumped_count ++;
#endif

			node = node->next;
            if (++count > RDB_SAVE_OPERATION_COUNT_PER_TIME && !dump_complete) {                
                break;
            } else if (qldhelper->count >= qldhelper->len) {
                break;
            }
        }

        if (node == NULL || qldhelper->count >= qldhelper->len) {
            dump_finished = 1;
            qldhelper->count = 0;   /* This count will be used in the bigkeyWrite step. */
        } else {
			bkdumper->cursor_field = node;
       	}
        break;
    }
    default:
        NOT_REACHED();
        break;
    }

    if (dump_finished) {
        bkdumper->cursor_field = NULL;
        dictDelete(bkdhelper->bigkeys_to_generate,key);
        val->version = db->version;

#ifdef HAVE_DEBUG_LOG
        ASSERT(bkdumper->elements_dumped_count == 
            bkdumper->elements_count);
#endif

        dlistPush(bkdhelper->bigkeys_to_write,bkdumper);
        bkdumper->state = BIGKEY_DUMP_STATE_WRITING;
    }
    
    return count;
}

static int rdbSaveBigkeysGenerate(redisDb *db, int need_complete)
{
    bigkeyDumpHelper *bkdhelper = db->bkdhelper;
    int count = 0, sample_count;
    bigkeyDumper *bkdumper, *bkdumper_best;
    dictEntry *de;

generate_one_key:
    sample_count = MIN(dictSize(bkdhelper->bigkeys_to_generate), 5);

    bkdumper_best = NULL;
    while (sample_count-- > 0) {
        de = dictGetRandomKey(bkdhelper->bigkeys_to_generate);
        bkdumper = dictGetVal(de);
		if (bkdumper_best == NULL) {
			bkdumper_best = bkdumper;
			continue;
		}

		if (bkdumper_best->encoding != OBJ_ENCODING_QUICKLIST && 
			bkdumper->encoding == OBJ_ENCODING_QUICKLIST) {
			bkdumper_best = bkdumper;
			continue;
		}

		if (bkdumper_best->encoding == OBJ_ENCODING_QUICKLIST && 
			bkdumper->encoding == OBJ_ENCODING_QUICKLIST) {
			quicklistDumpHelper *qldhelper_best = bkdumper_best->data;
			quicklistDumpHelper *qldhelper = bkdumper->data;
			if (qldhelper_best->count < qldhelper->count) {
				bkdumper_best = bkdumper;
				continue;
			}
		}
		
        if (bkdumper_best->encoding != OBJ_ENCODING_QUICKLIST && 
			bkdumper->encoding != OBJ_ENCODING_QUICKLIST &&
            sdslen(bkdumper_best->data) < 
            sdslen(bkdumper->data)) {
            bkdumper_best = bkdumper;
        }
    }
    
    if (bkdumper_best == NULL)
        return count;

    count += bigkeyRdbGenerate(db, bkdumper_best->key, NULL, bkdumper_best, 0);

    if (need_complete || count < RDB_SAVE_OPERATION_COUNT_PER_TIME)
        goto generate_one_key;

    return count;
}

static int rdbSaveBigkeyWrite(redisDb *db, int need_complete)
{
    bigkeyDumpHelper *bkdhelper = db->bkdhelper;
    int count = 0;

    while (dlistLength(bkdhelper->bigkeys_to_write) > 0) {
        dlistNode *ln = dlistFirst(bkdhelper->bigkeys_to_write);
        bigkeyDumper *bkdumper = dlistNodeValue(ln);
        size_t len, remain_len;
        int written;

        if (bkdumper->write_type == BIGKEY_DUMP_WRITE_TYPE_QUICKLIST) {
            int idx;
            quicklistNode *node;
            quicklistDumpHelper *qldhelper = bkdumper->data;

            ASSERT(bkdumper->type == OBJ_LIST);
            ASSERT(bkdumper->encoding == OBJ_ENCODING_QUICKLIST);
            
            if (qldhelper->count == 0) {
                /* Save the expire time */
                if (qldhelper->expire != -1) {
                    /* If this key is already expired skip it */
                    if (qldhelper->expire < dmsec_now()) {
                        dlistDelNode(bkdhelper->bigkeys_to_write, ln);
                        bigkeyDumperDestroy(bkdumper);
						bkdhelper->finished_bigkeys ++;
                        continue;
                    }
                    rdbSaveType(db->rdb_cached,RDB_OPCODE_EXPIRETIME_MS);
                    rdbSaveMillisecondTime(db->rdb_cached,qldhelper->expire);
                }

                /* Save type, key, value */
                rdbSaveType(db->rdb_cached,RDB_TYPE_LIST_QUICKLIST);
                rdbSaveRawString(db->rdb_cached,bkdumper->key,sdslen(bkdumper->key));
				rdbSaveLen(db->rdb_cached,qldhelper->len);
            }
            for (idx = qldhelper->count; idx < qldhelper->len; idx ++) {
                node = &(qldhelper->nodes[idx]);
				if (node->version >= db->version)
					continue;
				
                if (quicklistNodeIsCompressed(node)) {
                    void *data;
                    size_t compress_len = quicklistGetLzf(node, &data);
                    written += rdbSaveLzfBlob(db->rdb_cached,data,compress_len,node->sz);
                } else {
                    written += rdbSaveRawString(db->rdb_cached,node->zl,node->sz);
                }
                /* This field will never be used, free it as soon as possible. */
                dfree(node->zl); node->zl = NULL;
                
                qldhelper->count ++;
				count = written/1;
                if (!need_complete && count >= RDB_SAVE_OPERATION_COUNT_PER_TIME) {
                    if (qldhelper->count >= qldhelper->len) {
                        dlistDelNode(bkdhelper->bigkeys_to_write, ln);
                        bigkeyDumperDestroy(bkdumper);
						bkdhelper->finished_bigkeys ++;
                    }
                    return count;
                }
            }
        } else if (bkdumper->write_type == BIGKEY_DUMP_WRITE_TYPE_SDS) {
            while ((remain_len = sdslen(bkdumper->data)-bkdumper->written) > 0) {
                len = MIN(remain_len, 2048);
                written = rdbWriteRaw(db->rdb_cached, 
                    bkdumper->data+bkdumper->written, len);
                if (written < 0) {
                    log_error("Write to rdb file %s error: %s", 
                        db->rdb_tmpfilename, strerror(errno));
                    return -1;
                }
                bkdumper->written += (size_t)written;
                if (!need_complete && ++count >= RDB_SAVE_OPERATION_COUNT_PER_TIME) {
                    if (bkdumper->written >= sdslen(bkdumper->data)) {
                        dlistDelNode(bkdhelper->bigkeys_to_write, ln);
                        bigkeyDumperDestroy(bkdumper);
						bkdhelper->finished_bigkeys ++;
                    }
                    return count;
                }
            }
        }else {
            NOT_REACHED();
        }

        dlistDelNode(bkdhelper->bigkeys_to_write, ln);
        bigkeyDumperDestroy(bkdumper);
		bkdhelper->finished_bigkeys ++;
    }

    return count;
}

/* Save a key-value pair, with expire time, type, key, value.
 * On error -1 is returned.
 * On success if the key was actually saved 1 is returned, otherwise 0
 * is returned (the key was already expired). */
static int rdbSaveKeyComplete(redisDb *db, sds key, robj *val)
{
    long long now = 0;
    robj keyobj;
    bigkeyDumpHelper *bkdhelper = db->bkdhelper;
    long long expire;

    initStaticStringObject(keyobj,key);

    expire = getExpire(db,&keyobj);
    if (expire != -1) {
        now = dmsec_now();
        /* If this key is already expired skip it */
        if (expire < now) return 0;
    }

    /* If there are big keys waiting for write to rdb file,
     * we append this key rdb string behind the waiting list
     * no matter this key is big or not, as the first big key
     * in the waiting list just writed partial data at the end
     * of the rdb file. */
    if (dlistLength(bkdhelper->bigkeys_to_write) > 0) {
        int ret;
        bigkeyDumper *bkdumper = bigkeyDumperCreate();
        bkdumper->type = val->type;
        bkdumper->encoding = val->encoding;
        bkdumper->write_type = BIGKEY_DUMP_WRITE_TYPE_SDS;
        bkdumper->state = BIGKEY_DUMP_STATE_WRITING;
        if (!bkdumper->data) bkdumper->data = sdsempty();
        rioSetBuffer(bkdhelper->dump_to_buffer_helper, 
            bkdumper->data, sdslen(bkdumper->data));
        ret = rdbSaveKeyValuePair(db,bkdhelper->dump_to_buffer_helper,
            &keyobj,val,expire,now);
        
        /* Update the rdb string in the bigkeyDumper. */
        bkdumper->data = bkdhelper->dump_to_buffer_helper->io.buffer.ptr;
        dlistPush(bkdhelper->bigkeys_to_write, bkdumper);
        return ret;
    }

    return rdbSaveKeyValuePair(db,db->rdb_cached,&keyobj,val,expire,now);
}

/* Return -1: error; >=0: this key dumped fields count. */
int rdbSaveKeyIfNeeded(redisDb *db, dictEntry *de, sds key, robj *val, int dump_complete)
{
    int count = 0;
    bigkeyDumper *bkdumper;

    ASSERT(!(de == NULL && key == NULL));
    
    if (!(db->flags&DB_FLAGS_DUMPING))
        return 0;

    if (de == NULL && val == NULL) {
        ASSERT(key != NULL);
        de = dictFind(db->dict,key);
    } else if (key == NULL) {
        ASSERT(de != NULL);
        key = dictGetKey(de);
    }

    /* This key does not exist. */
    if (de == NULL && val == NULL)
        return 0;

#ifdef HAVE_DEBUG_LOG
    if (de == NULL)
        de = dictFind(db->dict,key);
#endif

    if (val == NULL) {
        ASSERT(de != NULL);
        val = dictGetVal(de);
    } else if (de) {
        ASSERT(val == dictGetVal(de));
    }
    ASSERT(val != NULL);
    if ((db->flags&DB_FLAGS_DUMP_FIRST_STEP)) {
        rdbSave(db,0);
    }

    if (val->version >= db->version)
        return 0;

    switch (val->encoding) {
    case OBJ_ENCODING_RAW:
    case OBJ_ENCODING_INT:
    case OBJ_ENCODING_ZIPMAP:
    case OBJ_ENCODING_ZIPLIST:
    case OBJ_ENCODING_INTSET:
    case OBJ_ENCODING_EMBSTR:
        count = 1;
        goto complete_dump;
    case OBJ_ENCODING_HT:
    {
        dict *ht;
        
        /* If this key is partial dumping. */
        if ((bkdumper = dictFetchValue(db->bkdhelper->bigkeys_to_generate, key)) != NULL) {
            return bigkeyRdbGenerate(db, key, val, bkdumper, dump_complete);
        }

        ht = val->ptr;
        if (dictSize(ht) <= RDB_SAVE_OPERATION_COUNT_PER_TIME || dump_complete) {
            count = dictSize(ht);
            goto complete_dump;
        }
        
        count = bigkeyRdbGenerate(db, key, val, NULL, dump_complete);
        break;
    }
    case OBJ_ENCODING_LINKEDLIST:
    {
        /* This encoding does not used now. */
        NOT_REACHED();
        break;
    }
    case OBJ_ENCODING_SKIPLIST:
    {
        zset *zs;
        
        /* If this key is partial dumping. */
        if ((bkdumper = dictFetchValue(db->bkdhelper->bigkeys_to_generate, key)) != NULL) {
            return bigkeyRdbGenerate(db, key, val, bkdumper, dump_complete);
        }

        zs = val->ptr;
        if (dictSize(zs->dict) <= RDB_SAVE_OPERATION_COUNT_PER_TIME || dump_complete) {
            count = dictSize(zs->dict);
            goto complete_dump;
        }

        count = bigkeyRdbGenerate(db, key, val, NULL, dump_complete);
        break;
    }
    case OBJ_ENCODING_QUICKLIST:
    {        
        /* If this key is partial dumping. */
        if ((bkdumper = dictFetchValue(db->bkdhelper->bigkeys_to_generate, key)) != NULL) {
            return bigkeyRdbGenerate(db, key, val, bkdumper, dump_complete);
        }

        quicklist *ql = val->ptr;
        if (ql->count <= RDB_SAVE_OPERATION_COUNT_PER_TIME || dump_complete) {
            count = ql->count;
            goto complete_dump;
        }

        count = bigkeyRdbGenerate(db, key, val, NULL, dump_complete);
        break;
    }
    default:
        NOT_REACHED();
        break;
    }

    return count;

complete_dump:
    
    rdbSaveKeyComplete(db, key, val);
    
    return count;
}

/* Produces a dump of the database in RDB format sending it to the specified
 * Redis I/O channel. On success dumped keys count is returned, otherwise -1
 * is returned and part of the output, or all the output, can be
 * missing because of I/O errors.
 *
 * When the function returns -1 and if 'error' is not NULL, the
 * integer pointed by 'error' is set to the value of errno just after the I/O
 * error. The integer pointed by 'finished' is set to 1 if this db dumped 
 * finished. */
static int rdbSaveRio(redisDb *db, int need_complete, int *error, int *finished) {
    int ret;
    rio *rdb = db->rdb_cached;
    dictIterator *di;
    dictEntry *de;
    char magic[10];
    int count = 0;
    long long now = dmsec_now();
    uint64_t cksum;

    *finished = 0;
    
#ifdef HAVE_DEBUG_LOG
	db->times_rdbSaveRio ++;
#endif

    if ((db->flags&DB_FLAGS_DUMP_FIRST_STEP)) {
        if (server.rdb_checksum)
            rdb->update_cksum = rioGenericUpdateChecksum;
        snprintf(magic,sizeof(magic),"REDIS%04d",RDB_VERSION);
        if (rdbWriteRaw(rdb,magic,9) == -1) goto werr;
        if (rdbSaveInfoAuxFields(rdb) == -1) goto werr;

        ASSERT(db->cursor_key == NULL);
        db->cursor_key = dictGetSafeIterator(db->dict);
    }

    if ((db->flags&DB_FLAGS_DUMP_FIRST_STEP)) {
        /* Write the SELECT DB opcode */
        if (rdbSaveType(rdb,RDB_OPCODE_SELECTDB) == -1) goto werr;
        if (rdbSaveLen(rdb,db->id/server.dbinum) == -1) goto werr;

        /* Write the RESIZE DB opcode. We trim the size to UINT32_MAX, which
         * is currently the largest type we are able to represent in RDB sizes.
         * However this does not limit the actual size of the DB to load since
         * these sizes are just hints to resize the hash tables. */
        uint32_t db_size, expires_size;
        db_size = (dictSize(db->dict) <= UINT32_MAX) ?
                                dictSize(db->dict) :
                                UINT32_MAX;
        expires_size = (dictSize(db->expires) <= UINT32_MAX) ?
                                dictSize(db->expires) :
                                UINT32_MAX;
        if (rdbSaveType(rdb,RDB_OPCODE_RESIZEDB) == -1) goto werr;
        if (rdbSaveLen(rdb,db_size) == -1) goto werr;
        if (rdbSaveLen(rdb,expires_size) == -1) goto werr;
        db->flags &= ~DB_FLAGS_DUMP_FIRST_STEP;
    }

one_more_time:
    /* 1st. Write the generated rdb string for big keys. */
    ret = rdbSaveBigkeyWrite(db,need_complete);
    if (ret < 0) {
        goto end;
    } else if (ret > 0) {
        count += ret;
        /* If there are big keys in the bigkeyDumpHelper, 
         * we'd better not dump new keys. */
        if (!need_complete && bigkeyDumpHelperIsBusy(db->bkdhelper))
			if (count > RDB_SAVE_OPERATION_COUNT_PER_TIME) goto end;
			else goto one_more_time;
    }

    /* 2nd. Generate rdb string for big keys. */
    ret = rdbSaveBigkeysGenerate(db,need_complete);
    if (ret < 0) {
        goto end;
    } else if (ret > 0) {
        count += ret;
        /* If there are big keys in the bigkeyDumpHelper, 
         * we'd better not dump new keys. */
        if (!need_complete && bigkeyDumpHelperIsBusy(db->bkdhelper))
            if (count > RDB_SAVE_OPERATION_COUNT_PER_TIME) goto end;
			else goto one_more_time;
    }

	/* 3rd. Iterate this DB to dump every entry */
	di = db->cursor_key;
    if (di == NULL)
    	return -1;
    while((de = dictNext(di)) != NULL) {
        ret = rdbSaveKeyIfNeeded(db,de,NULL,NULL,0);
        
        count += ret;
        if (!need_complete && count >= RDB_SAVE_OPERATION_COUNT_PER_TIME) {
            /* Break and continue for the next time. */
            break;
        }
    }
    
    if (de == NULL && !bigkeyDumpHelperIsBusy(db->bkdhelper)) {
        *finished = 1;
        dictReleaseIterator(di);
        db->cursor_key = NULL;
    }
    
end:

    if (need_complete) ASSERT(*finished == 1);
    
    if (*finished == 1) {
        /* EOF opcode */
        if (rdbSaveType(rdb,RDB_OPCODE_EOF) == -1) goto werr;

        /* CRC64 checksum. It will be zero if checksum computation is disabled, the
         * loading code skips the check in this case. */
        cksum = rdb->cksum;
        memrev64ifbe(&cksum);
        if (rioWrite(rdb,&cksum,8) == 0) goto werr;
        if (db->rdb_cached) {
            dfree(db->rdb_cached);
            db->rdb_cached= NULL;
        }
    }
    
    return count;

werr:
    if (error) *error = errno;
    if (di) dictReleaseIterator(di);
    return -1;
}

static int rdbSaveFinished(void)
{
    PERSIS_LOCK();
    rdb_generated_count ++;
    
    /* Check if all the rdb files generate finished. */
    if (rdb_generated_count >= server.dbnum) {
        ASSERT(rdb_is_generating == 1);
        rdb_generated_count = 0;
        rdb_is_generating = 0;
        last_rdb_saved_time = dmsec_now();
        rdb_save_time_last = (last_rdb_saved_time-rdb_save_time_start)/1000;
        log_notice("All rdb files saved, used %.3f seconds",(float)(last_rdb_saved_time-rdb_save_time_start)/1000);
    }
    PERSIS_UNLOCK();

    return VR_OK;
}

/* Save the DB on disk. Return success dump keys count, and -1 on error.*/
int rdbSave(redisDb *db, int need_complete) {
    FILE *fp;
    dict *d;
    dictIterator *di = NULL;
    dictEntry *de;
    int count;
    int error = 0, finished = 0;
    char cwd[MAXPATHLEN]; /* Current working dir path for error messages. */
    rio *rdb;

    if ((db->flags&DB_FLAGS_DUMP_FIRST_STEP)) {
        fp = fopen(db->rdb_tmpfilename,"w");
        if (!fp) {
            char *cwdp = getcwd(cwd,MAXPATHLEN);
            log_warn("Failed opening the RDB file %s (in server root dir %s) "
                "for saving: %s", db->rdb_tmpfilename, cwdp ? cwdp : "unknown",
                strerror(errno));
            return VR_ERROR;
        }

        rdb = dalloc(sizeof(rio));
        rioInitWithFile(rdb,fp);
        ASSERT(db->rdb_cached == NULL);
        db->rdb_cached = rdb;
        db->dirty_before_bgsave = db->dirty;
    } else {
        rdb = db->rdb_cached;
        ASSERT(rdb != NULL);
        fp = rdb->io.file.fp;
    }
    
    count = rdbSaveRio(db,need_complete,&error,&finished);
    if (count == -1) {
        errno = error;
        goto werr;
    }

    if (finished) {        
        /* Make sure data will not remain on the OS's output buffers */
        if (fflush(fp) == EOF) goto werr;
        if (fsync(fileno(fp)) == -1) goto werr;
        if (fclose(fp) == EOF) goto werr;

        /* Use RENAME to make sure the DB file is changed atomically only
         * if the generate DB file is ok. */
        if (rename(db->rdb_tmpfilename,db->rdb_filename) == -1) {
            char *cwdp = getcwd(cwd,MAXPATHLEN);
            log_warn(
                "Error moving temp DB file %s on the final "
                "destination %s (in server root dir %s): %s",
                db->rdb_tmpfilename,
                db->rdb_filename,
                cwdp ? cwdp : "unknown",
                strerror(errno));
            unlink(db->rdb_tmpfilename);
            return -1;
        }

        /* Use RENAME to make sure the DB file is changed atomically only
         * if the generate DB file is ok. */
        /*
        if (db->aof_enabled && rename(db->aof_tmpfilename,db->aof_filename) == -1) {
            char *cwdp = getcwd(cwd,MAXPATHLEN);
            log_warn(
                "Error moving temp aof file %s on the final "
                "destination %s (in server root dir %s): %s",
                db->aof_tmpfilename,
                db->aof_filename,
                cwdp ? cwdp : "unknown",
                strerror(errno));
            unlink(db->aof_tmpfilename);
            return -1;
        }*/

        log_notice("DB %d saved on disk", db->id);
        server.lastsave = time(NULL);
        server.lastbgsave_status = VR_OK;

        ASSERT((db->flags&DB_FLAGS_DUMP_FIRST_STEP) == 0);
        db->flags &= ~DB_FLAGS_DUMPING;
        db->dirty -= db->dirty_before_bgsave;
        db->dirty_before_bgsave = 0;

        rdbSaveFinished();
    }
    
    return count;

werr:
    log_warn("Write error saving DB on disk: %s", strerror(errno));
    fclose(fp);
    unlink(db->rdb_tmpfilename);
    return -1;
}

/* Dumping keys for an amount of time between us microseconds and us+1 microseconds */
static int rdbSaveMicroseconds(redisDb *db, int us) {
    long long start = dusec_now(), used_time;
    int dumps = 0, count;

#ifdef HAVE_DEBUG_LOG
	db->times_rdbSaveMicroseconds ++;
#endif

    while (1) {
        count = rdbSave(db,0);
        if (count <= 0) break;
        dumps += count;
		used_time = dusec_now()-start;
        if (used_time > us) break;
    }

	if (used_time > 2000)
		log_debug(LOG_NOTICE, "used too long time: %lld, dumps %d", used_time, dumps);
	
    return dumps;
}

/* Our db dump implementation performs dump keys incrementally while
 * we write/read from the hash table. So we try to use 1 millisecond
 * of CPU time at every call of this function to perform some dumping.
 *
 * The function returns 1 if some dumping was performed, otherwise 0
 * is returned. */
int incrementallyRdbSave(redisDb *db) {
    lockDbWrite(db);
    
    /* Perform dumping some keys if needed. */
    if (!(db->flags&DB_FLAGS_DUMPING)) {
        unlockDb(db);
        return 0;
    }

    rdbSaveMicroseconds(db, 200);

    flushAppendOnlyFile(db, 0);

    unlockDb(db);
    return 1;
}

/* This function just set rdb dump flag for every db, 
 * and the real dumping work will be doing in the databaseCron(). */
static int rdbSaveBackground(char *filename) {
    long long start;
    int idx;
    redisDb *db;
    long long version_old = -1;
    long long timestamp;

    PERSIS_LOCK();
    if (loading == 1 || rdb_is_generating == 1) {
        PERSIS_UNLOCK();
        return VR_ERROR;
    }
    rdb_is_generating = 1;
    rdb_save_time_start = dmsec_now();
    PERSIS_UNLOCK();

    timestamp = dmsec_now();
    for (idx = 0; idx < server.dbnum; idx ++) {
        db = darray_get(&server.dbs, (uint32_t)idx);
        lockDbWrite(db);

        ASSERT(!(db->flags&(DB_FLAGS_DUMPING|DB_FLAGS_DUMP_FIRST_STEP)));
        db->flags |= DB_FLAGS_DUMPING;
        db->flags |= DB_FLAGS_DUMP_FIRST_STEP;
        
        if (version_old < 0) {
            version_old = db->version;
        } else {
            ASSERT(version_old == db->version);
        }
        
        db->version ++;
        
        generateRdbFilename(db,timestamp);
        generateRdbTmpFilename(db,timestamp);

        /* Write the new command to the temp aof file, 
         * and after the bgsave finished, rename teme aof 
         * file to the aof file. The all data are always 
         * stored with rdb file and aof file. */
        if (db->aof_enabled) {
            char seldb[64];
            
            if (db->aof_fd > 0) {
                int retry = 0;
                
                /* Write the remained data to the old aof file. */
                while (sdslen(db->aof_buf) > 0 && retry < 3) {
                    writeAppendOnlyFile(db);
                    retry++;
                }
                if (sdslen(db->aof_buf) > 0) {
                    /* Try the next called time. */
                    log_warn("Old aof buffer data are not writed to old aof file absolutely.");
                    sdsclear(db->aof_buf);
                }
                close(db->aof_fd);
                db->aof_fd = -1;
                db->aof_current_size = 0;
            }

            generateAofFilename(db,timestamp);

            /*
            db->aof_fd = open(db->aof_filename,
                O_WRONLY|O_APPEND|O_CREAT,0644);
            if (db->aof_fd == -1) {
                log_error("Can't open the append-only file %s: %s",
                    db->aof_filename, strerror(errno));
                unlockDb(db);
                continue;
            }
            */
            /* Write the select db command at the front of this aof file. */
            /*
            ASSERT(sdslen(db->aof_buf) == 0);
            snprintf(seldb,sizeof(seldb),"%d",db->id/server.dbinum);
            db->aof_buf = sdscatprintf(db->aof_buf,"*2\r\n$6\r\nSELECT\r\n$%lu\r\n%s\r\n",
                (unsigned long)strlen(seldb),seldb);
            */
        }
        
        unlockDb(db);
    }    
    
    return VR_OK; /* unreached */
}

void bgsaveCommand(client *c) {
    if (server.rdb_child_pid != -1) {
        addReplyError(c,"Background save already in progress");
    } else if (server.aof_child_pid != -1) {
        addReplyError(c,"Can't BGSAVE while AOF log rewriting is in progress");
    } else if (rdbSaveBackground(server.rdb_filename) == VR_OK) {
        addReplyStatus(c,"Background saving started");
    } else {
        addReply(c,shared.err);
    }
}

typedef struct load_thread {
    int id;
    pthread_t thread_id;

    darray *pps; /* Stored persistence date part, type is 'struct persisPart*'. */
} load_thread;

static int num_load_threads = 0;
static int load_finished_threads_count = 0;

struct darray load_threads;

static int loadThreadInit(load_thread *loader)
{
    loader->id = 0;
    loader->thread_id = 0;
    loader->pps = NULL;

    loader->pps = darray_create(10, sizeof(persisPart*));
    return VR_OK;
}

static void loadThreadDeinit(load_thread *loader)
{
    persisPart **pp;
    while ((pp = darray_pop(loader->pps)) != NULL) {
        /* nothing */
    }
    darray_destroy(loader->pps);
    loader->pps = NULL;
}

/* Filename partition: prefix+dbId+internalDbCount+timestamp+dbVersion */
int generatePersisFilename(redisDb *db, long long timestamp, int type)
{
    char *filename_prefix;
    sds *filename;
    
    switch (type) {
    case PERSIS_FILE_TYPE_RDB:
        filename_prefix = rdb_filename_prefix;
        filename = &(db->rdb_filename);
        break;
    case PERSIS_FILE_TYPE_AOF:
        filename_prefix = aof_filename_prefix;
        filename = &(db->aof_filename);
        break;
    case PERSIS_FILE_TYPE_RDBTMP:
        filename_prefix = rdb_temp_filename_prefix;
        filename = &(db->rdb_tmpfilename);
        break;
    default:
        return VR_ERROR;
    }

    if (sdslen(*filename) > 0) {
        sdsclear(*filename);
    }
    *filename = sdscatfmt(*filename, "%s_%i_%i_%I_%i", 
        filename_prefix, db->id, server.dbinum, timestamp, (long int)db->version);

    return VR_OK;
}

int generateRdbFilename(redisDb *db, long long timestamp)
{    
    return generatePersisFilename(db,timestamp,PERSIS_FILE_TYPE_RDB);
}

int generateRdbTmpFilename(redisDb *db, long long timestamp)
{    
    return generatePersisFilename(db,timestamp,PERSIS_FILE_TYPE_RDBTMP);
}

int generateAofFilename(redisDb *db, long long timestamp)
{    
    return generatePersisFilename(db,timestamp,PERSIS_FILE_TYPE_AOF);
}

persisFile *persisFileCreate(char *filename, size_t filename_len)
{
    int type;
    long dbid, dbinum;
    long long timestamp;
    persisFile *pf;
    sds *parts;
    int parts_count;

    parts = sdssplitlen(filename,(int)filename_len,"_",1,&parts_count);
    if (parts == NULL)
        return NULL;

    if (parts_count != 5) {
        sdsfreesplitres(parts,parts_count);
        return NULL;
    }

    if (!strcmp(parts[0], rdb_filename_prefix)) {
        type = PERSIS_FILE_TYPE_RDB;
    } else if (!strcmp(parts[0], aof_filename_prefix)) {
        type = PERSIS_FILE_TYPE_AOF;
    } else if (!strcmp(parts[0], rdb_temp_filename_prefix)) {
        type = PERSIS_FILE_TYPE_RDBTMP;
    } else {
        sdsfreesplitres(parts,parts_count);
        return NULL;
    }

    if (!sdsIsNum(parts[1]) || !sdsIsNum(parts[2]) || !sdsIsNum(parts[4])) {
        sdsfreesplitres(parts,parts_count);
        return NULL;
    }

    if (sdslen(parts[3]) != 13 || !sdsIsNum(parts[3])) {
        sdsfreesplitres(parts,parts_count);
        return NULL;
    }

    string2l(parts[1], sdslen(parts[1]), &dbid);
    string2l(parts[2], sdslen(parts[2]), &dbinum);
    string2ll(parts[3], sdslen(parts[3]), &timestamp);

    sdsfreesplitres(parts,parts_count);
    
    pf = dalloc(sizeof(persisFile));
    pf->type = type;
    pf->dbid = (int)dbid;
    pf->dbinum = (int)dbinum;
    pf->timestamp = timestamp;
    pf->offset = 0;
    pf->filename = sdsnewlen(filename, filename_len);

    return pf;
}

void persisFileDestroy(persisFile *pf)
{
    if (pf == NULL)
        return;

    if (pf->filename)
        sdsfree(pf->filename);

    dfree(pf);
}

persisPart *persisPartCreate(void)
{
    persisPart *pp;

    pp = dalloc(sizeof(persisPart));
    if (pp == NULL) return NULL;

    pp->dbid = -1;
    pp->rdb_file = NULL;
    pp->rdb_files = dlistCreate();
    pp->aof_files = dlistCreate();
    pp->aof_start_node = NULL;

    return pp;
}

void persisPartDestroy(persisPart *pp)
{
    persisFile *pf;

    if (pp->rdb_files) {
        while ((pf = dlistPop(pp->rdb_files)) != NULL) {
            persisFileDestroy(pf);
        }
        dlistRelease(pp->rdb_files);
        pp->rdb_files = NULL;
    }
    
    if (pp->aof_files) {
        while ((pf = dlistPop(pp->aof_files)) != NULL) {
            persisFileDestroy(pf);
        }
        dlistRelease(pp->aof_files);
        pp->aof_files = NULL;
    }

    dfree(pp);
}

static void persisPartPrint(persisPart *pp, int loglevel)
{
    dlistNode *dn;
    persisFile *pf;

    if (!log_loggable(loglevel))
        return;

    log_debug(loglevel, "persistence part dbid: %d", pp->dbid);

    if (pp->rdb_files) {
        dn = dlistFirst(pp->rdb_files);
        while (dn != NULL) {
            pf = dlistNodeValue(dn);
            log_debug(loglevel, "persistence rdb file: %s", pf->filename);
            
            
            dn = dlistNextNode(dn);
        }
    }

    if (pp->aof_files) {
        dn = dlistFirst(pp->aof_files);
        while (dn != NULL) {
            pf = dlistNodeValue(dn);
            log_debug(loglevel, "persistence aof file: %s", pf->filename);
            
            
            dn = dlistNextNode(dn);
        }
    }

    if (pp->rdb_file) {
        pf = pp->rdb_file;
        log_debug(loglevel, "persistence rdb latest file: %s", pf->filename);
    }

    if (pp->aof_start_node) {
        pf = dlistNodeValue(pp->aof_start_node);
        log_debug(loglevel, "persistence aof begin file: %s", pf->filename);
    }
}

static int persistencePartCmp(const void *t1, const void *t2)
{
    const persisPart *pp1 = *(persisPart **)t1, *pp2 = *(persisPart **)t2;
    sds rdb_filename1, rdb_filename2;
    size_t len1, len2;

    if (pp1->rdb_file == NULL && pp2->rdb_file == NULL) {
        if (pp1->aof_files == NULL && pp2->aof_files == NULL)
            return 0;
        if (pp1->aof_files == NULL)
            return -1;
        if (pp2->aof_files == NULL)
            return 1;
        return (dlistLength(pp1->aof_files) - dlistLength(pp2->aof_files));
    }

    if (pp1->rdb_file == NULL)
        return -1;
    if (pp2->rdb_file == NULL)
        return 1;

    if (pp1->dbid)

    return (pp1->dbid - pp2->dbid);
}

static darray *getExistPersistenceParts_old(void)
{
    unsigned long long j, k;
    DIR *work_dir;
    struct dirent *ent = NULL;
    char dir_cur[1024];
    darray *pps;
    persisPart **pp;
    persisFile **rdbf, **aoff, *rdbf_p, *aoff_p, *pf_p;
    darray *rdbs, *aofs;

    if (getcwd(dir_cur,sizeof(dir_cur)) == NULL)
        dir_cur[0] = '\0';
    
    work_dir = opendir(dir_cur);
    if (work_dir == NULL) {
        log_warn("Open directory %s failed.", dir_cur);
        return NULL;
    }

    pps = darray_create(100,sizeof(persisPart*));
    rdbs = darray_create(50,sizeof(persisFile*));
    aofs = darray_create(50,sizeof(persisFile*));
    
    while ((ent=readdir(work_dir)) != NULL) {
        char *fname;
        size_t fname_len;
        
        if (ent->d_type != DT_REG)
            continue;
        
        fname = ent->d_name;
        fname_len = strlen(fname);

        pf_p = persisFileCreate(fname,fname_len);
        if (pf_p == NULL)
            continue;

        log_debug(LOG_INFO, "filename: %s, type: %d, dbid: %d, dbinum: %d, timestamp: %lld", 
            pf_p->filename, pf_p->type, pf_p->dbid, pf_p->dbinum, pf_p->timestamp);
        if (pf_p->type == PERSIS_FILE_TYPE_RDB) {
            rdbf = darray_push(rdbs);
            *rdbf = pf_p;
        } else if (pf_p->type == PERSIS_FILE_TYPE_AOF) {
            aoff = darray_push(aofs);
            *aoff = pf_p;
        } else {
            persisFileDestroy(pf_p);
        }
    }

    //log_debug(LOG_INFO, "pps len: %u", darray_n(pps));

    /* Push the rdb files. */
    for (j = 0; j < darray_n(rdbs); j ++) {
        int pushed = 0, skip = 0;
        rdbf = darray_get(rdbs, j);
        //log_debug(LOG_INFO, "pps len: %u, rdb dbid: %d", darray_n(pps), (*rdbf)->dbid);
        for (k = 0; k < darray_n(pps); k ++) {
            pp = darray_get(pps, k);
            if ((*pp)->dbid == (*rdbf)->dbid) {
                rdbf_p = (*pp)->rdb_file;
                ASSERT(rdbf_p != NULL);
                if (rdbf_p->timestamp < (*rdbf)->timestamp) {
                    (*pp)->rdb_file = *rdbf;
                    *rdbf = NULL;
                    pushed = 1;
                } else {
                    skip = 1;
                }
                break;
            }
        }

        if (skip || pushed) {
            continue;
        }
        
        pp = darray_push(pps);
        (*pp) = dalloc(sizeof(persisPart));
        (*pp)->dbid = (*rdbf)->dbid;
        (*pp)->rdb_file = *rdbf;
        (*pp)->aof_files = dlistCreate();
        *rdbf = NULL;
    }

    /* Push the aof files. */
    for (j = 0; j < darray_n(aofs); j ++) {
        int pushed = 0, skip = 0;
        aoff = darray_get(aofs, j);
        for(k = 0; k < darray_n(pps); k ++) {
            pp = darray_get(pps, k);
            if ((*aoff)->dbid == (*pp)->dbid) {
                dlistIter *it;
                dlistNode *ln;

                rdbf_p = (*pp)->rdb_file;
                if (rdbf_p != NULL && 
                    rdbf_p->timestamp > (*aoff)->timestamp) {
                    skip = 1;
                    break;
                }

                ln = dlistFirst((*pp)->aof_files);
                if (ln == NULL) {
                    dlistAddNodeTail((*pp)->aof_files, (*aoff));
                    *aoff = NULL;
                    pushed = 1;
                    break;
                } else {
                     aoff_p = dlistNodeValue(ln);
                     if ((*aoff)->timestamp < aoff_p->timestamp) {
                         dlistAddNodeHead((*pp)->aof_files, (*aoff));
                         *aoff = NULL;
                         pushed = 1;
                         break;
                     }
                }
                
                it = dlistGetIterator((*pp)->aof_files,AL_START_HEAD);
                while ((ln = dlistNext(it)) != NULL) {
                    aoff_p = dlistNodeValue(ln);

                    if ((*aoff)->timestamp > aoff_p->timestamp) {
                        dlistInsertNode((*pp)->aof_files,ln,(*aoff),1);
                        *aoff = NULL;
                        pushed = 1;
                        break;
                    }
                }
                dlistReleaseIterator(it);

                if (pushed == 0) {
                    dlistAddNodeTail((*pp)->aof_files, (*aoff));
                    *aoff = NULL;
                    pushed = 1;
                }
                
                break;
            }
        }

        if (skip == 1) {
            continue;
        }
        
        if (pushed == 0) {
            pp = darray_push(pps);
            (*pp) = dalloc(sizeof(persisPart));
            (*pp)->rdb_file = NULL;
            (*pp)->dbid = (*aoff)->dbid;
            (*pp)->aof_files = dlistCreate();
            dlistAddNodeTail((*pp)->aof_files, (*aoff));
            *aoff = NULL;
        }
    }

    darray_sort(pps, persistencePartCmp);

    while ((rdbf = darray_pop(rdbs)) != NULL) {
        if ((*rdbf) != NULL) {
            persisFileDestroy(*rdbf);
        }
    }
    darray_destroy(rdbs);
    while ((aoff = darray_pop(aofs)) != NULL) {
        if ((*aoff) != NULL) {
            persisFileDestroy(*aoff);
        }
    }
    darray_destroy(aofs);

    return pps;
}

static int persisFileTimeCompare(const void *ptr1, const void *ptr2)
{
    persisFile *pf1 = ptr1, *pf2 = ptr2;
    return  (int)(pf2->timestamp - pf1->timestamp);
}

static int persisPartFormat(persisPart *pp)
{
    int ret;
    dlistIter *di;
    dlistNode *dn;
    persisFile *rdbf_latest, *aoff;

    dlistSort(pp->rdb_files, persisFileTimeCompare);
    dlistSort(pp->aof_files, persisFileTimeCompare);

    if (dlistLength(pp->rdb_files) == 0) {
        pp->rdb_file = NULL;
        pp->aof_start_node = dlistFirst(pp->aof_files);
        return VR_OK;
    }

    dn = dlistLast(pp->rdb_files);
    rdbf_latest = dlistNodeValue(dn);
    pp->rdb_file = rdbf_latest;
    
    if (dlistLength(pp->aof_files) == 0) {
        pp->aof_start_node = NULL;
        return VR_OK;
    }

    di = dlistGetIterator(pp->aof_files, AL_START_HEAD);
    while ((dn = dlistNext(di)) != NULL) {
        aoff = dlistNodeValue(dn);
        if (aoff->timestamp >= rdbf_latest->timestamp) {
            pp->aof_start_node = dn;
            break;
        }
    }
    dlistReleaseIterator(di);
    
    return VR_OK;
}

static darray *persistencePartsCreate(void)
{
    int ret;
    unsigned long long j, k;
    DIR *work_dir;
    struct dirent *ent = NULL;
    char dir_cur[1024];
    darray *pps;
    persisPart **pp;
    persisFile **rdbf, **aoff, *rdbf_p, *aoff_p, *pf_p;
    darray *rdbs, *aofs;

    if (getcwd(dir_cur,sizeof(dir_cur)) == NULL)
        dir_cur[0] = '\0';
    
    work_dir = opendir(dir_cur);
    if (work_dir == NULL) {
        log_warn("Open directory %s failed.", dir_cur);
        return NULL;
    }

    pps = darray_create(100,sizeof(persisPart*));
    rdbs = darray_create(50,sizeof(persisFile*));
    aofs = darray_create(50,sizeof(persisFile*));

    /* Scan the data dir, get the rdb files and aof files. */
    while ((ent=readdir(work_dir)) != NULL) {
        char *fname;
        size_t fname_len;
        
		/*
        if (ent->d_type != DT_REG)
            continue;
		*/
        
        fname = ent->d_name;
        fname_len = strlen(fname);

        pf_p = persisFileCreate(fname,fname_len);
        if (pf_p == NULL)
            continue;

        log_debug(LOG_INFO, "filename: %s, type: %d, dbid: %d, dbinum: %d, timestamp: %lld", 
            pf_p->filename, pf_p->type, pf_p->dbid, pf_p->dbinum, pf_p->timestamp);
        if (pf_p->type == PERSIS_FILE_TYPE_RDB) {
            rdbf = darray_push(rdbs);
            *rdbf = pf_p;
        } else if (pf_p->type == PERSIS_FILE_TYPE_AOF) {
            aoff = darray_push(aofs);
            *aoff = pf_p;
        } else {
            persisFileDestroy(pf_p);
        }
    }

    /* Add the rdb files. */
    for (j = 0; j < darray_n(rdbs); j ++) {
        int pushed = 0;
        rdbf = darray_get(rdbs, j);
        for (k = 0; k < darray_n(pps); k ++) {
            pp = darray_get(pps, k);
            if ((*pp)->dbid == (*rdbf)->dbid) {
                dlistAddNodeTail((*pp)->rdb_files,*rdbf);
                *rdbf = NULL;
                pushed = 1;
                break;
            }
        }

        if (pushed) continue;
        
        pp = darray_push(pps);
        (*pp) = persisPartCreate();
        (*pp)->dbid = (*rdbf)->dbid;
        dlistAddNodeTail((*pp)->rdb_files, (*rdbf));
        *rdbf = NULL;
    }

    /* Add the aof files. */
    for (j = 0; j < darray_n(aofs); j ++) {
        int pushed = 0;
        aoff = darray_get(aofs, j);
        for(k = 0; k < darray_n(pps); k ++) {
            pp = darray_get(pps, k);
            if ((*aoff)->dbid == (*pp)->dbid) {
                dlistAddNodeTail((*pp)->aof_files,*aoff);
                *aoff = NULL;
                pushed = 1;
                break;
            }
        }

        if (pushed) continue;
        
        pp = darray_push(pps);
        (*pp) = persisPartCreate();
        (*pp)->dbid = (*aoff)->dbid;
        dlistAddNodeTail((*pp)->aof_files, (*aoff));
        *aoff = NULL;
    }

    /* Format the persistence parts. */
    for (j = 0; j < darray_n(pps); j ++) {
        pp = darray_get(pps, j);
        ret = persisPartFormat(*pp);
        if (ret != VR_OK) {
            goto clean;
        }
    }
    
    darray_sort(pps, persistencePartCmp);

clean:
    
    /* Clean the unused files. */
    while ((rdbf = darray_pop(rdbs)) != NULL) {
        if ((*rdbf) != NULL) {
            persisFileDestroy(*rdbf);
        }
    }
    darray_destroy(rdbs);
    while ((aoff = darray_pop(aofs)) != NULL) {
        if ((*aoff) != NULL) {
            persisFileDestroy(*aoff);
        }
    }
    darray_destroy(aofs);

    return pps;
}

static int dataLoadInit(void)
{
    load_thread *loader;
    darray *pps;
    int j;
    struct stat sb;

    PERSIS_LOCK();
    if (loading == 1) {
        PERSIS_UNLOCK();
        return VR_ERROR;
    }

    loading = 1;
    loading_loaded_bytes = 0;
    loading_total_bytes = 0;
    load_finished_threads_count = 0;
    loading_start_time = dmsec_now();

    pps = persistencePartsCreate();
    if (pps == NULL) {
        PERSIS_UNLOCK();
        return VR_ERROR;
    }
    
    if (num_load_threads == 0) {
        num_load_threads = DATA_LOAD_THREADS_COUNT_DEFAULT;
        darray_init(&load_threads,num_load_threads,sizeof(load_thread));
        for (j = 0; j < num_load_threads; j ++) {
            loader = darray_push(&load_threads);
            loadThreadInit(loader);
            loader->id = 0;
        }
    }

    for (j = 0; j < darray_n(pps); j ++) {
        persisPart **pp, **pp_addr;
        persisFile *pf;
        dlistIter *it;
        dlistNode *ln;
        
        pp = darray_get(pps, j);

        pf = (*pp)->rdb_file;
        if (pf && stat(pf->filename, &sb) == 0) {
            loading_total_bytes += sb.st_size;
        }
        it = dlistGetIterator((*pp)->aof_files,AL_START_HEAD);
        while ((ln = dlistNext(it)) != NULL) {
            pf = dlistNodeValue(ln);
            if (stat(pf->filename, &sb) == 0) {
                loading_total_bytes += sb.st_size;
            }
        }
        dlistReleaseIterator(it);

        loader = darray_get(&load_threads, j%num_load_threads);
        pp_addr = darray_push(loader->pps);
        (*pp_addr) = (*pp);
    }
    pps->nelem = 0;
    darray_destroy(pps);
    PERSIS_UNLOCK();

    for (j = 0; j < num_load_threads; j ++) {
        int k;
        persisPart **pp;
        loader = darray_get(&load_threads,j);
        pps = loader->pps;
        log_debug(LOG_NOTICE, "Load thread[%d] hold the following persistence parts:", j);
        for (k = 0; k < darray_n(pps); k ++) {            
            pp = darray_get(pps, k);
            persisPartPrint(*pp, LOG_NOTICE);
        }
        log_debug(LOG_NOTICE, "");
    }

    return VR_OK;
}

static int dataLoadFinished(void)
{
    PERSIS_LOCK();
    load_finished_threads_count ++;
    
    /* Check if all data file had load finished. */
    if (load_finished_threads_count >= num_load_threads) {
        log_notice("DB loaded from disk finished, %d threads used %.3f seconds",
            num_load_threads, (float)(dmsec_now()-loading_start_time)/1000);
        loading = 0;
    }
    PERSIS_UNLOCK();
    
    return VR_OK;
}

/* Track loading progress in order to serve client's from time to time
   and if needed calculate rdb checksum  */
static void rdbLoadProgressCallback(rio *r, const void *buf, size_t len) {
    if (server.rdb_checksum)
        rioGenericUpdateChecksum(r, buf, len);
    PERSIS_LOCK();
    loading_loaded_bytes += len;
    PERSIS_UNLOCK();
}

static int rdbLoadData(char *filename)
{
    int j;
    char buf[1024];
    long long expiretime, now = dmsec_now();
    int type, rdbver;
    uint32_t dbid = 0;
    uint32_t db_size, expires_size;
    FILE *fp;
    redisDb *db;
    rio rdb;

    log_debug(LOG_NOTICE, "start loading rdb file: %s", filename);
    
    if ((fp = fopen(filename,"r")) == NULL) return VR_ERROR;
    rioInitWithFile(&rdb,fp);

    rdb.update_cksum = rdbLoadProgressCallback;
    rdb.max_processing_chunk = server.loading_process_events_interval_bytes;
    if (rioRead(&rdb,buf,9) == 0) goto eoferr;
    buf[9] = '\0';
    if (memcmp(buf,"REDIS",5) != 0) {
        fclose(fp);
        log_warn("Wrong signature trying to load DB from file %s", filename);
        return VR_ERROR;
    }
    rdbver = atoi(buf+5);
    if (rdbver < 1 || rdbver > RDB_VERSION) {
        fclose(fp);
        log_warn("Can't handle RDB format version %d from file %s", 
            rdbver, filename);
        return VR_ERROR;
    }

    while(1) {
        /* Read type. */
        if ((type = rdbLoadType(&rdb)) == -1) goto eoferr;
        
        if (type == RDB_OPCODE_SELECTDB) {
            /* SELECTDB: Select the specified database. */
            if ((dbid = rdbLoadLen(&rdb,NULL)) == RDB_LENERR)
                goto eoferr;
            if ((int)dbid >= server.dblnum) {
                log_warn(
                    "FATAL: Data file was created with a Redis "
                    "server configured %d to handle more than %d "
                    "databases. Exiting\n", (int)dbid, server.dblnum);
                exit(1);
            }
            continue; /* Read type again. */
        } else if (type == RDB_OPCODE_RESIZEDB) {
            /* RESIZEDB: Hint about the size of the keys in the currently
             * selected data base, in order to avoid useless rehashing. */
            if ((db_size = rdbLoadLen(&rdb,NULL)) == RDB_LENERR)
                goto eoferr;
            if ((expires_size = rdbLoadLen(&rdb,NULL)) == RDB_LENERR)
                goto eoferr;
            continue; /* Read type again. */
        } else if (type == RDB_OPCODE_AUX) {
            /* AUX: generic string-string fields. Use to add state to RDB
             * which is backward compatible. Implementations of RDB loading
             * are requierd to skip AUX fields they don't understand.
             *
             * An AUX field is composed of two strings: key and value. */
            robj *auxkey, *auxval;
            if ((auxkey = rdbLoadStringObject(&rdb)) == NULL) goto eoferr;
            if ((auxval = rdbLoadStringObject(&rdb)) == NULL) goto eoferr;

            if (((char*)auxkey->ptr)[0] == '%') {
                /* All the fields with a name staring with '%' are considered
                 * information fields and are logged at startup with a log
                 * level of NOTICE. */
                log_debug(LOG_NOTICE, "RDB '%s': %s",
                    (char*)auxkey->ptr,
                    (char*)auxval->ptr);
            } else {
                /* We ignore fields we don't understand, as by AUX field
                 * contract. */
                log_debug(LOG_DEBUG,"Unrecognized RDB AUX field: '%s'",
                    (char*)auxkey->ptr);
            }

            freeObject(auxkey);
            freeObject(auxval);
            continue; /* Read type again. */
        }else {
            break;
        }
    }

    while(1) {
        robj *key, *val;
        expiretime = -1;

        /* Handle special types. */
        if (type == RDB_OPCODE_EXPIRETIME) {
            /* EXPIRETIME: load an expire associated with the next key
             * to load. Note that after loading an expire we need to
             * load the actual type, and continue. */
            if ((expiretime = rdbLoadTime(&rdb)) == -1) goto eoferr;
            /* We read the time so we need to read the object type again. */
            if ((type = rdbLoadType(&rdb)) == -1) goto eoferr;
            /* the EXPIRETIME opcode specifies time in seconds, so convert
             * into milliseconds. */
            expiretime *= 1000;
        } else if (type == RDB_OPCODE_EXPIRETIME_MS) {
            /* EXPIRETIME_MS: milliseconds precision expire times introduced
             * with RDB v3. Like EXPIRETIME but no with more precision. */
            if ((expiretime = rdbLoadMillisecondTime(&rdb)) == -1) goto eoferr;
            /* We read the time so we need to read the object type again. */
            if ((type = rdbLoadType(&rdb)) == -1) goto eoferr;
        } else if (type == RDB_OPCODE_EOF) {
            /* EOF: End of file, exit the main loop. */
            break;
        }

        /* Read key */
        if ((key = rdbLoadStringObject(&rdb)) == NULL) goto eoferr;        
        /* Read value */
        if ((val = rdbLoadObject(type,&rdb)) == NULL) goto eoferr;

        /* Check if the key already expired. This function is used when loading
         * an RDB file from disk, either at startup, or when an RDB was
         * received from the master. In the latter case, the master is
         * responsible for key expiry. If we would expire keys here, the
         * snapshot taken by the master may not be reflected on the slave. */
        if (repl.masterhost == NULL && expiretime != -1 && expiretime < now) {
            freeObject(key);
            freeObject(val);
            
            /* Read type. */
            if ((type = rdbLoadType(&rdb)) == -1) goto eoferr;
            continue;
        }

        db = fetchInternalDbByKey(dbid, key);
        lockDbWrite(db);
        /* Add the new object in the hash table */
        dbAdd(db,key,val);
        /* Set the expire time if needed */
        if (expiretime != -1) setExpire(db,key,expiretime);
        unlockDb(db);

        freeObject(key);

        /* Read type. */
        if ((type = rdbLoadType(&rdb)) == -1) goto eoferr;
    }
    
    /* Verify the checksum if RDB version is >= 5 */
    if (rdbver >= 5 && server.rdb_checksum) {
        uint64_t cksum, expected = rdb.cksum;

        if (rioRead(&rdb,&cksum,8) == 0) goto eoferr;
        memrev64ifbe(&cksum);
        if (cksum == 0) {
            log_warn("RDB file was saved with checksum disabled: no check performed.");
        } else if (cksum != expected) {
            log_warn("Wrong RDB checksum. Aborting now.");
            rdbExitReportCorruptRDB("RDB CRC error");
        }
    }

    fclose(fp);

    return VR_OK;

eoferr: /* unexpected end of file is handled here with a fatal exit */
    log_warn("Short read or OOM loading DB. Unrecoverable error, aborting now.");
    rdbExitReportCorruptRDB("Unexpected EOF reading RDB file");
    return VR_ERROR;
}

/* In Redis commands are always executed in the context of a client, so in
 * order to load the append only file we need to create a fake client. */
static client *createFakeClient(void) {
    client *c = dalloc(sizeof(*c));

    selectDb(c,0);
    c->conn = NULL;
    c->vel = NULL;
    c->name = NULL;
    c->querybuf = sdsempty();
    c->querybuf_peak = 0;
    c->argc = 0;
    c->argv = NULL;
    c->bufpos = 0;
    c->flags = 0;
    c->btype = BLOCKED_NONE;
    /* We set the fake client as a slave waiting for the synchronization
     * so that Redis will not try to send replies to this client. */
    c->replstate = SLAVE_STATE_WAIT_BGSAVE_START;
    c->reply = dlistCreate();
    c->reply_bytes = 0;
    c->obuf_soft_limit_reached_time = 0;
    c->watched_keys = dlistCreate();
    c->peerid = NULL;
    dlistSetFreeMethod(c->reply,freeObjectVoid);
    dlistSetDupMethod(c->reply,dupClientReplyValue);
    initClientMultiState(c);
    return c;
}

static void freeFakeClientArgv(client *c) {
    int j;

    for (j = 0; j < c->argc; j++)
        freeObject(c->argv[j]);
    dfree(c->argv);
}

static void freeFakeClient(client *c) {
    sdsfree(c->querybuf);
    dlistRelease(c->reply);
    dlistRelease(c->watched_keys);
    freeClientMultiState(c);
    dfree(c);
}

/* Replay the append log file. On success VR_OK is returned. On non fatal
 * error (the append only file is zero-length) VR_ERROR is returned. On
 * fatal error an error message is logged and the program exists. */
static int appendOnlyLoadData(persisFile *pf) {
    FILE *fp;
    client *fakeClient;
    int old_aof_state = server.aof_state;
    off_t valid_up_to = 0; /* Offset of the latest well-formed command loaded. */

    ASSERT(pf->type == PERSIS_FILE_TYPE_AOF);

    log_debug(LOG_NOTICE, "start loading aof file: %s", pf->filename);
    
    fp = fopen(pf->filename,"r");
    if (fp == NULL) {
        log_warn("Fatal error: can't open the append log file %s for reading: %s", 
            pf->filename, strerror(errno));
        exit(1);
    }

    fakeClient = createFakeClient();
    fakeClient->db = darray_get(&server.dbs, (uint32_t)pf->dbid);
    while(1) {
        int argc, j;
        unsigned long len;
        robj **argv;
        char buf[128];
        sds argsds;
        struct redisCommand *cmd;

        if (fgets(buf,sizeof(buf),fp) == NULL) {
            if (feof(fp))
                break;
            else
                goto readerr;
        }
        if (buf[0] != '*') goto fmterr;
        if (buf[1] == '\0') goto readerr;
        argc = atoi(buf+1);
        if (argc < 1) goto fmterr;

        argv = dalloc(sizeof(robj*)*argc);
        fakeClient->argc = argc;
        fakeClient->argv = argv;

        for (j = 0; j < argc; j++) {
            if (fgets(buf,sizeof(buf),fp) == NULL) {
                fakeClient->argc = j; /* Free up to j-1. */
                freeFakeClientArgv(fakeClient);
                goto readerr;
            }
            if (buf[0] != '$') goto fmterr;
            len = strtol(buf+1,NULL,10);
            argsds = sdsnewlen(NULL,len);
            if (len && fread(argsds,len,1,fp) == 0) {
                sdsfree(argsds);
                fakeClient->argc = j; /* Free up to j-1. */
                freeFakeClientArgv(fakeClient);
                goto readerr;
            }

            PERSIS_LOCK();
            loading_loaded_bytes += len;
            PERSIS_UNLOCK();
            
            argv[j] = createObject(OBJ_STRING,argsds);
            if (fread(buf,2,1,fp) == 0) {
                fakeClient->argc = j+1; /* Free up to j. */
                freeFakeClientArgv(fakeClient);
                goto readerr; /* discard CRLF */
            }
            
            PERSIS_LOCK();
            loading_loaded_bytes += 2;
            PERSIS_UNLOCK();
        }

        /* Command lookup */
        cmd = lookupCommand(argv[0]->ptr);
        if (!cmd) {
            log_warn("Unknown command '%s' reading the append only file", (char*)argv[0]->ptr);
            exit(1);
        }

        /* Run the command in the context of a fake client */
        cmd->proc(fakeClient);

        /* The fake client should not have a reply */
        ASSERT(fakeClient->bufpos == 0 && dlistLength(fakeClient->reply) == 0);
        /* The fake client should never get blocked */
        ASSERT((fakeClient->flags & CLIENT_BLOCKED) == 0);

        /* Clean up. Command code may have changed argv/argc so we use the
         * argv/argc of the client instead of the local variables. */
        freeFakeClientArgv(fakeClient);
        if (server.aof_load_truncated) valid_up_to = ftello(fp);
    }

    /* This point can only be reached when EOF is reached without errors.
     * If the client is in the middle of a MULTI/EXEC, log error and quit. */
    if (fakeClient->flags & CLIENT_MULTI) goto uxeof;

loaded_ok: /* DB loaded, cleanup and return C_OK to the caller. */
    fclose(fp);
    freeFakeClient(fakeClient);
    server.aof_state = old_aof_state;
    server.aof_rewrite_base_size = server.aof_current_size;
    return VR_OK;

readerr: /* Read error. If feof(fp) is true, fall through to unexpected EOF. */
    if (!feof(fp)) {
        if (fakeClient) freeFakeClient(fakeClient); /* avoid valgrind warning */
        log_warn("Unrecoverable error reading the append only file: %s", strerror(errno));
        exit(1);
    }

uxeof: /* Unexpected AOF end of file. */
    if (server.aof_load_truncated) {
        log_warn("!!! Warning: short read while loading the AOF file !!!");
        log_warn("!!! Truncating the AOF at offset %llu !!!",
            (unsigned long long) valid_up_to);
        if (valid_up_to == -1 || truncate(pf->filename,valid_up_to) == -1) {
            if (valid_up_to == -1) {
                log_warn("Last valid command offset is invalid");
            } else {
                log_warn("Error truncating the AOF file: %s",
                    strerror(errno));
            }
        } else {
            /* Make sure the AOF file descriptor points to the end of the
             * file after the truncate call. */
            if (server.aof_fd != -1 && lseek(server.aof_fd,0,SEEK_END) == -1) {
                log_warn("Can't seek the end of the AOF file: %s",
                    strerror(errno));
            } else {
                log_warn(
                    "AOF loaded anyway because aof-load-truncated is enabled");
                goto loaded_ok;
            }
        }
    }
    if (fakeClient) freeFakeClient(fakeClient); /* avoid valgrind warning */
    log_warn("Unexpected end of file reading the append only file. You can: 1) Make a backup of your AOF file, then use ./redis-check-aof --fix <filename>. 2) Alternatively you can set the 'aof-load-truncated' configuration option to yes and restart the server.");
    exit(1);

fmterr: /* Format error. */
    if (fakeClient) freeFakeClient(fakeClient); /* avoid valgrind warning */
    log_warn("Bad file format reading the append only file: make a backup of your AOF file, then use ./redis-check-aof --fix <filename>");
    exit(1);
}

static void *dataLoadRun(void *args)
{
    load_thread *loader = args;
    int j;
    char buf[1024];
    long long expiretime, now = dmsec_now();
    int type, rdbver;
    uint32_t dbid = 0;
    uint32_t db_size, expires_size;
    FILE *fp;
    redisDb *db;
    persisPart **pp;
    persisFile *pf;

    for (j = 0; j < darray_n(loader->pps); j ++) {
        dlistNode *ln;

        pp = darray_get(loader->pps, j);
        pf = (*pp)->rdb_file;
        if (pf != NULL) {
            rdbLoadData(pf->filename);
        }

        ln = (*pp)->aof_start_node;
        while (ln != NULL) {
            pf = dlistNodeValue(ln);
            appendOnlyLoadData(pf);
            ln = dlistNextNode(ln);
        }
    }

    dataLoadFinished();

    return 0;

eoferr: /* unexpected end of file is handled here with a fatal exit */
    log_warn("Short read or OOM loading DB. Unrecoverable error, aborting now.");
    rdbExitReportCorruptRDB("Unexpected EOF reading RDB file");
    return 0;
}

static int dataLoadStart(void)
{
    int j;

    for (j = 0; j < num_load_threads; j ++) {
        pthread_attr_t attr;
        load_thread *loader;

        pthread_attr_init(&attr);
        loader = darray_get(&load_threads, j);
        pthread_create(&loader->thread_id, 
            &attr, dataLoadRun, loader);
    }
    
    return VR_OK;
}

int dataLoad(void) {
    int ret;
    
    ret = dataLoadInit();
    if (ret != VR_OK)
        return VR_ERROR;

    dataLoadStart();
    
    return VR_OK;
}

/* Function called at startup to load RDB or AOF file in memory. */
void loadDataFromDisk(void) {
    if (dataLoad() != VR_OK) {
        log_warn("Fatal error loading the DB: %s. Exiting.",strerror(errno));
        exit(1);
    }
}

/* Return -1: error; 0: this val dumped success. */
int rdbSaveHashTypeSetValIfNeeded(redisDb *db, sds key, robj *set, robj *val)
{
    bigkeyDumper *bkdumper;
    dictEntry *de;

    ASSERT(key != NULL);
    ASSERT(set != NULL);
    ASSERT(val != NULL);
    
    if (!(db->flags&DB_FLAGS_DUMPING))
        return 0;

    ASSERT(set->type == OBJ_SET);
    ASSERT(set->encoding == OBJ_ENCODING_HT);
    
    if ((db->flags&DB_FLAGS_DUMP_FIRST_STEP)) {
        rdbSave(db,0);
    }

    rdbSaveKeyIfNeeded(db,NULL,key,set,0);
    if (set->version >= db->version)
        return 0;
    
    de = dictFind(set->ptr,val);
    if (de) {
        val = dictGetKey(de);
        ASSERT(val->version >= set->version);
        if (val->version < db->version) {
            rio *rdb = db->bkdhelper->dump_to_buffer_helper;
            bkdumper = dictFetchValue(db->bkdhelper->bigkeys_to_generate, key);
            ASSERT(bkdumper != NULL);
            ASSERT(bkdumper->type == OBJ_SET);
            ASSERT(bkdumper->encoding == OBJ_ENCODING_HT);
            ASSERT(bkdumper->state == BIGKEY_DUMP_STATE_GENERATING);
            ASSERT(bkdumper->write_type == BIGKEY_DUMP_WRITE_TYPE_SDS);
            rioSetBuffer(rdb, bkdumper->data, sdslen(bkdumper->data));
            rdbSaveStringObject(rdb,val);
            /* Update the rdb string in the bigkeyDumper. */
            bkdumper->data = rdb->io.buffer.ptr;
            val->version = db->version;

#ifdef HAVE_DEBUG_LOG
            bkdumper->elements_dumped_count ++;
#endif
        }
    }

    return 0;
}

/* Return -1: error; 0: this val dumped success. */
int rdbSaveHashTypeHashValIfNeeded(redisDb *db, sds key, robj *hash, robj *field)
{
    bigkeyDumper *bkdumper;
    dictEntry *de;
    robj *val;

    ASSERT(key != NULL);
    ASSERT(hash != NULL);
    ASSERT(field != NULL);
    
    if (!(db->flags&DB_FLAGS_DUMPING))
        return 0;

    ASSERT(hash->type == OBJ_HASH);
    ASSERT(hash->encoding == OBJ_ENCODING_HT);
    
    if ((db->flags&DB_FLAGS_DUMP_FIRST_STEP)) {
        rdbSave(db,0);
    }

    rdbSaveKeyIfNeeded(db,NULL,key,hash,0);
    if (hash->version >= db->version)
        return 0;
    
    de = dictFind(hash->ptr,field);
    if (de) {
        field = dictGetKey(de);
        val = dictGetVal(de);
        ASSERT(field->version >= hash->version);
        if (field->version < db->version) {
            rio *rdb = db->bkdhelper->dump_to_buffer_helper;
            bkdumper = dictFetchValue(db->bkdhelper->bigkeys_to_generate, key);
            ASSERT(bkdumper != NULL);
            ASSERT(bkdumper->type == OBJ_HASH);
            ASSERT(bkdumper->encoding == OBJ_ENCODING_HT);
            ASSERT(bkdumper->state == BIGKEY_DUMP_STATE_GENERATING);
            ASSERT(bkdumper->write_type == BIGKEY_DUMP_WRITE_TYPE_SDS);
            rioSetBuffer(rdb, bkdumper->data, sdslen(bkdumper->data));
            rdbSaveStringObject(rdb,field);
            rdbSaveStringObject(rdb,val);
            /* Update the rdb string in the bigkeyDumper. */
            bkdumper->data = rdb->io.buffer.ptr;
            field->version = db->version;
            val->version = db->version;

#ifdef HAVE_DEBUG_LOG
            bkdumper->elements_dumped_count ++;
#endif

        }
    }

    return 0;
}

/* Return -1: error; 0: this node no need to dump; 1: this node dumped success. */
int rdbSaveQuicklistTypeListNodeIfNeeded(redisDb *db, sds key, quicklist *qlist, quicklistNode *qnode)
{
    bigkeyDumper *bkdumper;
    quicklistDumpHelper *qldhelper;

    ASSERT(key != NULL);
    ASSERT(qlist != NULL);
    ASSERT(qnode != NULL);
    
    if (!(db->flags&DB_FLAGS_DUMPING))
        return 0;
    
    if ((db->flags&DB_FLAGS_DUMP_FIRST_STEP)) {
        rdbSave(db,0);
    }

    if (rdbSaveKeyIfNeeded(db,NULL,key,NULL,0) == 0)
		return 0;

    /* Get the big key dumper. */
    bkdumper = dictFetchValue(db->bkdhelper->bigkeys_to_generate, key);
	if (bkdumper == NULL)
		return 0;

    ASSERT(bkdumper->type == OBJ_LIST);
    ASSERT(bkdumper->encoding == OBJ_ENCODING_QUICKLIST);
    ASSERT(bkdumper->state == BIGKEY_DUMP_STATE_GENERATING);
    ASSERT(bkdumper->write_type == BIGKEY_DUMP_WRITE_TYPE_QUICKLIST);
    qldhelper = bkdumper->data;
    ASSERT(bkdumper->cursor_field != NULL);

    if (qnode->version >= db->version)
        return 0;
    
    ASSERT(qnode->index < qldhelper->len);
    qldhelper->nodes[qnode->index] = *qnode;
    if (qnode->encoding == QUICKLIST_NODE_ENCODING_LZF) {
        quicklistLZF *lzf = (quicklistLZF *)qnode->zl;
        size_t lzf_sz = sizeof(*lzf) + lzf->sz;
        qldhelper->nodes[qnode->index].zl = dalloc(lzf_sz);
        memcpy(qldhelper->nodes[qnode->index].zl, qnode->zl, lzf_sz);
    } else if (qnode->encoding == QUICKLIST_NODE_ENCODING_RAW) {
        qldhelper->nodes[qnode->index].zl = dalloc(qnode->sz);
        memcpy(qldhelper->nodes[qnode->index].zl, qnode->zl, qnode->sz);
    }
    qldhelper->count ++;
    
    qnode->version = db->version;
    qnode->index = 0;

#ifdef HAVE_DEBUG_LOG
    bkdumper->elements_dumped_count ++;
#endif
            
    return 1;
}

/* Return -1: error; 0: this element no need to dump; 1: this element dumped success. */
int rdbSaveSkiplistTypeZsetElementIfNeeded(redisDb *db, sds key, robj *val, robj *ele)
{
    bigkeyDumper *bkdumper;
    dictEntry *de;
    zset *zs;
    
    ASSERT(key != NULL);
    ASSERT(val != NULL);
    ASSERT(ele != NULL);
    
    if (!(db->flags&DB_FLAGS_DUMPING))
        return 0;

    ASSERT(val->type == OBJ_ZSET);
    ASSERT(val->encoding == OBJ_ENCODING_SKIPLIST);
    
    if ((db->flags&DB_FLAGS_DUMP_FIRST_STEP)) {
        rdbSave(db,0);
    }

    rdbSaveKeyIfNeeded(db,NULL,key,val,0);
    if (val->version >= db->version)
        return 0;

    zs = val->ptr;
    de = dictFind(zs->dict,ele);
    if (de) {
        robj *eleobj = dictGetKey(de);
        double *score = dictGetVal(de);

        ASSERT(eleobj->version >= val->version);
        if (eleobj->version < db->version) {
            rio *rdb = db->bkdhelper->dump_to_buffer_helper;
            
            bkdumper = dictFetchValue(db->bkdhelper->bigkeys_to_generate, key);
            ASSERT(bkdumper != NULL);
            ASSERT(bkdumper->type == OBJ_ZSET);
            ASSERT(bkdumper->encoding == OBJ_ENCODING_SKIPLIST);
            ASSERT(bkdumper->state == BIGKEY_DUMP_STATE_GENERATING);
            ASSERT(bkdumper->write_type == BIGKEY_DUMP_WRITE_TYPE_SDS);
            rioSetBuffer(rdb, bkdumper->data, sdslen(bkdumper->data));

            rdbSaveStringObject(rdb,eleobj);
            rdbSaveDoubleValue(rdb,*score);

            /* Update the rdb string in the bigkeyDumper. */
            bkdumper->data = rdb->io.buffer.ptr;
            eleobj->version = db->version;

#ifdef HAVE_DEBUG_LOG
            bkdumper->elements_dumped_count ++;
#endif
            
            return 1;
        }
    }

    return 0;
}

