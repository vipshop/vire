#ifndef _VR_PERSISTENCE_H_
#define _VR_PERSISTENCE_H_

#include <pthread.h>

#include <vr_rio.h>

#define PERSIS_FILE_TYPE_RDB    0
#define PERSIS_FILE_TYPE_AOF    1
#define PERSIS_FILE_TYPE_RDBTMP 2

#define BIGKEY_DUMP_WRITE_TYPE_NONE         0
#define BIGKEY_DUMP_WRITE_TYPE_SDS          1
#define BIGKEY_DUMP_WRITE_TYPE_QUICKLIST    2

#define BIGKEY_DUMP_STATE_GENERATING        0
#define BIGKEY_DUMP_STATE_WRITING           1

typedef struct persisFile {
    int type;   /* PERSIS_FILE_TYPE_* */
    int dbid;   /* Database id. */
    int dbinum; /* Internal db count. */
    long long timestamp;    /* This file create time. */
    long long offset;
    sds filename;
} persisFile;

typedef struct persisPart {
    int dbid;
    persisFile *rdb_file;
    dlist *rdb_files;   /* Rdb files list. */
    dlist *aof_files;   /* Aof files list. */
    dlistNode *aof_start_node;   /* Start node in the aof_files list for valid data. */
} persisPart;

typedef struct bigkeyDumper {
    sds key;
    unsigned type:4;    /* As the key type. */
    unsigned encoding:4;    /* As the value encoding. */
    unsigned state:4;   /* This key is in generating or writing. */
    unsigned write_type:4;  /* The bigkeyDumper->data field is sds or quicklistDumpHelper. */
    void *data;	/* sds or quicklistDumpHelper */
    void *cursor_field;
    size_t written;

 #ifdef HAVE_DEBUG_LOG
    /* The following two fields are used to check if 
     * dumped correct elements count in the bigkey, 
     * and they are only used in debug mode. */
    unsigned long elements_count;
    unsigned long elements_dumped_count;
 #endif
} bigkeyDumper;

typedef struct bigkeyDumpHelper {
    dict *bigkeys_to_generate;
    rio *dump_to_buffer_helper;
    
    dlist *bigkeys_to_write;

	unsigned long long finished_bigkeys;
} bigkeyDumpHelper;

extern pthread_mutex_t persistence_lock;
#define PERSIS_LOCK()     pthread_mutex_lock(&persistence_lock)
#define PERSIS_UNLOCK()   pthread_mutex_unlock(&persistence_lock)

extern volatile int loading;           /* We are loading data from disk if true. */
extern off_t loading_total_bytes;
extern off_t loading_loaded_bytes;
extern long long loading_start_time;
extern long long rdb_save_time_last;
extern long long last_rdb_saved_time;

extern volatile int rdb_is_generating;
extern int rdb_generated_count;
extern long long rdb_save_time_start;

extern dmtqueue *fsync_jobs;

int persistenceInit(void);
void persistenceDeinit(void);

bigkeyDumper *bigkeyDumperCreate(void);
void bigkeyDumperDestroy(bigkeyDumper *bkdumper);

bigkeyDumpHelper *bigkeyDumpHelperCreate(void);
void bigkeyDumpHelperDestroy(bigkeyDumpHelper *bkdhelper);

int bigkeyDumpHelperIsBusy(bigkeyDumpHelper *bkdhelper);

int bigkeyRdbGenerate(redisDb *db, sds key, robj *val, bigkeyDumper *bkdumper, int dump_complete);

int rdbSaveKeyIfNeeded(redisDb *db, dictEntry *de, sds key, robj *val, int dump_complete);

int rdbSave(redisDb *db, int need_complete);
int incrementallyRdbSave(redisDb *db);
void bgsaveCommand(client *c);

int generatePersisFilename(redisDb *db, long long timestamp, int type);
int generateRdbFilename(redisDb *db, long long timestamp);
int generateRdbTmpFilename(redisDb *db, long long timestamp);
int generateAofFilename(redisDb *db, long long timestamp);

persisFile *persisFileCreate(char *filename, size_t filename_len);
void persisFileDestroy(persisFile *pf);

persisPart *persisPartCreate(void);
void persisPartDestroy(persisPart *pp);

int dataLoad(void);
void loadDataFromDisk(void);

int rdbSaveHashTypeSetValIfNeeded(redisDb *db, sds key, robj *set, robj *val);
int rdbSaveHashTypeHashValIfNeeded(redisDb *db, sds key, robj *hash, robj *field);
int rdbSaveQuicklistTypeListNodeIfNeeded(redisDb *db, sds key, quicklist *qlist, quicklistNode *qnode);
int rdbSaveSkiplistTypeZsetElementIfNeeded(redisDb *db, sds key, robj *val, robj *ele, dictEntry *dentry, zskiplistNode *zslnode);

#endif
