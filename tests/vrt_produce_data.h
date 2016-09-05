#ifndef _VRT_PRODUCE_DATA_H_
#define _VRT_PRODUCE_DATA_H_

/* Producer flags. Please check the producer table defined in the vrt_produce_data.c file
 * for more information about the meaning of every flag.
 * This is the meaning of the flags:
 *
 * w: write command (may modify the key space).
 * r: read command  (will never modify the key space).
 * m: may increase memory usage once called. Don't allow if out of memory.
 * a: admin command, like SAVE or SHUTDOWN.
 * p: Pub/Sub related command.
 * f: force replication of this command, regardless of server.dirty.
 * s: command not allowed in scripts.
 * R: random command. Command is not deterministic, that is, the same command
 *    with the same arguments, with the same key space, may have different
 *    results. For instance SPOP and RANDOMKEY are two random commands.
 * S: Sort command output array if called from script, so that the output
 *    is deterministic.
 * l: Allow command while loading the database.
 * t: Allow command while a slave has stale data but is not allowed to
 *    server this data. Normally no command is accepted in this condition
 *    but just a few.
 * M: Do not automatically propagate the command on MONITOR.
 * k: Perform an implicit ASKING for this command, so the command will be
 *    accepted in cluster mode if the slot is marked as 'importing'.
 * F: Fast command: O(1) or O(log(N)) command that should never delay
 *    its execution as long as the kernel scheduler is giving us time.
 *    Note that commands that may trigger a DEL as a side effect (like SET)
 *    are not fast commands.
 * A: Add a new key if the key was not exist before.
 */

#define PRO_WRITE 1                   /* "w" flag */
#define PRO_READONLY 2                /* "r" flag */
#define PRO_DENYOOM 4                 /* "m" flag */
#define PRO_NOT_USED_1 8              /* no longer used flag */
#define PRO_ADMIN 16                  /* "a" flag */
#define PRO_PUBSUB 32                 /* "p" flag */
#define PRO_NOSCRIPT  64              /* "s" flag */
#define PRO_RANDOM 128                /* "R" flag */
#define PRO_SORT_FOR_SCRIPT 256       /* "S" flag */
#define PRO_LOADING 512               /* "l" flag */
#define PRO_STALE 1024                /* "t" flag */
#define PRO_SKIP_MONITOR 2048         /* "M" flag */
#define PRO_ASKING 4096               /* "k" flag */
#define PRO_FAST 8192                 /* "F" flag */
#define PRO_ADD 16384                 /* "A" flag */

struct data_producer;
struct produce_scheme;
struct key_cache_array;

typedef struct data_unit *redis_command_proc(struct data_producer *dp, struct produce_scheme *ps);
typedef int *redis_get_keys_proc(struct data_producer *dp, sds *argv, int argc, int *numkeys);
typedef int produce_need_cache_key_proc(struct redisReply *reply);
typedef struct data_producer {
    char *name;     /* Command name */
    redis_command_proc *proc;
    int arity;
    
    char *sflags; /* Flags as string representation, one char per flag. */
    int flags;    /* The actual flags, obtained from the 'sflags' field. */
    
    /* Use a function to determine keys arguments in a command line. */
    redis_get_keys_proc *getkeys_proc;
    /* What keys should be loaded in background when calling this command? */
    int firstkey; /* The first argument that's a key (0 = no keys) */
    int lastkey;  /* The last argument that's a key */
    int keystep;  /* The step between first and last key */
    int cmd_type;
    produce_need_cache_key_proc *need_cache_key_proc;
} data_producer;

typedef struct data_unit {
    data_producer *dp;
    int argc;       /* Num of arguments of current command. */
    sds *argv;    /* Arguments of current command. */
    
    unsigned int hashvalue;

    void *data;
} data_unit;

typedef struct produce_scheme {
    darray *kcps;   /* Key cached pools for every type command. */

    int hit_ratio;   /* Hit ratio for the read commands. [0%,100%] */
    int hit_ratio_idx;   /* [0,hit_ratio_array_len-1] */
    int hit_ratio_array_len; /* 100 usually */
    int *hit_ratio_array;    /* Stored 0 or 1 for every element, 1 means used key in the cached keys array. */
} produce_scheme;

extern data_producer *delete_data_producer;

extern int produce_data_threads_count;

extern int produce_threads_pause_finished_count;

struct key_cache_array *kcp_get_from_ps(produce_scheme *ps, data_producer *dp);

data_unit *data_unit_get(void);
void data_unit_put(data_unit *du);

int vrt_produce_data_init(int key_length_range_min, int key_length_range_max, 
    int string_max_length,int fields_max_count,
    int produce_cmd_types,darray *produce_cmd_blacklist,darray *produce_cmd_whitelist,
    unsigned int produce_threads_count, long long cached_keys,
    int hit_ratio);
void vrt_produce_data_deinit(void);

int vrt_start_produce_data(void);
int vrt_wait_produce_data(void);

int *get_keys_from_data_producer(data_producer *dp, sds *argv, int argc, int *numkeys);

sds get_one_key_from_data_unit(data_unit *du);

void print_producer_command(data_unit *du);

#endif
