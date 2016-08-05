#ifndef _VRT_PRODUCE_DATA_H_
#define _VRT_PRODUCE_DATA_H_

/* Producer flags. Please check the producer table defined in the vrt_produce_data.c file
 * for more information about the meaning of every flag. */
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

struct data_producer;

typedef struct data_unit *redis_command_proc(struct data_producer *dp);
typedef int *redis_get_keys_proc(struct data_producer *dp, sds **argv, int argc, int *numkeys);
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
} data_producer;

typedef struct data_unit {
    data_producer *dp;
    int argc;       /* Num of arguments of current command. */
    sds *argv;    /* Arguments of current command. */
    
    unsigned int hashvalue;
} data_unit;

data_unit *data_unit_get(void);
void data_unit_put(data_unit *du);

int vrt_produce_data_init(int key_length_range_min, int key_length_range_max, 
    int produce_cmd_types, unsigned int produce_threads_count);
void vrt_produce_data_deinit(void);

int vrt_start_produce_data(void);
int vrt_wait_produce_data(void);

int *get_keys_from_data_producer(data_producer *dp, sds *argv, int argc, int *numkeys);

#endif
