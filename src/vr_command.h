#ifndef _VR_COMMAND_H_
#define _VR_COMMAND_H_

/* Command flags. Please check the command table defined in the redis.c file
 * for more information about the meaning of every flag. */
#define CMD_WRITE 1                   /* "w" flag */
#define CMD_READONLY 2                /* "r" flag */
#define CMD_DENYOOM 4                 /* "m" flag */
#define CMD_NOT_USED_1 8              /* no longer used flag */
#define CMD_ADMIN 16                  /* "a" flag */
#define CMD_PUBSUB 32                 /* "p" flag */
#define CMD_NOSCRIPT  64              /* "s" flag */
#define CMD_RANDOM 128                /* "R" flag */
#define CMD_SORT_FOR_SCRIPT 256       /* "S" flag */
#define CMD_LOADING 512               /* "l" flag */
#define CMD_STALE 1024                /* "t" flag */
#define CMD_SKIP_MONITOR 2048         /* "M" flag */
#define CMD_ASKING 4096               /* "k" flag */
#define CMD_FAST 8192                 /* "F" flag */

/* Command call flags, see call() function */
#define CMD_CALL_NONE 0
#define CMD_CALL_SLOWLOG (1<<0)
#define CMD_CALL_STATS (1<<1)
#define CMD_CALL_PROPAGATE_AOF (1<<2)
#define CMD_CALL_PROPAGATE_REPL (1<<3)
#define CMD_CALL_PROPAGATE (CMD_CALL_PROPAGATE_AOF|CMD_CALL_PROPAGATE_REPL)
#define CMD_CALL_FULL (CMD_CALL_SLOWLOG | CMD_CALL_STATS | CMD_CALL_PROPAGATE)

/* Command propagation flags, see propagate() function */
#define PROPAGATE_NONE 0
#define PROPAGATE_AOF 1
#define PROPAGATE_REPL 2

/* SHUTDOWN flags */
#define SHUTDOWN_NOFLAGS 0      /* No flags. */
#define SHUTDOWN_SAVE 1         /* Force SAVE on SHUTDOWN even if no save
                                   points are configured. */
#define SHUTDOWN_NOSAVE 2       /* Don't SAVE on SHUTDOWN. */

typedef void redisCommandProc(struct client *c);
typedef int *redisGetKeysProc(struct redisCommand *cmd, robj **argv, int argc, int *numkeys);
struct redisCommand {
    char *name;
    redisCommandProc *proc;
    int arity;
    char *sflags; /* Flags as string representation, one char per flag. */
    int flags;    /* The actual flags, obtained from the 'sflags' field. */
    /* Use a function to determine keys arguments in a command line.
     * Used for Redis Cluster redirect. */
    redisGetKeysProc *getkeys_proc;
    /* What keys should be loaded in background when calling this command? */
    int firstkey; /* The first argument that's a key (0 = no keys) */
    int lastkey;  /* The last argument that's a key */
    int keystep;  /* The step between first and last key */
    int idx;
    int needadmin;
};

typedef struct commandStats {
    char *name;
    long long microseconds;
    long long calls;
}commandStats;

/* The redisOp structure defines a Redis Operation, that is an instance of
 * a command with an argument vector, database ID, propagation target
 * (PROPAGATE_*), and command pointer.
 *
 * Currently only used to additionally propagate more commands to AOF/Replication
 * after the propagation of the executed command. */
typedef struct redisOp {
    robj **argv;
    int argc, dbid, target;
    struct redisCommand *cmd;
} redisOp;

/* Defines an array of Redis operations. There is an API to add to this
 * structure in a easy way.
 *
 * redisOpArrayInit();
 * redisOpArrayAppend();
 * redisOpArrayFree();
 */
typedef struct redisOpArray {
    redisOp *ops;
    int numops;
} redisOpArray;

extern dictType commandTableDictType;

void populateCommandTable(void);
int populateCommandsNeedAdminpass(void);

struct redisCommand *lookupCommand(sds name);
struct redisCommand *lookupCommandOrOriginal(sds name);
struct redisCommand *lookupCommandByCString(char *s);

void call(struct client *c, int flags);
int processCommand(struct client *c);

void redisOpArrayInit(redisOpArray *oa);
int redisOpArrayAppend(redisOpArray *oa, struct redisCommand *cmd, int dbid, robj **argv, int argc, int target);
void redisOpArrayFree(redisOpArray *oa);

void propagate(struct redisCommand *cmd, int dbid, robj **argv, int argc, int flags);
void alsoPropagate(struct redisCommand *cmd, int dbid, robj **argv, int argc, int target);
void forceCommandPropagation(struct client *c, int flags);
void preventCommandPropagation(struct client *c);
void preventCommandAOF(struct client *c);
void preventCommandReplication(struct client *c);

void commandCommand(struct client *c);

struct darray *commandStatsTableCreate(void);
void commandStatsTableDestroy(struct darray *cstatstable);

#endif
