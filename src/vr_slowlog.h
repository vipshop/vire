#ifndef _VR_SLOWLOG_H_
#define _VR_SLOWLOG_H_

#define CONFIG_DEFAULT_SLOWLOG_LOG_SLOWER_THAN 10000
#define CONFIG_DEFAULT_SLOWLOG_MAX_LEN 128

#define SLOWLOG_ENTRY_MAX_ARGC 32
#define SLOWLOG_ENTRY_MAX_STRING 128

/* This structure defines an entry inside the slow log list */
typedef struct slowlogEntry {
    robj **argv;
    int argc;
    long long id;       /* Unique entry identifier. */
    long long duration; /* Time spent by the query, in nanoseconds. */
    time_t time;        /* Unix time at which the query was executed. */
} slowlogEntry;

/* Exported API */
void slowlogInit(void);
void slowlogPushEntryIfNeeded(robj **argv, int argc, long long duration);

/* Exported commands */
void slowlogCommand(client *c);

#endif
