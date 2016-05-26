#ifndef _VR_AOF_H_
#define _VR_AOF_H_

/* AOF states */
#define AOF_OFF 0             /* AOF is off */
#define AOF_ON 1              /* AOF is on */
#define AOF_WAIT_REWRITE 2    /* AOF waits rewrite to start appending */

#define AOF_AUTOSYNC_BYTES (1024*1024*32) /* fdatasync every 32MB */

/* ----------------------------------------------------------------------------
 * AOF rewrite buffer implementation.
 *
 * The following code implement a simple buffer used in order to accumulate
 * changes while the background process is rewriting the AOF file.
 *
 * We only need to append, but can't just use realloc with a large block
 * because 'huge' reallocs are not always handled as one could expect
 * (via remapping of pages at OS level) but may involve copying data.
 *
 * For this reason we use a list of blocks, every block is
 * AOF_RW_BUF_BLOCK_SIZE bytes.
 * ------------------------------------------------------------------------- */

#define AOF_RW_BUF_BLOCK_SIZE (1024*1024*10)    /* 10 MB per block */

typedef struct aofrwblock {
    unsigned long used, free;
    char buf[AOF_RW_BUF_BLOCK_SIZE];
} aofrwblock;

unsigned long aofRewriteBufferSize(void);
void aofChildWriteDiffData(aeEventLoop *el, int fd, void *privdata, int mask);
sds catAppendOnlyExpireAtCommand(sds buf, struct redisCommand *cmd, robj *key, robj *seconds);
sds catAppendOnlyGenericCommand(sds dst, int argc, robj **argv);
void aofRewriteBufferAppend(unsigned char *s, unsigned long len);
void feedAppendOnlyFile(struct redisCommand *cmd, int dictid, robj **argv, int argc);

#endif
