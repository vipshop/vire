#include <string.h>
#include <stdlib.h>
#ifndef _MSC_VER
#include <unistd.h>
#endif
#include <assert.h>
#include <errno.h>
#include <ctype.h>

#include "himcread.h"
#include "himcdep/sds.h"

#define PARSE_OK    0        /* Parsing ok */
#define PARSE_ERROR 1        /* Parsing error */
#define PARSE_AGAIN 3        /* Incomplete -> parse again */

#define RSP_TYPE_UNKNOWN        0
#define RSP_TYPE_NUM            1
#define RSP_TYPE_STORED         2
#define RSP_TYPE_NOT_STORED     3
#define RSP_TYPE_EXISTS         4
#define RSP_TYPE_NOT_FOUND      5
#define RSP_TYPE_END            6
#define RSP_TYPE_VALUE          7
#define RSP_TYPE_DELETED        8
#define RSP_TYPE_ERROR          9
#define RSP_TYPE_CLIENT_ERROR   10
#define RSP_TYPE_SERVER_ERROR   11

static void memcachedReaderReset(mcReader *r);

static void __memcachedReaderSetError(mcReader *r, int type, const char *str) {
    size_t len;

    memcachedReaderReset(r);

    /* Clear input buffer on errors. */
    if (r->buf != NULL) {
        sdsfree(r->buf);
        r->buf = NULL;
        r->pos = r->len = 0;
    }

    /* Set error. */
    r->err = type;
    len = strlen(str);
    len = len < (sizeof(r->errstr)-1) ? len : (sizeof(r->errstr)-1);
    memcpy(r->errstr,str,len);
    r->errstr[len] = '\0';
}

static size_t chrtos(char *buf, size_t size, char byte) 
{
    size_t len = 0;

    switch(byte) {
    case '\\':
    case '"':
        len = snprintf(buf,size,"\"\\%c\"",byte);
        break;
    case '\n': len = snprintf(buf,size,"\"\\n\""); break;
    case '\r': len = snprintf(buf,size,"\"\\r\""); break;
    case '\t': len = snprintf(buf,size,"\"\\t\""); break;
    case '\a': len = snprintf(buf,size,"\"\\a\""); break;
    case '\b': len = snprintf(buf,size,"\"\\b\""); break;
    default:
        if (isprint(byte))
            len = snprintf(buf,size,"\"%c\"",byte);
        else
            len = snprintf(buf,size,"\"\\x%02x\"",(unsigned char)byte);
        break;
    }

    return len;
}

static void __memcachedReaderSetErrorProtocolByte(mcReader *r, char byte) {
    char cbuf[8], sbuf[128];

    chrtos(cbuf,sizeof(cbuf),byte);
    snprintf(sbuf,sizeof(sbuf),
        "Protocol error, got %s as reply type byte", cbuf);
    __memcachedReaderSetError(r,MC_ERR_PROTOCOL,sbuf);
}

static void __memcachedReaderSetErrorOOM(mcReader *r) {
    __memcachedReaderSetError(r,MC_ERR_OOM,"Out of memory");
}

static int elementArrayCreate(mcReader *r)
{
    assert(r->alloc_len == 0);
    assert(r->element == NULL);
    assert(r->elements == 0);

    r->element = malloc(10*sizeof(void*));
    if (r->element == NULL) {
        __memcachedReaderSetErrorOOM(r);
        return MC_ERR;
    }
    r->alloc_len = 10;
    r->elements = 0;
    
    return MC_OK;
}

static void elementArrayDestroy(mcReader *r)
{
    unsigned int i;

    if (r->element == NULL)
        return;

    if (r->fn && r->fn->freeObject) {
        for (i = 0; i < r->elements; i ++) {
            if (r->element[i])
                r->fn->freeObject(r->element[i]);
        }
    }
    free(r->element);
    r->element = NULL;
    r->elements = 0;
    r->alloc_len = 0;
    
    return MC_OK;
}

#define EXPAND_MAX_SIZE_PER_TIME 300
static int elementArrayExpand(mcReader *r) 
{
    size_t new_length;
    if (r->alloc_len <= 150) {
        new_length = r->alloc_len*2;
    } else if (r->alloc_len <= 500) {
        new_length = r->alloc_len+EXPAND_MAX_SIZE_PER_TIME;
    }
    r->element = realloc(r->element,new_length*sizeof(void*));
    if (r->element == NULL) {
        __memcachedReaderSetErrorOOM(r);
        return MC_ERR;
    }
    r->alloc_len = new_length;

    return MC_OK;
}

static int elementArrayAdd(mcReader *r, void *reply)
{
    assert(r->elements <= r->alloc_len);
    if (r->elements == r->alloc_len) {
        if (elementArrayExpand(r) != MC_OK)
            return MC_ERR;
    }
    r->element[r->elements++] = reply;

    return MC_OK;
}

static void memcachedParseResponse(mcReader *r)
{
    void *obj;
    char *p, *m;
    char ch;
    enum {
        SW_START,
        SW_RSP_NUM,
        SW_RSP_STR,
        SW_SPACES_BEFORE_KEY,
        SW_KEY,
        SW_SPACES_BEFORE_FLAGS,     /* 5 */
        SW_FLAGS,
        SW_SPACES_BEFORE_VLEN,
        SW_VLEN,
        SW_RUNTO_VAL,
        SW_VAL,                     /* 10 */
        SW_VAL_LF,
        SW_END,
        SW_RUNTO_CRLF,
        SW_CRLF,
        SW_ALMOST_DONE,             /* 15 */
        SW_SENTINEL
    } state;

    state = r->state;

    assert(state >= SW_START && state < SW_SENTINEL);

    /* validate the parsing marker */
    assert(r->buf != NULL);
    assert(r->pos < r->len);

    for (p = r->buf+r->pos; p <= r->buf+r->len; p++) {
        ch = *p;

        switch (state) {
        case SW_START:
            if (isdigit(ch)) {
                state = SW_RSP_NUM;
            } else {
                state = SW_RSP_STR;
            }
            p = p - 1; /* go back by 1 byte */

            break;

        case SW_RSP_NUM:
            if (r->token == NULL) {
                /* rsp_start <- p; type_start <- p */
                r->token = p;
            }

            if (isdigit(ch)) {
                /* num <- num * 10 + (ch - '0') */
                r->integer = r->integer*10 + (long long)(ch-'0');
            } else if (ch == ' ' || ch == '\r') {
                /* type_end <- p - 1 */
                r->token = NULL;
                r->integer = 0;
                r->type = RSP_TYPE_NUM;
                p = p - 1; /* go back by 1 byte */
                state = SW_CRLF;
            } else {
                goto error;
            }

            break;

        case SW_RSP_STR:
            if (r->token == NULL) {
                /* rsp_start <- p; type_start <- p */
                r->token = p;
            }

            if (ch == ' ' || ch == '\r') {
                /* type_end <- p - 1 */
                m = r->token;
                /* r->token = NULL; */
                r->type = RSP_TYPE_UNKNOWN;
                assert(r->str == NULL && r->strlen == 0);
                
                switch (p - m) {
                case 3:
                    if (!strncmp(m,"END\r",4)) {
                        r->type = RSP_TYPE_END;
                        /* end_start <- m; end_end <- p - 1 */
                    }

                    break;

                case 5:
                    if (!strncmp(m,"VALUE",5)) {
                        /*
                                           * Encompasses responses for 'get', 'gets' and
                                           * 'cas' command.
                                           */
                        r->type = RSP_TYPE_VALUE;
                        break;
                    }

                    if (!strncmp(m,"ERROR",5)) {
                        r->type = RSP_TYPE_ERROR;
                        break;
                    }

                    break;

                case 6:
                    if (!strncmp(m,"STORED",6)) {
                        r->type = RSP_TYPE_STORED;

                        r->str = m;
                        r->strlen = 6;
                        break;
                    }

                    if (!strncmp(m,"EXISTS",6)) {
                        r->type = RSP_TYPE_EXISTS;

                        r->str = m;
                        r->strlen = 6;
                        break;
                    }

                    break;

                case 7:
                    if (!strncmp(m,"DELETED",7)) {
                        r->type = RSP_TYPE_DELETED;
                        
                        r->str = m;
                        r->strlen = 7;
                        break;
                    }

                    break;

                case 9:
                    if (!strncmp(m,"NOT_FOUND",9)) {
                        r->type = RSP_TYPE_NOT_FOUND;

                        r->str = m;
                        r->strlen = 9;
                        break;
                    }

                    break;

                case 10:
                    if (!strncmp(m,"NOT_STORED",10)) {
                        r->type = RSP_TYPE_NOT_STORED;

                        r->str = m;
                        r->strlen = 10;
                        break;
                    }

                    break;

                case 12:
                    if (!strncmp(m,"CLIENT_ERROR",12)) {
                        r->type = RSP_TYPE_CLIENT_ERROR;
                        break;
                    }

                    if (!strncmp(m,"SERVER_ERROR",12)) {
                        r->type = RSP_TYPE_SERVER_ERROR;
                        break;
                    }

                    break;
                }

                switch (r->type) {
                case RSP_TYPE_UNKNOWN:
                    goto error;

                case RSP_TYPE_STORED:
                case RSP_TYPE_NOT_STORED:
                case RSP_TYPE_EXISTS:
                case RSP_TYPE_NOT_FOUND:
                case RSP_TYPE_DELETED:
                    state = SW_CRLF;
                    break;

                case RSP_TYPE_END:
                    state = SW_CRLF;
                    break;

                case RSP_TYPE_VALUE:
                    state = SW_SPACES_BEFORE_KEY;
                    break;

                case RSP_TYPE_ERROR:
                    state = SW_CRLF;
                    break;

                case RSP_TYPE_CLIENT_ERROR:
                case RSP_TYPE_SERVER_ERROR:
                    r->token = NULL;
                    state = SW_RUNTO_CRLF;
                    break;

                default:
                    NOT_REACHED();
                }

                p = p - 1; /* go back by 1 byte */
            }

            break;

        case SW_SPACES_BEFORE_KEY:
            if (ch != ' ') {
                state = SW_KEY;
                p = p - 1; /* go back by 1 byte */
                r->token = NULL;
            }

            break;

        case SW_KEY:
            if (r->token == NULL) {
                r->token = p;
            }
            
            if (ch == ' ') {
                assert(r->str == NULL && r->strlen == 0);
                m = r->token;
                r->token = NULL;
                state = SW_SPACES_BEFORE_FLAGS;
                r->strlen = p-m;
                r->str = m;
            }

            break;

        case SW_SPACES_BEFORE_FLAGS:
            if (ch != ' ') {
                if (!isdigit(ch)) {
                    goto error;
                }
                state = SW_FLAGS;
                p = p - 1; /* go back by 1 byte */
                r->kflags = 0;
            }

            break;

        case SW_FLAGS:
            if (isdigit(ch)) {
                /* flags <- flags * 10 + (ch - '0') */
                r->kflags = r->kflags*10 + (int)(ch-'0');
            } else if (ch == ' ') {
                /* flags_end <- p - 1 */
                /* r->token = NULL; */
                state = SW_SPACES_BEFORE_VLEN;
            } else {
                goto error;
            }

            break;

        case SW_SPACES_BEFORE_VLEN:
            if (ch != ' ') {
                if (!isdigit(ch)) {
                    goto error;
                }
                p = p - 1; /* go back by 1 byte */
                state = SW_VLEN;
                r->integer = 0;
            }

            break;

        case SW_VLEN:
            if (isdigit(ch)) {
                r->integer = r->integer*10 + (long long)(ch-'0');
            } else if (ch == ' ' || ch == '\r') {
                /* vlen_end <- p - 1 */
                p = p - 1; /* go back by 1 byte */
                /* r->token = NULL; */
                state = SW_RUNTO_CRLF;
            } else {
                goto error;
            }

            break;

        case SW_RUNTO_VAL:
            switch (ch) {
            case '\n':
                /* val_start <- p + 1 */
                state = SW_VAL;
                r->token = NULL;
                break;

            default:
                goto error;
            }

            break;

        case SW_VAL:
            if (r->token == NULL) {
                /* flags_start <- p */
                r->token = p;
            }

            m = r->token + r->integer;
            if (m > r->buf+r->len) {
                p = r->buf + r->len;
                break;
            }
            
            switch (*m) {
            case '\r':
                /* val_end <- p - 1 */
                p = m; /* move forward by vlen bytes */
                state = SW_VAL_LF;
                break;

            default:
                goto error;
            }

            break;

        case SW_VAL_LF:
            switch (ch) {
            case '\n':
                /* state = SW_END; */
                if (r->fn && r->fn->createString)
                    obj = r->fn->createString(MC_REPLY_STRING,r->str,r->strlen,
                        r->token,r->integer,r->kflags,r->kversion);
                else
                    obj = (void*)MC_REPLY_STRING;
                if (r->element) {
                    assert(r->subreply == NULL);
                    elementArrayAdd(r,r->subreply);
                } else if (r->subreply) {
                    elementArrayCreate(r);
                    elementArrayAdd(r,r->subreply);
                    r->subreply = NULL;
                    elementArrayAdd(r,obj);
                } else {
                    r->subreply = obj;
                }
                
                r->token = NULL;
                r->str = NULL;
                r->strlen = 0;
                r->kflags = 0;
                r->kversion = -1;
                state = SW_RSP_STR;
                break;

            default:
                goto error;
            }

            break;

        case SW_END:
            if (r->token == NULL) {
                if (ch != 'E') {
                    goto error;
                }
                /* end_start <- p */
                r->token = p;
            } else if (ch == '\r') {
                /* end_end <- p */
                m = r->token;
                r->token = NULL;

                switch (p - m) {
                case 3:
                    if (!strncmp(m,"END\r",4)) {
                        state = SW_ALMOST_DONE;
                    }
                    break;

                default:
                    goto error;
                }
            }

            break;

        case SW_RUNTO_CRLF:
            switch (ch) {
            case '\r':
                if (r->type == RSP_TYPE_VALUE) {
                    state = SW_RUNTO_VAL;
                } else {
                    if (r->type == RSP_TYPE_CLIENT_ERROR || 
                        r->type == RSP_TYPE_SERVER_ERROR) {
                        m = r->token;
                        r->token = NULL;
                        r->strlen = p-m;
                        r->str = m;
                    }
                    state = SW_ALMOST_DONE;
                }

                break;

            default:
                break;
            }

            break;

        case SW_CRLF:
            switch (ch) {
            case ' ':
                break;

            case '\r':
                state = SW_ALMOST_DONE;
                break;

            default:
                goto error;
            }

            break;

        case SW_ALMOST_DONE:
            switch (ch) {
            case '\n':
                /* rsp_end <- p */
                goto done;

            default:
                goto error;
            }

            break;

        case SW_SENTINEL:
        default:
            NOT_REACHED();
            break;

        }
    }

    assert(p == r->buf+r->len);
    r->pos = r->len;
    r->state = state;
    
    r->result = PARSE_AGAIN;

    return;

done:
    r->pos = p-r->buf+1;
    assert(r->pos <= r->len);
    r->state = SW_START;
    r->token = NULL;
    r->result = PARSE_OK;

    return;

error:
    r->result = PARSE_ERROR;
    r->state = state;
    errno = EINVAL;
}

mcReader *memcachedReaderCreateWithFunctions(mcReplyObjectFunctions *fn) 
{
    mcReader *r;

    r = calloc(sizeof(mcReader),1);
    if (r == NULL)
        return NULL;

    r->err = 0;
    r->errstr[0] = '\0';
    r->buf = sdsempty();
    r->maxbuf = MC_READER_MAX_BUF;
    if (r->buf == NULL) {
        free(r);
        return NULL;
    }

    r->subreply = NULL;
    r->alloc_len = 0;
    r->elements = 0;
    r->element = NULL;

    r->state = 0;
    r->token = NULL;

    r->str = NULL;
    r->strlen = 0;
    r->kflags = 0;
    r->kversion = -1;
    r->integer = 0;
    r->type = RSP_TYPE_UNKNOWN;
    r->result = PARSE_OK;

    r->fn = fn;
    
    return r;
}

void memcachedReaderFree(mcReader *r) 
{
    memcachedReaderReset(r);
    
    if (r->buf != NULL)
        sdsfree(r->buf);
    free(r);
}

int memcachedReaderFeed(mcReader *r, const char *buf, size_t len) 
{
    sds newbuf;

    /* Return early when this reader is in an erroneous state. */
    if (r->err)
        return MC_ERR;

    /* Copy the provided buffer. */
    if (buf != NULL && len >= 1) {
        /* Destroy internal buffer when it is empty and is quite large. */
        if (r->len == 0 && r->maxbuf != 0 && sdsavail(r->buf) > r->maxbuf) {
            sdsfree(r->buf);
            r->buf = sdsempty();
            r->pos = 0;

            /* r->buf should not be NULL since we just free'd a larger one. */
            assert(r->buf != NULL);
        }

        newbuf = sdscatlen(r->buf,buf,len);
        if (newbuf == NULL) {
            __memcachedReaderSetErrorOOM(r);
            return MC_ERR;
        }

        r->buf = newbuf;
        r->len = sdslen(r->buf);
    }

    return MC_OK;
}

static void memcachedReaderReset(mcReader *r) 
{
    r->str = NULL;
    r->strlen = 0;
    r->kflags = 0;
    r->kversion = -1;

    r->state = 0;
    r->token = 0;
 
    r->integer = 0;

    r->type = RSP_TYPE_UNKNOWN;
    r->result = PARSE_OK;
    
    if (r->subreply != NULL) {
        if (r->fn && r->fn->freeObject)
            r->fn->freeObject(r->subreply);
        
        r->subreply = NULL;
    }

    elementArrayDestroy(r);

    r->err = 0;
    r->errstr[0] = '\0';
}

static void *getReplyFromReader(mcReader *r)
{
    void *reply;

    switch (r->type) {
        case RSP_TYPE_VALUE:
            if (r->element) {
                assert(r->subreply == NULL);
                if (r->fn && r->fn->createArray) {
                    reply = r->fn->createArray(r->elements,r->element);
                    r->element = NULL;
                    r->elements = 0;
                    r->alloc_len = 0;
                } else {
                    reply = (void*)MC_REPLY_ARRAY;
                }
            } else if (r->subreply) {
                reply = r->subreply;
            }
            break;
        case RSP_TYPE_NUM:
            if (r->fn && r->fn->createInteger)
                reply = r->fn->createInteger(r->integer);
            else
                reply = (void*)MC_REPLY_INTEGER;
            break;
        case RSP_TYPE_END:
            if (r->fn && r->fn->createNil)
                reply = r->fn->createNil();
            else
                reply = (void*)MC_REPLY_NIL;
            break;
        case RSP_TYPE_CLIENT_ERROR:
        case RSP_TYPE_SERVER_ERROR:
            if (r->fn && r->fn->createString)
                reply = r->fn->createString(MC_REPLY_ERROR,
                    NULL,0,r->str,r->strlen,0,0);
            else
                reply = (void*)MC_REPLY_ERROR;
            break;
        case RSP_TYPE_ERROR:
            if (r->fn && r->fn->createString)
                reply = r->fn->createString(MC_REPLY_ERROR,
                    NULL,0,"",0,0,0);
            else
                reply = (void*)MC_REPLY_ERROR;
            break;
        case RSP_TYPE_STORED:
        case RSP_TYPE_NOT_STORED:
        case RSP_TYPE_EXISTS:
        case RSP_TYPE_NOT_FOUND:
        case RSP_TYPE_DELETED:
            if (r->fn && r->fn->createString)
                reply = r->fn->createString(MC_REPLY_STATUS,
                    NULL,0,r->str,r->strlen,0,0);
            else
                reply = (void*)MC_REPLY_STATUS;
            break;
        default:
            reply = NULL;
            break;
    }
    
    return reply;
}

int memcachedReaderGetReply(mcReader *r, void **reply) {
    /* Default target pointer to NULL. */
    if (reply != NULL)
        *reply = NULL;

    /* Return early when this reader is in an erroneous state. */
    if (r->err)
        return MC_ERR;

    /* When the buffer is empty, there will never be a reply. */
    if (r->len == 0)
        return MC_OK;

    memcachedParseResponse(r);

    /* Return ASAP when an error occurred. */
    if (r->err)
        return MC_ERR;

    /*
    printf("!######### r->result: %d, r->state: %d, r->type: %d, r->pos: %d, r->len: %d," 
        "r->kflags: %d, r->kversion : %lld, r->strlen: %zu, r->integer: %lld r->buf: %s, r->str: %s\n", 
        r->result, r->state, r->type, r->pos, r->len, r->kflags, r->kversion, 
        r->strlen, r->integer, r->buf, r->str);
        */
    /* Emit a reply when there is one. */
    if (r->result == PARSE_OK) {
        if (reply != NULL) {
            *reply = getReplyFromReader(r);
        }
        memcachedReaderReset(r);
    }

    /* Discard part of the buffer when we've consumed at least 1k, to avoid
     * doing unnecessary calls to memmove() in sds.c. */
    if (r->pos >= 1024 && r->token == NULL && r->str == NULL) {
        sdsrange(r->buf,r->pos,-1);
        r->pos = 0;
        r->len = sdslen(r->buf);
    }

    return MC_OK;
}
