#include <stdlib.h>
#include <errno.h>
#include <assert.h>

#include "himemcached.h"

#define REQ_TYPE_UNKNOWN        0
#define REQ_TYPE_STORAGE        1
#define REQ_TYPE_CAS            2
#define REQ_TYPE_RETRIEVAL      3
#define REQ_TYPE_ARITHMETIC     4
#define REQ_TYPE_DELETE         5

static mcReply *createReplyObject(int type);
static void *createStringObject(int type, char *key, size_t keylen, char *str, size_t len, int flags, long long version);
static void *createArrayObject(size_t elements, void **element);
static void *createIntegerObject(long long value);
static void *createNilObject(void);

/* Default set of functions to build the reply. Keep in mind that such a
 * function returning NULL is interpreted as OOM. */
static mcReplyObjectFunctions defaultFunctions = {
    createStringObject,
    createArrayObject,
    createIntegerObject,
    createNilObject,
    freeMcReplyObject
};

/* Create a reply object */
static mcReply *createReplyObject(int type) {
    mcReply *r = calloc(1,sizeof(*r));

    if (r == NULL)
        return NULL;

    r->type = type;
    
    return r;
}

/* Free a reply object */
void freeMcReplyObject(void *reply) {
    mcReply *r = reply;
    size_t j;

    if (r == NULL)
        return;

    switch(r->type) {
    case MC_REPLY_INTEGER:
    case MC_REPLY_NIL:
        break; /* Nothing to free */
    case MC_REPLY_ARRAY:
        if (r->element != NULL) {
            for (j = 0; j < r->elements; j++)
                if (r->element[j] != NULL)
                    freeMcReplyObject(r->element[j]);
            free(r->element);
        }
        break;
    case MC_REPLY_ERROR:
    case MC_REPLY_STATUS:
    case MC_REPLY_STRING:
        if (r->key != NULL)
            free(r->key);
        if (r->str != NULL)
            free(r->str);
        break;
    default:
        assert(0);
        break;
    }
    free(r);
}

static void *createStringObject(int type, char *key, size_t keylen, char *str, size_t len, int flags, long long version) {
    mcReply *r, *parent;
    char *buf;

    assert(type == MC_REPLY_ERROR  ||
           type == MC_REPLY_STATUS ||
           type == MC_REPLY_STRING);

    r = createReplyObject(type);
    if (r == NULL)
        return NULL;

    if (key != NULL) {
        r->key = malloc(keylen+1);
        if (r->key == NULL) {
            freeMcReplyObject(r);
            return NULL;
        }
        if (keylen > 0)
            /* Copy string value */
            memcpy(r->key,key,keylen);
        r->key[keylen] = '\0';
        r->keylen = keylen;
    }

    buf = malloc(len+1);
    if (buf == NULL) {
        freeMcReplyObject(r);
        return NULL;
    }
    if (len > 0)
        /* Copy string value */
        memcpy(buf,str,len);
    buf[len] = '\0';
    r->str = buf;
    r->len = len;

    r->flags = flags;
    r->version = version;
    
    return r;
}

static void *createArrayObject(size_t elements, void **element) {
    mcReply *r;

    r = createReplyObject(MC_REPLY_ARRAY);
    if (r == NULL)
        return NULL;

    r->elements = elements;
    r->element = (mcReply **)element;

    return r;
}

static void *createIntegerObject(long long value) {
    mcReply *r;

    r = createReplyObject(MC_REPLY_INTEGER);
    if (r == NULL)
        return NULL;

    r->integer = value;
    
    return r;
}

static void *createNilObject(void) {
    mcReply *r;

    r = createReplyObject(MC_REPLY_NIL);
    if (r == NULL)
        return NULL;

    return r;
}

void __memcachedSetError(mcContext *c, int type, const char *str) {
    size_t len;

    c->err = type;
    if (str != NULL) {
        len = strlen(str);
        len = len < (sizeof(c->errstr)-1) ? len : (sizeof(c->errstr)-1);
        memcpy(c->errstr,str,len);
        c->errstr[len] = '\0';
    } else {
        /* Only REDIS_ERR_IO may lack a description! */
        assert(type == MC_ERR_IO);
        //__redis_strerror_r(errno, c->errstr, sizeof(c->errstr));
    }
}


/* Write the output buffer to the socket.
 *
 * Returns MC_OK when the buffer is empty, or (a part of) the buffer was
 * succesfully written to the socket. When the buffer is empty after the
 * write operation, "done" is set to 1 (if given).
 *
 * Returns MC_ERR if an error occured trying to write and sets
 * c->errstr to hold the appropriate error string.
 */
int memcachedBufferWrite(mcContext *c, int *done) {
    int nwritten;

    /* Return early when the context has seen an error. */
    if (c->err)
        return MC_ERR;

    if (sdslen(c->obuf) > 0) {
        nwritten = write(c->fd,c->obuf,sdslen(c->obuf));
        if (nwritten == -1) {
            if ((errno == EAGAIN && !(c->flags & MC_BLOCK)) || (errno == EINTR)) {
                /* Try again later */
            } else {
                __memcachedSetError(c,MC_ERR_IO,NULL);
                return MC_ERR;
            }
        } else if (nwritten > 0) {
            if (nwritten == (signed)sdslen(c->obuf)) {
                sdsfree(c->obuf);
                c->obuf = sdsempty();
            } else {
                sdsrange(c->obuf,nwritten,-1);
            }
        }
    }
    if (done != NULL) *done = (sdslen(c->obuf) == 0);
    return MC_OK;
}

/* Internal helper function to try and get a reply from the reader,
 * or set an error in the context otherwise. */
int memcachedGetReplyFromReader(mcContext *c, void **reply) {
    if (memcachedReaderGetReply(c->reader,reply) == MC_ERR) {
        __memcachedSetError(c,c->reader->err,c->reader->errstr);
        return MC_ERR;
    }
    return MC_OK;
}

int memcachedGetReply(mcContext *c, void **reply) {
    int wdone = 0;
    void *aux = NULL;

    /* Try to read pending replies */
    if (memcachedGetReplyFromReader(c,&aux) == MC_ERR)
        return MC_ERR;

    /* For the blocking context, flush output buffer and read reply */
    if (aux == NULL && c->flags & MC_BLOCK) {
        /* Write until done */
        do {
            if (memcachedBufferWrite(c,&wdone) == MC_ERR)
                return MC_ERR;
        } while (!wdone);

        /* Read until there is a reply */
        do {
            if (memcachedBufferRead(c) == MC_ERR)
                return MC_ERR;
            if (memcachedGetReplyFromReader(c,&aux) == MC_ERR)
                return MC_ERR;
        } while (aux == NULL);
    }

    /* Set reply object */
    if (reply != NULL) *reply = aux;
    return MC_OK;
}

mcReader *memcachedReaderCreate(void) {
    return memcachedReaderCreateWithFunctions(&defaultFunctions);
}

mcContext *memcachedContextInit(void) {
    mcContext *c;

    c = calloc(1,sizeof(mcContext));
    if (c == NULL)
        return NULL;

    c->err = 0;
    c->errstr[0] = '\0';
    c->obuf = sdsempty();
    c->reader = memcachedReaderCreate();
    c->tcp.host = NULL;
    c->tcp.source_addr = NULL;
    c->unix_sock.path = NULL;
    c->timeout = NULL;

    if (c->obuf == NULL || c->reader == NULL) {
        memcachedFree(c);
        return NULL;
    }

    return c;
}

void memcachedFree(mcContext *c) {
    if (c == NULL)
        return;
    if (c->fd > 0)
        close(c->fd);
    if (c->obuf != NULL)
        sdsfree(c->obuf);
    if (c->reader != NULL)
        memcachedReaderFree(c->reader);
    if (c->tcp.host)
        free(c->tcp.host);
    if (c->tcp.source_addr)
        free(c->tcp.source_addr);
    if (c->unix_sock.path)
        free(c->unix_sock.path);
    if (c->timeout)
        free(c->timeout);
    free(c);
}

/* Use this function to handle a read event on the descriptor. It will try
 * and read some bytes from the socket and feed them to the reply parser.
 *
 * After this function is called, you may use memcachedContextReadReply to
 * see if there is a reply available. */
int memcachedBufferRead(mcContext *c) {
    char buf[1024*16];
    int nread;

    /* Return early when the context has seen an error. */
    if (c->err)
        return MC_ERR;

    nread = read(c->fd,buf,sizeof(buf));
    if (nread == -1) {
        if ((errno == EAGAIN && !(c->flags & MC_BLOCK)) || (errno == EINTR)) {
            /* Try again later */
        } else {
            __memcachedSetError(c,MC_ERR_IO,NULL);
            return MC_ERR;
        }
    } else if (nread == 0) {
        __memcachedSetError(c,MC_ERR_EOF,"Server closed the connection");
        return MC_ERR;
    } else {
        if (memcachedReaderFeed(c->reader,buf,nread) != MC_OK) {
            __memcachedSetError(c,c->reader->err,c->reader->errstr);
            return MC_ERR;
        }
    }
    return MC_OK;
}

static int getRequestTypeFromString(char *str, size_t len)
{
    if (str == NULL || len == 0)
        return -1;

    if (len == 3) {
        if (!strncasecmp(str,"set",3) || 
            !strncasecmp(str,"add",3)) {
            return REQ_TYPE_STORAGE;
        } else if (!strncasecmp(str,"cas",3)) {
            return REQ_TYPE_CAS;
        } else if (!strncasecmp(str,"get",3)) {
            return REQ_TYPE_RETRIEVAL;
        } else {
            return -1;
        }
    } else if (len == 4) {
        if (!strncasecmp(str,"gets",4)) {
            return REQ_TYPE_RETRIEVAL;
        } else if (!strncasecmp(str,"incr",4) || 
            !strncasecmp(str,"decr",4)) {
            return REQ_TYPE_ARITHMETIC;
        } else {
            return -1;
        }
    } else if (len == 6) {
        if (!strncasecmp(str,"append",6)) {
            return REQ_TYPE_STORAGE;
        } else if (!strncasecmp(str,"delete",6)) {
            return REQ_TYPE_DELETE;
        } else {
            return -1;
        }
    } else if (len == 7) {
        if (!strncasecmp(str,"replace",7) || 
            !strncasecmp(str,"prepend",7)) {
            return REQ_TYPE_STORAGE;
        } else {
            return -1;
        }
    }

    return -1;
}

#define ARGUMENTLEN(_argtype,_argv,_argvlen,_idx) \
    (_argtype==0?sdslen(_argv[_idx]):(_argvlen==NULL?strlen(_argv[_idx]):_argvlen[_idx]))

/* argtype==0 : argv is sds array 
 * argtype==1 : argv is c-string array and an array with their lengths. 
 * If the length array is set to NULL, strlen will be used to compute the
 * argument lengths.
 */
static int checkCmdValidAndGetTotalLen(int cmdtype, int argtype, int argc, char **argv, size_t *argvlen)
{
    size_t len;
    int totlen, j;
    
    switch (cmdtype) {
        case REQ_TYPE_STORAGE:
            if (argc != 6 && argc != 7) {
                return -1;
            }
            if (argc == 7 && (ARGUMENTLEN(argtype,argv,argvlen,5) != 7 || 
                strncasecmp(argv[5],"noreply",7))) {
                return -1;
            }

            totlen = 0;
            for (j = 0; j < argc-1; j ++) {
                totlen += ARGUMENTLEN(argtype,argv,argvlen,j) + 1;
            }
            totlen += 2 + ARGUMENTLEN(argtype,argv,argvlen,argc-1) + 2;
            break;
        case REQ_TYPE_CAS:
            if (argc != 7 && argc != 8) {
                return -1;
            }
            if (argc == 8 && (ARGUMENTLEN(argtype,argv,argvlen,6) != 7 || 
                strncasecmp(argv[6],"noreply",7))) {
                return -1;
            }

            totlen = 0;
            for (j = 0; j < argc-1; j ++) {
                totlen += ARGUMENTLEN(argtype,argv,argvlen,j) + 1;
            }
            totlen += 2 + ARGUMENTLEN(argtype,argv,argvlen,argc-1) + 2;
            break;
        case REQ_TYPE_ARITHMETIC:
            if (argc != 3) {
                return -1;
            }
            totlen = ARGUMENTLEN(argtype,argv,argvlen,0) + 1 + 
                ARGUMENTLEN(argtype,argv,argvlen,1) + 1 + 
                ARGUMENTLEN(argtype,argv,argvlen,2) + 2;
            break;
        case REQ_TYPE_RETRIEVAL:
            if (argc <= 1) {
                return -1;
            }

            totlen = 0;
            for (j = 0; j < argc-1; j ++) {
                totlen += ARGUMENTLEN(argtype,argv,argvlen,j) + 1;
            }
            totlen += ARGUMENTLEN(argtype,argv,argvlen,argc-1) + 2;
            break;
        case REQ_TYPE_DELETE:
            if (argc != 2 && argc != 3) {
                return -1;
            }
            
            totlen = ARGUMENTLEN(argtype,argv,argvlen,0) + 1 + 
                ARGUMENTLEN(argtype,argv,argvlen,1);
            if (argc == 3) {
                if (strncasecmp(argv[2],"noreply",7)) {
                    return -1;
                }
                totlen += 1 + ARGUMENTLEN(argtype,argv,argvlen,2);
            }
            totlen += 2;
            break;
        default:
            totlen = -1;
            break;
    }

    return totlen;
}

/* Like the checkCmdValidAndGetTotalLen() function */
static int genericMemcachedCommand(int cmdtype, char *cmd, int argtype, int argc, char **argv, size_t *argvlen)
{
    int j;
    size_t len;
    int pos = 0; /* position in final command */
    
    switch (cmdtype) {
        case REQ_TYPE_STORAGE:
        case REQ_TYPE_CAS:
            for (j = 0; j < argc-1; j ++) {
                len = ARGUMENTLEN(argtype,argv,argvlen,j);
                memcpy(cmd+pos,argv[j],len);
                pos += (int)len;
                cmd[pos++] = ' ';
            }
            cmd[pos++] = '\r';
            cmd[pos++] = '\n';
            len = ARGUMENTLEN(argtype,argv,argvlen,argc-1);
            memcpy(cmd+pos,argv[argc-1],len);
            pos += (int)len;
            cmd[pos++] = '\r';
            cmd[pos++] = '\n';
            break;
        case REQ_TYPE_ARITHMETIC:
        case REQ_TYPE_RETRIEVAL:
        case REQ_TYPE_DELETE:
            for (j = 0; j < argc-1; j ++) {
                len = ARGUMENTLEN(argtype,argv,argvlen,j);
                memcpy(cmd+pos,argv[j],len);
                pos += len;
                cmd[pos++] = ' ';
            }
            len = ARGUMENTLEN(argtype,argv,argvlen,argc-1);
            memcpy(cmd+pos,argv[argc-1],len);
            pos += (int)len;
            cmd[pos++] = '\r';
            cmd[pos++] = '\n';
            break;
        default:
            pos = -1;
            break;
    }

    return pos;
}

/* Format a command according to the Memcached protocol. This function
 * takes the number of arguments, an array with arguments and an sds array. 
 */
int memcachedFormatCommandSdsArgv(char **target, int argc, const sds *argv) {
    char *cmd = NULL; /* final command */
    int pos; /* position in final command */
    int totlen;
    int type;

    /* Abort on a NULL target */
    if (target == NULL || argc < 1)
        return -1;

    type = getRequestTypeFromString(argv[0], sdslen(argv[0]));
    if (type < 0)
        goto format_err;

    totlen = checkCmdValidAndGetTotalLen(type, 0, argc, argv, NULL);
    if (totlen < 0) {
        goto format_err;
    }

    /* Build the command at protocol level */
    cmd = malloc(totlen+1);
    if (cmd == NULL) goto memory_err;

    pos = genericMemcachedCommand(type, cmd, 0, argc, argv, NULL);
    if (pos < 0) goto format_err;
    
    assert(pos == totlen);
    cmd[pos] = '\0';

    *target = cmd;
    return totlen;

format_err:
    if (cmd) free(cmd);
    return -2;

memory_err:
    return -1;
}

int memcachedvFormatCommand(char **target, const char *format, va_list ap)
{
    const char *c = format;
    char *cmd = NULL; /* final command */
    int pos; /* position in final command */
    sds curarg, newarg; /* current argument */
    int touched = 0; /* was the current argument touched? */
    char **curargv = NULL, **newargv = NULL;
    int argc = 0;
    int totlen;
    int error_type = 0; /* 0 = no error; -1 = memory error; -2 = format error */
    int j;

    /* Abort if there is not target to set */
    if (target == NULL)
        return -1;

    /* Build the command string accordingly to protocol */
    curarg = sdsempty();
    if (curarg == NULL)
        return -1;

    while(*c != '\0') {
        if (*c != '%' || c[1] == '\0') {
            if (*c == ' ') {
                if (touched) {
                    newargv = realloc(curargv,sizeof(char*)*(argc+1));
                    if (newargv == NULL) goto memory_err;
                    curargv = newargv;
                    curargv[argc++] = curarg;

                    /* curarg is put in argv so it can be overwritten. */
                    curarg = sdsempty();
                    if (curarg == NULL) goto memory_err;
                    touched = 0;
                }
            } else {
                newarg = sdscatlen(curarg,c,1);
                if (newarg == NULL) goto memory_err;
                curarg = newarg;
                touched = 1;
            }
        } else {
            char *arg;
            size_t size;

            /* Set newarg so it can be checked even if it is not touched. */
            newarg = curarg;

            switch(c[1]) {
            case 's':
                arg = va_arg(ap,char*);
                size = strlen(arg);
                if (size > 0)
                    newarg = sdscatlen(curarg,arg,size);
                break;
            case 'b':
                arg = va_arg(ap,char*);
                size = va_arg(ap,size_t);
                if (size > 0)
                    newarg = sdscatlen(curarg,arg,size);
                break;
            case '%':
                newarg = sdscat(curarg,"%");
                break;
            default:
                /* Try to detect printf format */
                {
                    static const char intfmts[] = "diouxX";
                    static const char flags[] = "#0-+ ";
                    char _format[16];
                    const char *_p = c+1;
                    size_t _l = 0;
                    va_list _cpy;

                    /* Flags */
                    while (*_p != '\0' && strchr(flags,*_p) != NULL) _p++;

                    /* Field width */
                    while (*_p != '\0' && isdigit(*_p)) _p++;

                    /* Precision */
                    if (*_p == '.') {
                        _p++;
                        while (*_p != '\0' && isdigit(*_p)) _p++;
                    }

                    /* Copy va_list before consuming with va_arg */
                    va_copy(_cpy,ap);

                    /* Integer conversion (without modifiers) */
                    if (strchr(intfmts,*_p) != NULL) {
                        va_arg(ap,int);
                        goto fmt_valid;
                    }

                    /* Double conversion (without modifiers) */
                    if (strchr("eEfFgGaA",*_p) != NULL) {
                        va_arg(ap,double);
                        goto fmt_valid;
                    }

                    /* Size: char */
                    if (_p[0] == 'h' && _p[1] == 'h') {
                        _p += 2;
                        if (*_p != '\0' && strchr(intfmts,*_p) != NULL) {
                            va_arg(ap,int); /* char gets promoted to int */
                            goto fmt_valid;
                        }
                        goto fmt_invalid;
                    }

                    /* Size: short */
                    if (_p[0] == 'h') {
                        _p += 1;
                        if (*_p != '\0' && strchr(intfmts,*_p) != NULL) {
                            va_arg(ap,int); /* short gets promoted to int */
                            goto fmt_valid;
                        }
                        goto fmt_invalid;
                    }

                    /* Size: long long */
                    if (_p[0] == 'l' && _p[1] == 'l') {
                        _p += 2;
                        if (*_p != '\0' && strchr(intfmts,*_p) != NULL) {
                            va_arg(ap,long long);
                            goto fmt_valid;
                        }
                        goto fmt_invalid;
                    }

                    /* Size: long */
                    if (_p[0] == 'l') {
                        _p += 1;
                        if (*_p != '\0' && strchr(intfmts,*_p) != NULL) {
                            va_arg(ap,long);
                            goto fmt_valid;
                        }
                        goto fmt_invalid;
                    }

                fmt_invalid:
                    va_end(_cpy);
                    goto format_err;

                fmt_valid:
                    _l = (_p+1)-c;
                    if (_l < sizeof(_format)-2) {
                        memcpy(_format,c,_l);
                        _format[_l] = '\0';
                        newarg = sdscatvprintf(curarg,_format,_cpy);

                        /* Update current position (note: outer blocks
                         * increment c twice so compensate here) */
                        c = _p-1;
                    }

                    va_end(_cpy);
                    break;
                }
            }

            if (newarg == NULL) goto memory_err;
            curarg = newarg;

            touched = 1;
            c++;
        }
        c++;
    }

    /* Add the last argument if needed */
    if (touched) {
        newargv = realloc(curargv,sizeof(char*)*(argc+1));
        if (newargv == NULL) goto memory_err;
        curargv = newargv;
        curargv[argc++] = curarg;
    } else {
        sdsfree(curarg);
    }

    /* Clear curarg because it was put in curargv or was free'd. */
    curarg = NULL;

    totlen = memcachedFormatCommandSdsArgv(&cmd, argc,curargv);
    if (totlen < 0) {
        error_type = totlen;
        goto cleanup;
    }

    free(curargv);
    *target = cmd;
    return totlen;

format_err:
    error_type = -2;
    goto cleanup;

memory_err:
    error_type = -1;
    goto cleanup;

cleanup:
    if (curargv) {
        while(argc--)
            sdsfree(curargv[argc]);
        free(curargv);
    }

    sdsfree(curarg);

    /* No need to check cmd since it is the last statement that can fail,
     * but do it anyway to be as defensive as possible. */
    if (cmd != NULL)
        free(cmd);

    return error_type;
}

/* Format a command according to the Memcached protocol. This function
 * takes a format similar to printf:
 *
 * %s represents a C null terminated string you want to interpolate
 * %b represents a binary safe string
 *
 * When using %b you need to provide both the pointer to the string
 * and the length in bytes as a size_t. Examples:
 *
 * len = memcachedFormatCommand(target, "GET %s", mykey);
 * len = memcachedFormatCommand(target, "SET %s %d, %lld %zu %s", mykey, myflags, myexptime, myvallen, myval);
 */
int memcachedFormatCommand(char **target, const char *format, ...) {
    va_list ap;
    int len;
    va_start(ap,format);
    len = memcachedvFormatCommand(target,format,ap);
    va_end(ap);

    /* The API says "-1" means bad result, but we now also return "-2" in some
     * cases.  Force the return value to always be -1. */
    if (len < 0)
        len = -1;

    return len;
}

/* Format a command according to the Redis protocol. This function takes the
 * number of arguments, an array with arguments and an array with their
 * lengths. If the latter is set to NULL, strlen will be used to compute the
 * argument lengths.
 */
int memcachedFormatCommandArgv(char **target, int argc, const char **argv, const size_t *argvlen) {
    char *cmd = NULL; /* final command */
    int pos; /* position in final command */
    int totlen;
    int type;

    /* Abort on a NULL target */
    if (target == NULL || argc < 1)
        return -1;

    type = getRequestTypeFromString(argv[0], argvlen==NULL?strlen(argv[0]):argvlen[0]);
    if (type < 0) {
        goto format_err;
    }

    totlen = checkCmdValidAndGetTotalLen(type, 1, argc, argv, argvlen);
    if (totlen < 0) {
        goto format_err;
    }

    /* Build the command at protocol level */
    cmd = malloc(totlen+1);
    if (cmd == NULL) goto memory_err;

    pos = genericMemcachedCommand(type, cmd, 1, argc, argv, argvlen);
    if (pos < 0) {
        goto format_err;
    }
    
    assert(pos == totlen);
    cmd[pos] = '\0';

    *target = cmd;
    return totlen;

format_err:
    if (cmd) free(cmd);
    return -2;

memory_err:
    return -1;
}

