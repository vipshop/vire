#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <fcntl.h>
#include <errno.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/utsname.h>

#include <hiredis.h>
#include <darray.h>

#include <vrt_util.h>
#include <vrt_public.h>
#include <vrt_produce_data.h>

typedef struct produce_thread {
    int id;
    pthread_t thread_id;
    
} produce_thread;

static unsigned int key_length_min;
static unsigned int key_length_max;
static unsigned int key_length_range_gap;
static unsigned int field_length_max;
static unsigned int string_length_max;

static int cmd_type;

static darray needed_cmd_type_producer;  /* type:  data_producer*/
static unsigned int needed_cmd_type_producer_count;

static darray produce_threads;

static unsigned int get_random_num(void)
{
    return (unsigned int)rand();
}

static char get_random_char(void)
{
    return (char)rand()%250 + 5;
}

static sds get_random_key(void)
{
    unsigned int i, len;
    sds str = sdsempty();
    
    len = get_random_num()%key_length_range_gap+key_length_min;
    if (len == 0) len ++;
    str = sdsMakeRoomFor(str,(size_t)len);
    sdsIncrLen(str, (int)len);

    for (i = 0; i < len; i ++) {
        str[i] = (char)get_random_char();
    }

    return str;
}

static sds get_random_string(void)
{
    unsigned int i, len;
    sds str = sdsempty();
    
    len = get_random_num()%string_length_max;
    str = sdsMakeRoomFor(str,(size_t)len);
    sdsIncrLen(str, (int)len);

    for (i = 0; i < len; i ++) {
        str[i] = get_random_char();
    }

    return str;
}

static data_unit *get_cmd_producer(data_producer *dp);
static data_unit *set_cmd_producer(data_producer *dp);
static data_unit *del_cmd_producer(data_producer *dp);

static int producers_count;
data_producer redis_data_producer_table[] = {
    {"get",get_cmd_producer,2,"rF",0,NULL,1,1,1,TEST_CMD_TYPE_STRING},
    {"set",set_cmd_producer,-3,"wm",0,NULL,1,1,1,TEST_CMD_TYPE_STRING},
    {"del",del_cmd_producer,-2,"w",0,NULL,1,-1,1,TEST_CMD_TYPE_KEY},
};

static data_unit *get_cmd_producer(data_producer *dp)
{
    data_unit *du;

    du = data_unit_get();
    du->dp = dp;
    du->argc = 2;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key();
    
    return du;
}

static data_unit *set_cmd_producer(data_producer *dp)
{
    data_unit *du;

    du = data_unit_get();
    du->dp = dp;
    du->argc = 3;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key();
    du->argv[2] = get_random_string();
    
    return du;
}

static data_unit *del_cmd_producer(data_producer *dp)
{
    data_unit *du;

    du = data_unit_get();
    du->dp = dp;
    du->argc = 2;
    du->argv = malloc(du->argc*sizeof(sds));
    du->argv[0] = sdsnew(dp->name);
    du->argv[1] = get_random_key();
    
    return du;
}

data_unit *data_unit_get(void)
{
    data_unit *du = malloc(sizeof(data_unit));
    du->dp = NULL;
    du->argc = 0;
    du->argv = NULL;
    du->hashvalue = 0;
    return du;
}

void data_unit_put(data_unit *du)
{
    int idx;
    
    for (idx = 0; idx < du->argc; idx ++) {
        if (du->argv[idx])
            sdsfree(du->argv[idx]);
    }
    free(du->argv);
    free(du);
}

static int vrt_produce_threads_init(unsigned int produce_threads_count)
{
    unsigned int idx;
    darray_init(&produce_threads, produce_threads_count, sizeof(produce_thread));
    for (idx = 0; idx < produce_threads_count; idx ++) {
        produce_thread *pt = darray_push(&produce_threads);
        pt->id = idx;
        pt->thread_id = 0;
    }
    
    return VRT_OK;
}

static void vrt_produce_threads_deinit(void)
{
    produce_threads.nelem = 0;
    darray_deinit(&produce_threads);
}

static void *vrt_produce_thread_run(void *args)
{
    int ret;
    produce_thread *pt = args;
    unsigned int idx, j;
    data_producer **dp;
    data_unit *du;

    srand(vrt_usec_now()^(int)pthread_self());

    while (1) {
        idx = rand()%needed_cmd_type_producer_count;
        dp = darray_get(&needed_cmd_type_producer,idx);
        du = (*dp)->proc(*dp);
        
        ret = data_dispatch(du);
        if (ret == -1) {
            data_unit_put(du);
        } else if (ret == 1) {
            usleep(10000);
        }
    }
    
    return NULL;
}

int vrt_produce_data_init(int key_length_range_min, int key_length_range_max, 
    int produce_cmd_types, unsigned int produce_threads_count)
{
    int j;
    
    key_length_min = key_length_range_min;
    key_length_max = key_length_range_max;
    if (key_length_max <= key_length_min) return VRT_ERROR;
    key_length_range_gap = key_length_max-key_length_min;
    field_length_max = 128;
    string_length_max = 128;
    cmd_type = produce_cmd_types;
    darray_init(&needed_cmd_type_producer, 100, sizeof(data_producer*));

    producers_count = sizeof(redis_data_producer_table)/sizeof(data_producer);
    for (j = 0; j < producers_count; j++) {
        data_producer *dp = redis_data_producer_table+j;
        char *f = dp->sflags;

        while(*f != '\0') {
            switch(*f) {
            case 'w': dp->flags |= PRO_WRITE; break;
            case 'r': dp->flags |= PRO_READONLY; break;
            case 'm': dp->flags |= PRO_DENYOOM; break;
            case 'a': dp->flags |= PRO_ADMIN; break;
            case 'p': dp->flags |= PRO_PUBSUB; break;
            case 's': dp->flags |= PRO_NOSCRIPT; break;
            case 'R': dp->flags |= PRO_RANDOM; break;
            case 'S': dp->flags |= PRO_SORT_FOR_SCRIPT; break;
            case 'l': dp->flags |= PRO_LOADING; break;
            case 't': dp->flags |= PRO_STALE; break;
            case 'M': dp->flags |= PRO_SKIP_MONITOR; break;
            case 'k': dp->flags |= PRO_ASKING; break;
            case 'F': dp->flags |= PRO_FAST; break;
            default: return VRT_ERROR;
            }
            f++;
        }

        if (dp->cmd_type&cmd_type) {
            data_producer **dp_elem = darray_push(
                &needed_cmd_type_producer);
            *dp_elem = dp;
            needed_cmd_type_producer_count ++;
        }
    }

    if (needed_cmd_type_producer_count == 0) {
        return VRT_ERROR;
    }

    vrt_produce_threads_init(produce_threads_count);
    
    return VRT_OK;
}

void vrt_produce_data_deinit(void)
{
    vrt_produce_threads_deinit();

    needed_cmd_type_producer.nelem = 0;
    darray_deinit(&needed_cmd_type_producer);
}

int vrt_start_produce_data(void)
{
    unsigned int i;
    for (i = 0; i < darray_n(&produce_threads); i ++) {
        pthread_attr_t attr;
        produce_thread *pt;
        pthread_attr_init(&attr);
        pt = darray_get(&produce_threads, i);
        pthread_create(&pt->thread_id, 
            &attr, vrt_produce_thread_run, pt);
    }
    
    return VRT_OK;
}

int vrt_wait_produce_data(void)
{
    unsigned int i;
    /* wait for the produce threads finish */
	for(i = 0; i < darray_n(&produce_threads); i ++){
		produce_thread *pt = darray_get(&produce_threads, i);
		pthread_join(pt->thread_id, NULL);
	}
    
    return VRT_OK;
}

/* -----------------------------------------------------------------------------
 * API to get key arguments from data producers
 * ---------------------------------------------------------------------------*/

/* The base case is to use the keys position as given in the data producer table
 * (firstkey, lastkey, step). */
static int *get_keys_using_data_producer_table(data_producer *dp,sds **argv, int argc, int *numkeys) {
    int j, i = 0, last, *keys;

    if (dp->firstkey == 0) {
        *numkeys = 0;
        return NULL;
    }
    last = dp->lastkey;
    if (last < 0) last = argc+last;
    keys = malloc(sizeof(int)*((last - dp->firstkey)+1));
    for (j = dp->firstkey; j <= last; j += dp->keystep) {
        keys[i++] = j;
    }
    *numkeys = i;
    return keys;
}

/* Return all the arguments that are keys in the command passed via argc / argv.
 *
 * The command returns the positions of all the key arguments inside the array,
 * so the actual return value is an heap allocated array of integers. The
 * length of the array is returned by reference into *numkeys.
 *
 * 'cmd' must be point to the corresponding entry into the redisCommand
 * table, according to the command name in argv[0].
 *
 * This function uses the command table if a command-specific helper function
 * is not required, otherwise it calls the command-specific function. */
int *get_keys_from_data_producer(data_producer *dp, sds *argv, int argc, int *numkeys) {
    if (dp->getkeys_proc) {
        return dp->getkeys_proc(dp,argv,argc,numkeys);
    } else {
        return get_keys_using_data_producer_table(dp,argv,argc,numkeys);
    }
}
