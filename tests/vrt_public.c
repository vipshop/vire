#include <stdio.h>
#include <stdlib.h>
#include <getopt.h>
#include <fcntl.h>
#include <errno.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/utsname.h>

#include <hiredis.h>

#include <vrt_util.h>
#include <vrt_public.h>

#define VIRE_TEST_CONFIG_DEFAULT_EXECUTE_FILE "src/vire"

static char *execute_file = VIRE_TEST_CONFIG_DEFAULT_EXECUTE_FILE;

static sds workdir = NULL;

static int vireport = 55556; /* The available port for vire to start */

void set_execute_file(char *file)
{
    execute_file = file;
}

static sds vire_conf_create(char *dir, int port)
{
    sds conf_file;
    int fd;
    sds line;
    
    conf_file = sdscatfmt(sdsempty(),"%s\/vire.conf",dir);

    fd = open(conf_file,O_WRONLY|O_CREAT|O_TRUNC,0644);
    if (fd < 0) {
        test_log_error("Open conf file %s failed: %s", conf_file, strerror(errno));
        sdsfree(conf_file);
        return NULL;
    }

    line = sdsempty();

    line = sdscatfmt(line,"port %i\n",port);
    write(fd, line, sdslen(line));

    sdsclear(line);
    line = sdscatfmt(line,"\n");
    write(fd, line, sdslen(line));
    
    close(fd);
    sdsfree(line);
    return conf_file;
}

vire_instance *vire_instance_create(int port)
{
    vire_instance *vi;

    vi = malloc(sizeof(vire_instance));
    vi->host = NULL;
    vi->port = 0;
    vi->dir = NULL;
    vi->conf_file = NULL;
    vi->pid_file = NULL;
    vi->log_file = NULL;
    vi->running = 0;
    vi->pid = -1;
    vi->ctx = NULL;

    vi->host = sdsnew("127.0.0.1");
    vi->port = port;
    vi->dir = sdscatfmt(sdsempty(),"%s\/%i",workdir,port);

    if (mkdir(vi->dir,0755) < 0) {
        vire_instance_destroy(vi);
        return NULL;
    }

    vi->conf_file = vire_conf_create(vi->dir, port);
    if (vi->conf_file == NULL) {
        vire_instance_destroy(vi);
        return NULL;
    }

    vi->pid_file = sdscatfmt(sdsempty(),"%s\/vire.pid",vi->dir);
    vi->log_file = sdscatfmt(sdsempty(),"%s\/vire.log",vi->dir);

    test_log_debug("vire host: %s", vi->host);
    test_log_debug("vire port: %d", vi->port);
    test_log_debug("vire dir: %s", vi->dir);
    test_log_debug("vire conf_file: %s", vi->conf_file);
    test_log_debug("vire pid_file: %s", vi->pid_file);
    test_log_debug("vire log_file: %s", vi->log_file);

    return vi;
}

void vire_instance_destroy(vire_instance *vi)
{
    if (vi->running) {
        vire_server_stop(vi);
    }

    if (vi->dir) {
        destroy_dir(vi->dir);
        sdsfree(vi->dir);
    }

    if (vi->conf_file) {
        sdsfree(vi->conf_file);
    }

    if (vi->pid_file) {
        sdsfree(vi->pid_file);
    }

    if (vi->log_file) {
        sdsfree(vi->log_file);
    }

    if (vi->ctx) {
        redisFree(vi->ctx);        
    }

    if (vi->host) {
        sdsfree(vi->host);
    }

    free(vi);
}

int vire_server_run(vire_instance *vi)
{
    int ret;
    pid_t pid;
    int status;
    struct timeval timeout = { 3, 500000 }; // 3.5 seconds
    
    if ((pid = fork()) < 0) {
        test_log_error("Fork a chind failed: %s", strerror(errno));
        return VRT_ERROR;
    } else if (pid == 0) {
        ret = execl(execute_file,"vire","-c",vi->conf_file,
            "-p",vi->pid_file,"-o",vi->log_file,"-v","8",NULL);
        if (ret < 0) {
            test_log_error("Execl the vire server failed: %s", strerror(errno));
            return VRT_ERROR;
        }
        return;
    }    

    sleep(1);

    ret = waitpid(pid,NULL,WNOHANG);
    if (ret != 0) {
        test_log_debug("Run vire server(port %d) failed",vi->port);
        return VRT_ERROR;
    }

    vi->ctx = redisConnectWithTimeout(vi->host,vi->port,timeout);
    if (vi->ctx == NULL || vi->ctx->err) {
        test_log_error("Connect to %s:%d failed: %s", 
            vi->host, vi->port, vi->ctx?vi->ctx->errstr:"out of memory");
        if (vi->ctx) {
            redisFree(vi->ctx);
            vi->ctx = NULL;
        }
        return VRT_ERROR;
    }

    vi->pid = get_pid_from_reply(vi->ctx,vi->host,vi->port);
    if (vi->pid < 0) {
        test_log_error("Get pid from %s:%d reply error", vi->host, vi->port);
        return VRT_ERROR;
    } else if (vi->pid != pid) {
        test_log_error("Get wrong pid from %s:%d reply", vi->host, vi->port);
        return VRT_ERROR;
    }

    test_log_debug("Run vire server(port %d) success",vi->port);

    vi->running = 1;

    return VRT_OK;
}

void vire_server_stop(vire_instance *vi)
{
    long pid;

    if (!vi->running) return;

    if (vi->pid > 0) {
        pid = vi->pid;
    } else if (vi->pid_file) {
        int fd;
        char pid_str[20];
        size_t nread;
        fd = open(vi->pid_file, O_RDONLY);
        if (fd < 0) {
            test_log_error("Open pid file %s failed", vi->pid_file);
            return;
        }
        nread = read(fd,pid_str,20);
        if (string2l(pid_str,nread,&pid) == 0) {
            test_log_error("Convert pid string %.*s to long failed",nread,pid_str);
            return;
        }
    } else {
        pid = get_pid_from_reply(vi->ctx, vi->host, vi->port);
    }

    if (pid < 0) {
        test_log_error("Get pid failed");
        return;
    }

    kill(pid,9);

    vi->running = 0;
    vi->pid = -1;
    if (vi->ctx) {
        redisFree(vi->ctx);
        vi->ctx = NULL;
    }
}

int create_work_dir(void)
{
    sds dirname;
    dirname = sdscatfmt(sdsempty(), "tmp_test_%I", vrt_usec_now());
    workdir = getAbsolutePath(dirname);
    sdsfree(dirname);

    if (create_dir(workdir) != VRT_OK) {
        test_log_error("Create workdir %s failed",workdir);
        return VRT_ERROR;
    }

    test_log_debug("Create workdir: %s",workdir);
    
    return VRT_OK;
}

int destroy_work_dir(void)
{
    if (workdir == NULL) return VRT_OK;

    if (destroy_dir(workdir) != VRT_OK) {
        test_log_error("Delete the workdir %s failed",workdir);
    } else {
        test_log_debug("Delete the workdir: %s",workdir);
    }
    
    sdsfree(workdir);
    workdir = NULL;
    
    return VRT_OK;
}

static int get_next_port(void)
{
    int port = vireport;
    vireport += 11;

    return port;
}

vire_instance *start_one_vire_instance(void)
{
    int ret;
    int retry = 0;
    vire_instance *vi;
    
    vi = vire_instance_create(get_next_port());
    if (vi == NULL) {
        return NULL;
    }
    
    ret = vire_server_run(vi);
    while (ret != VRT_OK && retry++ < 10) {
        vire_instance_destroy(vi);
        vi = vire_instance_create(get_next_port());
        if (vi == NULL) {
            return NULL;
        }
        ret = vire_server_run(vi);
    }

    if (ret != VRT_OK) {
        vire_instance_destroy(vi);
        return NULL;
    }

    return vi;
}

void show_test_result(int result,char *test_content,char *errmsg)
{
    if (result == VRT_TEST_OK) {
        test_log_out("[\033[32mOK\033[0m]: %s", test_content);
    } else if (result == VRT_TEST_ERR) {
        test_log_out("[\033[31mERR\033[0m]: %s, fail cause: %s", test_content, 
            (errmsg==NULL||strlen(errmsg)==0)?"unknown":errmsg);
    }
}
