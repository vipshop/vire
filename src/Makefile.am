MAINTAINERCLEANFILES = Makefile.in

AM_CPPFLAGS =
if !OS_SOLARIS
AM_CPPFLAGS += -D_GNU_SOURCE
endif
AM_CPPFLAGS += -I $(top_srcdir)/dep/dhashkit
AM_CPPFLAGS += -I $(top_srcdir)/dep/ae
AM_CPPFLAGS += -I $(top_srcdir)/dep/util
AM_CPPFLAGS += -I $(top_srcdir)/dep/jemalloc-4.2.0/include
AM_CPPFLAGS += -I $(top_srcdir)/dep/dmalloc
AM_CPPFLAGS += -I $(top_srcdir)/dep/sds
AM_CPPFLAGS += -I $(top_srcdir)/dep/darray
AM_CPPFLAGS += -I $(top_srcdir)/dep/dlist

AM_CFLAGS = 
AM_CFLAGS += -fno-strict-aliasing
AM_CFLAGS += -Wall -Wshadow
AM_CFLAGS += -Wpointer-arith
AM_CFLAGS += -Winline
AM_CFLAGS += -Wunused-function -Wunused-variable -Wunused-value
AM_CFLAGS += -Wno-unused-parameter -Wno-unused-value
AM_CFLAGS += -Wconversion -Wsign-compare
AM_CFLAGS += -Wstrict-prototypes -Wmissing-prototypes -Wredundant-decls -Wmissing-declarations

AM_LDFLAGS =
AM_LDFLAGS += -lm -lpthread -rdynamic
if !OS_DARWIN
AM_LDFLAGS += -lrt
endif
if OS_SOLARIS
AM_LDFLAGS += -lnsl -lsocket
endif
if OS_FREEBSD
AM_LDFLAGS += -lexecinfo
endif

sbin_PROGRAMS = vire

vire_SOURCES =                          \
    vr_aof.c vr_aof.h                   \
    vr_block.c vr_block.h               \
    vr_client.c vr_client.h             \
    vr_command.c vr_command.h           \
    vr_conf.c vr_conf.h                 \
    vr_connection.c vr_connection.h     \
    vr_core.c vr_core.h                 \
    vr_db.c vr_db.h                     \
    vr_dict.c vr_dict.h                 \
    vr_eventloop.c vr_eventloop.h       \
    vr_intset.c vr_intset.h             \
    vr_listen.c vr_listen.h             \
    vr_lzf.h vr_lzfP.h                  \
    vr_lzf_c.c vr_lzf_d.c               \
    vr_master.c vr_master.h             \
    vr_multi.c vr_multi.h               \
    vr_notify.c vr_notify.h             \
    vr_object.c vr_object.h             \
    vr_pubsub.c vr_pubsub.h             \
    vr_quicklist.c vr_quicklist.h       \
    vr_rbtree.c vr_rbtree.h             \
    vr_rdb.c vr_rdb.h                   \
    vr_replication.c vr_replication.h   \
    vr_scripting.c vr_scripting.h       \
    vr_server.c vr_server.h             \
    vr_signal.c vr_signal.h             \
    vr_slowlog.c vr_slowlog.h           \
    vr_specialconfig.h                  \
    vr_stats.c vr_stats.h               \
    vr_thread.c vr_thread.h             \
    vr_t_hash.c vr_t_hash.h             \
    vr_t_list.c vr_t_list.h             \
    vr_t_set.c vr_t_set.h               \
    vr_t_string.c vr_t_string.h         \
    vr_t_zset.c vr_t_zset.h             \
    vr_util.c vr_util.h                 \
    vr_worker.c vr_worker.h             \
    vr_backend.c vr_backend.h           \
    vr_ziplist.c vr_ziplist.h           \
    vr_zipmap.c vr_zipmap.h             \
    vr_bitops.c vr_bitops.h             \
    vr_hyperloglog.c vr_hyperloglog.h   \
    vr.c
    
vire_LDADD = $(top_builddir)/dep/util/libdutil.a
vire_LDADD += $(top_builddir)/dep/ae/libae.a
vire_LDADD += $(top_builddir)/dep/sds/libsds.a
vire_LDADD += $(top_builddir)/dep/darray/libdarray.a
vire_LDADD += $(top_builddir)/dep/dlist/libdlist.a
vire_LDADD += $(top_builddir)/dep/dhashkit/libdhashkit.a
vire_LDADD += $(top_builddir)/dep/jemalloc-4.2.0/lib/libjemalloc.a
vire_LDADD += $(top_builddir)/dep/dmalloc/libdmalloc.a