MAINTAINERCLEANFILES = Makefile.in

AM_CPPFLAGS =
if !OS_SOLARIS
AM_CPPFLAGS += -D_GNU_SOURCE
endif
AM_CPPFLAGS += -I $(top_srcdir)/dep/ae
AM_CPPFLAGS += -I $(top_srcdir)/dep/jemalloc-4.2.0/include
AM_CPPFLAGS += -I $(top_srcdir)/dep/hiredis-0.13.3
AM_CPPFLAGS += -I $(top_srcdir)/dep/dhashkit
AM_CPPFLAGS += -I $(top_srcdir)/dep/dlist
AM_CPPFLAGS += -I $(top_srcdir)/dep/darray
AM_CPPFLAGS += -I $(top_srcdir)/dep/util
AM_CPPFLAGS += -I $(top_srcdir)/dep/himemcached-0.1.0

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

noinst_PROGRAMS = viretest

viretest_SOURCES =                      \
    vrt_util.c vrt_util.h               \
    vrt_public.c vrt_public.h           \
    vrt_simple.c vrt_simple.h           \
    vrtest.c
    
viretest_LDADD = $(top_builddir)/dep/ae/libae.a
viretest_LDADD += $(top_builddir)/dep/hiredis-0.13.3/libhiredis.a
viretest_LDADD += $(top_builddir)/dep/darray/libdarray.a
viretest_LDADD += $(top_builddir)/dep/dmalloc/libdmalloc.a
viretest_LDADD += $(top_builddir)/dep/util/libdutil.a
viretest_LDADD += $(top_builddir)/dep/jemalloc-4.2.0/lib/libjemalloc.a

noinst_PROGRAMS += vireabtest

vireabtest_SOURCES =                        \
    vrt_util.c vrt_util.h                   \
    vrt_public.c vrt_public.h               \
    vrt_produce_data.c vrt_produce_data.h   \
    vrt_dispatch_data.c vrt_dispatch_data.h \
    vrt_check_data.c vrt_check_data.h       \
    vrt_backend.c vrt_backend.h             \
    vrabtest.c vrabtest.h
    
vireabtest_LDADD = $(top_builddir)/dep/ae/libae.a
vireabtest_LDADD += $(top_builddir)/dep/hiredis-0.13.3/libhiredis.a
vireabtest_LDADD += $(top_builddir)/dep/dhashkit/libdhashkit.a
vireabtest_LDADD += $(top_builddir)/dep/dlist/libdlist.a
vireabtest_LDADD += $(top_builddir)/dep/darray/libdarray.a
vireabtest_LDADD += $(top_builddir)/dep/dmalloc/libdmalloc.a
vireabtest_LDADD += $(top_builddir)/dep/util/libdutil.a
vireabtest_LDADD += $(top_builddir)/dep/jemalloc-4.2.0/lib/libjemalloc.a

noinst_PROGRAMS += vire-benchmark

vire_benchmark_SOURCES =                    \
    vrt_util.c vrt_util.h                   \
    vrt_public.c vrt_public.h               \
    vrt_benchmark.c
    
vire_benchmark_LDADD = $(top_builddir)/dep/ae/libae.a
vire_benchmark_LDADD += $(top_builddir)/dep/hiredis-0.13.3/libhiredis.a
vire_benchmark_LDADD += $(top_builddir)/dep/himemcached-0.1.0/libhimemcached.a
vire_benchmark_LDADD += $(top_builddir)/dep/dlist/libdlist.a
vire_benchmark_LDADD += $(top_builddir)/dep/darray/libdarray.a
vire_benchmark_LDADD += $(top_builddir)/dep/dmalloc/libdmalloc.a
vire_benchmark_LDADD += $(top_builddir)/dep/util/libdutil.a
vire_benchmark_LDADD += $(top_builddir)/dep/jemalloc-4.2.0/lib/libjemalloc.a