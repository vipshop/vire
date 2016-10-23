MAINTAINERCLEANFILES = Makefile.in

AM_CPPFLAGS =
if !OS_SOLARIS
AM_CPPFLAGS += -D_GNU_SOURCE
endif
AM_CPPFLAGS += -I $(top_srcdir)/dep/util
AM_CPPFLAGS += -I $(top_srcdir)/dep/jemalloc-4.2.0/include
AM_CPPFLAGS += -I $(top_srcdir)/dep/dmalloc

AM_CFLAGS = -Wall -Wshadow
AM_CFLAGS += -Wno-unused-parameter -Wno-unused-value

noinst_LIBRARIES = libdlist.a

noinst_HEADERS = dlist.h dmtqueue.h dlockqueue.h

libdlist_a_SOURCES =	            \
	dlist.c dlist.h                 \
    dmtqueue.c dmtqueue.h           \
    dlockqueue.c dlockqueue.h