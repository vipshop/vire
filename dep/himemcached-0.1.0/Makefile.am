MAINTAINERCLEANFILES = Makefile.in

AM_CFLAGS = -Wall -Wshadow
AM_CFLAGS += -Wno-unused-parameter -Wno-unused-value

noinst_LIBRARIES = libhimemcached.a

noinst_HEADERS = himemcached.h himcread.h himcdep/sds.h

libhimemcached_a_SOURCES =          \
    himcdep/sds.c himcdep/sds.h     \
    himcread.c himcread.h           \
	himemcached.c himemcached.h