MAINTAINERCLEANFILES = Makefile.in

AM_CPPFLAGS =
if !OS_SOLARIS
AM_CPPFLAGS += -D_GNU_SOURCE
endif
AM_CPPFLAGS += -I $(top_srcdir)/dep/dhashkit

AM_CFLAGS = -Wall -Wshadow
AM_CFLAGS += -Wno-unused-parameter -Wno-unused-value

noinst_LIBRARIES = libdhashtable.a

noinst_HEADERS = dhashtable.h

libdhashtable_a_SOURCES =	        \
	dhashtable.c dhashtable.h
    
libdhashtable_a_LIBADD = $(top_builddir)/dep/dhashkit/libdhashkit.a