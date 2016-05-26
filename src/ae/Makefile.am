MAINTAINERCLEANFILES = Makefile.in

AM_CPPFLAGS = -I $(top_srcdir)/src
AM_CPPFLAGS += -I $(top_srcdir)/src/hashkit
AM_CPPFLAGS += -I $(top_srcdir)/dep/jemalloc-4.0.4/include

AM_CFLAGS = -Wall -Wshadow
AM_CFLAGS += -Wno-unused-parameter -Wno-unused-value

noinst_LIBRARIES = libae.a

noinst_HEADERS = ae.h

libae_a_SOURCES =	\
	ae.c ae.h