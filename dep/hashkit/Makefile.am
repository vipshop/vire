MAINTAINERCLEANFILES = Makefile.in

AM_CFLAGS = -Wall -Wshadow
AM_CFLAGS += -Wno-unused-parameter -Wno-unused-value

noinst_LIBRARIES = libhashkit.a

noinst_HEADERS = vr_hashkit.h

libhashkit_a_SOURCES =	\
	vr_hashkit.h		\
	vr_crc16.c			\
	vr_crc32.c			\
	vr_fnv.c			\
	vr_hsieh.c			\
	vr_jenkins.c		\
	vr_ketama.c			\
	vr_md5.c			\
	vr_modula.c			\
	vr_murmur.c			\
	vr_one_at_a_time.c	\
	vr_random.c			\
	vr_sha1.c