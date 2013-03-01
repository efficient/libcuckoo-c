noinst_PROGRAMS = test_cuckoo test_cuckoo_mt

test_cuckoo_SOURCES = test_cuckoo.c
test_cuckoo_LDFLAGS = -L../lib -L$(top_srcdir)/lib -lcuckoo
test_cuckoo_DEPENDENCIES = $(top_srcdir)/lib/libcuckoo.la

test_cuckoo_mt_SOURCES = test_cuckoo_mt.c
test_cuckoo_mt_LDFLAGS = -L../lib -L$(top_srcdir)/lib -lcuckoo
test_cuckoo_mt_DEPENDENCIES = $(top_srcdir)/lib/libcuckoo.la

AM_CFLAGS = -Wall
AM_CPPFLAGS = -I../lib -I$(top_srcdir)/lib
