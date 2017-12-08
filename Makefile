# contrib/cbtree/Makefile

MODULE_big = cbtree
OBJS =  cbtbuild.o cbtinsert.o cbtree.o cbtsearch.o cbtvacuum.o cbtcost.o $(WIN32RES)

EXTENSION = cbtree
DATA = cbtree--1.0.sql
PGFILEDESC = "counted btree access method"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/cbtree
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

wal-check: temp-install
	$(prove_check)
