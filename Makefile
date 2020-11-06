
EXTENSION    = kafka_fdw
EXTVERSION   = $(shell grep default_version $(EXTENSION).control | sed -e "s/default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/")

DATA 		 = $(filter-out $(EXTENSION)--$(EXTVERSION).sql, $(wildcard *--*.sql)) $(EXTENSION)--$(EXTVERSION).sql
# DOCS         = $(wildcard doc/*.md)
TESTS        = $(wildcard test/sql/*.sql)
REGRESS      ?= $(patsubst test/sql/%.sql,%,$(TESTS))
REGRESS_OPTS = --inputdir=test --load-extension=$(EXTENSION)
EXTRA_CLEAN  = $(EXTENSION)--$(EXTVERSION).sql
MODULE_big   = $(EXTENSION)
OBJS         =  $(patsubst %.c,%.o,$(wildcard src/*.c))
PG_CONFIG   ?= pg_config
PG_CPPFLAGS  = -std=c99 -Wall -Wextra -Wno-unused-parameter

ifndef NOINIT
REGRESS_PREP = prep_kafka
endif

ifdef DEBUG
PG_CPPFLAGS+= -DDO_DEBUG
endif

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

ifeq ($(shell test $(VERSION_NUM) -lt 100000; echo $$?),0)
REGRESS := $(filter-out parallel, $(REGRESS))
endif

ifeq ($(shell test $(VERSION_NUM) -ge 90600; echo $$?),0)
PGOPTIONS+= "--max_parallel_workers_per_gather=0"
endif


PLATFORM 	 = $(shell uname -s)

ifeq ($(PLATFORM),Darwin)
SHLIB_LINK += -lrdkafka -lz -lpthread
PG_LIBS += -lrdkafka -lz -lpthread
else
SHLIB_LINK += -lrdkafka -lz -lpthread -lrt
PG_LIBS += -lrdkafka -lz -lpthread -lrt
endif

ifdef TEST
REGRESS = $(TEST)
endif


all: $(EXTENSION)--$(EXTVERSION).sql

$(EXTENSION)--$(EXTVERSION).sql: sql/$(EXTENSION).sql
	cp $< $@

installcheck: submake $(REGRESS_PREP)
	PGOPTIONS=$(PGOPTIONS) $(pg_regress_installcheck) $(REGRESS_OPTS) $(REGRESS)

prep_kafka:
	./test/init_kafka.sh

.PHONY:	prep_kafka
