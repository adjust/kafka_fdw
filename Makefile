
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


PLATFORM 	 = $(shell uname -s)

ifndef NOINIT
REGRESS_PREP = prep_kafka
endif

ifdef DEBUG
PG_CPPFLAGS+= -DDO_DEBUG
endif

ifeq ($(PLATFORM),Darwin)
PG_CPPFLAGS += -I/opt/homebrew/include
SHLIB_LINK += -lrdkafka -lz -lpthread -L/opt/homebrew/opt/librdkafka/lib -lrdkafka
PG_LIBS += -lrdkafka -lz -lpthread -L/opt/homebrew/opt/librdkafka/lib -lrdkafka
else
SHLIB_LINK += -lrdkafka -lz -lpthread -lrt
PG_LIBS += -lrdkafka -lz -lpthread -lrt
endif

ifeq ($(shell test $(VERSION_NUM) -lt 100000; echo $$?),0)
REGRESS := $(filter-out parallel, $(REGRESS))
endif

ifdef TEST
REGRESS = $(TEST)
endif

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

all: $(EXTENSION)--$(EXTVERSION).sql

$(EXTENSION)--$(EXTVERSION).sql: sql/$(EXTENSION).sql
	cp $< $@

prep_kafka:
	./test/init_kafka.sh

.PHONY:	prep_kafka
