UNAME := $(shell uname)
PROG=json_formatter.so

ifeq ($(UNAME), Darwin)
    CC=cc
    ARCHFLAGS=-m32
    %.so : CFLAGS=-Wall -bundle -flat_namespace -undefined suppress
endif

ifeq ($(UNAME), Linux)
    CC=cc
    %.so : CFLAGS=-Wall -shared
endif

LD = -L$(shell pg_config --libdir) -L$(shell pg_config --pkglibdir) -ljansson #-Wl,-v
PGINC = $(shell pg_config --includedir)
INCLUDEDIRS = -I$(PGINC) -I$(PGINC)/postgresql/internal -I$(PGINC)/postgresql/server -I$(PGINC)/jansson

lib/%.o : CFLAGS=-fpic -Wall $(INCLUDEDIRS) 

all: lib/$(PROG)

lib/$(PROG): lib/$(PROG:%.so=%.o)
	$(CC) $(LD) $(CFLAGS) $(ARCHFLAGS) -o $@ $<

lib/$(PROG:%.so=%.o): src/$(PROG:%.so=%.c)
	$(CC) $(CFLAGS) $(ARCHFLAGS) -c $< -o $@

clean:
	rm -rf lib/*.so
	rm -rf lib/*.o

install:
	test -f lib/$(PROG)
	cp lib/$(PROG) $(GPHOME)/lib/postgresql
	psql -f sql/install.sql

.PHONY: test
test:
	roundup test/test.sh
