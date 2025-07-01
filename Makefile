CC      ?= cc
MODE    ?= release
OUT     := build/$(MODE)
CFLAGS  := -std=c11 -D_GNU_SOURCE -g -Wall -Wextra -Wshadow -Werror -Icommon -pthread
LDLIBS  := -pthread -lm

ifeq ($(MODE),release)
CFLAGS  += -O2
else ifeq ($(MODE),asan)
CFLAGS  += -O1 -fno-omit-frame-pointer -fsanitize=address,undefined -fno-sanitize-recover=all
LDLIBS  += -fsanitize=address,undefined
else ifeq ($(MODE),tsan)
CFLAGS  += -O1 -fsanitize=thread
LDLIBS  += -fsanitize=thread
else
$(error MODE must be release, asan or tsan)
endif

COMMON  := $(OUT)/common/proto.o $(OUT)/common/net.o $(OUT)/common/misc.o $(OUT)/common/store.o

PROGS   := kvserver kvcli kvbench cacheserver lacclient lbclient pbnode pbclient shardkv

kvserver_SRC := 01-kv-store/kvserver.c
kvcli_SRC    := 01-kv-store/kvcli.c
kvbench_SRC  := 01-kv-store/kvbench.c
cacheserver_SRC := 02-look-aside-cache/cacheserver.c 02-look-aside-cache/lru.c
lacclient_SRC := 02-look-aside-cache/lacclient.c
lbclient_SRC := 03-p2c-load-balancing/lbclient.c
pbnode_SRC   := 04-primary-backup/pbnode.c
pbclient_SRC := 04-primary-backup/pbclient.c
shardkv_SRC  := 05-sharded-kv/shardkv.c 05-sharded-kv/ring.c

BINS    := $(addprefix $(OUT)/bin/,$(PROGS))

all: $(BINS)

define prog
$(OUT)/bin/$(1): $(patsubst %.c,$(OUT)/%.o,$($(1)_SRC)) $(COMMON)
	@mkdir -p $$(@D)
	$$(CC) -o $$@ $$^ $$(LDLIBS)
endef
$(foreach p,$(PROGS),$(eval $(call prog,$(p))))

$(OUT)/%.o: %.c $(wildcard common/*.h */*.h)
	@mkdir -p $(@D)
	$(CC) $(CFLAGS) -c -o $@ $<

asan tsan:
	$(MAKE) MODE=$@

ifeq ($(MODE),tsan)
RUN := setarch $(shell uname -m) -R
endif

test: all
	$(RUN) tests/run.sh $(OUT)/bin

test-asan:
	$(MAKE) MODE=asan test

test-tsan:
	$(MAKE) MODE=tsan test

clean:
	rm -rf build

.PHONY: all asan tsan test test-asan test-tsan clean
