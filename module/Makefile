# find the OS
uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')

# Compile flags for linux / osx
ifeq ($(uname_S),Linux)
	SHOBJ_CFLAGS ?= -D_POSIX_SOURCE -D_BSD_SOURCE -W -Wall -fno-common -g -ggdb -std=c99 -O2
	SHOBJ_LDFLAGS ?= -shared
else
	SHOBJ_CFLAGS ?= -W -Wall -dynamic -fno-common -g -ggdb -std=c99 -O2
	SHOBJ_LDFLAGS ?= -bundle -undefined dynamic_lookup
endif

.SUFFIXES: .c .so .xo .o

all: rpm.so

.c.xo:
	$(CC) -DREDISMODULE_EXPERIMENTAL_API -I. $(CFLAGS) $(SHOBJ_CFLAGS) -fPIC -c $< -o $@

RPM_OBJECTS = rpm.xo rpm_allocator.xo rpm_chained_buffer.xo rpm_hashmap.xo rpm_redis.xo

rpm.xo: redismodule.h

rpm.so: $(RPM_OBJECTS)
	$(LD) -o $@ $(RPM_OBJECTS) $(SHOBJ_LDFLAGS) $(LIBS) -lc

clean:
	rm -rf *.xo *.so
