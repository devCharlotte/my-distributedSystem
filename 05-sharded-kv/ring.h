#ifndef RING_H
#define RING_H

#include <netinet/in.h>
#include <stdint.h>

struct ring_point {
	uint64_t hash;
	int shard;
};

struct ring {
	struct ring_point *points;
	int npoints;
	int nshards;
	int use_ring;
};

void ring_build(struct ring *r, const struct sockaddr_in *shards, int nshards, int vnodes, int use_ring);
int ring_owner(const struct ring *r, const char *key);
void ring_free(struct ring *r);

#endif
