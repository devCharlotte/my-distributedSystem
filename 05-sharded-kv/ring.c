#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "net.h"
#include "ring.h"
#include "misc.h"

static int by_hash(const void *a, const void *b)
{
	const struct ring_point *x = a, *y = b;

	if (x->hash != y->hash)
		return x->hash < y->hash ? -1 : 1;
	return x->shard - y->shard;
}

void ring_build(struct ring *r, const struct sockaddr_in *shards, int nshards, int vnodes, int use_ring)
{
	memset(r, 0, sizeof(*r));
	r->nshards = nshards;
	r->use_ring = use_ring;
	if (!use_ring)
		return;
	r->npoints = nshards * vnodes;
	r->points = xmalloc(sizeof(*r->points) * r->npoints);
	for (int s = 0; s < nshards; s++) {
		char name[64], label[96];

		addr_str(&shards[s], name, sizeof(name));
		for (int v = 0; v < vnodes; v++) {
			int n = snprintf(label, sizeof(label), "%s#%d", name, v);

			r->points[s * vnodes + v].hash = hash_bytes(label, n);
			r->points[s * vnodes + v].shard = s;
		}
	}
	qsort(r->points, r->npoints, sizeof(*r->points), by_hash);
}

int ring_owner(const struct ring *r, const char *key)
{
	uint64_t h = hash_bytes(key, strlen(key));
	int lo = 0, hi = r->npoints;

	if (!r->use_ring)
		return (int)(h % r->nshards);
	while (lo < hi) {
		int mid = lo + (hi - lo) / 2;

		if (r->points[mid].hash < h)
			lo = mid + 1;
		else
			hi = mid;
	}
	return r->points[lo == r->npoints ? 0 : lo].shard;
}

void ring_free(struct ring *r)
{
	free(r->points);
	r->points = NULL;
}
