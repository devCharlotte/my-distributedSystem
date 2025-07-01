#ifndef LRU_H
#define LRU_H

#include <pthread.h>
#include <stddef.h>
#include <stdint.h>

struct lru_node {
	struct lru_node *prev, *next, *hnext;
	char *key;
	char *val;
	size_t vlen;
	uint64_t lease;
};

struct lru {
	struct lru_node **slots;
	size_t nslots;
	struct lru_node head;
	size_t size, cap;
	uint64_t next_lease;
	unsigned long hits, misses, evictions, fills, rejected;
	pthread_mutex_t lock;
};

enum { LRU_HIT, LRU_MISS };

struct lru *lru_new(size_t cap);
void lru_free(struct lru *c);
int lru_get(struct lru *c, const char *key, char *out, size_t cap, size_t *len, uint64_t *lease);
int lru_fill(struct lru *c, const char *key, const char *val, size_t vlen, uint64_t lease);
void lru_invalidate(struct lru *c, const char *key);
size_t lru_stats(struct lru *c, char *buf, size_t cap);

#endif
