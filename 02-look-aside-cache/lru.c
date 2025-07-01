#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "lru.h"
#include "misc.h"

struct lru *lru_new(size_t cap)
{
	struct lru *c = xcalloc(1, sizeof(*c));
	size_t n = 64;

	while (n < cap * 2)
		n <<= 1;
	c->slots = xcalloc(n, sizeof(*c->slots));
	c->nslots = n;
	c->cap = cap;
	c->head.prev = c->head.next = &c->head;
	c->next_lease = 1;
	pthread_mutex_init(&c->lock, NULL);
	return c;
}

static void node_free(struct lru_node *n)
{
	free(n->key);
	free(n->val);
	free(n);
}

void lru_free(struct lru *c)
{
	struct lru_node *n = c->head.next;

	while (n != &c->head) {
		struct lru_node *next = n->next;

		node_free(n);
		n = next;
	}
	free(c->slots);
	pthread_mutex_destroy(&c->lock);
	free(c);
}

static struct lru_node **lookup(struct lru *c, const char *key)
{
	struct lru_node **pp = &c->slots[hash_bytes(key, strlen(key)) & (c->nslots - 1)];

	while (*pp && strcmp((*pp)->key, key))
		pp = &(*pp)->hnext;
	return pp;
}

static void unlink_node(struct lru_node *n)
{
	n->prev->next = n->next;
	n->next->prev = n->prev;
}

static void push_front(struct lru *c, struct lru_node *n)
{
	n->next = c->head.next;
	n->prev = &c->head;
	c->head.next->prev = n;
	c->head.next = n;
}

static void drop(struct lru *c, struct lru_node **link)
{
	struct lru_node *n = *link;

	*link = n->hnext;
	unlink_node(n);
	node_free(n);
	c->size--;
}

static void evict(struct lru *c)
{
	while (c->size > c->cap) {
		struct lru_node *victim = c->head.prev;

		drop(c, lookup(c, victim->key));
		c->evictions++;
	}
}

int lru_get(struct lru *c, const char *key, char *out, size_t cap, size_t *len, uint64_t *lease)
{
	struct lru_node **link, *n;
	int ret = LRU_MISS;

	pthread_mutex_lock(&c->lock);
	link = lookup(c, key);
	n = *link;
	if (n && n->val && n->vlen < cap) {
		memcpy(out, n->val, n->vlen);
		out[n->vlen] = '\0';
		*len = n->vlen;
		unlink_node(n);
		push_front(c, n);
		c->hits++;
		ret = LRU_HIT;
		goto out;
	}
	c->misses++;
	if (!n) {
		n = xcalloc(1, sizeof(*n));
		n->key = xstrdup(key);
		*link = n;
		c->size++;
	} else {
		unlink_node(n);
	}
	push_front(c, n);
	n->lease = c->next_lease++;
	*lease = n->lease;
	evict(c);
out:
	pthread_mutex_unlock(&c->lock);
	return ret;
}

int lru_fill(struct lru *c, const char *key, const char *val, size_t vlen, uint64_t lease)
{
	struct lru_node **link, *n;
	char *copy = xmalloc(vlen + 1);
	int ret = -1;

	memcpy(copy, val, vlen);
	copy[vlen] = '\0';

	pthread_mutex_lock(&c->lock);
	link = lookup(c, key);
	n = *link;
	if (lease && (!n || n->val || n->lease != lease)) {
		c->rejected++;
		goto out;
	}
	if (!n) {
		n = xcalloc(1, sizeof(*n));
		n->key = xstrdup(key);
		*link = n;
		c->size++;
	} else {
		unlink_node(n);
	}
	free(n->val);
	n->val = copy;
	n->vlen = vlen;
	n->lease = 0;
	copy = NULL;
	push_front(c, n);
	c->fills++;
	evict(c);
	ret = 0;
out:
	pthread_mutex_unlock(&c->lock);
	free(copy);
	return ret;
}

void lru_invalidate(struct lru *c, const char *key)
{
	struct lru_node **link;

	pthread_mutex_lock(&c->lock);
	link = lookup(c, key);
	if (*link)
		drop(c, link);
	pthread_mutex_unlock(&c->lock);
}

size_t lru_stats(struct lru *c, char *buf, size_t cap)
{
	int n;

	pthread_mutex_lock(&c->lock);
	n = snprintf(buf, cap, "hits=%lu misses=%lu fills=%lu rejected=%lu evictions=%lu size=%zu cap=%zu",
		     c->hits, c->misses, c->fills, c->rejected, c->evictions, c->size, c->cap);
	pthread_mutex_unlock(&c->lock);
	return n < 0 ? 0 : (size_t)n < cap ? (size_t)n : cap - 1;
}
