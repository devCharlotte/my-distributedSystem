#include <stdlib.h>
#include <string.h>

#include "store.h"
#include "misc.h"

static size_t slot_of(const char *key, size_t nslots)
{
	return hash_bytes(key, strlen(key)) & (nslots - 1);
}

struct store *store_new(size_t hint)
{
	struct store *st = xcalloc(1, sizeof(*st));
	size_t n = 64;

	while (n < hint)
		n <<= 1;
	st->slots = xcalloc(n, sizeof(*st->slots));
	st->nslots = n;
	pthread_rwlock_init(&st->lock, NULL);
	return st;
}

static void drop_all(struct store *st)
{
	for (size_t i = 0; i < st->nslots; i++) {
		struct store_entry *e = st->slots[i];

		while (e) {
			struct store_entry *next = e->next;

			free(e->key);
			free(e->val);
			free(e);
			e = next;
		}
		st->slots[i] = NULL;
	}
	st->count = 0;
}

void store_clear(struct store *st)
{
	pthread_rwlock_wrlock(&st->lock);
	drop_all(st);
	pthread_rwlock_unlock(&st->lock);
}

void store_free(struct store *st)
{
	if (!st)
		return;
	drop_all(st);
	free(st->slots);
	pthread_rwlock_destroy(&st->lock);
	free(st);
}

static struct store_entry *find(struct store *st, const char *key, struct store_entry ***link)
{
	struct store_entry **pp = &st->slots[slot_of(key, st->nslots)];

	for (; *pp; pp = &(*pp)->next) {
		if (!strcmp((*pp)->key, key)) {
			if (link)
				*link = pp;
			return *pp;
		}
	}
	if (link)
		*link = pp;
	return NULL;
}

static void grow(struct store *st)
{
	size_t n = st->nslots * 2;
	struct store_entry **slots = calloc(n, sizeof(*slots));

	if (!slots)
		return;
	for (size_t i = 0; i < st->nslots; i++) {
		struct store_entry *e = st->slots[i];

		while (e) {
			struct store_entry *next = e->next;
			size_t s = slot_of(e->key, n);

			e->next = slots[s];
			slots[s] = e;
			e = next;
		}
	}
	free(st->slots);
	st->slots = slots;
	st->nslots = n;
}

int store_get(struct store *st, const char *key, char *out, size_t cap, size_t *len)
{
	struct store_entry *e;
	int ret = -1;

	pthread_rwlock_rdlock(&st->lock);
	e = find(st, key, NULL);
	if (e && e->vlen < cap) {
		memcpy(out, e->val, e->vlen);
		out[e->vlen] = '\0';
		*len = e->vlen;
		ret = 0;
	}
	pthread_rwlock_unlock(&st->lock);
	return ret;
}

int store_put(struct store *st, const char *key, const char *val, size_t vlen)
{
	struct store_entry **link, *e;
	char *copy = malloc(vlen + 1);

	if (!copy)
		return -1;
	memcpy(copy, val, vlen);
	copy[vlen] = '\0';

	pthread_rwlock_wrlock(&st->lock);
	e = find(st, key, &link);
	if (e) {
		free(e->val);
		e->val = copy;
		e->vlen = vlen;
		pthread_rwlock_unlock(&st->lock);
		return 0;
	}
	e = malloc(sizeof(*e));
	if (!e || !(e->key = strdup(key))) {
		pthread_rwlock_unlock(&st->lock);
		free(e);
		free(copy);
		return -1;
	}
	e->val = copy;
	e->vlen = vlen;
	e->next = NULL;
	*link = e;
	if (++st->count > st->nslots)
		grow(st);
	pthread_rwlock_unlock(&st->lock);
	return 0;
}

int store_del(struct store *st, const char *key)
{
	struct store_entry **link, *e;

	pthread_rwlock_wrlock(&st->lock);
	e = find(st, key, &link);
	if (e) {
		*link = e->next;
		st->count--;
	}
	pthread_rwlock_unlock(&st->lock);
	if (!e)
		return -1;
	free(e->key);
	free(e->val);
	free(e);
	return 0;
}

size_t store_count(struct store *st)
{
	size_t n;

	pthread_rwlock_rdlock(&st->lock);
	n = st->count;
	pthread_rwlock_unlock(&st->lock);
	return n;
}

void store_each(struct store *st, void (*fn)(const char *key, const char *val, size_t vlen, void *arg),
	     void *arg)
{
	pthread_rwlock_rdlock(&st->lock);
	for (size_t i = 0; i < st->nslots; i++)
		for (struct store_entry *e = st->slots[i]; e; e = e->next)
			fn(e->key, e->val, e->vlen, arg);
	pthread_rwlock_unlock(&st->lock);
}
