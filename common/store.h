#ifndef STORE_H
#define STORE_H

#include <pthread.h>
#include <stddef.h>

struct store_entry {
	struct store_entry *next;
	char *key;
	char *val;
	size_t vlen;
};

struct store {
	struct store_entry **slots;
	size_t nslots;
	size_t count;
	pthread_rwlock_t lock;
};

struct store *store_new(size_t hint);
void store_free(struct store *st);
void store_clear(struct store *st);
int store_get(struct store *st, const char *key, char *out, size_t cap, size_t *len);
int store_put(struct store *st, const char *key, const char *val, size_t vlen);
int store_del(struct store *st, const char *key);
size_t store_count(struct store *st);
void store_each(struct store *st, void (*fn)(const char *key, const char *val, size_t vlen, void *arg),
	     void *arg);

#endif
