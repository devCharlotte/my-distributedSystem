#ifndef MISC_H
#define MISC_H

#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>

extern atomic_int stop;

void on_signals(void);
void die(const char *fmt, ...) __attribute__((format(printf, 1, 2), noreturn));
void *xmalloc(size_t n);
void *xcalloc(size_t n, size_t sz);
char *xstrdup(const char *s);
long parse_num(const char *s, long lo, long hi, const char *what);
uint64_t now_ns(void);
uint64_t now_ms(void);
void sleep_ms(int ms);

struct rng {
	uint64_t s;
};

void rng_seed(struct rng *r, uint64_t seed);
uint64_t rng_next(struct rng *r);
uint32_t rng_below(struct rng *r, uint32_t n);
double rng_unit(struct rng *r);

struct zipf {
	double *cdf;
	uint32_t n;
};

int zipf_init(struct zipf *z, uint32_t n, double theta);
uint32_t zipf_next(struct zipf *z, struct rng *r);
void zipf_free(struct zipf *z);

void make_key(char *buf, size_t cap, uint64_t i);
uint64_t hash_bytes(const void *data, size_t len);

int cmp_u64(const void *a, const void *b);
uint64_t pct(const uint64_t *sorted, size_t n, double p);

#endif
