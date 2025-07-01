#include <errno.h>
#include <math.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "misc.h"

atomic_int stop;

static void handle_stop(int sig)
{
	int saved = errno;

	(void)sig;
	atomic_store(&stop, 1);
	errno = saved;
}

void on_signals(void)
{
	struct sigaction sa;

	memset(&sa, 0, sizeof(sa));
	sa.sa_handler = handle_stop;
	sigemptyset(&sa.sa_mask);
	sigaction(SIGINT, &sa, NULL);
	sigaction(SIGTERM, &sa, NULL);
	signal(SIGPIPE, SIG_IGN);
}

void die(const char *fmt, ...)
{
	va_list ap;

	va_start(ap, fmt);
	vfprintf(stderr, fmt, ap);
	va_end(ap);
	fputc('\n', stderr);
	exit(1);
}

void *xmalloc(size_t n)
{
	void *p = malloc(n);

	if (!p)
		die("out of memory");
	return p;
}

void *xcalloc(size_t n, size_t sz)
{
	void *p = calloc(n, sz);

	if (!p)
		die("out of memory");
	return p;
}

char *xstrdup(const char *s)
{
	char *p = strdup(s);

	if (!p)
		die("out of memory");
	return p;
}

long parse_num(const char *s, long lo, long hi, const char *what)
{
	char *end;
	long v;

	errno = 0;
	v = strtol(s, &end, 10);
	if (errno || end == s || *end || v < lo || v > hi)
		die("bad %s: %s (expected %ld..%ld)", what, s, lo, hi);
	return v;
}

uint64_t now_ns(void)
{
	struct timespec ts;

	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (uint64_t)ts.tv_sec * 1000000000ull + ts.tv_nsec;
}

uint64_t now_ms(void)
{
	return now_ns() / 1000000;
}

void sleep_ms(int ms)
{
	struct timespec ts = { ms / 1000, (long)(ms % 1000) * 1000000 };

	while (nanosleep(&ts, &ts) && errno == EINTR && !stop)
		;
}

void rng_seed(struct rng *r, uint64_t seed)
{
	r->s = seed ? seed : 0x9e3779b97f4a7c15ull;
}

uint64_t rng_next(struct rng *r)
{
	uint64_t x = r->s;

	x ^= x >> 12;
	x ^= x << 25;
	x ^= x >> 27;
	r->s = x;
	return x * 0x2545f4914f6cdd1dull;
}

uint32_t rng_below(struct rng *r, uint32_t n)
{
	return (uint32_t)(((rng_next(r) >> 32) * (uint64_t)n) >> 32);
}

double rng_unit(struct rng *r)
{
	return (rng_next(r) >> 11) * (1.0 / 9007199254740992.0);
}

int zipf_init(struct zipf *z, uint32_t n, double theta)
{
	double sum = 0;

	z->n = n;
	z->cdf = malloc(sizeof(double) * n);
	if (!z->cdf)
		return -1;
	for (uint32_t i = 0; i < n; i++) {
		sum += 1.0 / pow(i + 1, theta);
		z->cdf[i] = sum;
	}
	for (uint32_t i = 0; i < n; i++)
		z->cdf[i] /= sum;
	return 0;
}

uint32_t zipf_next(struct zipf *z, struct rng *r)
{
	double u = rng_unit(r);
	uint32_t lo = 0, hi = z->n - 1;

	while (lo < hi) {
		uint32_t mid = lo + (hi - lo) / 2;

		if (z->cdf[mid] < u)
			lo = mid + 1;
		else
			hi = mid;
	}
	return lo;
}

void zipf_free(struct zipf *z)
{
	free(z->cdf);
	z->cdf = NULL;
}

uint64_t hash_bytes(const void *data, size_t len)
{
	const unsigned char *p = data;
	uint64_t h = 0xcbf29ce484222325ull;

	for (size_t i = 0; i < len; i++) {
		h ^= p[i];
		h *= 0x100000001b3ull;
	}
	h ^= h >> 33;
	h *= 0xff51afd7ed558ccdull;
	h ^= h >> 33;
	h *= 0xc4ceb9fe1a85ec53ull;
	h ^= h >> 33;
	return h;
}

void make_key(char *buf, size_t cap, uint64_t i)
{
	uint64_t x = i + 0x9e3779b97f4a7c15ull;

	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ull;
	x = (x ^ (x >> 27)) * 0x94d049bb133111ebull;
	x ^= x >> 31;
	snprintf(buf, cap, "user%016llx", (unsigned long long)x);
}

int cmp_u64(const void *a, const void *b)
{
	uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b;

	return (x > y) - (x < y);
}

uint64_t pct(const uint64_t *sorted, size_t n, double p)
{
	size_t i;

	if (n == 0)
		return 0;
	i = (size_t)(p * (n - 1) + 0.5);
	return sorted[i < n ? i : n - 1];
}
