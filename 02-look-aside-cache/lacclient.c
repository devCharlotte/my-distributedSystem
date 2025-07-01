#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "net.h"
#include "misc.h"

struct worker {
	int idx;
	pthread_t tid;
	uint32_t next_id;
	long hits, misses, fills, rejected, writes, stale, failures;
	uint64_t hit_ns, miss_ns;
};

static struct sockaddr_in cache_addr, store_addr;
static int nthreads = 4, nkeys = 10000, read_pct = 95, use_lease = 1, shared, fill_delay_us;
static long nops = 20000;
static double theta = 0.99;
static struct zipf zipf;
static uint32_t *version;
static atomic_uint version_seq;

static int call(int fd, const struct sockaddr_in *to, struct worker *w, int op, const char *key,
		const char *val, uint64_t arg, struct msg *rep)
{
	struct msg req;

	msg_init(&req, op, ++w->next_id);
	msg_set_key(&req, key);
	if (val)
		msg_set_val(&req, val, strlen(val));
	req.arg = arg;
	if (udp_call(fd, to, &req, rep, 300, 5) < 0) {
		w->failures++;
		return -1;
	}
	return 0;
}

static uint32_t pick(struct worker *w, struct rng *r)
{
	uint32_t k = zipf_next(&zipf, r);

	if (shared)
		return k;
	k = k - k % nthreads + w->idx;
	return k < (uint32_t)nkeys ? k : (uint32_t)w->idx;
}

static void read_key(int fd, struct worker *w, const char *key, uint32_t k)
{
	struct msg rep;
	char want[MAX_VAL + 1];
	uint64_t t0 = now_ns();

	if (call(fd, &cache_addr, w, OP_GET, key, NULL, 0, &rep) < 0)
		return;
	if (rep.status == ST_OK) {
		w->hits++;
		w->hit_ns += now_ns() - t0;
	} else {
		uint64_t lease = use_lease ? rep.arg : 0;

		w->misses++;
		if (call(fd, &store_addr, w, OP_GET, key, NULL, 0, &rep) < 0)
			return;
		if (rep.status == ST_OK) {
			char val[MAX_VAL + 1];
			struct msg fill;

			memcpy(val, rep.val, rep.vlen + 1);
			if (fill_delay_us)
				usleep(fill_delay_us);
			if (call(fd, &cache_addr, w, OP_PUT, key, val, lease, &fill) == 0) {
				if (fill.status == ST_OK)
					w->fills++;
				else
					w->rejected++;
			}
			memcpy(rep.val, val, strlen(val) + 1);
		}
		w->miss_ns += now_ns() - t0;
	}
	if (!shared) {
		snprintf(want, sizeof(want), "%s:%u", key, version[k]);
		if (strcmp(rep.val, want))
			w->stale++;
	}
}

static void write_key(int fd, struct worker *w, const char *key, uint32_t k)
{
	struct msg rep;
	char val[MAX_VAL + 1];
	uint32_t v = atomic_fetch_add(&version_seq, 1) + 1;

	snprintf(val, sizeof(val), "%s:%u", key, v);
	if (call(fd, &store_addr, w, OP_PUT, key, val, 0, &rep) < 0 || rep.status != ST_OK)
		return;
	if (!shared)
		version[k] = v;
	call(fd, &cache_addr, w, OP_DEL, key, NULL, 0, &rep);
	w->writes++;
}

static void *preload(void *arg)
{
	struct worker *w = arg;
	struct msg rep;
	char key[MAX_KEY + 1], val[MAX_VAL + 1];
	int fd = udp_open(0);

	for (int k = w->idx; k < nkeys && !stop; k += nthreads) {
		make_key(key, sizeof(key), k);
		snprintf(val, sizeof(val), "%s:0", key);
		call(fd, &store_addr, w, OP_PUT, key, val, 0, &rep);
	}
	close(fd);
	return NULL;
}

static void *run(void *arg)
{
	struct worker *w = arg;
	struct rng r;
	char key[MAX_KEY + 1];
	int fd = udp_open(0);

	if (fd < 0)
		die("socket failed");
	rng_seed(&r, now_ns() * (w->idx + 1));
	for (long i = 0; i < nops && !stop; i++) {
		uint32_t k = pick(w, &r);

		make_key(key, sizeof(key), k);
		if ((int)rng_below(&r, 100) < read_pct)
			read_key(fd, w, key, k);
		else
			write_key(fd, w, key, k);
	}
	close(fd);
	return NULL;
}

static long audit(void)
{
	struct worker w = { 0 };
	struct msg c, s;
	char key[MAX_KEY + 1];
	long bad = 0;
	int fd = udp_open(0);

	for (int k = 0; k < nkeys; k++) {
		make_key(key, sizeof(key), k);
		if (call(fd, &cache_addr, &w, OP_GET, key, NULL, 0, &c) < 0 || c.status != ST_OK)
			continue;
		if (call(fd, &store_addr, &w, OP_GET, key, NULL, 0, &s) < 0)
			continue;
		if (s.status != ST_OK || strcmp(c.val, s.val))
			bad++;
	}
	close(fd);
	return bad + w.failures;
}

static void usage(const char *prog)
{
	fprintf(stderr, "usage: %s -c cache -s storage [-t threads] [-n ops] [-k keys] [-z theta] "
			"[-r read_pct] [-S shared keys] [-L no lease] [-D fill_delay_us]\n", prog);
	exit(2);
}

int main(int argc, char **argv)
{
	struct worker *ws, sum = { 0 };
	int opt;
	long bad;
	uint64_t t0, elapsed;

	while ((opt = getopt(argc, argv, "c:s:t:n:k:z:r:SLD:")) != -1) {
		switch (opt) {
		case 'c':
			if (parse_addr(optarg, &cache_addr) < 0)
				die("bad cache address: %s", optarg);
			break;
		case 's':
			if (parse_addr(optarg, &store_addr) < 0)
				die("bad storage address: %s", optarg);
			break;
		case 't': nthreads = parse_num(optarg, 1, 128, "threads"); break;
		case 'n': nops = parse_num(optarg, 1, 100000000, "ops"); break;
		case 'k': nkeys = parse_num(optarg, 1, 10000000, "keys"); break;
		case 'z': theta = atof(optarg); break;
		case 'r': read_pct = parse_num(optarg, 0, 100, "read pct"); break;
		case 'S': shared = 1; break;
		case 'L': use_lease = 0; break;
		case 'D': fill_delay_us = parse_num(optarg, 0, 1000000, "fill delay"); break;
		default: usage(argv[0]);
		}
	}
	if (!cache_addr.sin_port || !store_addr.sin_port || theta < 0 || theta > 3)
		usage(argv[0]);
	if (nkeys < nthreads)
		die("need at least one key per thread");
	on_signals();
	if (zipf_init(&zipf, nkeys, theta) < 0)
		die("out of memory");
	version = xcalloc(nkeys, sizeof(*version));
	ws = xcalloc(nthreads, sizeof(*ws));

	for (int i = 0; i < nthreads; i++) {
		ws[i].idx = i;
		ws[i].next_id = (uint32_t)i << 24;
		pthread_create(&ws[i].tid, NULL, preload, &ws[i]);
	}
	for (int i = 0; i < nthreads; i++)
		pthread_join(ws[i].tid, NULL);

	t0 = now_ns();
	for (int i = 0; i < nthreads; i++)
		pthread_create(&ws[i].tid, NULL, run, &ws[i]);
	for (int i = 0; i < nthreads; i++) {
		pthread_join(ws[i].tid, NULL);
		sum.hits += ws[i].hits;
		sum.misses += ws[i].misses;
		sum.fills += ws[i].fills;
		sum.rejected += ws[i].rejected;
		sum.writes += ws[i].writes;
		sum.stale += ws[i].stale;
		sum.failures += ws[i].failures;
		sum.hit_ns += ws[i].hit_ns;
		sum.miss_ns += ws[i].miss_ns;
	}
	elapsed = now_ns() - t0;
	bad = audit();

	printf("threads=%d ops=%ld keys=%d zipf=%.2f read=%d%% lease=%s keys=%s\n", nthreads,
	       nops * nthreads, nkeys, theta, read_pct, use_lease ? "on" : "off",
	       shared ? "shared" : "per-thread");
	printf("hit ratio %.2f%% (hits=%ld misses=%ld)\n",
	       100.0 * sum.hits / (sum.hits + sum.misses ? sum.hits + sum.misses : 1), sum.hits, sum.misses);
	printf("avg latency us  hit=%.1f miss=%.1f\n", sum.hits ? sum.hit_ns / 1e3 / sum.hits : 0.0,
	       sum.misses ? sum.miss_ns / 1e3 / sum.misses : 0.0);
	printf("writes=%ld fills=%ld rejected_fills=%ld\n", sum.writes, sum.fills, sum.rejected);
	printf("throughput %.0f ops/s\n", nops * nthreads / (elapsed / 1e9));
	printf("stale_reads=%ld stale_entries=%ld failures=%ld\n", sum.stale, bad, sum.failures);

	zipf_free(&zipf);
	free(version);
	free(ws);
	return sum.stale || bad || sum.failures ? 1 : 0;
}
