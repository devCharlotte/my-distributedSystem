#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "net.h"
#include "misc.h"

enum policy { RANDOM, ROUND_ROBIN, P2C, P2C_COUNT };

static const char *policy_names[] = { "random", "rr", "p2c", "p2c-count" };

struct worker {
	int idx;
	pthread_t tid;
	uint64_t *lat;
	long done, failures;
};

static struct sockaddr_in *backends;
static int nback;
static enum policy policy = P2C;
static atomic_int *inflight;
static atomic_long *sent;
static atomic_uint rr_next;
static long nops = 2000;

static int choose(struct rng *r)
{
	int a, b;

	switch (policy) {
	case RANDOM:
		return rng_below(r, nback);
	case ROUND_ROBIN:
		return atomic_fetch_add(&rr_next, 1) % nback;
	case P2C:
	case P2C_COUNT:
		break;
	}
	if (nback == 1)
		return 0;
	a = rng_below(r, nback);
	b = rng_below(r, nback - 1);
	if (b >= a)
		b++;
	if (policy == P2C) {
		int la = atomic_load(&inflight[a]), lb = atomic_load(&inflight[b]);

		if (la != lb)
			return la < lb ? a : b;
	}
	return atomic_load(&sent[a]) <= atomic_load(&sent[b]) ? a : b;
}

static void *run(void *arg)
{
	struct worker *w = arg;
	struct msg req, rep;
	struct rng r;
	char key[MAX_KEY + 1];
	uint32_t id = (uint32_t)w->idx << 24;
	int fd = udp_open(0);

	if (fd < 0)
		die("socket failed");
	rng_seed(&r, now_ns() * (w->idx + 3));
	for (long i = 0; i < nops && !stop; i++) {
		int s = choose(&r);
		uint64_t t0;

		make_key(key, sizeof(key), rng_below(&r, 100000));
		msg_init(&req, OP_GET, ++id);
		msg_set_key(&req, key);
		atomic_fetch_add(&inflight[s], 1);
		atomic_fetch_add(&sent[s], 1);
		t0 = now_ns();
		if (udp_call(fd, &backends[s], &req, &rep, 1000, 3) < 0)
			w->failures++;
		else
			w->lat[w->done++] = now_ns() - t0;
		atomic_fetch_sub(&inflight[s], 1);
	}
	close(fd);
	return NULL;
}

static void simulate(long balls, struct rng *r)
{
	long *bins = xcalloc(nback, sizeof(long));

	for (int p = RANDOM; p <= P2C_COUNT; p++) {
		long max = 0;

		if (p == P2C)
			continue;
		memset(bins, 0, sizeof(long) * nback);
		for (long i = 0; i < balls; i++) {
			int a = rng_below(r, nback), b;

			if (p == ROUND_ROBIN) {
				a = i % nback;
			} else if (p == P2C_COUNT && nback > 1) {
				b = rng_below(r, nback - 1);
				if (b >= a)
					b++;
				if (bins[b] < bins[a])
					a = b;
			}
			bins[a]++;
		}
		for (int i = 0; i < nback; i++)
			if (bins[i] > max)
				max = bins[i];
		printf("%-10s bins=%d balls=%ld max=%ld avg=%.1f max-avg=%.1f\n", policy_names[p], nback, balls,
		       max, (double)balls / nback, max - (double)balls / nback);
	}
	free(bins);
}

static void usage(const char *prog)
{
	fprintf(stderr, "usage: %s -b host:port,... [-p random|rr|p2c|p2c-count] [-t threads] [-n ops]\n"
			"       %s -B bins -S balls\n", prog, prog);
	exit(2);
}

int main(int argc, char **argv)
{
	int nthreads = 16, opt, bins = 0;
	long balls = 0, total = 0, failures = 0, at = 0, maxc = 0, minc = -1;
	struct worker *ws;
	uint64_t *all, t0, elapsed;
	struct rng r;

	while ((opt = getopt(argc, argv, "b:p:t:n:B:S:")) != -1) {
		switch (opt) {
		case 'b':
			nback = parse_addr_list(optarg, &backends);
			if (nback <= 0)
				die("bad backend list: %s", optarg);
			break;
		case 'p':
			for (policy = RANDOM; policy <= P2C_COUNT; policy++)
				if (!strcmp(optarg, policy_names[policy]))
					break;
			if (policy > P2C_COUNT)
				die("unknown policy: %s", optarg);
			break;
		case 't': nthreads = parse_num(optarg, 1, 512, "threads"); break;
		case 'n': nops = parse_num(optarg, 1, 100000000, "ops"); break;
		case 'B': bins = parse_num(optarg, 1, 100000, "bins"); break;
		case 'S': balls = parse_num(optarg, 1, 1000000000, "balls"); break;
		default: usage(argv[0]);
		}
	}
	rng_seed(&r, now_ns());
	if (bins && balls) {
		nback = bins;
		simulate(balls, &r);
		return 0;
	}
	if (nback <= 0)
		usage(argv[0]);
	on_signals();

	inflight = xcalloc(nback, sizeof(*inflight));
	sent = xcalloc(nback, sizeof(*sent));
	ws = xcalloc(nthreads, sizeof(*ws));
	t0 = now_ns();
	for (int i = 0; i < nthreads; i++) {
		ws[i].idx = i;
		ws[i].lat = xmalloc(sizeof(uint64_t) * nops);
		if (pthread_create(&ws[i].tid, NULL, run, &ws[i]))
			die("pthread_create failed");
	}
	for (int i = 0; i < nthreads; i++) {
		pthread_join(ws[i].tid, NULL);
		total += ws[i].done;
		failures += ws[i].failures;
	}
	elapsed = now_ns() - t0;

	all = xmalloc(sizeof(uint64_t) * (total ? total : 1));
	for (int i = 0; i < nthreads; i++) {
		memcpy(all + at, ws[i].lat, sizeof(uint64_t) * ws[i].done);
		at += ws[i].done;
		free(ws[i].lat);
	}
	qsort(all, total, sizeof(uint64_t), cmp_u64);

	printf("policy=%s backends=%d threads=%d requests=%ld\n", policy_names[policy], nback, nthreads, total);
	for (int i = 0; i < nback; i++) {
		long c = atomic_load(&sent[i]);
		char buf[64];

		printf("  %-21s %6ld (%.1f%%)\n", addr_str(&backends[i], buf, sizeof(buf)), c,
		       100.0 * c / (total + failures ? total + failures : 1));
		if (c > maxc)
			maxc = c;
		if (minc < 0 || c < minc)
			minc = c;
	}
	printf("max/min %.2f\n", minc > 0 ? (double)maxc / minc : 0.0);
	printf("latency ms  p50=%.2f p99=%.2f max=%.2f\n", pct(all, total, 0.5) / 1e6, pct(all, total, 0.99) / 1e6,
	       total ? all[total - 1] / 1e6 : 0.0);
	printf("throughput %.0f req/s failures=%ld\n", total / (elapsed / 1e9), failures);

	free(all);
	free(ws);
	free(inflight);
	free(sent);
	free(backends);
	return failures ? 1 : 0;
}
