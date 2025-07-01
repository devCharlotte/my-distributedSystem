#include <math.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "net.h"
#include "ring.h"
#include "misc.h"

struct worker {
	int idx;
	pthread_t tid;
	long reads, writes, wrong, failures;
};

static struct sockaddr_in *shards;
static int nshards, vnodes = 128, use_ring = 1;
static struct ring ring;
static int nthreads = 4, nkeys = 20000, read_pct = 50;
static long nops = 10000;
static uint32_t *version;

static int call(int fd, int s, int op, const char *key, const char *val, uint32_t id, struct msg *rep)
{
	struct msg req;

	msg_init(&req, op, id);
	if (key && msg_set_key(&req, key) < 0)
		die("key must be 1..%d bytes", MAX_KEY);
	if (val && msg_set_val(&req, val, strlen(val)) < 0)
		die("value must be at most %d bytes", MAX_VAL);
	return udp_call(fd, &shards[s], &req, rep, 300, 5);
}

static void *run(void *arg)
{
	struct worker *w = arg;
	struct msg rep;
	struct rng r;
	char key[MAX_KEY + 1], val[MAX_VAL + 1];
	uint32_t id = (uint32_t)w->idx << 24;
	uint32_t mine = (nkeys - w->idx + nthreads - 1) / nthreads;
	int fd = udp_open(0);

	if (fd < 0)
		die("socket failed");
	rng_seed(&r, now_ns() * (w->idx + 11));
	for (long i = 0; i < nops && !stop; i++) {
		uint32_t k = rng_below(&r, mine) * nthreads + w->idx;
		int s;

		make_key(key, sizeof(key), k);
		s = ring_owner(&ring, key);
		if ((int)rng_below(&r, 100) < read_pct) {
			w->reads++;
			if (call(fd, s, OP_GET, key, NULL, ++id, &rep) < 0) {
				w->failures++;
				continue;
			}
			snprintf(val, sizeof(val), "%s:%u", key, version[k]);
			if (version[k] ? rep.status != ST_OK || strcmp(rep.val, val) : rep.status != ST_NOT_FOUND)
				w->wrong++;
		} else {
			uint32_t v = version[k] + 1;

			w->writes++;
			snprintf(val, sizeof(val), "%s:%u", key, v);
			if (call(fd, s, OP_PUT, key, val, ++id, &rep) < 0 || rep.status != ST_OK) {
				w->failures++;
				continue;
			}
			version[k] = v;
		}
	}
	close(fd);
	return NULL;
}

static long misplaced(void)
{
	struct msg rep;
	char key[MAX_KEY + 1];
	long bad = 0;
	uint32_t id = 0xff000000u;
	int fd = udp_open(0);

	for (int k = 0; k < nkeys; k += 7) {
		int owner;

		make_key(key, sizeof(key), k);
		owner = ring_owner(&ring, key);
		for (int s = 0; s < nshards; s++) {
			if (s == owner)
				continue;
			if (call(fd, s, OP_GET, key, NULL, ++id, &rep) < 0 || rep.status != ST_NOT_FOUND)
				bad++;
		}
	}
	close(fd);
	return bad;
}

static void shard_stats(void)
{
	struct msg rep;
	char buf[64];
	int fd = udp_open(0);

	for (int s = 0; s < nshards; s++) {
		if (call(fd, s, OP_STATS, NULL, NULL, 0xfe000000u + s, &rep) < 0)
			printf("  %-21s down\n", addr_str(&shards[s], buf, sizeof(buf)));
		else
			printf("  %-21s %s\n", addr_str(&shards[s], buf, sizeof(buf)), rep.val);
	}
	close(fd);
}

static int workload(void)
{
	struct worker *ws = xcalloc(nthreads, sizeof(*ws));
	long reads = 0, writes = 0, wrong = 0, failures = 0, bad;
	uint64_t t0 = now_ns(), elapsed;

	version = xcalloc(nkeys, sizeof(*version));
	for (int i = 0; i < nthreads; i++) {
		ws[i].idx = i;
		if (pthread_create(&ws[i].tid, NULL, run, &ws[i]))
			die("pthread_create failed");
	}
	for (int i = 0; i < nthreads; i++) {
		pthread_join(ws[i].tid, NULL);
		reads += ws[i].reads;
		writes += ws[i].writes;
		wrong += ws[i].wrong;
		failures += ws[i].failures;
	}
	elapsed = now_ns() - t0;
	bad = misplaced();

	printf("routing=%s shards=%d threads=%d reads=%ld writes=%ld\n", use_ring ? "ring" : "mod", nshards,
	       nthreads, reads, writes);
	printf("throughput %.0f ops/s\n", (reads + writes) / (elapsed / 1e9));
	printf("wrong_reads=%ld failures=%ld misplaced=%ld\n", wrong, failures, bad);
	shard_stats();
	free(ws);
	free(version);
	return wrong || failures || bad ? 1 : 0;
}

static double moved_fraction(int from, int to, int ring_mode, long keys)
{
	struct sockaddr_in *fake = xcalloc(to, sizeof(*fake));
	struct ring a, b;
	char key[MAX_KEY + 1];
	long moved = 0;

	for (int i = 0; i < to; i++) {
		fake[i].sin_family = AF_INET;
		fake[i].sin_addr.s_addr = htonl(0x0a000001 + i);
		fake[i].sin_port = htons(7000);
	}
	ring_build(&a, fake, from, vnodes, ring_mode);
	ring_build(&b, fake, to, vnodes, ring_mode);
	for (long k = 0; k < keys; k++) {
		make_key(key, sizeof(key), k);
		if (ring_owner(&a, key) != ring_owner(&b, key))
			moved++;
	}
	ring_free(&a);
	ring_free(&b);
	free(fake);
	return (double)moved / keys;
}

static double spread(int n, int ring_mode, long keys)
{
	struct sockaddr_in *fake = xcalloc(n, sizeof(*fake));
	long *count = xcalloc(n, sizeof(long));
	struct ring r;
	char key[MAX_KEY + 1];
	double mean = (double)keys / n, var = 0;

	for (int i = 0; i < n; i++) {
		fake[i].sin_family = AF_INET;
		fake[i].sin_addr.s_addr = htonl(0x0a000001 + i);
		fake[i].sin_port = htons(7000);
	}
	ring_build(&r, fake, n, vnodes, ring_mode);
	for (long k = 0; k < keys; k++) {
		make_key(key, sizeof(key), k);
		count[ring_owner(&r, key)]++;
	}
	for (int i = 0; i < n; i++)
		var += (count[i] - mean) * (count[i] - mean);
	ring_free(&r);
	free(fake);
	free(count);
	return sqrt(var / n) / mean;
}

static void analyze(int n, long keys)
{
	printf("keys=%ld vnodes=%d\n", keys, vnodes);
	printf("shards   moved when adding one   load stddev\n");
	printf("         mod      ring           mod      ring\n");
	for (int s = 2; s <= n; s++)
		printf("%-6d %6.1f%%   %6.1f%%        %5.1f%%   %5.1f%%\n", s, 100 * moved_fraction(s - 1, s, 0, keys),
		       100 * moved_fraction(s - 1, s, 1, keys), 100 * spread(s, 0, keys), 100 * spread(s, 1, keys));
}

static void usage(const char *prog)
{
	fprintf(stderr,
		"usage: %s -s host:port,... [-m ring|mod] [-v vnodes] get KEY | put KEY VALUE | del KEY | where KEY\n"
		"       %s -s host:port,... [-m ring|mod] [-t threads] [-n ops] [-k keys] [-r read_pct] run\n"
		"       %s [-v vnodes] analyze MAX_SHARDS KEYS\n",
		prog, prog, prog);
	exit(2);
}

int main(int argc, char **argv)
{
	struct msg rep;
	const char *cmd;
	int opt, rc = 0, fd;

	while ((opt = getopt(argc, argv, "s:m:v:t:n:k:r:")) != -1) {
		switch (opt) {
		case 's':
			nshards = parse_addr_list(optarg, &shards);
			if (nshards <= 0)
				die("bad shard list: %s", optarg);
			break;
		case 'm':
			if (!strcmp(optarg, "ring"))
				use_ring = 1;
			else if (!strcmp(optarg, "mod"))
				use_ring = 0;
			else
				die("routing must be ring or mod");
			break;
		case 'v': vnodes = parse_num(optarg, 1, 10000, "vnodes"); break;
		case 't': nthreads = parse_num(optarg, 1, 128, "threads"); break;
		case 'n': nops = parse_num(optarg, 1, 100000000, "ops"); break;
		case 'k': nkeys = parse_num(optarg, 1, 10000000, "keys"); break;
		case 'r': read_pct = parse_num(optarg, 0, 100, "read pct"); break;
		default: usage(argv[0]);
		}
	}
	if (optind >= argc)
		usage(argv[0]);
	cmd = argv[optind++];

	if (!strcmp(cmd, "analyze")) {
		if (argc - optind != 2)
			usage(argv[0]);
		analyze(parse_num(argv[optind], 2, 64, "shards"), parse_num(argv[optind + 1], 1, 10000000, "keys"));
		return 0;
	}
	if (nshards <= 0)
		usage(argv[0]);
	if (nkeys < nthreads)
		die("need at least one key per thread");
	on_signals();
	ring_build(&ring, shards, nshards, vnodes, use_ring);

	if (!strcmp(cmd, "run") && optind == argc) {
		rc = workload();
	} else if (!strcmp(cmd, "where") && argc - optind == 1) {
		char buf[64];

		printf("%s\n", addr_str(&shards[ring_owner(&ring, argv[optind])], buf, sizeof(buf)));
	} else if ((!strcmp(cmd, "get") || !strcmp(cmd, "del")) && argc - optind == 1) {
		int op = cmd[0] == 'g' ? OP_GET : OP_DEL;

		fd = udp_open(0);
		if (call(fd, ring_owner(&ring, argv[optind]), op, argv[optind], NULL, getpid(), &rep) < 0)
			die("shard did not answer");
		close(fd);
		printf("%s\n", rep.status == ST_OK && op == OP_GET ? rep.val : status_name(rep.status));
		rc = rep.status == ST_OK ? 0 : 1;
	} else if (!strcmp(cmd, "put") && argc - optind == 2) {
		fd = udp_open(0);
		if (call(fd, ring_owner(&ring, argv[optind]), OP_PUT, argv[optind], argv[optind + 1], getpid(), &rep) < 0)
			die("shard did not answer");
		close(fd);
		printf("%s\n", status_name(rep.status));
		rc = rep.status == ST_OK ? 0 : 1;
	} else {
		usage(argv[0]);
	}
	ring_free(&ring);
	free(shards);
	return rc;
}
