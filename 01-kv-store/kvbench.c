#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "net.h"
#include "misc.h"

struct job {
	int idx;
	long ops;
	uint64_t *lat;
	long done, errors, timeouts, mismatches;
	pthread_t tid;
};

static struct sockaddr_in server;
static int nkeys = 10000;
static int read_pct = 90;
static int vsize = 32;

static void fill_value(char *buf, int len, const char *key, uint32_t ver)
{
	int n = snprintf(buf, len + 1, "%s:%u:", key, ver);

	for (int i = n; i < len; i++)
		buf[i] = 'a' + (i + ver) % 26;
	buf[len] = '\0';
}

static void *run(void *arg)
{
	struct job *j = arg;
	struct msg req, rep;
	struct rng r;
	char key[MAX_KEY + 1], val[MAX_VAL + 1];
	int fd = udp_open(0);
	uint32_t id = (uint32_t)j->idx << 24;

	if (fd < 0)
		die("socket failed");
	rng_seed(&r, now_ns() + j->idx * 7919);
	for (long i = 0; i < j->ops && !stop; i++) {
		uint32_t k = rng_below(&r, nkeys);
		int is_read = (int)rng_below(&r, 100) < read_pct;
		uint64_t t0;

		make_key(key, sizeof(key), k);
		msg_init(&req, is_read ? OP_GET : OP_PUT, ++id);
		msg_set_key(&req, key);
		if (!is_read) {
			fill_value(val, vsize, key, id);
			msg_set_val(&req, val, vsize);
		}
		t0 = now_ns();
		if (udp_call(fd, &server, &req, &rep, 200, 5) < 0) {
			j->timeouts++;
			continue;
		}
		j->lat[j->done++] = now_ns() - t0;
		if (rep.status != ST_OK && rep.status != ST_NOT_FOUND)
			j->errors++;
		else if (is_read && rep.status == ST_OK && strncmp(rep.val, key, strlen(key)))
			j->mismatches++;
	}
	close(fd);
	return NULL;
}

static void usage(const char *prog)
{
	fprintf(stderr, "usage: %s -s host:port [-c clients] [-n ops_per_client] [-k keys] "
			"[-r read_pct] [-v value_size]\n", prog);
	exit(2);
}

int main(int argc, char **argv)
{
	int nclients = 4, opt;
	long ops = 20000, total = 0, errors = 0, timeouts = 0, mismatches = 0, at = 0;
	struct job *jobs;
	uint64_t *all, t0, elapsed;

	while ((opt = getopt(argc, argv, "s:c:n:k:r:v:")) != -1) {
		switch (opt) {
		case 's':
			if (parse_addr(optarg, &server) < 0)
				die("bad server address: %s", optarg);
			break;
		case 'c': nclients = parse_num(optarg, 1, 256, "clients"); break;
		case 'n': ops = parse_num(optarg, 1, 100000000, "ops"); break;
		case 'k': nkeys = parse_num(optarg, 1, 100000000, "keys"); break;
		case 'r': read_pct = parse_num(optarg, 0, 100, "read pct"); break;
		case 'v': vsize = parse_num(optarg, 24, MAX_VAL, "value size"); break;
		default: usage(argv[0]);
		}
	}
	if (!server.sin_port)
		usage(argv[0]);
	on_signals();

	jobs = xcalloc(nclients, sizeof(*jobs));
	t0 = now_ns();
	for (int i = 0; i < nclients; i++) {
		jobs[i].idx = i;
		jobs[i].ops = ops;
		jobs[i].lat = xmalloc(sizeof(uint64_t) * ops);
		if (pthread_create(&jobs[i].tid, NULL, run, &jobs[i]))
			die("pthread_create failed");
	}
	for (int i = 0; i < nclients; i++) {
		pthread_join(jobs[i].tid, NULL);
		total += jobs[i].done;
		errors += jobs[i].errors;
		timeouts += jobs[i].timeouts;
		mismatches += jobs[i].mismatches;
	}
	elapsed = now_ns() - t0;

	all = xmalloc(sizeof(uint64_t) * (total ? total : 1));
	for (int i = 0; i < nclients; i++) {
		memcpy(all + at, jobs[i].lat, sizeof(uint64_t) * jobs[i].done);
		at += jobs[i].done;
		free(jobs[i].lat);
	}
	qsort(all, total, sizeof(uint64_t), cmp_u64);

	printf("clients=%d ops=%ld read=%d%% keys=%d value=%dB\n", nclients, total, read_pct, nkeys, vsize);
	printf("throughput %.0f ops/s\n", total / (elapsed / 1e9));
	printf("latency us  p50=%.1f p99=%.1f p999=%.1f max=%.1f\n", pct(all, total, 0.50) / 1e3,
	       pct(all, total, 0.99) / 1e3, pct(all, total, 0.999) / 1e3,
	       total ? all[total - 1] / 1e3 : 0.0);
	printf("timeouts=%ld errors=%ld mismatches=%ld\n", timeouts, errors, mismatches);
	free(all);
	free(jobs);
	return timeouts || errors || mismatches ? 1 : 0;
}
