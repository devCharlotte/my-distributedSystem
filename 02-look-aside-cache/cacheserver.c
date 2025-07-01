#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "lru.h"
#include "net.h"
#include "misc.h"

static struct lru *cache;
static int sock;

static void handle(struct msg *m)
{
	size_t len;
	uint64_t lease;

	uint64_t token = m->arg;

	m->arg = 0;
	switch (m->op) {
	case OP_GET:
		if (lru_get(cache, m->key, m->val, sizeof(m->val), &len, &lease) == LRU_HIT) {
			m->vlen = len;
			msg_reply(m, ST_OK);
		} else {
			m->vlen = 0;
			m->arg = lease;
			msg_reply(m, ST_MISS);
		}
		break;
	case OP_PUT:
		msg_reply(m, lru_fill(cache, m->key, m->val, m->vlen, token) ? ST_STALE : ST_OK);
		m->vlen = 0;
		break;
	case OP_DEL:
		lru_invalidate(cache, m->key);
		m->vlen = 0;
		msg_reply(m, ST_OK);
		break;
	case OP_STATS:
		m->vlen = lru_stats(cache, m->val, sizeof(m->val));
		msg_reply(m, ST_OK);
		break;
	default:
		m->vlen = 0;
		msg_reply(m, ST_BAD_REQ);
	}
}

static void *worker(void *arg)
{
	struct msg m;
	struct sockaddr_in from;

	(void)arg;
	while (!stop) {
		if (udp_recv(sock, &m, &from, 200) != 1 || (m.op & OP_REPLY))
			continue;
		if (m.op != OP_STATS && m.klen == 0) {
			m.vlen = 0;
			msg_reply(&m, ST_BAD_REQ);
		} else {
			handle(&m);
		}
		udp_send(sock, &from, &m);
	}
	return NULL;
}

static void usage(const char *prog)
{
	fprintf(stderr, "usage: %s -p port [-c capacity] [-t threads]\n", prog);
	exit(2);
}

int main(int argc, char **argv)
{
	int port = 0, nthreads = 4, opt;
	long cap = 1000;
	pthread_t *threads;
	char stats[256];

	while ((opt = getopt(argc, argv, "p:c:t:")) != -1) {
		switch (opt) {
		case 'p': port = parse_num(optarg, 1, 65535, "port"); break;
		case 'c': cap = parse_num(optarg, 1, 10000000, "capacity"); break;
		case 't': nthreads = parse_num(optarg, 1, 64, "threads"); break;
		default: usage(argv[0]);
		}
	}
	if (!port)
		usage(argv[0]);

	on_signals();
	setvbuf(stdout, NULL, _IOLBF, 0);
	sock = udp_open(port);
	if (sock < 0)
		die("cannot bind udp port %d", port);
	cache = lru_new(cap);

	threads = xcalloc(nthreads, sizeof(*threads));
	for (int i = 0; i < nthreads; i++)
		if (pthread_create(&threads[i], NULL, worker, NULL))
			die("pthread_create failed");
	printf("cache listening on %d (capacity %ld)\n", port, cap);
	for (int i = 0; i < nthreads; i++)
		pthread_join(threads[i], NULL);

	lru_stats(cache, stats, sizeof(stats));
	printf("cache %d: %s\n", port, stats);
	free(threads);
	lru_free(cache);
	close(sock);
	return 0;
}
