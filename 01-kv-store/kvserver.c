#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "store.h"
#include "net.h"
#include "misc.h"

static struct store *db;
static int sock;
static int delay_us;
static int verbose;
static atomic_ulong n_get, n_put, n_del, n_bad;

static void handle(struct msg *m)
{
	size_t len;

	m->arg = 0;
	switch (m->op) {
	case OP_GET:
		atomic_fetch_add(&n_get, 1);
		if (store_get(db, m->key, m->val, sizeof(m->val), &len) == 0) {
			m->vlen = len;
			msg_reply(m, ST_OK);
		} else {
			m->vlen = 0;
			msg_reply(m, ST_NOT_FOUND);
		}
		break;
	case OP_PUT:
		atomic_fetch_add(&n_put, 1);
		msg_reply(m, store_put(db, m->key, m->val, m->vlen) ? ST_ERR : ST_OK);
		m->vlen = 0;
		break;
	case OP_DEL:
		atomic_fetch_add(&n_del, 1);
		msg_reply(m, store_del(db, m->key) ? ST_NOT_FOUND : ST_OK);
		m->vlen = 0;
		break;
	case OP_STATS:
		m->arg = atomic_load(&n_get) + atomic_load(&n_put) + atomic_load(&n_del);
		len = snprintf(m->val, sizeof(m->val), "get=%lu put=%lu del=%lu bad=%lu keys=%zu",
			       atomic_load(&n_get), atomic_load(&n_put), atomic_load(&n_del),
			       atomic_load(&n_bad), store_count(db));
		m->vlen = len;
		msg_reply(m, ST_OK);
		break;
	default:
		atomic_fetch_add(&n_bad, 1);
		m->vlen = 0;
		msg_reply(m, ST_BAD_REQ);
	}
}

static int needs_key(int op)
{
	return op == OP_GET || op == OP_PUT || op == OP_DEL;
}

static void *worker(void *arg)
{
	struct msg m;
	struct sockaddr_in from;

	(void)arg;
	while (!stop) {
		int r = udp_recv(sock, &m, &from, 200);

		if (r == -2)
			atomic_fetch_add(&n_bad, 1);
		if (r != 1)
			continue;
		if (m.op & OP_REPLY)
			continue;
		if (needs_key(m.op) && m.klen == 0) {
			atomic_fetch_add(&n_bad, 1);
			m.vlen = 0;
			msg_reply(&m, ST_BAD_REQ);
		} else {
			if (delay_us && m.op != OP_STATS)
				usleep(delay_us);
			if (verbose)
				printf("%s %s\n", op_name(m.op), m.key);
			handle(&m);
		}
		udp_send(sock, &from, &m);
	}
	return NULL;
}

static void usage(const char *prog)
{
	fprintf(stderr, "usage: %s -p port [-t threads] [-d delay_us] [-v]\n", prog);
	exit(2);
}

int main(int argc, char **argv)
{
	int port = 0, nthreads = 4, opt;
	pthread_t *threads;

	while ((opt = getopt(argc, argv, "p:t:d:v")) != -1) {
		switch (opt) {
		case 'p': port = parse_num(optarg, 1, 65535, "port"); break;
		case 't': nthreads = parse_num(optarg, 1, 64, "threads"); break;
		case 'd': delay_us = parse_num(optarg, 0, 1000000, "delay"); break;
		case 'v': verbose = 1; break;
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
	db = store_new(1024);

	threads = xcalloc(nthreads, sizeof(*threads));
	for (int i = 0; i < nthreads; i++)
		if (pthread_create(&threads[i], NULL, worker, NULL))
			die("pthread_create failed");
	printf("kvserver listening on %d (%d threads)\n", port, nthreads);
	for (int i = 0; i < nthreads; i++)
		pthread_join(threads[i], NULL);

	printf("kvserver %d: get=%lu put=%lu del=%lu keys=%zu\n", port, atomic_load(&n_get),
	       atomic_load(&n_put), atomic_load(&n_del), store_count(db));
	free(threads);
	store_free(db);
	close(sock);
	return 0;
}
