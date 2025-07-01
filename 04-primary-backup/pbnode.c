#include <errno.h>
#include <poll.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include "store.h"
#include "net.h"
#include "misc.h"

#define MAX_NODES 8
#define MAX_CONNS 16

enum { BACKUP, PRIMARY };

struct conn {
	int fd;
	int owner;
	int syncing;
};

static struct sockaddr_in *peers;
static int npeers, self = -1;
static int repl_offset = 1000, hb_ms = 100, fail_ms = 500;
static struct store *db;
static int usock;

static pthread_mutex_t state_lock = PTHREAD_MUTEX_INITIALIZER;
static int role;
static uint64_t epoch;
static int primary = -1;
static uint64_t last_hb;
static uint32_t applied;

static pthread_mutex_t repl_lock = PTHREAD_MUTEX_INITIALIZER;
static int bfd[MAX_NODES];
static uint64_t next_try[MAX_NODES];
static int backoff[MAX_NODES];
static uint32_t seq;
static atomic_uint seq_pub;
static atomic_int nbackups;

static uint32_t seq_now(void)
{
	return atomic_load(&seq_pub);
}

static struct sockaddr_in repl_addr(int i)
{
	struct sockaddr_in sa = peers[i];

	sa.sin_port = htons(ntohs(sa.sin_port) + repl_offset);
	return sa;
}

static void become_backup(uint64_t ep, int prim)
{
	if (role == PRIMARY)
		printf("node %d: stepping down, epoch %llu\n", self, (unsigned long long)ep);
	role = BACKUP;
	epoch = ep;
	primary = prim;
	last_hb = now_ms();
}

static int current(uint64_t *ep)
{
	int r;

	pthread_mutex_lock(&state_lock);
	r = role;
	if (ep)
		*ep = epoch;
	pthread_mutex_unlock(&state_lock);
	return r;
}

static void step_down_to(uint64_t ep)
{
	pthread_mutex_lock(&state_lock);
	if (ep > epoch || role == PRIMARY)
		become_backup(ep > epoch ? ep : epoch, -1);
	pthread_mutex_unlock(&state_lock);
}

static void close_backup(int i, const char *why)
{
	if (bfd[i] < 0)
		return;
	if (why)
		printf("node %d: dropped backup %d (%s)\n", self, i, why);
	close(bfd[i]);
	bfd[i] = -1;
	next_try[i] = now_ms() + 200;
	atomic_fetch_sub(&nbackups, 1);
}

static void close_all_backups(void)
{
	for (int i = 0; i < npeers; i++)
		close_backup(i, NULL);
}

struct snap {
	int fd;
	uint64_t ep;
	int err;
};

static void send_entry(const char *key, const char *val, size_t vlen, void *arg)
{
	struct snap *s = arg;
	struct msg m;

	if (s->err)
		return;
	msg_init(&m, OP_REPL_PUT, 0);
	m.arg = s->ep;
	if (msg_set_key(&m, key) < 0 || msg_set_val(&m, val, vlen) < 0 || frame_send(s->fd, &m) < 0)
		s->err = 1;
}

static void sync_backup(int i)
{
	struct sockaddr_in sa = repl_addr(i);
	struct snap s = { 0 };
	struct msg m, ack;
	size_t keys;
	int fd = tcp_connect(&sa, 200);

	if (fd < 0) {
		pthread_mutex_lock(&repl_lock);
		next_try[i] = now_ms() + 500;
		pthread_mutex_unlock(&repl_lock);
		return;
	}

	pthread_mutex_lock(&repl_lock);
	if (current(&s.ep) != PRIMARY || bfd[i] >= 0)
		goto drop;
	s.fd = fd;
	msg_init(&m, OP_SYNC_BEGIN, self);
	m.arg = s.ep;
	if (frame_send(fd, &m) < 0)
		goto drop;
	store_each(db, send_entry, &s);
	keys = store_count(db);
	msg_init(&m, OP_SYNC_END, seq);
	m.arg = s.ep;
	if (s.err || frame_send(fd, &m) < 0 || frame_recv(fd, &ack, 1000) < 0)
		goto slow;
	if (ack.op != (OP_SYNC_END | OP_REPLY))
		goto drop;
	if (ack.status == ST_STALE) {
		step_down_to(ack.arg);
		close_all_backups();
		goto drop;
	}
	bfd[i] = fd;
	backoff[i] = 0;
	atomic_fetch_add(&nbackups, 1);
	printf("node %d: backup %d in sync (%zu keys, epoch %llu)\n", self, i, keys,
	       (unsigned long long)s.ep);
	pthread_mutex_unlock(&repl_lock);
	return;
slow:
	backoff[i] = backoff[i] ? (backoff[i] * 2 > 8000 ? 8000 : backoff[i] * 2) : 1000;
	printf("node %d: backup %d did not finish sync, retry in %d ms\n", self, i, backoff[i]);
	next_try[i] = now_ms() + backoff[i];
	pthread_mutex_unlock(&repl_lock);
	close(fd);
	return;
drop:
	next_try[i] = now_ms() + 500;
	pthread_mutex_unlock(&repl_lock);
	close(fd);
}

static int replicate(struct msg *req)
{
	struct msg f, ack;
	uint64_t ep, stale = 0;
	int st;

	pthread_mutex_lock(&repl_lock);
	if (current(&ep) != PRIMARY) {
		pthread_mutex_unlock(&repl_lock);
		return ST_NOT_PRIMARY;
	}
	msg_init(&f, req->op == OP_PUT ? OP_REPL_PUT : OP_REPL_DEL, ++seq);
	atomic_store(&seq_pub, seq);
	f.arg = ep;
	msg_set_key(&f, req->key);
	msg_set_val(&f, req->val, req->vlen);

	for (int i = 0; i < npeers; i++)
		if (bfd[i] >= 0 && frame_send(bfd[i], &f) < 0)
			close_backup(i, strerror(errno));
	for (int i = 0; i < npeers; i++) {
		if (bfd[i] < 0)
			continue;
		if (frame_recv(bfd[i], &ack, 500) < 0) {
			close_backup(i, errno == ETIMEDOUT ? "ack timeout" : "connection lost");
			continue;
		}
		if (ack.id != f.id || ack.op != (f.op | OP_REPLY)) {
			close_backup(i, "unexpected ack");
			continue;
		}
		if (ack.status == ST_STALE) {
			uint64_t theirs = ack.arg > ep ? ack.arg : ep;

			if (theirs > stale)
				stale = theirs;
		}
	}
	if (stale) {
		step_down_to(stale);
		close_all_backups();
		pthread_mutex_unlock(&repl_lock);
		return ST_NOT_PRIMARY;
	}

	if (req->op == OP_PUT)
		st = store_put(db, req->key, req->val, req->vlen) ? ST_ERR : ST_OK;
	else
		st = store_del(db, req->key) ? ST_NOT_FOUND : ST_OK;
	pthread_mutex_unlock(&repl_lock);
	return st;
}

static void send_hb(int to, uint64_t ep)
{
	struct msg m;

	msg_init(&m, OP_HB, self);
	m.arg = ep;
	udp_send(usock, &peers[to], &m);
}

static void on_heartbeat(int from, uint64_t ep)
{
	int answer = 0;
	uint64_t mine;

	if (from < 0 || from >= npeers || from == self)
		return;
	pthread_mutex_lock(&state_lock);
	if (ep > epoch || (ep == epoch && role == BACKUP))
		become_backup(ep, from);
	else if (ep == epoch && role == PRIMARY && from < self)
		become_backup(ep, from);
	else if (role == PRIMARY)
		answer = 1;
	mine = epoch;
	pthread_mutex_unlock(&state_lock);
	if (answer)
		send_hb(from, mine);
}

static void *udp_worker(void *arg)
{
	struct msg m;
	struct sockaddr_in from;

	(void)arg;
	while (!stop) {
		size_t len;
		int r = udp_recv(usock, &m, &from, 200);

		if (r != 1 || (m.op & OP_REPLY))
			continue;
		if (m.op == OP_HB) {
			on_heartbeat((int)m.id, m.arg);
			continue;
		}
		if (m.op == OP_STATS) {
			pthread_mutex_lock(&state_lock);
			m.vlen = snprintf(m.val, sizeof(m.val),
					  "id=%d role=%s epoch=%llu primary=%d keys=%zu backups=%d", self,
					  role == PRIMARY ? "primary" : "backup", (unsigned long long)epoch,
					  primary, store_count(db), atomic_load(&nbackups));
			m.arg = epoch << 32 | (role == PRIMARY ? seq_now() : applied);
			pthread_mutex_unlock(&state_lock);
			msg_reply(&m, ST_OK);
			udp_send(usock, &from, &m);
			continue;
		}
		if ((m.op != OP_GET && m.op != OP_PUT && m.op != OP_DEL) || m.klen == 0) {
			m.vlen = 0;
			m.arg = 0;
			msg_reply(&m, ST_BAD_REQ);
			udp_send(usock, &from, &m);
			continue;
		}

		pthread_mutex_lock(&state_lock);
		r = role;
		m.arg = primary >= 0 && primary != self ? (uint64_t)primary + 1 : 0;
		pthread_mutex_unlock(&state_lock);

		if (r != PRIMARY) {
			m.vlen = 0;
			msg_reply(&m, ST_NOT_PRIMARY);
		} else if (m.op == OP_GET) {
			m.arg = 0;
			if (store_get(db, m.key, m.val, sizeof(m.val), &len) == 0) {
				m.vlen = len;
				msg_reply(&m, ST_OK);
			} else {
				m.vlen = 0;
				msg_reply(&m, ST_NOT_FOUND);
			}
		} else {
			int st = replicate(&m);

			m.vlen = 0;
			m.arg = 0;
			if (st == ST_NOT_PRIMARY) {
				pthread_mutex_lock(&state_lock);
				m.arg = primary >= 0 && primary != self ? (uint64_t)primary + 1 : 0;
				pthread_mutex_unlock(&state_lock);
			}
			msg_reply(&m, st);
		}
		udp_send(usock, &from, &m);
	}
	return NULL;
}

static int may_take_over(void)
{
	struct msg req, rep;
	uint64_t mine;
	int fd = udp_open(0), ok = 1;

	if (fd < 0)
		return 0;
	pthread_mutex_lock(&state_lock);
	mine = epoch << 32 | applied;
	pthread_mutex_unlock(&state_lock);

	for (int i = 0; i < npeers && ok; i++) {
		if (i == self)
			continue;
		msg_init(&req, OP_STATS, (uint32_t)now_ns());
		if (udp_call(fd, &peers[i], &req, &rep, 100, 2) < 0)
			continue;
		if (strstr(rep.val, "role=primary") && rep.arg >> 32 >= mine >> 32) {
			pthread_mutex_lock(&state_lock);
			if (role == BACKUP && rep.arg >> 32 >= epoch)
				become_backup(rep.arg >> 32, i);
			pthread_mutex_unlock(&state_lock);
			ok = 0;
		} else if (rep.arg > mine) {
			printf("node %d: node %d has newer data, not taking over\n", self, i);
			pthread_mutex_lock(&state_lock);
			last_hb = now_ms();
			pthread_mutex_unlock(&state_lock);
			ok = 0;
		}
	}
	close(fd);
	return ok;
}

static void *heartbeat(void *arg)
{
	(void)arg;
	while (!stop) {
		uint64_t ep, lh, now = now_ms();
		int r, p;

		pthread_mutex_lock(&state_lock);
		r = role;
		ep = epoch;
		p = primary;
		lh = last_hb;
		pthread_mutex_unlock(&state_lock);

		if (r == PRIMARY) {
			for (int i = 0; i < npeers; i++)
				if (i != self)
					send_hb(i, ep);
			for (int i = 0; i < npeers; i++) {
				int want;

				if (i == self)
					continue;
				pthread_mutex_lock(&repl_lock);
				want = bfd[i] < 0 && now_ms() >= next_try[i];
				pthread_mutex_unlock(&repl_lock);
				if (want)
					sync_backup(i);
			}
		} else {
			int rank = p >= 0 ? (self - p - 1 + npeers) % npeers + 1 : self + 1;

			pthread_mutex_lock(&repl_lock);
			close_all_backups();
			pthread_mutex_unlock(&repl_lock);

			if (now - lh > (uint64_t)fail_ms * rank) {
				int promoted = 0;

				if (!may_take_over())
					continue;
				pthread_mutex_lock(&state_lock);
				if (role == BACKUP && last_hb == lh) {
					epoch++;
					role = PRIMARY;
					primary = self;
					promoted = 1;
					printf("node %d: no heartbeat for %llu ms, taking over (epoch %llu)\n", self,
					       (unsigned long long)(now - lh), (unsigned long long)epoch);
				}
				pthread_mutex_unlock(&state_lock);
				if (promoted)
					continue;
			}
		}
		sleep_ms(hb_ms);
	}
	return NULL;
}

static void set_applied(uint32_t s)
{
	pthread_mutex_lock(&state_lock);
	applied = s;
	pthread_mutex_unlock(&state_lock);
}

static void ack(struct conn *c, const struct msg *m, int status, uint64_t ep)
{
	struct msg a;

	msg_init(&a, m->op, m->id);
	a.arg = ep;
	msg_reply(&a, status);
	if (frame_send(c->fd, &a) < 0) {
		close(c->fd);
		c->fd = -1;
	}
}

static void on_repl(struct conn *c, struct msg *m)
{
	uint64_t ep;

	if (m->op == OP_SYNC_BEGIN)
		c->owner = (int)m->id;

	pthread_mutex_lock(&state_lock);
	if (m->arg < epoch || (m->arg == epoch && role == PRIMARY) || c->owner < 0) {
		ep = epoch;
		pthread_mutex_unlock(&state_lock);
		ack(c, m, ST_STALE, ep);
		if (c->fd >= 0)
			close(c->fd);
		c->fd = -1;
		return;
	}
	if (m->arg > epoch || primary != c->owner)
		become_backup(m->arg, c->owner);
	last_hb = now_ms();
	ep = epoch;
	pthread_mutex_unlock(&state_lock);

	switch (m->op) {
	case OP_SYNC_BEGIN:
		store_clear(db);
		c->syncing = 1;
		return;
	case OP_REPL_PUT:
		if (store_put(db, m->key, m->val, m->vlen) < 0)
			die("out of memory");
		if (c->syncing)
			return;
		set_applied(m->id);
		break;
	case OP_REPL_DEL:
		store_del(db, m->key);
		set_applied(m->id);
		break;
	case OP_SYNC_END:
		c->syncing = 0;
		set_applied(m->id);
		printf("node %d: synced from %d (%zu keys, epoch %llu)\n", self, c->owner, store_count(db),
		       (unsigned long long)ep);
		break;
	default:
		ack(c, m, ST_BAD_REQ, ep);
		return;
	}
	ack(c, m, ST_OK, ep);
}

static void *repl_listener(void *arg)
{
	struct conn conns[MAX_CONNS];
	int lfd = *(int *)arg, n = 0;

	while (!stop) {
		struct pollfd pfd[MAX_CONNS + 1];

		pfd[0].fd = lfd;
		pfd[0].events = POLLIN;
		for (int i = 0; i < n; i++) {
			pfd[i + 1].fd = conns[i].fd;
			pfd[i + 1].events = POLLIN;
		}
		if (poll(pfd, n + 1, 200) <= 0)
			continue;
		for (int i = 0; i < n; i++) {
			struct msg m;

			if (!(pfd[i + 1].revents & (POLLIN | POLLHUP | POLLERR)))
				continue;
			if (frame_recv(conns[i].fd, &m, 1000) < 0) {
				close(conns[i].fd);
				conns[i].fd = -1;
				continue;
			}
			on_repl(&conns[i], &m);
		}
		for (int i = 0; i < n;) {
			if (conns[i].fd < 0)
				conns[i] = conns[--n];
			else
				i++;
		}
		if (pfd[0].revents & POLLIN) {
			int fd = accept4(lfd, NULL, NULL, SOCK_CLOEXEC);

			if (fd < 0)
				continue;
			if (n == MAX_CONNS) {
				close(fd);
				continue;
			}
			conns[n].fd = fd;
			conns[n].owner = -1;
			conns[n].syncing = 0;
			n++;
		}
	}
	for (int i = 0; i < n; i++)
		close(conns[i].fd);
	return NULL;
}

static void usage(const char *prog)
{
	fprintf(stderr, "usage: %s -i id -n host:port,host:port,... [-j] [-o repl_port_offset] "
			"[-b heartbeat_ms] [-f failure_ms]\n", prog);
	exit(2);
}

int main(int argc, char **argv)
{
	int opt, join = 0, lfd;
	pthread_t workers[2], hb, rl;
	struct sockaddr_in ra;

	while ((opt = getopt(argc, argv, "i:n:jo:b:f:")) != -1) {
		switch (opt) {
		case 'i': self = parse_num(optarg, 0, MAX_NODES - 1, "id"); break;
		case 'n':
			npeers = parse_addr_list(optarg, &peers);
			if (npeers <= 0 || npeers > MAX_NODES)
				die("node list must have 1..%d entries", MAX_NODES);
			break;
		case 'j': join = 1; break;
		case 'o': repl_offset = parse_num(optarg, 1, 30000, "offset"); break;
		case 'b': hb_ms = parse_num(optarg, 10, 10000, "heartbeat"); break;
		case 'f': fail_ms = parse_num(optarg, 50, 60000, "failure timeout"); break;
		default: usage(argv[0]);
		}
	}
	if (self < 0 || !npeers || self >= npeers)
		usage(argv[0]);
	if (fail_ms <= 2 * hb_ms)
		die("failure timeout must be more than two heartbeats");

	on_signals();
	setvbuf(stdout, NULL, _IOLBF, 0);
	usock = udp_open(ntohs(peers[self].sin_port));
	ra = repl_addr(self);
	lfd = tcp_listen(ntohs(ra.sin_port));
	if (usock < 0 || lfd < 0)
		die("cannot bind ports %d/%d", ntohs(peers[self].sin_port), ntohs(ra.sin_port));
	db = store_new(1024);
	for (int i = 0; i < MAX_NODES; i++)
		bfd[i] = -1;

	if (!join && self == 0) {
		role = PRIMARY;
		epoch = 1;
		primary = 0;
	} else {
		role = BACKUP;
		epoch = join ? 0 : 1;
		primary = join ? -1 : 0;
	}
	last_hb = now_ms();
	printf("node %d: %s, epoch %llu, udp %d, repl %d\n", self, role == PRIMARY ? "primary" : "backup",
	       (unsigned long long)epoch, ntohs(peers[self].sin_port), ntohs(ra.sin_port));

	pthread_create(&rl, NULL, repl_listener, &lfd);
	pthread_create(&hb, NULL, heartbeat, NULL);
	for (int i = 0; i < 2; i++)
		pthread_create(&workers[i], NULL, udp_worker, NULL);
	for (int i = 0; i < 2; i++)
		pthread_join(workers[i], NULL);
	pthread_join(hb, NULL);
	pthread_join(rl, NULL);

	pthread_mutex_lock(&repl_lock);
	close_all_backups();
	pthread_mutex_unlock(&repl_lock);
	printf("node %d: exit with %zu keys\n", self, store_count(db));
	store_free(db);
	free(peers);
	close(lfd);
	close(usock);
	return 0;
}
