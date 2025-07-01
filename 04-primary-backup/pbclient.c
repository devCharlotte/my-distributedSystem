#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "net.h"
#include "misc.h"

static struct sockaddr_in *nodes;
static int nnodes, target, fd;
static uint32_t next_id;
static int deadline_ms = 8000;

static int cluster_call(int op, const char *key, const char *val, struct msg *rep)
{
	struct msg req;
	uint64_t deadline = now_ms() + deadline_ms;

	msg_init(&req, op, ++next_id);
	if (key && msg_set_key(&req, key) < 0)
		die("key must be 1..%d bytes", MAX_KEY);
	if (val && msg_set_val(&req, val, strlen(val)) < 0)
		die("value must be at most %d bytes", MAX_VAL);

	while (now_ms() < deadline && !stop) {
		if (udp_call(fd, &nodes[target], &req, rep, 300, 1) < 0) {
			target = (target + 1) % nnodes;
			continue;
		}
		if (rep->status != ST_NOT_PRIMARY)
			return 0;
		if (rep->arg > 0 && rep->arg <= (uint64_t)nnodes)
			target = (int)rep->arg - 1;
		else
			target = (target + 1) % nnodes;
		sleep_ms(50);
	}
	return -1;
}

static void key_of(char *buf, size_t cap, const char *prefix, long i)
{
	snprintf(buf, cap, "%s%06ld", prefix, i);
}

static void val_of(char *buf, size_t cap, const char *prefix, long i)
{
	snprintf(buf, cap, "%s-value-%ld", prefix, i);
}

static int fill(long n, const char *prefix)
{
	char key[MAX_KEY + 1], val[MAX_VAL + 1];
	struct msg rep;
	long fails = 0;
	uint64_t worst = 0;

	for (long i = 0; i < n && !stop; i++) {
		uint64_t t0 = now_ms();

		key_of(key, sizeof(key), prefix, i);
		val_of(val, sizeof(val), prefix, i);
		if (cluster_call(OP_PUT, key, val, &rep) < 0 || rep.status != ST_OK)
			fails++;
		if (now_ms() - t0 > worst)
			worst = now_ms() - t0;
	}
	printf("fill %s: %ld writes, %ld failed, slowest %llu ms\n", prefix, n, fails, (unsigned long long)worst);
	return fails ? 1 : 0;
}

static int check(long n, const char *prefix)
{
	char key[MAX_KEY + 1], val[MAX_VAL + 1];
	struct msg rep;
	long bad = 0;

	for (long i = 0; i < n && !stop; i++) {
		key_of(key, sizeof(key), prefix, i);
		val_of(val, sizeof(val), prefix, i);
		if (cluster_call(OP_GET, key, NULL, &rep) < 0 || rep.status != ST_OK || strcmp(rep.val, val))
			bad++;
	}
	printf("check %s: %ld reads, %ld wrong or missing\n", prefix, n, bad);
	return bad ? 1 : 0;
}

static int status(void)
{
	struct msg req, rep;
	char buf[64];

	for (int i = 0; i < nnodes; i++) {
		msg_init(&req, OP_STATS, ++next_id);
		if (udp_call(fd, &nodes[i], &req, &rep, 300, 2) < 0)
			printf("%s down\n", addr_str(&nodes[i], buf, sizeof(buf)));
		else
			printf("%s %s\n", addr_str(&nodes[i], buf, sizeof(buf)), rep.val);
	}
	return 0;
}

static void usage(const char *prog)
{
	fprintf(stderr,
		"usage: %s -n host:port,... [-d deadline_ms] get KEY | put KEY VALUE | del KEY\n"
		"       %s -n host:port,... status | fill N PREFIX | check N PREFIX\n",
		prog, prog);
	exit(2);
}

int main(int argc, char **argv)
{
	struct msg rep;
	const char *cmd;
	int opt, rc;

	while ((opt = getopt(argc, argv, "n:d:")) != -1) {
		switch (opt) {
		case 'n':
			nnodes = parse_addr_list(optarg, &nodes);
			if (nnodes <= 0)
				die("bad node list: %s", optarg);
			break;
		case 'd': deadline_ms = parse_num(optarg, 100, 600000, "deadline"); break;
		default: usage(argv[0]);
		}
	}
	if (nnodes <= 0 || optind >= argc)
		usage(argv[0]);
	on_signals();
	fd = udp_open(0);
	if (fd < 0)
		die("socket failed");
	next_id = (uint32_t)(now_ns() ^ getpid());

	cmd = argv[optind++];
	if (!strcmp(cmd, "status") && optind == argc) {
		rc = status();
	} else if (!strcmp(cmd, "fill") && argc - optind == 2) {
		rc = fill(parse_num(argv[optind], 1, 10000000, "count"), argv[optind + 1]);
	} else if (!strcmp(cmd, "check") && argc - optind == 2) {
		rc = check(parse_num(argv[optind], 1, 10000000, "count"), argv[optind + 1]);
	} else if ((!strcmp(cmd, "get") || !strcmp(cmd, "del")) && argc - optind == 1) {
		int op = cmd[0] == 'g' ? OP_GET : OP_DEL;

		if (cluster_call(op, argv[optind], NULL, &rep) < 0)
			die("no primary answered");
		printf("%s\n", rep.status == ST_OK && op == OP_GET ? rep.val : status_name(rep.status));
		rc = rep.status == ST_OK ? 0 : 1;
	} else if (!strcmp(cmd, "put") && argc - optind == 2) {
		if (cluster_call(OP_PUT, argv[optind], argv[optind + 1], &rep) < 0)
			die("no primary answered");
		printf("%s\n", status_name(rep.status));
		rc = rep.status == ST_OK ? 0 : 1;
	} else {
		usage(argv[0]);
	}
	free(nodes);
	close(fd);
	return rc;
}
