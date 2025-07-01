#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "net.h"
#include "misc.h"

static void usage(const char *prog)
{
	fprintf(stderr,
		"usage: %s -s host:port [-a arg] get KEY\n"
		"       %s -s host:port put KEY VALUE\n"
		"       %s -s host:port del KEY\n"
		"       %s -s host:port stats\n",
		prog, prog, prog, prog);
	exit(2);
}

int main(int argc, char **argv)
{
	struct sockaddr_in addr;
	struct msg req, rep;
	const char *server = NULL, *cmd;
	int opt, fd, op, need;
	uint64_t arg = 0;
	struct rng r;

	while ((opt = getopt(argc, argv, "s:a:")) != -1) {
		if (opt == 's')
			server = optarg;
		else if (opt == 'a')
			arg = strtoull(optarg, NULL, 10);
		else
			usage(argv[0]);
	}
	if (!server || optind >= argc)
		usage(argv[0]);
	if (parse_addr(server, &addr) < 0)
		die("bad server address: %s", server);

	cmd = argv[optind++];
	if (!strcmp(cmd, "get")) {
		op = OP_GET;
		need = 1;
	} else if (!strcmp(cmd, "put")) {
		op = OP_PUT;
		need = 2;
	} else if (!strcmp(cmd, "del")) {
		op = OP_DEL;
		need = 1;
	} else if (!strcmp(cmd, "stats")) {
		op = OP_STATS;
		need = 0;
	} else {
		usage(argv[0]);
	}
	if (argc - optind != need)
		usage(argv[0]);

	rng_seed(&r, now_ns() ^ getpid());
	msg_init(&req, op, (uint32_t)rng_next(&r));
	req.arg = arg;
	if (need >= 1 && msg_set_key(&req, argv[optind]) < 0)
		die("key must be 1..%d bytes", MAX_KEY);
	if (need == 2 && msg_set_val(&req, argv[optind + 1], strlen(argv[optind + 1])) < 0)
		die("value must be at most %d bytes", MAX_VAL);

	fd = udp_open(0);
	if (fd < 0)
		die("socket failed");
	if (udp_call(fd, &addr, &req, &rep, 500, 3) < 0) {
		close(fd);
		die("no reply from %s", server);
	}
	close(fd);

	if (rep.status != ST_OK) {
		if (rep.arg)
			printf("%s %llu\n", status_name(rep.status), (unsigned long long)rep.arg);
		else
			printf("%s\n", status_name(rep.status));
		return rep.status == ST_NOT_FOUND ? 1 : 2;
	}
	if (op == OP_GET || op == OP_STATS)
		printf("%s\n", rep.val);
	else
		printf("OK\n");
	return 0;
}
