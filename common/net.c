#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include "net.h"
#include "misc.h"

int parse_addr(const char *s, struct sockaddr_in *sa)
{
	char host[64] = "127.0.0.1";
	const char *colon = strrchr(s, ':');
	const char *port = s;
	char *end;
	long p;

	if (colon) {
		size_t n = colon - s;

		if (n == 0 || n >= sizeof(host))
			return -1;
		memcpy(host, s, n);
		host[n] = '\0';
		port = colon + 1;
	}
	errno = 0;
	p = strtol(port, &end, 10);
	if (errno || end == port || *end || p <= 0 || p > 65535)
		return -1;
	memset(sa, 0, sizeof(*sa));
	sa->sin_family = AF_INET;
	sa->sin_port = htons(p);
	if (inet_pton(AF_INET, strcmp(host, "localhost") ? host : "127.0.0.1", &sa->sin_addr) != 1)
		return -1;
	return 0;
}

int parse_addr_list(const char *s, struct sockaddr_in **out)
{
	char *copy = xstrdup(s), *save = NULL, *tok;
	struct sockaddr_in *list = NULL;
	int n = 0;

	for (tok = strtok_r(copy, ",", &save); tok; tok = strtok_r(NULL, ",", &save)) {
		struct sockaddr_in *grown = realloc(list, sizeof(*list) * (n + 1));

		if (!grown)
			die("out of memory");
		list = grown;
		if (parse_addr(tok, &list[n]) < 0) {
			free(copy);
			free(list);
			return -1;
		}
		n++;
	}
	free(copy);
	*out = list;
	return n;
}

const char *addr_str(const struct sockaddr_in *sa, char *buf, size_t cap)
{
	char ip[INET_ADDRSTRLEN];

	inet_ntop(AF_INET, &sa->sin_addr, ip, sizeof(ip));
	snprintf(buf, cap, "%s:%d", ip, ntohs(sa->sin_port));
	return buf;
}

int udp_open(int port)
{
	struct sockaddr_in sa;
	int fd = socket(AF_INET, SOCK_DGRAM | SOCK_CLOEXEC, 0);

	if (fd < 0)
		return -1;
	if (port > 0) {
		int one = 1;

		setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
		memset(&sa, 0, sizeof(sa));
		sa.sin_family = AF_INET;
		sa.sin_port = htons(port);
		sa.sin_addr.s_addr = htonl(INADDR_ANY);
		if (bind(fd, (struct sockaddr *)&sa, sizeof(sa)) < 0) {
			close(fd);
			return -1;
		}
	}
	return fd;
}

int udp_send(int fd, const struct sockaddr_in *to, const struct msg *m)
{
	uint8_t buf[MAX_MSG];
	size_t len = msg_encode(m, buf, sizeof(buf));
	ssize_t n;

	if (!len)
		return -1;
	do
		n = sendto(fd, buf, len, 0, (const struct sockaddr *)to, sizeof(*to));
	while (n < 0 && errno == EINTR);
	return n == (ssize_t)len ? 0 : -1;
}

int udp_recv(int fd, struct msg *m, struct sockaddr_in *from, int timeout_ms)
{
	uint8_t buf[MAX_MSG + 1];
	struct sockaddr_in tmp;
	socklen_t alen = sizeof(tmp);
	struct pollfd pfd = { .fd = fd, .events = POLLIN };
	ssize_t n;
	int r;

	r = poll(&pfd, 1, timeout_ms);
	if (r < 0)
		return errno == EINTR ? 0 : -1;
	if (r == 0)
		return 0;
	n = recvfrom(fd, buf, sizeof(buf), MSG_DONTWAIT, (struct sockaddr *)&tmp, &alen);
	if (n < 0)
		return (errno == EAGAIN || errno == EINTR) ? 0 : -1;
	if (msg_decode(m, buf, n) < 0)
		return -2;
	if (from)
		*from = tmp;
	return 1;
}

int udp_call(int fd, const struct sockaddr_in *to, struct msg *req, struct msg *rep,
	     int timeout_ms, int tries)
{
	for (int t = 0; t < tries; t++) {
		uint64_t deadline;

		if (udp_send(fd, to, req) < 0)
			return -1;
		deadline = now_ms() + timeout_ms;
		for (;;) {
			uint64_t now = now_ms();
			struct sockaddr_in from;
			int r;

			if (now >= deadline)
				break;
			r = udp_recv(fd, rep, &from, (int)(deadline - now));
			if (r < 0 && r != -2)
				return -1;
			if (r != 1)
				continue;
			if (from.sin_port != to->sin_port || from.sin_addr.s_addr != to->sin_addr.s_addr)
				continue;
			if (rep->id == req->id && rep->op == (req->op | OP_REPLY))
				return 0;
		}
	}
	errno = ETIMEDOUT;
	return -1;
}

int tcp_listen(int port)
{
	struct sockaddr_in sa;
	int one = 1;
	int fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);

	if (fd < 0)
		return -1;
	setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
	memset(&sa, 0, sizeof(sa));
	sa.sin_family = AF_INET;
	sa.sin_port = htons(port);
	sa.sin_addr.s_addr = htonl(INADDR_ANY);
	if (bind(fd, (struct sockaddr *)&sa, sizeof(sa)) < 0 || listen(fd, 16) < 0) {
		close(fd);
		return -1;
	}
	return fd;
}

int tcp_connect(const struct sockaddr_in *sa, int timeout_ms)
{
	struct pollfd pfd;
	struct timeval tv = { 2, 0 };
	int err = 0, one = 1;
	socklen_t len = sizeof(err);
	int fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0);

	if (fd < 0)
		return -1;
	if (connect(fd, (const struct sockaddr *)sa, sizeof(*sa)) < 0) {
		if (errno != EINPROGRESS)
			goto fail;
		pfd.fd = fd;
		pfd.events = POLLOUT;
		if (poll(&pfd, 1, timeout_ms) != 1)
			goto fail;
		if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) < 0 || err)
			goto fail;
	}
	fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) & ~O_NONBLOCK);
	setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
	setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
	return fd;
fail:
	close(fd);
	return -1;
}

int write_full(int fd, const void *buf, size_t n)
{
	const char *p = buf;

	while (n > 0) {
		ssize_t w = send(fd, p, n, MSG_NOSIGNAL);

		if (w < 0) {
			if (errno == EINTR)
				continue;
			return -1;
		}
		p += w;
		n -= w;
	}
	return 0;
}

int read_full(int fd, void *buf, size_t n, int timeout_ms)
{
	char *p = buf;
	uint64_t deadline = now_ms() + timeout_ms;

	while (n > 0) {
		struct pollfd pfd = { .fd = fd, .events = POLLIN };
		uint64_t now = now_ms();
		ssize_t r;

		if (now >= deadline) {
			errno = ETIMEDOUT;
			return -1;
		}
		r = poll(&pfd, 1, (int)(deadline - now));
		if (r < 0 && errno != EINTR)
			return -1;
		if (r <= 0)
			continue;
		r = recv(fd, p, n, 0);
		if (r == 0) {
			errno = ECONNRESET;
			return -1;
		}
		if (r < 0) {
			if (errno == EINTR || errno == EAGAIN)
				continue;
			return -1;
		}
		p += r;
		n -= r;
	}
	return 0;
}

int frame_send(int fd, const struct msg *m)
{
	uint8_t buf[4 + MAX_MSG];
	size_t len = msg_encode(m, buf + 4, sizeof(buf) - 4);

	if (!len)
		return -1;
	buf[0] = len >> 24;
	buf[1] = len >> 16;
	buf[2] = len >> 8;
	buf[3] = len;
	return write_full(fd, buf, len + 4);
}

int frame_recv(int fd, struct msg *m, int timeout_ms)
{
	uint8_t hdr[4], buf[MAX_MSG];
	uint32_t len;

	if (read_full(fd, hdr, 4, timeout_ms) < 0)
		return -1;
	len = (uint32_t)hdr[0] << 24 | (uint32_t)hdr[1] << 16 | (uint32_t)hdr[2] << 8 | hdr[3];
	if (len < HDR_LEN || len > MAX_MSG) {
		errno = EPROTO;
		return -1;
	}
	if (read_full(fd, buf, len, timeout_ms) < 0)
		return -1;
	if (msg_decode(m, buf, len) < 0) {
		errno = EPROTO;
		return -1;
	}
	return 0;
}
