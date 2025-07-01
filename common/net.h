#ifndef NET_H
#define NET_H

#include <netinet/in.h>

#include "proto.h"

int parse_addr(const char *s, struct sockaddr_in *sa);
int parse_addr_list(const char *s, struct sockaddr_in **out);
const char *addr_str(const struct sockaddr_in *sa, char *buf, size_t cap);

int udp_open(int port);
int udp_send(int fd, const struct sockaddr_in *to, const struct msg *m);
int udp_recv(int fd, struct msg *m, struct sockaddr_in *from, int timeout_ms);
int udp_call(int fd, const struct sockaddr_in *to, struct msg *req, struct msg *rep,
	     int timeout_ms, int tries);

int tcp_listen(int port);
int tcp_connect(const struct sockaddr_in *sa, int timeout_ms);
int write_full(int fd, const void *buf, size_t n);
int read_full(int fd, void *buf, size_t n, int timeout_ms);
int frame_send(int fd, const struct msg *m);
int frame_recv(int fd, struct msg *m, int timeout_ms);

#endif
