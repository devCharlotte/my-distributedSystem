#include <string.h>

#include "proto.h"

static void put16(uint8_t *p, uint16_t v)
{
	p[0] = v >> 8;
	p[1] = v;
}

static void put32(uint8_t *p, uint32_t v)
{
	p[0] = v >> 24;
	p[1] = v >> 16;
	p[2] = v >> 8;
	p[3] = v;
}

static void put64(uint8_t *p, uint64_t v)
{
	put32(p, v >> 32);
	put32(p + 4, (uint32_t)v);
}

static uint16_t get16(const uint8_t *p)
{
	return (uint16_t)(p[0] << 8 | p[1]);
}

static uint32_t get32(const uint8_t *p)
{
	return (uint32_t)p[0] << 24 | (uint32_t)p[1] << 16 | (uint32_t)p[2] << 8 | p[3];
}

static uint64_t get64(const uint8_t *p)
{
	return (uint64_t)get32(p) << 32 | get32(p + 4);
}

void msg_init(struct msg *m, int op, uint32_t id)
{
	memset(m, 0, sizeof(*m));
	m->op = op;
	m->id = id;
}

int msg_set_key(struct msg *m, const char *key)
{
	size_t n = strlen(key);

	if (n == 0 || n > MAX_KEY)
		return -1;
	memcpy(m->key, key, n + 1);
	m->klen = n;
	return 0;
}

int msg_set_val(struct msg *m, const char *val, size_t len)
{
	if (len > MAX_VAL)
		return -1;
	memcpy(m->val, val, len);
	m->val[len] = '\0';
	m->vlen = len;
	return 0;
}

void msg_reply(struct msg *m, int status)
{
	m->op |= OP_REPLY;
	m->status = status;
}

size_t msg_encode(const struct msg *m, uint8_t *buf, size_t cap)
{
	size_t len = HDR_LEN + m->klen + m->vlen;

	if (m->klen > MAX_KEY || m->vlen > MAX_VAL || len > cap)
		return 0;
	buf[0] = m->op;
	buf[1] = m->status;
	put32(buf + 2, m->id);
	put64(buf + 6, m->arg);
	buf[14] = m->klen;
	put16(buf + 15, m->vlen);
	memcpy(buf + HDR_LEN, m->key, m->klen);
	memcpy(buf + HDR_LEN + m->klen, m->val, m->vlen);
	return len;
}

int msg_decode(struct msg *m, const uint8_t *buf, size_t len)
{
	if (len < HDR_LEN)
		return -1;
	m->op = buf[0];
	m->status = buf[1];
	m->id = get32(buf + 2);
	m->arg = get64(buf + 6);
	m->klen = buf[14];
	m->vlen = get16(buf + 15);
	if (m->klen > MAX_KEY || m->vlen > MAX_VAL)
		return -1;
	if (len != (size_t)HDR_LEN + m->klen + m->vlen)
		return -1;
	memcpy(m->key, buf + HDR_LEN, m->klen);
	m->key[m->klen] = '\0';
	memcpy(m->val, buf + HDR_LEN + m->klen, m->vlen);
	m->val[m->vlen] = '\0';
	if (memchr(m->key, '\0', m->klen))
		return -1;
	return 0;
}

const char *op_name(int op)
{
	switch (op & ~OP_REPLY) {
	case OP_GET: return "GET";
	case OP_PUT: return "PUT";
	case OP_DEL: return "DEL";
	case OP_STATS: return "STATS";
	case OP_HB: return "HB";
	case OP_REPL_PUT: return "REPL_PUT";
	case OP_REPL_DEL: return "REPL_DEL";
	case OP_SYNC_BEGIN: return "SYNC_BEGIN";
	case OP_SYNC_END: return "SYNC_END";
	}
	return "?";
}

const char *status_name(int st)
{
	switch (st) {
	case ST_OK: return "OK";
	case ST_NOT_FOUND: return "NOT_FOUND";
	case ST_MISS: return "MISS";
	case ST_NOT_PRIMARY: return "NOT_PRIMARY";
	case ST_STALE: return "STALE";
	case ST_BAD_REQ: return "BAD_REQ";
	case ST_ERR: return "ERR";
	}
	return "?";
}
