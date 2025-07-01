#ifndef PROTO_H
#define PROTO_H

#include <stddef.h>
#include <stdint.h>

#define MAX_KEY 64
#define MAX_VAL 1024
#define HDR_LEN 17
#define MAX_MSG (HDR_LEN + MAX_KEY + MAX_VAL)

enum op {
	OP_GET = 1,
	OP_PUT,
	OP_DEL,
	OP_STATS,
	OP_HB,
	OP_REPL_PUT,
	OP_REPL_DEL,
	OP_SYNC_BEGIN,
	OP_SYNC_END,
	OP_REPLY = 0x80,
};

enum status {
	ST_OK = 0,
	ST_NOT_FOUND,
	ST_MISS,
	ST_NOT_PRIMARY,
	ST_STALE,
	ST_BAD_REQ,
	ST_ERR,
};

struct msg {
	uint8_t op;
	uint8_t status;
	uint32_t id;
	uint64_t arg;
	uint8_t klen;
	uint16_t vlen;
	char key[MAX_KEY + 1];
	char val[MAX_VAL + 1];
};

void msg_init(struct msg *m, int op, uint32_t id);
int msg_set_key(struct msg *m, const char *key);
int msg_set_val(struct msg *m, const char *val, size_t len);
void msg_reply(struct msg *m, int status);
size_t msg_encode(const struct msg *m, uint8_t *buf, size_t cap);
int msg_decode(struct msg *m, const uint8_t *buf, size_t len);
const char *op_name(int op);
const char *status_name(int st);

#endif
