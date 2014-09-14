#ifndef _MERSENNE_PAXOS_H_
#define _MERSENNE_PAXOS_H_

#include <uuid/uuid.h>
#include <evfibers/fiber.h>


struct me_cli_connection;

enum me_cli_value_state {
	ME_CLI_PVS_NEW = 0,
	ME_CLI_PVS_SUBMITTED,
	ME_CLI_PVS_ARRIVED,
	ME_CLI_PVS_PROCESSED,
	ME_CLI_PVS_MAX,
};

struct me_cli_value {
	uint8_t *data;
	unsigned data_len;
	struct fbr_mutex mutex;
	struct fbr_cond_var state_changed;
	enum me_cli_value_state state;
	uint64_t value_id;
	uint64_t iid;
	ev_tstamp time_submitted;
	ev_tstamp latency;
	struct me_cli_connection *conn;
	unsigned ref;
};

typedef void (*me_cli_foreign_func_t)(struct fbr_context *fctx,
		const uint8_t *data, unsigned data_len,
		uint64_t iid, int was_self, void *arg);

struct me_cli_config {
	struct fbr_context *fctx;
	struct ev_loop *loop;
	char **peers;
	unsigned peers_size;
	unsigned port;
	uint64_t starting_iid;
	me_cli_foreign_func_t foreign_func;
	void *foreign_func_arg;
};

static inline
const char *me_cli_value_state_strval(enum me_cli_value_state state)
{
	switch (state) {
	case ME_CLI_PVS_NEW:       return "ME_CLI_PVS_NEW";
	case ME_CLI_PVS_SUBMITTED: return "ME_CLI_PVS_SUBMITTED";
	case ME_CLI_PVS_ARRIVED:   return "ME_CLI_PVS_ARRIVED";
	case ME_CLI_PVS_PROCESSED: return "ME_CLI_PVS_PROCESSED";
	case ME_CLI_PVS_MAX:       return "ME_CLI_PVS_MAX";
	}
	return NULL;
}

static inline
void me_cli_config_init(struct me_cli_config *config)
{
	memset(config, 0x00, sizeof(*config));
}

struct me_cli_connection *me_cli_open(struct me_cli_config *config);
void me_cli_close(struct me_cli_connection *conn);
struct me_cli_value *me_cli_value_new(struct me_cli_connection *conn);
int me_cli_value_submit(struct me_cli_value *value, ev_tstamp timeout);
void me_cli_value_processed(struct me_cli_value *value);
void me_cli_value_dispose(struct me_cli_value *value);

#endif
