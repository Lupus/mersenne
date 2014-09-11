#include <stdio.h>
#include <errno.h>
#include <err.h>
#include <netinet/tcp.h>
#include <msgpack.h>

#include <mersenne/khash.h>
#include <mersenne/proto.h>
#include "paxos.h"

struct me_cli_value;
KHASH_MAP_INIT_INT64(submitted, struct me_cli_value *)

struct me_cli_connection {
	struct sockaddr_in *peers;
	size_t peer_count;
	int fd;
	uint64_t last_iid;
	int initial_port;
	struct fbr_mutex mutex;
	uint64_t next_value_id;
	struct fbr_cond_var conn_init_cond;
	int conn_initialized;
	khash_t(submitted) *submitted;
	uuid_t server_id;
	fbr_id_t replay_fiber;
	struct fbr_context *fctx;
	struct ev_loop *loop;
	uint64_t starting_iid;
	me_cli_foreign_func_t foreign_func;
	void *foreign_func_arg;
};

struct value_header {
	uint8_t version;
	uuid_t server_id;
	uint64_t value_id;
} __attribute__((packed));

void me_cli_value_dispose(struct me_cli_value *value)
{
	value->ref--;
	if (0 == value->ref) {
		fbr_cond_destroy(value->conn->fctx, &value->state_changed);
		fbr_mutex_destroy(value->conn->fctx, &value->mutex);
		free(value);
	}
}

struct me_cli_value *me_cli_value_new(struct me_cli_connection *conn)
{
	struct me_cli_value *value;
	value = calloc(1, sizeof(*value));
	value->conn = conn;
	value->value_id = conn->next_value_id++;
	fbr_mutex_init(conn->fctx, &value->mutex);
	fbr_cond_init(conn->fctx, &value->state_changed);
	value->ref++;
	return value;
}

static void record_submitted_value(struct me_cli_connection *conn, struct me_cli_value *value)
{
	khiter_t k;
	int ret;
	k = kh_put(submitted, conn->submitted, value->value_id, &ret);
	switch (ret) {
	case -1: /* if the operation failed */
		errx(EXIT_FAILURE, "kh_put(submitted, ...) failed");
	case 0: /* if the key is present in the hash table */
		errx(EXIT_FAILURE, "kh_put(submitted, ...) attempted to"
			       " overwrite an existing value entry");
	case 1: /* if the bucket is empty (never used) */
	case 2: /* if the element in the bucket has been deleted */
		break;
	default:
		errx(EXIT_FAILURE, "unexpected ret from kh_put: %d", ret);
	};
	kh_value(conn->submitted, k) = value;
}

static struct me_cli_value *get_submitted_value(struct me_cli_connection *conn, uint64_t value_id)
{
	khiter_t k;
	k = kh_get(submitted, conn->submitted, value_id);
	if (k == kh_end(conn->submitted))
		errx(EXIT_FAILURE, "unable to find submitted value by value_id");
	return kh_value(conn->submitted, k);
}

static void remove_submitted_value(struct me_cli_connection *conn, uint64_t value_id)
{
	khiter_t k;
	k = kh_get(submitted, conn->submitted, value_id);
	if (k == kh_end(conn->submitted))
		errx(EXIT_FAILURE, "unable to find submitted value by value_id");
	kh_del(submitted, conn->submitted, k);
}

static void wait_conn_initialize(struct me_cli_connection *conn)
{
	struct fbr_mutex mutex;

	if (conn->conn_initialized)
		return;

	fbr_mutex_init(conn->fctx, &mutex);

	while (!conn->conn_initialized) {
		fbr_mutex_lock(conn->fctx, &mutex);
		fbr_cond_wait(conn->fctx, &conn->conn_init_cond, &mutex);
		fbr_mutex_unlock(conn->fctx, &mutex);
	}

	fbr_mutex_destroy(conn->fctx, &mutex);
}

static void submit_value(struct me_cli_connection *conn,
		struct me_cli_value *value)
{
	msgpack_sbuffer *buf = msgpack_sbuffer_new();
	msgpack_packer pk;
	union me_cli_any me_msg;
	ssize_t retval;
	struct value_header *hdr;

	hdr = malloc(value->data_len + sizeof(*hdr));
	hdr->version = 1;
	memcpy(hdr->server_id, conn->server_id, sizeof(uuid_t));
	hdr->value_id = value->value_id;
	memcpy(hdr + 1, value->data, value->data_len);

	me_msg.m_type = ME_CMT_NEW_VALUE;
	me_msg.new_value.buf = hdr;
	me_msg.new_value.size = value->data_len + sizeof(*hdr);
	memset(me_msg.new_value.request_id, 0x00,
			sizeof(me_msg.new_value.request_id));
	msgpack_packer_init(&pk, buf, msgpack_sbuffer_write);
	retval = me_cli_msg_pack(&pk, &me_msg);
	if (retval)
		errx(EXIT_FAILURE, "failed to pack a message");
	free(hdr);

	assert(buf->size > 0);
	wait_conn_initialize(conn);
retry:
	fbr_log_d(conn->fctx, "aquiering connection lock");
	fbr_mutex_lock(conn->fctx, &conn->mutex);
	fbr_log_d(conn->fctx, "writing %zd bytes to paxos socket", buf->size);
	retval = fbr_write_all(conn->fctx, conn->fd, buf->data, buf->size);
	if (retval < (ssize_t)buf->size) {
		fbr_log_w(conn->fctx, "failed to submit a value to paxos: %s",
				strerror(errno));
		fbr_log_w(conn->fctx, "waiting for reconnect");
		fbr_mutex_unlock(conn->fctx, &conn->mutex);
		fbr_log_d(conn->fctx, "released connection lock");
		wait_conn_initialize(conn);
		goto retry;
	}
	fbr_mutex_unlock(conn->fctx, &conn->mutex);
	fbr_log_d(conn->fctx, "released connection lock");
	msgpack_sbuffer_free(buf);
}

static int value_wait_for_state(struct me_cli_connection *conn,
		struct me_cli_value *value, enum me_cli_value_state desired,
		ev_tstamp timeout)
{
	int retval;
	struct fbr_ev_cond_var cond;
	struct fbr_ev_base *events[2];
	fbr_ev_cond_var_init(conn->fctx, &cond, &value->state_changed,
			&value->mutex);
	events[0] = &cond.ev_base;
	events[1] = NULL;
	fbr_log_d(conn->fctx,
			"awaiting paxos state %s for value 0x%lx, timeout %f",
			me_cli_value_state_strval(desired), value->value_id,
			timeout);
	while (desired != value->state) {
		fbr_mutex_lock(conn->fctx, &value->mutex);
		retval = fbr_ev_wait_to(conn->fctx, events, timeout);
		if (-1 == retval)
			errx(EXIT_FAILURE, "fbr_ev_wait_to failed: %s",
					fbr_strerror(conn->fctx,
						conn->fctx->f_errno));
		if (cond.ev_base.arrived) {
			fbr_mutex_unlock(conn->fctx, &value->mutex);
			continue;
		} else {
			fbr_log_i(conn->fctx, "paxos value 0x%lx timed out",
					value->value_id);
			return -1;
		}
	}
	fbr_log_d(conn->fctx,
			"paxos state has changed to %s for value 0x%lx",
			me_cli_value_state_strval(desired), value->value_id);
	return 0;
}

static void value_wait_for_state_simple(struct me_cli_connection *conn,
		struct me_cli_value *value, enum me_cli_value_state desired)
{
	fbr_log_d(conn->fctx,
			"awaiting paxos state %s for value 0x%lx, no timeout",
			me_cli_value_state_strval(desired), value->value_id);
	while (desired != value->state) {
		fbr_mutex_lock(conn->fctx, &value->mutex);
		fbr_cond_wait(conn->fctx, &value->state_changed, &value->mutex);
		fbr_mutex_unlock(conn->fctx, &value->mutex);
	}
	fbr_log_d(conn->fctx,
			"state has changed to %s for value 0x%lx",
			me_cli_value_state_strval(desired), value->value_id);
}

int me_cli_value_submit(struct me_cli_value *value, ev_tstamp timeout)
{
	int retval;
	struct me_cli_connection *conn = value->conn;

	assert(ME_CLI_PVS_NEW == value->state);
	fbr_log_d(conn->fctx, "submitting value 0x%lx...", value->value_id);
	submit_value(conn, value);
	value->state = ME_CLI_PVS_SUBMITTED;
	value->time_submitted = ev_now(conn->loop);
	value->ref++;
	record_submitted_value(conn, value);
	retval = value_wait_for_state(conn, value, ME_CLI_PVS_ARRIVED, timeout);
	if (retval)
		return retval;
	fbr_log_d(conn->fctx, "paxos value latency: %f",
			ev_now(conn->loop) - value->time_submitted);
	return 0;
}

void me_cli_value_processed(struct me_cli_value *value)
{
	value->state = ME_CLI_PVS_PROCESSED;
	fbr_cond_signal(value->conn->fctx, &value->state_changed);
}

static int load_initial_peer_list(struct me_cli_connection *conn, char **peers,
		unsigned peers_size, unsigned port)
{
	int retval;
	size_t i;
	if (NULL != conn->peers)
		free(conn->peers);
	conn->peers = calloc(peers_size, sizeof(struct sockaddr_in));
	conn->peer_count = peers_size;
	fbr_log_d(conn->fctx, "loaded initial paxos peers list:");
	conn->initial_port = htons(port);
	for (i = 0; i < peers_size; i++) {
		fbr_log_d(conn->fctx, " - %s", peers[i]);
		retval = inet_aton(peers[i], &conn->peers[i].sin_addr);
		if (0 == retval) {
			fbr_log_w(conn->fctx, "inet_aton: %s", strerror(errno));
			return -1;
		}
		conn->peers[i].sin_family = AF_INET;
		conn->peers[i].sin_port = conn->initial_port;
	}
	return 0;
}

static void tcp_nodelay(struct me_cli_connection *conn)
{
	static int yes = 1;
	int retval;
	retval = setsockopt(conn->fd, IPPROTO_TCP, TCP_NODELAY, &yes,
			sizeof(yes));
	if (retval)
		fbr_log_w(conn->fctx, "setsockopt(TCP_NODELAY) failed: %s",
				strerror(errno));
}

static void reconnect_to_any_online_l(struct me_cli_connection *conn, int lock)
{
	int retval;
	unsigned int i;
	unsigned int retries = 0;

	if (lock) {
		fbr_log_d(conn->fctx, "aquiering connection lock");
		fbr_mutex_lock(conn->fctx, &conn->mutex);
	}

	if (0 == conn->peer_count)
		errx(EXIT_FAILURE, "no paxos peers to reconnect");

	fbr_log_d(conn->fctx, "reconnecting to any online instance");
retry:
	if (lock)
	if (retries > 0)
		fbr_log_n(conn->fctx, "connecting to paxos: retry #%d",
				retries);
	for (i = 0; i < conn->peer_count; i++) {
		conn->conn_initialized = 0;
		shutdown(conn->fd, SHUT_RDWR);
		close(conn->fd);

		retval = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
		if (retval < 0)
			err(EXIT_FAILURE, "failed to create a client socket");
		conn->fd = retval;

		if (fbr_fd_nonblock(conn->fctx, conn->fd))
			err(EXIT_FAILURE, "fbr_fd_nonblock failed");

		retval = fbr_connect(conn->fctx, conn->fd,
				(void *)&conn->peers[i],
				sizeof(conn->peers[i]));
		if (-1 == retval) {
			fbr_log_n(conn->fctx, "reconnect to %s failed: %s",
					inet_ntoa(conn->peers[i].sin_addr),
					strerror(errno));
			continue;
		}
		tcp_nodelay(conn);
		fbr_log_i(conn->fctx, "reconnected to %s",
				inet_ntoa(conn->peers[i].sin_addr));
		fbr_mutex_unlock(conn->fctx, &conn->mutex);
		fbr_log_d(conn->fctx, "released connection lock");
		return;
	}
	retries++;
	fbr_sleep(conn->fctx, 1.0);
	goto retry;
}

static void reconnect_to_any_online(struct me_cli_connection *conn)
{
	conn->conn_initialized = 0;
	reconnect_to_any_online_l(conn, 1);
}

static void reconnect(struct me_cli_connection *conn, const char *ip)
{
	int retval;
	struct sockaddr_in addr;

	fbr_log_i(conn->fctx, "we have been redirected to %s", ip);
	fbr_log_d(conn->fctx, "aquiering connection lock");
	fbr_mutex_lock(conn->fctx, &conn->mutex);
	conn->conn_initialized = 0;
	shutdown(conn->fd, SHUT_RDWR);
	close(conn->fd);

	memset(&addr, 0, sizeof(addr));
	addr.sin_family = AF_INET;

	retval = inet_aton(ip, &addr.sin_addr);
	if (0 == retval)
		err(EXIT_FAILURE, "inet_aton failed on string `%s'", ip);

	addr.sin_port = conn->initial_port;

	if ((conn->fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0)
		err(EXIT_FAILURE, "failed to create a client socket");

	if (fbr_fd_nonblock(conn->fctx, conn->fd))
		err(EXIT_FAILURE, "fbr_fd_nonblock failed");

	retval = fbr_connect(conn->fctx, conn->fd, (void *)&addr,
			sizeof(addr));
	if (-1 == retval) {
		fbr_log_i(conn->fctx, "connection to the new leader failed");
		fbr_log_i(conn->fctx, "will try to reconnect to any online");
		fbr_sleep(conn->fctx, 1.0);
		reconnect_to_any_online_l(conn, 0);
		return;
	}

	fbr_log_i(conn->fctx, "connected to new leader (%s)", ip);
	fbr_mutex_unlock(conn->fctx, &conn->mutex);
	fbr_log_d(conn->fctx, "released connection lock");
}

static void load_peer_list(struct me_cli_connection *conn, union me_cli_any *u)
{
	int retval;
	unsigned int i;
	struct me_cli_server_hello *server_hello = &u->server_hello;
	if (NULL != conn->peers)
		free(conn->peers);
	conn->peers = calloc(server_hello->count,
			sizeof(struct sockaddr_in));
	conn->peer_count = server_hello->count;
	fbr_log_i(conn->fctx, "updated peers list:");
	for (i = 0; i < server_hello->count; i++) {
		fbr_log_i(conn->fctx, " - %s", server_hello->peers[i]);
		retval = inet_aton(server_hello->peers[i],
				&conn->peers[i].sin_addr);
		if (0 == retval)
			errx(EXIT_FAILURE, "inet_aton: %s", strerror(errno));
		conn->peers[i].sin_family = AF_INET;
		conn->peers[i].sin_port = conn->initial_port;
	}
	me_cli_msg_free(u);
}

static int send_client_hello(struct me_cli_connection *conn)
{
	msgpack_sbuffer *buf = msgpack_sbuffer_new();
	msgpack_packer pk;
	union me_cli_any me_msg;
	ssize_t retval;

	me_msg.m_type = ME_CMT_CLIENT_HELLO;
	if (conn->last_iid > 0)
		me_msg.client_hello.starting_iid = conn->last_iid + 1;
	else
		me_msg.client_hello.starting_iid = 0;
	msgpack_packer_init(&pk, buf, msgpack_sbuffer_write);
	retval = me_cli_msg_pack(&pk, &me_msg);
	if (retval)
		errx(EXIT_FAILURE, "failed to pack a message");

	fbr_mutex_lock(conn->fctx, &conn->mutex);
	assert(buf->size > 0);
	retval = fbr_write_all(conn->fctx, conn->fd, buf->data, buf->size);
	fbr_mutex_unlock(conn->fctx, &conn->mutex);
	if (retval < (ssize_t)buf->size) {
		msgpack_sbuffer_free(buf);
		return -1;
	}
	msgpack_sbuffer_free(buf);
	return 0;
}

static void replay_value(struct me_cli_connection *conn, void *ptr, size_t size,
		uint64_t iid)
{
	struct me_cli_value *value;
	struct value_header *hdr = ptr;
	ptr = hdr + 1;

	if (0 != uuid_compare(hdr->server_id, conn->server_id)) {
		fbr_log_d(conn->fctx, "received foreign paxos value");
		if (conn->foreign_func)
			conn->foreign_func(conn->fctx, ptr, size - sizeof(*hdr),
					iid, 0, conn->foreign_func_arg);
		return;
	}
	value = get_submitted_value(conn, hdr->value_id);
	fbr_log_d(conn->fctx, "received own paxos value 0x%lx",
			value->value_id);
	if (ME_CLI_PVS_PROCESSED == value->state) {
		/* We are no longer waiting for this value */
		/* So treat it as a foreign value, but set was_self flag */
		fbr_log_d(conn->fctx, "treating paxos value 0x%lx as foreign",
				value->value_id);
		remove_submitted_value(conn, value->value_id);
		if (conn->foreign_func)
			conn->foreign_func(conn->fctx, ptr, size - sizeof(*hdr),
					iid, 1, conn->foreign_func_arg);
		me_cli_value_dispose(value);
		return;
	}
	value->iid = iid;
	value->state = ME_CLI_PVS_ARRIVED;
	fbr_cond_signal(conn->fctx, &value->state_changed);
	value_wait_for_state_simple(conn, value, ME_CLI_PVS_PROCESSED);
	remove_submitted_value(conn, value->value_id);
	me_cli_value_dispose(value);
}

static void replay_fiber_func(struct fbr_context *fctx, void *_arg)
{
	(void)fctx;
	struct me_cli_connection *conn = _arg;
	msgpack_unpacker pac;
	msgpack_unpacked result;
	union me_cli_any u;
	ssize_t retval;
	char *error = NULL;

	fbr_log_d(conn->fctx, "starting paxos log replay from %ld",
			conn->starting_iid);
	conn->last_iid = conn->starting_iid;
	reconnect_to_any_online(conn);

on_redirect:
	retval = send_client_hello(conn);
	if (retval) {
		fbr_log_w(conn->fctx, "send_client_hello failed: %s",
				strerror(errno));
		reconnect_to_any_online(conn);
		goto on_redirect;
	}
	msgpack_unpacker_init(&pac, MSGPACK_UNPACKER_INIT_BUFFER_SIZE);
	msgpack_unpacked_init(&result);

	for (;;) {
		msgpack_unpacker_reserve_buffer(&pac,
				MSGPACK_UNPACKER_RESERVE_SIZE);
		retval = fbr_read(conn->fctx, conn->fd,
				msgpack_unpacker_buffer(&pac),
				msgpack_unpacker_buffer_capacity(&pac));
		if (-1 == retval) {
			fbr_log_w(conn->fctx, "fbr_read failed: %s",
					strerror(errno));
			reconnect_to_any_online(conn);
			goto on_redirect;
		}
		if (0 == retval) {
			fbr_log_w(conn->fctx, "disconnected from paxos log");
			reconnect_to_any_online(conn);
			goto on_redirect;
		}
		msgpack_unpacker_buffer_consumed(&pac, retval);
		while (msgpack_unpacker_next(&pac, &result)) {
			/*
			printf("\n");
			msgpack_object_print(stdout, result.data);
			printf("\n");
			*/
			retval = me_cli_msg_unpack(&result.data, &u,
					0 /* don't alloc memory */, &error);
			if (retval)
				errx(EXIT_FAILURE, "me_cli_msg_unpack: %s",
						error);

			switch (u.m_type) {
			case ME_CMT_ARRIVED_VALUE:
				/* Synchronously apply the value here */
				fbr_log_d(conn->fctx, "replay_value start");
				replay_value(conn, u.arrived_value.buf,
						u.arrived_value.size,
						u.arrived_value.iid);
				fbr_log_d(conn->fctx, "replay_value finish");
				conn->last_iid = u.arrived_value.iid;
				break;
			case ME_CMT_REDIRECT:
				reconnect(conn, u.redirect.ip);
				msgpack_unpacker_reset(&pac);
				goto on_redirect;
			case ME_CMT_SERVER_HELLO:
				load_peer_list(conn, &u);
				conn->conn_initialized = 1;
				fbr_cond_broadcast(conn->fctx,
						&conn->conn_init_cond);
				fbr_log_d(conn->fctx, "initialized connection");
				break;
			default:
				errx(EXIT_FAILURE, "unexpected message");
			}
		}
	}
}

struct me_cli_connection *me_cli_open(struct me_cli_config *config)
{
	fbr_id_t replay_fiber;
	int retval;
	struct me_cli_connection *conn;
	conn = calloc(1, sizeof(*conn));
	if (NULL == conn)
		return NULL;
	conn->fctx = config->fctx;
	conn->loop = config->loop;
	conn->starting_iid = config->starting_iid;
	conn->foreign_func = config->foreign_func;
	conn->foreign_func_arg = config->foreign_func_arg;
	fbr_mutex_init(conn->fctx, &conn->mutex);
	uuid_generate_time(conn->server_id);
	conn->conn_initialized = 0;
	fbr_cond_init(conn->fctx, &conn->conn_init_cond);
	retval = load_initial_peer_list(conn, config->peers, config->peers_size,
			config->port);
	if (retval) {
		fbr_log_w(conn->fctx, "unable to load intial peer list");
		free(conn);
		return NULL;
	}
	conn->submitted = kh_init(submitted);
	replay_fiber = fbr_create(conn->fctx, "paxos/replay", replay_fiber_func,
			conn, 0);
	fbr_transfer(conn->fctx, replay_fiber);
	conn->replay_fiber = replay_fiber;
	return conn;
}

void me_cli_close(struct me_cli_connection *conn)
{
	fbr_reclaim(conn->fctx, conn->replay_fiber);
	free(conn);
}
