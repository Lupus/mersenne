/********************************************************************

  Copyright 2012 Konstantin Olkhovskiy <lupus@oxnull.net>

  This file is part of Mersenne.

  Mersenne is free software: you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  any later version.

  Mersenne is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with Mersenne.  If not, see <http://www.gnu.org/licenses/>.

 ********************************************************************/

#include <stdio.h>
#include <assert.h>
#include <err.h>
#include <errno.h>
#include <unistd.h>
#include <ev.h>
#include <time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/un.h>
#include <uuid/uuid.h>

#include <evfibers/fiber.h>
#include <mersenne/proto.h>
#include <mersenne/md5.h>

#define HASH_FIND_BUFFER(head,buffer,out) \
	HASH_FIND(hh,head,(buffer)->ptr,(buffer)->size,out)
#define HASH_ADD_BUFFER(head,bufferfield,add) \
	HASH_ADD_KEYPTR(hh,head,(add)->bufferfield->ptr,(add)->bufferfield->size,add)

#define VALUE_TO 1.0
#define VALUE_SIZE 1400

struct buffer {
	char *ptr;
	size_t size;
};

struct my_value {
	struct buffer *buf;
	struct ev_timer timer;
	int timed_out;
	int nreceived;
	int nsent;
	ev_tstamp sent;
	unsigned value_id;
	uint64_t last_iid;
};

struct client_stats {
	int received;
	int timeouts;
	int other;
	int duplicates;
	int extra_values;
	ev_tstamp turnaround;
	struct timespec started;
};

struct client_context {
	int fd;
	struct fbr_cond_var conn_init_cond;
	int conn_initialized;
	struct fbr_context fbr;
	struct ev_loop *loop;
	fbr_id_t mersenne_read;
	fbr_id_t main;
	struct fbr_cond_var timeouts_cond;
	struct my_value **values;
	size_t values_size;
	size_t values_alloc;
	struct fbr_mutex *mutex;
	int concurrency;
	struct client_stats stats;
	char *initial_ipp;
	int initial_port;
	struct sockaddr_in *peers;
	unsigned int peer_count;
	uint64_t last_iid;
	int last_value_id;
	fbr_id_t reader;
};

static struct timespec diff(struct timespec start, struct timespec end)
{
	struct timespec temp;
	if ((end.tv_nsec-start.tv_nsec) < 0) {
		temp.tv_sec = end.tv_sec - start.tv_sec - 1;
		temp.tv_nsec = 1e9 + end.tv_nsec - start.tv_nsec;
	} else {
		temp.tv_sec = end.tv_sec - start.tv_sec;
		temp.tv_nsec = end.tv_nsec - start.tv_nsec;
	}
	return temp;
}

static void randomize_buffer(struct buffer *buffer)
{
	unsigned int i;


	for (i = 0; i < buffer->size; i++) {
		/* ASCII characters 33 to 126 */
		buffer->ptr[i] = rand() % (126 - 33 + 1) + 33;
	}
}

static void wait_conn_initialize(struct client_context *cc)
{
	struct fbr_mutex mutex;

	if (cc->conn_initialized)
		return;

	fbr_mutex_init(&cc->fbr, &mutex);

	while (!cc->conn_initialized) {
		fbr_mutex_lock(&cc->fbr, &mutex);
		fbr_cond_wait(&cc->fbr, &cc->conn_init_cond, &mutex);
		fbr_mutex_unlock(&cc->fbr, &mutex);
	}

	fbr_mutex_destroy(&cc->fbr, &mutex);
}

static void submit_value(struct client_context *cc, struct my_value *value)
{
	msgpack_sbuffer *buf = msgpack_sbuffer_new();
	msgpack_packer pk;
	union me_cli_any me_msg;
	ssize_t retval;

	me_msg.m_type = ME_CMT_NEW_VALUE;
	me_msg.new_value.buf = value->buf->ptr;
	me_msg.new_value.size = value->buf->size;
	uuid_generate_time(me_msg.new_value.request_id);
	msgpack_packer_init(&pk, buf, msgpack_sbuffer_write);
	retval = me_cli_msg_pack(&pk, &me_msg);
	if (retval)
		errx(EXIT_FAILURE, "failed to pack a message");

	wait_conn_initialize(cc);

	fbr_mutex_lock(&cc->fbr, cc->mutex);
	assert(buf->size > 0);
	fbr_log_d(&cc->fbr, "writing value #%d to the socket...",
			value->value_id);
	//printf("trying to write...\n");
	value->nsent++;
	retval = fbr_write_all(&cc->fbr, cc->fd, buf->data, buf->size);
	if (retval < (ssize_t)buf->size) {
		warn("submit_value(): fbr_write_all() failed");
		fbr_mutex_unlock(&cc->fbr, cc->mutex);
		return;
	}
	fbr_log_d(&cc->fbr, "finished writing value to the socket");
	fbr_mutex_unlock(&cc->fbr, cc->mutex);
	msgpack_sbuffer_free(buf);
	ev_now_update(cc->loop);
	value->sent = ev_now(cc->loop);
	/*
	if (1 == value->nsent)
		printf("%f\tsubmitted value #%d\n", ev_now(cc->loop),
				value->value_id);
	else
		printf("%f\tsubmitted value #%d (+%d)\n", ev_now(cc->loop),
				value->value_id, value->nsent - 1);
	*/
}

static void client_finished(struct client_context *cc)
{
	struct timespec current_time;
	struct timespec run_time;
	ev_tstamp elapsed;
	ev_tstamp turnaround;
	clock_gettime(CLOCK_MONOTONIC, &current_time);
	run_time = diff(cc->stats.started, current_time);
	elapsed = run_time.tv_sec + run_time.tv_nsec * (double)1e-9;
	turnaround = cc->stats.turnaround / cc->stats.received;
	printf("Run statistics:\n");
	printf("==========================================\n");
	printf("Total run time: %.3f\n", elapsed);
	printf("Value size (bytes): %d\n", VALUE_SIZE);
	printf("Value timeout (seconds): %f\n", VALUE_TO);
	printf("Value concurrency: %d\n", cc->concurrency);
	printf("Values received: %d\n", cc->stats.received);
	printf("Values timed out: %d\n", cc->stats.timeouts);
	printf("Duplicate values received: %d\n", cc->stats.duplicates);
	printf("Average turnaround time: %f\n", turnaround);
	printf("Other values received: %d\n", cc->stats.other);
	printf("Extra values received: %d\n", cc->stats.extra_values);
	printf("Average throughput (transactions/second): %.3f\n",
			cc->stats.received / elapsed);
	printf("Average throughput (bytes/second): %.3f\n",
			VALUE_SIZE * cc->stats.received / elapsed);
	printf("==========================================\n");
}

static void value_timeout_cb(EV_P_ ev_timer *w, int revents)
{
	struct client_context *cc = (struct client_context *)w->data;
	struct my_value *value;
	value = fbr_container_of(w, struct my_value, timer);
	value->timed_out = 1;
	fbr_cond_signal(&cc->fbr, &cc->timeouts_cond);
}


static struct my_value *new_value(struct client_context *cc)
{
	struct my_value *value;
	MD5_CTX context;
	uint8_t digest[16];
	char valud_id_buf[11];
	unsigned header_length;
	unsigned l;
	value = calloc(1, sizeof(struct my_value));
	ev_timer_init(&value->timer, value_timeout_cb, VALUE_TO, VALUE_TO);
	value->timer.data = cc;
	value->buf = malloc(sizeof(struct buffer));
	value->buf->ptr = calloc(1, VALUE_SIZE + 1);
	value->buf->size = VALUE_SIZE;
	value->value_id = cc->last_value_id++;
	randomize_buffer(value->buf);

	sprintf(valud_id_buf, "%010d", value->value_id);
	header_length = 10 + 32 + 4;
        MD5_Init(&context);
        MD5_Update(&context, (uint8_t *)&value->value_id,
			sizeof(value->value_id));
        MD5_Update(&context, (uint8_t *)value->buf->ptr + header_length,
			VALUE_SIZE - header_length);
        MD5_Final(digest, &context);
	l = sprintf(value->buf->ptr, "{%010d,%016lx%016lx}",
			value->value_id,
			*((int64_t *)digest), *((int64_t *)digest + 1));
	value->buf->ptr[l] = ' ';
	return value;
}

static void report_other(struct client_context *cc, struct buffer *buf,
		uint64_t iid)
{
	printf("Other value received at iid %lu: ```%.*s'''\n", iid,
					(unsigned)buf->size, buf->ptr);
	cc->stats.other++;
}

static void record_value(struct client_context *cc, struct my_value *value)
{
	if (cc->values_size == cc->values_alloc) {
		cc->values_alloc *= 2;
		cc->values = realloc(cc->values,
				cc->values_alloc * sizeof(void *));
	}
	cc->values[value->value_id] = value;
	cc->values_size++;
}

static void next_value(struct client_context *cc, struct buffer *buf,
		uint64_t iid)
{
	struct my_value *value = NULL;
	MD5_CTX context;
	uint8_t digest[16];
	uint8_t test_digest[16];
	unsigned header_length;
	unsigned l;
	unsigned value_id;

	//printf("buf: '''%.*s'''\n", (unsigned)buf->size, buf->ptr);
	l = sscanf(buf->ptr, "{%010d,%016lx%016lx}",
			&value_id,
			(int64_t *)test_digest, (int64_t *)test_digest + 1);
	if (3 != l) {
		report_other(cc, buf, iid);
		return;
	}
	header_length = 10 + 32 + 4;
        MD5_Init(&context);
        MD5_Update(&context, (uint8_t *)&value_id, sizeof(value_id));
        MD5_Update(&context, (uint8_t *)buf->ptr + header_length,
			buf->size - header_length);
        MD5_Final(digest, &context);

	if (0 != memcmp(digest, test_digest, sizeof(test_digest))) {
		report_other(cc, buf, iid);
		return;
	}

	if (value_id > cc->values_size) {
		printf("Got value id from the future: %d > %zd (values size)\n",
				value_id, cc->values_size);
		exit(1);
	}
	value = cc->values[value_id];
	assert(value);
	if (value->nreceived > 0) {
		cc->stats.duplicates++;
		value->nreceived++;
		if (value->nreceived > value->nsent) {
			printf("Extra value received for #%d: %d > %d, "
					"last iid was %ld, new is %ld\n",
					value->value_id, value->nreceived,
					value->nsent, value->last_iid, iid);
			cc->stats.extra_values++;
		}
		value->last_iid = iid;
		/*
		printf("%f\treceived value #%d (+%d)\n", ev_now(cc->loop),
				value->value_id,
				value->nreceived - 1);
		*/
		return;
	}
	/*
	printf("%f\treceived value #%d\n", ev_now(cc->loop),
			value->value_id);
	*/
	value->nreceived = 1;
	value->last_iid = iid;
	cc->stats.received++;
	ev_now_update(cc->loop);
	cc->stats.turnaround += ev_now(cc->loop) - value->sent;
	ev_timer_stop(cc->loop, &value->timer);
	value->timed_out = 0;
	free(value->buf->ptr);
	free(value->buf);

	value = new_value(cc);
	record_value(cc, value);
	submit_value(cc, value);
	ev_timer_start(cc->loop, &value->timer);
}

static void run_timeout_cb(EV_P_ ev_timer *w, int revents)
{
	struct client_context *cc = (struct client_context *)w->data;
	client_finished(cc);
	ev_break(cc->loop, EVBREAK_ALL);
}

static void tcp_nodelay(int fd)
{
	static int yes = 1;
	int retval;
	retval = setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));
	if (retval)
		err(EXIT_FAILURE, "setsockopt(TCP_NODELAY)");
}

static void reconnect_to_any_online_l(struct client_context *cc, int lock)
{
	int retval;
	unsigned int i;
	unsigned int retries = 0;
	const char *saddr;

	if (lock)
		fbr_mutex_lock(&cc->fbr, cc->mutex);

	if (0 == cc->peer_count)
		errx(EXIT_FAILURE, "No peers to reconnect");

retry:
	for (i = 0; i < cc->peer_count; i++) {
		cc->conn_initialized = 0;
		shutdown(cc->fd, SHUT_RDWR);
		close(cc->fd);

		if ((cc->fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0)
			err(EXIT_FAILURE, "failed to create a client socket");

		if (fbr_fd_nonblock(&cc->fbr, cc->fd))
			err(EXIT_FAILURE, "fbr_fd_nonblock");

		saddr = inet_ntoa(cc->peers[i].sin_addr);
		fbr_log_d(&cc->fbr, "connecting to %s...", saddr);
		retval = fbr_connect(&cc->fbr, cc->fd, (void *)&cc->peers[i],
				sizeof(cc->peers[i]));
		if (-1 == retval) {
			fbr_log_d(&cc->fbr, "connection to %s failed: %s",
					saddr, strerror(errno));
			continue;
		}
		fbr_log_d(&cc->fbr, "connected to %s", saddr);
		tcp_nodelay(cc->fd);
		fbr_mutex_unlock(&cc->fbr, cc->mutex);
		return;
	}
	retries++;
	if (retries > 5)
		errx(EXIT_FAILURE, "unable to reconnect to any online instance");
	fbr_sleep(&cc->fbr, 1.0);
	goto retry;
}

static void reconnect_to_any_online(struct client_context *cc)
{
	cc->conn_initialized = 0;
	reconnect_to_any_online_l(cc, 1);
}

static void reconnect(struct client_context *cc, const char *ip)
{
	int retval;
	struct sockaddr_in addr;

	fbr_mutex_lock(&cc->fbr, cc->mutex);
	cc->conn_initialized = 0;
	shutdown(cc->fd, SHUT_RDWR);
	close(cc->fd);

	memset(&addr, 0, sizeof(addr));
	addr.sin_family = AF_INET;

	retval = inet_aton(ip, &addr.sin_addr);
	if (0 == retval)
		errx(EXIT_FAILURE, "inet_aton: %s", strerror(errno));

	addr.sin_port = cc->initial_port;

	if ((cc->fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0)
		err(EXIT_FAILURE, "failed to create a client socket");

	if (fbr_fd_nonblock(&cc->fbr, cc->fd))
		err(EXIT_FAILURE, "fbr_fd_nonblock");

	fbr_log_d(&cc->fbr, "connecting to %s...", inet_ntoa(addr.sin_addr));
	retval = fbr_connect(&cc->fbr, cc->fd, (void *)&addr, sizeof(addr));
	if (-1 == retval) {
		fbr_log_d(&cc->fbr, "connection to %s failed: %s",
				inet_ntoa(addr.sin_addr),
				strerror(errno));
		fbr_sleep(&cc->fbr, 0.5);
		reconnect_to_any_online_l(cc, 0);
		return;
	}

	tcp_nodelay(cc->fd);

	fbr_log_d(&cc->fbr, "connected to %s", inet_ntoa(addr.sin_addr));
	fbr_mutex_unlock(&cc->fbr, cc->mutex);
}

static void load_peer_list(struct client_context *cc, union me_cli_any *u)
{
	int retval;
	unsigned int i;
	struct me_cli_server_hello *server_hello = &u->server_hello;
	if (NULL != cc->peers)
		free(cc->peers);
	cc->peers = calloc(server_hello->count, sizeof(struct sockaddr_in));
	cc->peer_count = server_hello->count;
	fbr_log_d(&cc->fbr, "updated peers list:");
	for (i = 0; i < server_hello->count; i++) {
		fbr_log_d(&cc->fbr, " - %s", server_hello->peers[i]);
		retval = inet_aton(server_hello->peers[i],
				&cc->peers[i].sin_addr);
		if (0 == retval)
			errx(EXIT_FAILURE, "inet_aton: %s", strerror(errno));
		cc->peers[i].sin_family = AF_INET;
		cc->peers[i].sin_port = cc->initial_port;
	}
}

static int send_client_hello(struct client_context *cc)
{
	msgpack_sbuffer *buf = msgpack_sbuffer_new();
	msgpack_packer pk;
	union me_cli_any me_msg;
	ssize_t retval;

	me_msg.m_type = ME_CMT_CLIENT_HELLO;
	me_msg.client_hello.starting_iid = cc->last_iid + 1;
	fbr_log_d(&cc->fbr, "sending client hello, starting iid = %ld",
			me_msg.client_hello.starting_iid);
	msgpack_packer_init(&pk, buf, msgpack_sbuffer_write);
	retval = me_cli_msg_pack(&pk, &me_msg);
	if (retval)
		errx(EXIT_FAILURE, "failed to pack a message");

	fbr_mutex_lock(&cc->fbr, cc->mutex);
	fbr_log_d(&cc->fbr, "writing client hello to the socket...");
	assert(buf->size > 0);
	retval = fbr_write_all(&cc->fbr, cc->fd, buf->data, buf->size);
	fbr_log_d(&cc->fbr, "finished writing client hello to the socket");
	fbr_mutex_unlock(&cc->fbr, cc->mutex);
	if (retval < (ssize_t)buf->size) {
		msgpack_sbuffer_free(buf);
		return -1;
	}
	msgpack_sbuffer_free(buf);
	return 0;
}

void fiber_reader(struct fbr_context *fiber_context, void *_arg)
{
	struct client_context *cc;
	struct buffer buffer;
	msgpack_unpacker pac;
	msgpack_unpacked result;
	union me_cli_any u;
	ssize_t retval;
	char *error = NULL;

	cc = fbr_container_of(fiber_context, struct client_context, fbr);

on_redirect:
	retval = send_client_hello(cc);
	if (retval) {
		warn("send_client_hello");
		reconnect_to_any_online(cc);
		goto on_redirect;
	}
	msgpack_unpacker_init(&pac, MSGPACK_UNPACKER_INIT_BUFFER_SIZE);
	msgpack_unpacked_init(&result);

	for (;;) {
		msgpack_unpacker_reserve_buffer(&pac,
				MSGPACK_UNPACKER_RESERVE_SIZE);
		retval = fbr_read(&cc->fbr, cc->fd,
				msgpack_unpacker_buffer(&pac),
				msgpack_unpacker_buffer_capacity(&pac));
		if (-1 == retval) {
			warn("fbr_read");
			reconnect_to_any_online(cc);
			goto on_redirect;
		}
		if (0 == retval) {
			warnx("disconnected from mersenne");
			reconnect_to_any_online(cc);
			goto on_redirect;
		}
		msgpack_unpacker_buffer_consumed(&pac, retval);
		while (msgpack_unpacker_next(&pac, &result)) {
			retval = me_cli_msg_unpack(&result.data, &u,
					0 /* don't alloc memory */, &error);
			if (retval)
				errx(EXIT_FAILURE, "me_cli_msg_unpack: %s",
						error);

			fbr_log_d(&cc->fbr, "got a message");
			switch (u.m_type) {
			case ME_CMT_ARRIVED_VALUE:
				buffer.ptr = u.arrived_value.buf;
				buffer.size = u.arrived_value.size;
				cc->last_iid = u.arrived_value.iid;
				/*
				printf("Instance #%ld has arrived\n",
						u.arrived_value.iid);
				*/
				next_value(cc, &buffer, u.arrived_value.iid);
				break;
			case ME_CMT_REDIRECT:
				fbr_log_d(&cc->fbr, "got redirected to %s",
						u.redirect.ip);
				reconnect(cc, u.redirect.ip);
				msgpack_unpacker_reset(&pac);
				goto on_redirect;
			case ME_CMT_SERVER_HELLO:
				load_peer_list(cc, &u);
				cc->conn_initialized = 1;
				fbr_cond_broadcast(&cc->fbr,
						&cc->conn_init_cond);
				fbr_log_d(&cc->fbr, "initialized connection");
				break;
			default:
				errx(EXIT_FAILURE, "unexpected message");
			}
		}
	}
}

static void start_reader_fiber(struct client_context *cc)
{
	fbr_log_d(&cc->fbr, "starting socket reader fiber");
	cc->reader = fbr_create(&cc->fbr, "mersenne_read", fiber_reader,
			NULL, 0);
	fbr_transfer(&cc->fbr, cc->reader);
}

void fiber_stats(struct fbr_context *fiber_context, void *_arg)
{
	struct client_context *cc;
	struct client_stats last_stats;
	int current, last;
	ev_tstamp interval = 5.0;
	double tx_per_second;

	cc = fbr_container_of(fiber_context, struct client_context, fbr);
	memset(&last_stats, 0x00, sizeof(last_stats));
	for (;;) {
		fbr_sleep(fiber_context, interval);
		current = cc->stats.other + cc->stats.received;
		//printf("[STATS] Current = %d\n", current);
		last = last_stats.other + last_stats.received;
		//printf("[STATS] Last = %d\n", last);
		tx_per_second = (current - last) / interval;
		printf("[STATS] %.3f transactions per second, values received:"
				" %d, timed out: %d, duplicates: %d, extra: %d, other: %d\n",
				tx_per_second,
				cc->stats.received,
				cc->stats.timeouts,
				cc->stats.duplicates,
				cc->stats.extra_values,
				cc->stats.other);
		last_stats = cc->stats;
	}
}

static void parse_ipp(const char *ipp_buf, struct sockaddr_in *addr)
{
	int port;
	int retval;
	char *colon = strchr(ipp_buf, ':');

	if (colon == NULL)
		errx(EXIT_FAILURE, "Endpoint spec should be in format"
					" xxx.xxx.xxx.xxx:yyyyy");
	*colon = 0;

	memset(addr, 0, sizeof(*addr));
	addr->sin_family = AF_INET;

	if (strcmp(ipp_buf, "0.0.0.0") != 0) {
		retval = inet_aton(ipp_buf, &addr->sin_addr);
		if (0 == retval)
			errx(EXIT_FAILURE, "inet_aton: %s", strerror(errno));
	} else {
		addr->sin_addr.s_addr = INADDR_ANY;
	}

	port = atoi(colon + 1); /* port is next after ':' */
	if (port <= 0 || port >= 0xffff)
		errx(EXIT_FAILURE, "bad port: %s", colon + 1);

	addr->sin_port = htons(port);
}

void set_up_socket(struct client_context *cc)
{
	int retval;
	struct sockaddr_in addr;
	parse_ipp(cc->initial_ipp, &addr);
	cc->initial_port = addr.sin_port;
	if ((cc->fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0)
		err(EXIT_FAILURE, "failed to create a client socket");

	fbr_fd_nonblock(&cc->fbr, cc->fd);
	tcp_nodelay(cc->fd);
	fbr_log_d(&cc->fbr, "connecting to %s...", inet_ntoa(addr.sin_addr));
	retval = fbr_connect(&cc->fbr, cc->fd, (void *)&addr, sizeof(addr));
	if (-1 == retval)
		err(EXIT_FAILURE, "fbr_connect");
	fbr_log_d(&cc->fbr, "connected to %s", inet_ntoa(addr.sin_addr));
	start_reader_fiber(cc);
}

static void init_values(struct client_context *cc)
{
	int i;
	struct my_value *value;
	cc->values_size = 0;
	cc->values_alloc = 1024;
	cc->values = calloc(cc->values_alloc, sizeof(void *));
	clock_gettime(CLOCK_MONOTONIC, &cc->stats.started);
	for (i = 0; i < cc->concurrency; i++) {
		value = new_value(cc);
		record_value(cc, value);
		submit_value(cc, value);
		ev_timer_start(cc->loop, &value->timer);
	}
}

void fiber_main(struct fbr_context *fiber_context, void *_arg)
{
	struct client_context *cc;
	fbr_id_t stats;
	struct fbr_buffer fb;
	struct fbr_mutex mutex;
	struct my_value *v;
	unsigned i;

	cc = fbr_container_of(fiber_context, struct client_context, fbr);

	fbr_buffer_init(&cc->fbr, &fb, 0);
	fbr_set_user_data(&cc->fbr, fbr_self(&cc->fbr), &fb);

	set_up_socket(cc);
	init_values(cc);

	fbr_mutex_init(&cc->fbr, &mutex);

	stats = fbr_create(&cc->fbr, "client_stats", fiber_stats, NULL, 0);
	fbr_transfer(&cc->fbr, stats);

	for (;;) {
		fbr_mutex_lock(&cc->fbr, &mutex);
		fbr_cond_wait(&cc->fbr, &cc->timeouts_cond, &mutex);
		fbr_mutex_unlock(&cc->fbr, &mutex);

		for (i = 0; i < cc->values_size; i++) {
			v = cc->values[i];
			if (0 == v->timed_out)
				continue;
			if (v->nreceived > 0)
				continue;
			/*
			printf("%f\ttimed out value #%d, total %d\n",
					ev_now(cc->loop),
					v->value_id, cc->stats.timeouts + 1);
			*/
			cc->stats.timeouts++;
			submit_value(cc, v);
			ev_timer_again(cc->loop, &v->timer);
			v->timed_out = 0;
		}
	}
}

int main(int argc, char *argv[]) {
	struct client_context cc;
	struct fbr_mutex mutex;
	ev_timer stop_timer;

	signal(SIGPIPE, SIG_IGN);

	srand((unsigned int) time(NULL));
	memset(&cc, 0x00, sizeof(cc));
	cc.loop = EV_DEFAULT;
	fbr_init(&cc.fbr, cc.loop);
	fbr_set_log_level(&cc.fbr, FBR_LOG_INFO);

	ev_timer_init(&stop_timer, run_timeout_cb, atoi(argv[3]), 0.0);
	stop_timer.data = &cc;
	ev_timer_start(cc.loop, &stop_timer);
	assert(4 == argc);
	cc.initial_ipp = argv[1];
	cc.concurrency = atoi(argv[2]);
	cc.last_value_id = 0;
	cc.stats.received = 0;
	cc.stats.timeouts = 0;
	cc.stats.duplicates = 0;
	cc.stats.extra_values = 0;
	cc.stats.other = 0;
	cc.last_iid = 0;
	cc.conn_initialized = 0;
	fbr_mutex_init(&cc.fbr, &mutex);
	fbr_cond_init(&cc.fbr, &cc.timeouts_cond);
	fbr_cond_init(&cc.fbr, &cc.conn_init_cond);
	cc.mutex = &mutex;

	cc.main = fbr_create(&cc.fbr, "main", fiber_main, NULL, 0);
	fbr_transfer(&cc.fbr, cc.main);

	ev_loop(cc.loop, 0);

	fbr_destroy(&cc.fbr);

	return 0;
}
