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
#include <execinfo.h>
#include <sys/un.h>
#include <uuid/uuid.h>

#include <evfibers/fiber.h>
#include <mersenne/proto.h>
#include <mersenne/md5.h>

#include <cmdline.h>
#include "paxos.h"

#define HASH_FIND_BUFFER(head,buffer,out) \
	HASH_FIND(hh,head,(buffer)->ptr,(buffer)->size,out)
#define HASH_ADD_BUFFER(head,bufferfield,add) \
	HASH_ADD_KEYPTR(hh,head,(add)->bufferfield->ptr,(add)->bufferfield->size,add)

#define VALUE_TO 1.0
#define VALUE_SIZE 1350

struct buffer {
	char *ptr;
	size_t size;
};

struct my_value {
	struct buffer *buf;
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
	struct gengetopt_args_info args_info;
	struct me_cli_connection *conn;
};

static int wait_for_debugger;

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

static struct my_value *new_value(struct client_context *cc)
{
	struct my_value *value;
	MD5_CTX context;
	uint8_t digest[16];
	char valud_id_buf[11];
	unsigned header_length;
	unsigned l;
	value = calloc(1, sizeof(struct my_value));
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
	fbr_log_d(&cc->fbr, "Other value received at iid %lu: ```%.*s'''\n",
			iid, (unsigned)buf->size, buf->ptr);
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
			fbr_log_n(&cc->fbr, "Extra value received for #%d: %d >"
					" %d, last iid was %ld, new is %ld\n",
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
	value->timed_out = 0;
	free(value->buf->ptr);
	free(value->buf);
}

static void run_timeout_cb(EV_P_ ev_timer *w, int revents)
{
	struct client_context *cc = (struct client_context *)w->data;
	client_finished(cc);
	ev_break(cc->loop, EVBREAK_ALL);
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

static void foreign_cb(struct fbr_context *fctx, const uint8_t *data,
		unsigned data_len, uint64_t iid, int was_self, void *arg)
{
	struct client_context *cc = arg;
	struct buffer buffer;
	assert(data);
	buffer.ptr = (char *)data;
	buffer.size = data_len;
	cc->last_iid = iid;
	if (was_self)
		next_value(cc, &buffer, iid);
	else
		report_other(cc, &buffer, iid);
}

void fiber_value(struct fbr_context *fiber_context, void *_arg)
{
	struct client_context *cc;
	struct my_value *value;
	struct me_cli_value *mv;
	int retval;
	cc = fbr_container_of(fiber_context, struct client_context, fbr);
	for (;;) {
		value = new_value(cc);
		record_value(cc, value);
		assert(value->buf);
		assert(value->buf->ptr);
		for (;;) {
			if (value->nsent > 0)
				break;
			mv = me_cli_value_new(cc->conn);
			mv->data = (uint8_t *)value->buf->ptr;
			mv->data_len = value->buf->size;
			value->nsent++;
			ev_now_update(cc->loop);
			retval = me_cli_value_submit(mv,
					cc->args_info.instance_timeout_arg);
			if (0 == retval) {
				value->sent = mv->time_submitted;
				cc->last_iid = mv->iid;
				assert(value->buf);
				assert(value->buf->ptr);
				next_value(cc, value->buf, mv->iid);
				me_cli_value_processed(mv);
				me_cli_value_dispose(mv);
				break;
			}
			me_cli_value_processed(mv);
			me_cli_value_dispose(mv);
			cc->stats.timeouts++;
			assert(value->buf);
			assert(value->buf->ptr);
		}
	}
}

void fiber_main(struct fbr_context *fiber_context, void *_arg)
{
	struct client_context *cc;
	fbr_id_t id, stats;
	struct fbr_buffer fb;
	unsigned i;
	char buf[32];

	cc = fbr_container_of(fiber_context, struct client_context, fbr);

	fbr_buffer_init(&cc->fbr, &fb, 0);
	fbr_set_user_data(&cc->fbr, fbr_self(&cc->fbr), &fb);

	cc->values_size = 0;
	cc->values_alloc = 1024;
	cc->values = calloc(cc->values_alloc, sizeof(void *));
	clock_gettime(CLOCK_MONOTONIC, &cc->stats.started);
	for (i = 0; i < cc->concurrency; i++) {
		snprintf(buf, sizeof(buf), "value_feed(% 2d)", i);
		id = fbr_create(&cc->fbr, buf, fiber_value, cc, 0);
		fbr_transfer(&cc->fbr, id);
	}

	stats = fbr_create(&cc->fbr, "client_stats", fiber_stats, NULL, 0);
	fbr_transfer(&cc->fbr, stats);

	for (;;)
		fbr_sleep(&cc->fbr, 1000);
}

static void sigsegv_handler(int signum)
{
	const int bt_size = 32;
	void *array[bt_size];
	size_t size;
	volatile int set_me_to_zero = 1;

	size = backtrace(array, bt_size);
	fprintf(stderr, "--------------------------------------------------------------\n");
	fprintf(stderr, "Program received signal SIGSEGV, Segmentation fault.\n");
	backtrace_symbols_fd(array, size, STDERR_FILENO);
	fprintf(stderr, "--------------------------------------------------------------\n");
	if(wait_for_debugger) {
		fprintf(stderr, "Pid is %d, waiting for a debugger...\n", getpid());
		while (set_me_to_zero)
			sleep(100);
	} else
		exit(EXIT_FAILURE);
}

static void setup_logging(struct client_context *cc)
{
	enum fbr_log_level log_level = FBR_LOG_INFO;
	switch(cc->args_info.log_level_arg) {
		case log_level_arg_error:
			log_level = FBR_LOG_ERROR;
			break;
		case log_level_arg_warning:
			log_level = FBR_LOG_WARNING;
			break;
		case log_level_arg_notice:
			log_level = FBR_LOG_NOTICE;
			break;
		case log_level_arg_info:
			log_level = FBR_LOG_INFO;
			break;
		case log_level_arg_debug:
			log_level = FBR_LOG_DEBUG;
			break;
		case log_level__NULL:
			errx(EXIT_FAILURE, "invalid log level");
	}
	fbr_set_log_level(&cc->fbr, log_level);
}

int main(int argc, char *argv[]) {
	struct client_context cc;
	struct fbr_mutex mutex;
	ev_timer stop_timer;
	struct cmdline_parser_params *params;
	struct me_cli_config config;

	signal(SIGPIPE, SIG_IGN);
	signal(SIGSEGV, sigsegv_handler);

	srand((unsigned int) time(NULL));
	memset(&cc, 0x00, sizeof(cc));

	params = cmdline_parser_params_create();

	params->initialize = 1;
	params->print_errors = 1;
	params->check_required = 0;

	if (0 == access("strings-client.conf", R_OK)) {
		if(0 != cmdline_parser_config_file("strings-client.conf",
					&cc.args_info, params))
			exit(EXIT_FAILURE + 1);
		params->initialize = 0;
	}
	params->check_required = 1;
	if(0 != cmdline_parser_ext(argc, argv, &cc.args_info, params))
		exit(EXIT_FAILURE + 1);

	cmdline_parser_dump(stdout, &cc.args_info);
	wait_for_debugger = cc.args_info.wait_for_debugger_flag;
	cc.loop = EV_DEFAULT;
	fbr_init(&cc.fbr, cc.loop);
	setup_logging(&cc);

	ev_timer_init(&stop_timer, run_timeout_cb,
			cc.args_info.run_time_arg, 0.0);
	stop_timer.data = &cc;
	ev_timer_start(cc.loop, &stop_timer);
	cc.concurrency = cc.args_info.concurrency_arg;
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

	config.fctx = &cc.fbr;
	config.loop = cc.loop;
	config.peers = cc.args_info.mersenne_ip_arg;
	config.peers_size = cc.args_info.mersenne_ip_given;
	config.port = cc.args_info.port_arg;
	config.starting_iid = 0;
	config.foreign_func = foreign_cb;
	config.foreign_func_arg = &cc;
	cc.conn = me_cli_open(&config);

	cc.main = fbr_create(&cc.fbr, "main", fiber_main, NULL, 0);
	fbr_transfer(&cc.fbr, cc.main);

	ev_loop(cc.loop, 0);

	me_cli_close(cc.conn);
	fbr_destroy(&cc.fbr);
	free(params);

	return 0;
}
