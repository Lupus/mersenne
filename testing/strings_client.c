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
#include <unistd.h>
#include <ev.h>
#include <uthash.h>
#include <time.h>

#include <evfibers/fiber.h>
#include <mersenne/cl_protocol.h>
#include <mersenne/me_protocol.h>
#include <mersenne/util.h>
#include <mersenne/sharedmem.h>

#define HASH_FIND_BUFFER(head,buffer,out) \
	HASH_FIND(hh,head,(buffer)->ptr,(buffer)->size1,out)
#define HASH_ADD_BUFFER(head,bufferfield,add) \
	HASH_ADD_KEYPTR(hh,head,(add)->bufferfield->ptr,(add)->bufferfield->size1,add)

#define VALUE_TO 1.0
#define VALUE_SIZE 1400

struct my_value {
	struct buffer *v;
	struct ev_timer timer;
	UT_hash_handle hh;
};

struct client_stats {
	int total;
	int received;
	int timeouts;
	int other;
	struct timespec started;
};

struct client_context {
	int fd;
	struct fbr_context fbr;
	struct ev_loop *loop;
	struct fbr_fiber *mersenne_read;
	struct fbr_fiber *main;
	struct my_value *values;
	struct fbr_mutex *mutex;
	int concurrency;
	struct client_stats stats;
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


	for (i = 0; i < buffer->size1; i++) {
		/* ASCII characters 33 to 126 */
		buffer->ptr[i] = rand() % (126 - 33 + 1) + 33;
	}
}

static void submit_value(struct client_context *cc, struct my_value *value)
{
	struct cl_message msg;
	XDR xdrs;
	char buf[ME_MAX_XDR_MESSAGE_LEN];
	uint16_t size, send_size;
	ssize_t retval;

	msg.type = CL_NEW_VALUE;
	msg.cl_message_u.new_value.value = value->v;

	xdrmem_create(&xdrs, buf, ME_MAX_XDR_MESSAGE_LEN, XDR_ENCODE);
	if(!xdr_cl_message(&xdrs, &msg))
		err(EXIT_FAILURE, "unable to encode a client message");
	size = xdr_getpos(&xdrs);
	xdr_destroy(&xdrs);

	fbr_mutex_lock(&cc->fbr, cc->mutex);
	send_size = htons(size);
	retval = fbr_write_all(&cc->fbr, cc->fd, &send_size, sizeof(uint16_t));
	if(-1 == retval)
		err(EXIT_FAILURE, "fbr_write_all");
	retval = fbr_write_all(&cc->fbr, cc->fd, buf, size);
	if(-1 == retval)
		err(EXIT_FAILURE, "fbr_write_all");
	fbr_mutex_unlock(&cc->fbr, cc->mutex);
}

static void client_finished(struct client_context *cc)
{
	struct timespec current_time;
	struct timespec run_time;
	double elapsed;
	clock_gettime(CLOCK_MONOTONIC, &current_time);
	run_time = diff(cc->stats.started, current_time);
	elapsed = run_time.tv_sec + run_time.tv_nsec * (double)1e-9;
	printf("Run statistics:\n");
	printf("==========================================\n");
	printf("Total run time: %.3f\n", elapsed);
	printf("Value size (bytes): %d\n", VALUE_SIZE);
	printf("Value timeout (seconds): %f\n", VALUE_TO);
	printf("Value concurrency: %d\n", cc->concurrency);
	printf("Values received: %d\n", cc->stats.received);
	printf("Values timed out: %d\n", cc->stats.timeouts);
	printf("Other values received: %d\n", cc->stats.other);
	printf("Average throughput (transactions/second): %.3f\n", cc->stats.received / elapsed);
	printf("Average throughput (bytes/second): %.3f\n", VALUE_SIZE * cc->stats.received / elapsed);
	printf("==========================================\n");
}

static void next_value(struct client_context *cc, struct buffer *buf)
{
	struct my_value *value = NULL;

	HASH_FIND_BUFFER(cc->values, buf, value);
	if(NULL == value) {
		cc->stats.other++;
		return;
	}
	HASH_DEL(cc->values, value);
	cc->stats.received++;
	if(cc->stats.received == cc->stats.total) {
		client_finished(cc);
		ev_break(cc->loop, EVBREAK_ALL);
		return;
	}
	randomize_buffer(value->v);
	HASH_ADD_BUFFER(cc->values, v, value);
	submit_value(cc, value);
	ev_timer_again(cc->loop, &value->timer);
}

static void value_timeout_cb (EV_P_ ev_timer *w, int revents)
{
	struct client_context *cc = (struct client_context *)w->data;
	struct my_value *value;
	value = container_of(w, struct my_value, timer);
	fbr_call(&cc->fbr, cc->main, 1, fbr_arg_v(value));
}

void fiber_reader(struct fbr_context *fiber_context)
{
	struct client_context *cc;
	char buf[ME_MAX_XDR_MESSAGE_LEN];
	struct cl_message msg;
	ssize_t retval;
	uint16_t size;
	XDR xdrs;
	struct buffer *value;
	//char str_buf[ME_MAX_XDR_MESSAGE_LEN];

	cc = container_of(fiber_context, struct client_context, fbr);
	fbr_next_call_info(&cc->fbr, NULL);

	for(;;) {
		retval = fbr_read_all(&cc->fbr, cc->fd, &size, sizeof(uint16_t));
		if(-1 == retval)
			err(EXIT_FAILURE, "fbr_read_all");
		if(0 == retval) goto conn_finish;
		size = ntohs(size);
		retval = fbr_read_all(&cc->fbr, cc->fd, buf, size);
		if(-1 == retval)
			err(EXIT_FAILURE, "fbr_read_all");
		if(0 == retval) goto conn_finish;

		xdrmem_create(&xdrs, buf, size, XDR_DECODE);
		memset(&msg, 0, sizeof(msg));
		if(!xdr_cl_message(&xdrs, &msg))
			errx(EXIT_FAILURE, "xdr_cl_message: unable to decode");
		if(CL_LEARNED_VALUE != msg.type)
			errx(EXIT_FAILURE, "Mersenne has sent unexpected message");
		value = msg.cl_message_u.learned_value.value;
		/*
		snprintf(str_buf, value->size1 + 1, "%s", value->ptr);
		printf("Mersenne has closed an instance #%lu with value: ``%s''\n",
				msg.cl_message_u.learned_value.i, str_buf);
				*/
		next_value(cc, value);
		xdr_free((xdrproc_t)xdr_cl_message, (caddr_t)&msg);
		xdr_destroy(&xdrs);
	}
conn_finish:
	close(cc->fd);
	errx(EXIT_FAILURE, "disconnected from mersenne");
}

void fiber_stats(struct fbr_context *fiber_context)
{
	struct client_context *cc;
	struct client_stats last_stats;
	int current, last;
	ev_tstamp interval = 5.0;
	double tx_per_second;

	cc = container_of(fiber_context, struct client_context, fbr);
	fbr_next_call_info(&cc->fbr, NULL);
	memset(&last_stats, 0x00, sizeof(last_stats));
	for(;;) {
		fbr_sleep(fiber_context, interval);
		current = cc->stats.other + cc->stats.received;
		//printf("[STATS] Current = %d\n", current);
		last = last_stats.other + last_stats.received;
		//printf("[STATS] Last = %d\n", last);
		tx_per_second = (current - last) / interval;
		printf("[STATS] %.3f transactions per second...\n", tx_per_second);
		last_stats = cc->stats;
	}
}

void set_up_socket(struct client_context *cc)
{
	struct sockaddr_un addr;
	char *rendezvous = getenv("UNIX_SOCKET");

	if ((cc->fd = socket(AF_UNIX, SOCK_STREAM, 0)) < 0)
		err(EXIT_FAILURE, "failed to create a client socket");

	make_socket_non_blocking(cc->fd);

	addr.sun_family = AF_UNIX;
	strcpy(addr.sun_path, rendezvous);
	if (-1 == connect(cc->fd, (struct sockaddr *)&addr, sizeof(addr)))
		err(EXIT_FAILURE, "connect to unix socket failed");
}

static void init_values(struct client_context *cc)
{
	int i;
	struct my_value *values = calloc(sizeof(struct my_value), cc->concurrency);
	cc->values = NULL;
	clock_gettime(CLOCK_MONOTONIC, &cc->stats.started);
	for(i = 0; i < cc->concurrency; i++) {
		ev_timer_init(&values[i].timer, value_timeout_cb, VALUE_TO, VALUE_TO);
		values[i].timer.data = cc;
		values[i].v = malloc(sizeof(struct buffer));
		buf_init(values[i].v, VALUE_SIZE);
		randomize_buffer(values[i].v);
		HASH_ADD_BUFFER(cc->values, v, values + i);
		submit_value(cc, values + i);
		ev_timer_start(cc->loop, &(values[i].timer));
	}
}


void fiber_main(struct fbr_context *fiber_context)
{
	struct client_context *cc;
	struct fbr_call_info *info = NULL;
	struct my_value *value;
	struct fbr_fiber *reader, *stats;

	cc = container_of(fiber_context, struct client_context, fbr);

	set_up_socket(cc);
	init_values(cc);

	reader = fbr_create(&cc->fbr, "mersenne_read", fiber_reader, 0);
	fbr_call(&cc->fbr, reader, 0);
	stats = fbr_create(&cc->fbr, "client_stats", fiber_stats, 0);
	fbr_call(&cc->fbr, stats, 0);

	fbr_next_call_info(&cc->fbr, NULL);

	for(;;) {
		fbr_yield(&cc->fbr);
		while(fbr_next_call_info(&cc->fbr, &info)) {
			value = info->argv[0].v;
			cc->stats.timeouts++;
			submit_value(cc, value);
			ev_timer_again(cc->loop, &value->timer);
		}
	}
}

int main(int argc, char *argv[]) {
	struct client_context cc;

	signal(SIGPIPE, SIG_IGN);

	srand((unsigned int) time(NULL));
	cc.loop = EV_DEFAULT;
	fbr_init(&cc.fbr, cc.loop);

	assert(3 == argc);
	cc.concurrency = atoi(argv[1]);
	cc.stats.total = atoi(argv[2]);
	cc.stats.received = 0;
	cc.stats.timeouts = 0;
	cc.stats.other = 0;
	cc.mutex = fbr_mutex_create(&cc.fbr);

	cc.main = fbr_create(&cc.fbr, "main", fiber_main, 0);
	fbr_call(&cc.fbr, cc.main, 0);

	ev_loop(cc.loop, 0);

	fbr_destroy(&cc.fbr);

	return 0;
}
