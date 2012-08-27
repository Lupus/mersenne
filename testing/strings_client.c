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

#define HASH_FIND_BUFFER(head,buffer,out) \
	HASH_FIND(hh,head,(buffer)->ptr,(buffer)->size1,out)
#define HASH_ADD_BUFFER(head,bufferfield,add) \
	HASH_ADD_KEYPTR(hh,head,(add)->bufferfield.ptr,(add)->bufferfield.size1,add)

#define VALUE_TO 10.0
#define VALUE_SIZE 1400

struct my_value {
	struct buffer v;
	struct ev_timer timer;
	UT_hash_handle hh;
};

struct client_context {
	int fd;
	struct fbr_context fbr;
	struct ev_loop *loop;
	struct fbr_fiber *mersenne_read;
	struct fbr_fiber *main;
	struct my_value *values;
	int concurrency;
	struct {
		int total;
		int received;
		int timeouts;
		int other;
		time_t started;
	} stats;
};

static void randomize_buffer(struct buffer *buffer)
{
	unsigned int i;


	for (i = 0; i < buffer->size1; i++) {
		/* ASCII characters 33 to 126 */
		buffer->ptr[i] = rand() % (126 - 33 + 1) + 33;
	}
	buffer->state = BS_FULL;
}

static void submit_value(struct client_context *cc, struct my_value *value)
{
	struct cl_message msg;
	struct buffer *msg_value;
	XDR xdrs;
	char buf[ME_MAX_XDR_MESSAGE_LEN];
	uint16_t size, send_size;
	ssize_t retval;

	msg.type = CL_NEW_VALUE;
	msg_value = &msg.cl_message_u.new_value.value;
	buf_init(msg_value, NULL, 0, BS_EMPTY);
	buf_share(msg_value, &value->v);
	
	xdrmem_create(&xdrs, buf, ME_MAX_XDR_MESSAGE_LEN, XDR_ENCODE);
	if(!xdr_cl_message(&xdrs, &msg))
		err(EXIT_FAILURE, "unable to encode a client message");
	size = xdr_getpos(&xdrs);
	xdr_destroy(&xdrs);

	send_size = htons(size);
	retval = fbr_write_all(&cc->fbr, cc->fd, &send_size, sizeof(uint16_t));
	if(-1 == retval)
		err(EXIT_FAILURE, "fbr_write_all");
	retval = fbr_write_all(&cc->fbr, cc->fd, buf, size);
	if(-1 == retval)
		err(EXIT_FAILURE, "fbr_write_all");
	//snprintf(buf, value->v.size1 + 1, "%s", value->v.ptr);
	//printf("Submitted value: ``%s''\n", buf);
	ev_timer_start(cc->loop, &value->timer);
}

static void client_finished(struct client_context *cc)
{
	time_t run_time = time(NULL) - cc->stats.started;
	printf("Run statistics:\n");
	printf("==========================================\n");
	printf("Total run time: %ld\n", run_time);
	printf("Value size (bytes): %d\n", VALUE_SIZE);
	printf("Value timeout (seconds): %f\n", VALUE_TO);
	printf("Value concurrency: %d\n", cc->concurrency);
	printf("Values received: %d\n", cc->stats.received);
	printf("Values timed out: %d\n", cc->stats.timeouts);
	printf("Other values received: %d\n", cc->stats.other);
	printf("Average throughput (bytes/second): %.2f\n", (float)VALUE_SIZE * cc->stats.received / run_time);
	printf("==========================================\n");
	exit(0);
}

static void next_value(struct client_context *cc, struct buffer *buf)
{
	struct my_value *value = NULL;

	HASH_FIND_BUFFER(cc->values, buf, value);
	if(NULL == value) {
		cc->stats.other++;
		return;
	}
	ev_timer_stop(cc->loop, &value->timer);
	HASH_DEL(cc->values, value);
	cc->stats.received++;
	if(cc->stats.received == cc->stats.total)
		client_finished(cc);
	randomize_buffer(&value->v);
	HASH_ADD_BUFFER(cc->values, v, value);
	submit_value(cc, value);
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
		value = &msg.cl_message_u.learned_value.value;
		//snprintf(str_buf, value->size1 + 1, "%s", value->ptr);
		//printf("Mersenne has closed an instance #%lu with value: ``%s''\n",
		//		msg.cl_message_u.learned_value.i, str_buf);
		next_value(cc, value);
		xdr_free((xdrproc_t)xdr_cl_message, (caddr_t)&msg);
		xdr_destroy(&xdrs);
	}
conn_finish:
	close(cc->fd);
	errx(EXIT_FAILURE, "disconnected from mersenne");
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
	cc->stats.started = time(NULL);
	for(i = 0; i < cc->concurrency; i++) {
		ev_timer_init(&values[i].timer, value_timeout_cb, VALUE_TO, 0.);
		values[i].timer.data = cc;
		buf_init(&values[i].v, malloc(VALUE_SIZE), VALUE_SIZE, BS_EMPTY);
		randomize_buffer(&values[i].v);
		HASH_ADD_BUFFER(cc->values, v, values + i);
		submit_value(cc, values + i);
	}
}


void fiber_main(struct fbr_context *fiber_context)
{
	struct client_context *cc;
	struct fbr_call_info *info = NULL;
	struct my_value *value;

	cc = container_of(fiber_context, struct client_context, fbr);
	set_up_socket(cc);
	init_values(cc);
	cc->mersenne_read = fbr_create(&cc->fbr, "mersenne_read", fiber_reader);
	fbr_call(&cc->fbr, cc->mersenne_read, 0);
	
	fbr_next_call_info(&cc->fbr, NULL);

	for(;;) {
		fbr_yield(&cc->fbr);
		while(fbr_next_call_info(&cc->fbr, &info)) {
			printf("Timeout! Resubmitting...\n");
			value = info->argv[0].v;
			cc->stats.timeouts++;
			submit_value(cc, value);
		}
	}
}

int main(int argc, char *argv[]) {
	struct client_context cc;

	srand((unsigned int) time(NULL));  
	cc.loop = EV_DEFAULT;
	fbr_init(&cc.fbr, cc.loop);

	assert(3 == argc);
	cc.concurrency = atoi(argv[1]);
	cc.stats.total = atoi(argv[2]);
	cc.stats.received = 0;
	cc.stats.timeouts = 0;
	cc.stats.other = 0;
	
	cc.main = fbr_create(&cc.fbr, "main", fiber_main);
	fbr_call(&cc.fbr, cc.main, 0);
	
	ev_loop(cc.loop, 0);

	return 0;
}
