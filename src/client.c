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
#include <err.h>
#include <assert.h>
#include <netinet/tcp.h>
#include <errno.h>
#include <msgpack.h>

#include <mersenne/client.h>
#include <mersenne/context.h>
#include <mersenne/fiber_args.h>
#include <mersenne/util.h>
#include <mersenne/sharedmem.h>
#include <mersenne/proto.h>

static int tcp_cork(ME_P_ int fd)
{
	int yes = 1;
	return setsockopt(fd, IPPROTO_TCP, TCP_CORK, &yes, sizeof(yes));
}

static int tcp_nocork(ME_P_ int fd)
{
	int no = 0;
	return setsockopt(fd, IPPROTO_TCP, TCP_CORK, &no, sizeof(no));
}

static int inform_client(ME_P_ int fd, uint64_t iid, struct buffer *buffer)
{
	msgpack_sbuffer *buf = msgpack_sbuffer_new();
	msgpack_packer pk;
	union me_cli_any me_msg;
	int retval;

	me_msg.m_type = ME_CMT_ARRIVED_OTHER_VALUE;
	me_msg.other_value.buf = buffer->ptr;
	me_msg.other_value.size = buffer->size1;
	me_msg.other_value.iid = iid;
	msgpack_packer_init(&pk, buf, msgpack_sbuffer_write);
	retval = me_cli_msg_pack(&pk, &me_msg);
	if (retval)
		return retval;

	retval = fbr_write_all(&mctx->fbr, fd, buf->data, buf->size);
	if (retval < buf->size) {
		fbr_log_w(&mctx->fbr, "fbr_write_all: %s",
				strerror(errno));
		return -1;
	}
	msgpack_sbuffer_free(buf);
	fbr_log_d(&mctx->fbr, "informed client about instance %lu", iid);
	return 0;
}

struct connection_fiber_arg {
	int fd;
	int is_tcp;
};

void client_informer_loop_iter(ME_P_ struct fbr_buffer *buffer, int fd)
{
	int retval;
	struct lea_instance_info instance, *instance_ptr;
	instance_ptr = fbr_buffer_read_address(&mctx->fbr,
			buffer, sizeof(instance));
	memcpy(&instance, instance_ptr, sizeof(instance));
	fbr_buffer_read_advance(&mctx->fbr, buffer);

	retval = inform_client(ME_A_ fd, instance.iid, instance.buffer);

	sm_free(instance.buffer);

	if (retval)
		fbr_log_w(&mctx->fbr, "informing of a client has"
				" failed");
}

void client_informer_tcp_loop_iter(ME_P_ struct fbr_buffer *buffer, int fd)
{
	int retval;
	size_t count, i;
	size_t sz = sizeof(struct lea_instance_info);
	fbr_buffer_wait_read(&mctx->fbr, buffer, sz);
	count = fbr_buffer_bytes(&mctx->fbr, buffer) / sz;

	retval = tcp_cork(ME_A_ fd);
	if (retval)
		fbr_log_w(&mctx->fbr, "enabling of TCP_CORK has failed: %s",
				strerror(errno));
	for (i = 0; i < count; i++) {
		client_informer_loop_iter(ME_A_ buffer, fd);
	}
	if (0 == retval) {
		retval = tcp_nocork(ME_A_ fd);
		if (retval)
			fbr_log_w(&mctx->fbr,
					"disabling of TCP_CORK has failed: %s",
					strerror(errno));
	}
}

void client_informer_fiber(struct fbr_context *fiber_context, void *_arg)
{
	struct me_context *mctx;
	fbr_id_t learner;
	struct connection_fiber_arg *arg_p = _arg;
	struct connection_fiber_arg arg = *arg_p;
	int fd = arg.fd;
	int retval;
	struct lea_fiber_arg lea_arg;
	struct fbr_buffer lea_buffer;

	mctx = container_of(fiber_context, struct me_context, fbr);

	fbr_buffer_init(&mctx->fbr, &lea_buffer, 1);
	lea_arg.buffer = &lea_buffer;
	lea_arg.starting_iid = 0;
	learner = fbr_create(&mctx->fbr, "informer/learner", lea_fiber,
			&lea_arg, 0);
	retval = fbr_transfer(&mctx->fbr, learner);
	assert(0 == retval);

	if (arg.is_tcp) {
		for (;;)
			client_informer_tcp_loop_iter(ME_A_ lea_arg.buffer, fd);
	} else {
		for (;;)
			client_informer_loop_iter(ME_A_ lea_arg.buffer, fd);
	}
}

int process_message(ME_P_ struct me_cli_new_value *nv,
		struct fbr_buffer *pro_buf)
{
	struct buffer *value;
	struct pro_msg_client_value *msg_client_value;
	char *buf;
	value = buf_sm_copy(nv->buf, nv->size);
	if (fbr_need_log(&mctx->fbr, FBR_LOG_DEBUG)) {
		buf = malloc(value->size1 + 1);
		memcpy(buf, value->ptr, value->size1);
		buf[value->size1] = '\0';
		fbr_log_d(&mctx->fbr, "Got value: ``%s''", buf);
		free(buf);
	}
	buffer_ensure_writable(ME_A_ pro_buf,
			sizeof(struct pro_msg_client_value));
	msg_client_value = fbr_buffer_alloc_prepare(&mctx->fbr,
			pro_buf, sizeof(struct pro_msg_client_value));
	msg_client_value->base.type = PRO_MSG_CLIENT_VALUE;
	msg_client_value->value = sm_in_use(value);
	fbr_buffer_alloc_commit(&mctx->fbr, pro_buf);
	sm_free(value);
	return 0;
}

static void connection_fiber(struct fbr_context *fiber_context, void *_arg)
{
	struct me_context *mctx;
	struct connection_fiber_arg *arg_p = _arg;
	struct connection_fiber_arg arg = *arg_p;
	int fd = arg.fd;
	msgpack_unpacker pac;
	msgpack_unpacked result;
	union me_cli_any u;
	ssize_t retval;
	struct fbr_buffer *pro_buf;
	fbr_id_t informer;
	char *error = NULL;

       	mctx = container_of(fiber_context, struct me_context, fbr);

	msgpack_unpacker_init(&pac, MSGPACK_UNPACKER_INIT_BUFFER_SIZE);
	msgpack_unpacked_init(&result);

	pro_buf = fbr_get_user_data(&mctx->fbr, mctx->fiber_proposer);
	assert(NULL != pro_buf);

	retval = fbr_fd_nonblock(&mctx->fbr, fd);
	if (retval) {
		fbr_log_e(&mctx->fbr, "fbr_fd_nonblock: %s", strerror(errno));
		goto conn_finish;
	}

	informer = fbr_create(&mctx->fbr, "informer",
			client_informer_fiber, &arg, 0);
	fbr_transfer(&mctx->fbr, informer);

	fbr_log_i(&mctx->fbr, "Connection fiber has started");
	for (;;) {
		msgpack_unpacker_reserve_buffer(&pac,
				MSGPACK_UNPACKER_RESERVE_SIZE);
		retval = fbr_read(&mctx->fbr, fd, msgpack_unpacker_buffer(&pac),
				msgpack_unpacker_buffer_capacity(&pac));
		if (-1 == retval) {
			fbr_log_w(&mctx->fbr, "fbr_read: %s", strerror(errno));
			goto conn_finish;
		}
		if (0 == retval) {
			goto conn_finish;
		}
		msgpack_unpacker_buffer_consumed(&pac, retval);
		while (msgpack_unpacker_next(&pac, &result)) {
			retval = me_cli_msg_unpack(&result.data, &u,
					0 /* don't alloc memory */, &error);
			if (retval) {
				fbr_log_w(&mctx->fbr,
						"me_cli_msg_unpack: %s", error);
				free(error);
				goto conn_finish;
			}

			if (ME_CMT_NEW_VALUE != u.m_type) {
				fbr_log_w(&mctx->fbr, "bad m_type: %d",
						u.m_type);
				goto conn_finish;
			}

			retval = process_message(ME_A_ &u.new_value, pro_buf);
			if (retval)
				goto conn_finish;
		}
	}
conn_finish:
	msgpack_unpacker_destroy(&pac);
	fbr_log_i(&mctx->fbr, "Connection fiber has finished");
	close(fd);
}

void clt_fiber(struct fbr_context *fiber_context, void *_arg)
{
	struct me_context *mctx;
	int sockfd;
	struct sockaddr_un addr;
	socklen_t addrlen = sizeof(addr);
	fbr_id_t fiber;
	struct connection_fiber_arg arg;

	mctx = container_of(fiber_context, struct me_context, fbr);
	arg.is_tcp = 0;

	for (;;) {
		sockfd = fbr_accept(&mctx->fbr, mctx->client_fd,
				(struct	sockaddr *)&addr, &addrlen);
		if (-1 == sockfd)
			err(EXIT_FAILURE, "fbr_accept failed");

		arg.fd = sockfd;
		fiber = fbr_create(&mctx->fbr, "client",
				connection_fiber, &arg, 0);
		fbr_transfer(&mctx->fbr, fiber);
	}
}

void clt_tcp_fiber(struct fbr_context *fiber_context, void *_arg)
{
	struct me_context *mctx;
	int sockfd;
	struct sockaddr_in addr;
	socklen_t addrlen = sizeof(addr);
	fbr_id_t fiber;
	struct connection_fiber_arg arg;

	mctx = container_of(fiber_context, struct me_context, fbr);
	arg.is_tcp = 1;

	for (;;) {
		sockfd = fbr_accept(&mctx->fbr, mctx->client_tcp_fd,
				(struct	sockaddr *)&addr, &addrlen);
		if (-1 == sockfd)
			err(EXIT_FAILURE, "fbr_accept failed on tcp socket");
		arg.fd = sockfd;
		fiber = fbr_create(&mctx->fbr, "tcp_client",
				connection_fiber, &arg, 0);
		fbr_transfer(&mctx->fbr, fiber);
	}
}
