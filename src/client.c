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
#include <pthread.h>

#include <mersenne/client.h>
#include <mersenne/context.h>
#include <mersenne/fiber_args.h>
#include <mersenne/util.h>
#include <mersenne/sharedmem.h>
#include <mersenne/proto.h>

struct connection_fiber_arg {
	int fd;
	struct fbr_mutex mutex;
	uint64_t starting_iid;
	struct sockaddr_in addr;
};

static int tcp_cork(ME_P_ int fd)
{
	static int yes = 1;
	return setsockopt(fd, IPPROTO_TCP, TCP_CORK, &yes, sizeof(yes));
}

static int tcp_nocork(ME_P_ int fd)
{
	static int no = 0;
	return setsockopt(fd, IPPROTO_TCP, TCP_CORK, &no, sizeof(no));
}

static int inform_client(ME_P_ struct connection_fiber_arg *arg, uint64_t iid,
		struct buffer *buffer)
{
	msgpack_sbuffer *buf = msgpack_sbuffer_new();
	msgpack_packer pk;
	union me_cli_any me_msg;
	int retval;

	me_msg.m_type = ME_CMT_ARRIVED_VALUE;
	me_msg.arrived_value.buf = buffer->ptr;
	me_msg.arrived_value.size = buffer->size1;
	me_msg.arrived_value.iid = iid;
	msgpack_packer_init(&pk, buf, msgpack_sbuffer_write);
	retval = me_cli_msg_pack(&pk, &me_msg);
	if (retval)
		return retval;

	fbr_mutex_lock(&mctx->fbr, &arg->mutex);
	retval = fbr_write_all(&mctx->fbr, arg->fd, buf->data, buf->size);
	fbr_mutex_unlock(&mctx->fbr, &arg->mutex);
	if (retval < buf->size) {
		fbr_log_w(&mctx->fbr, "fbr_write_all: %s",
				strerror(errno));
		return -1;
	}
	msgpack_sbuffer_free(buf);
	fbr_log_d(&mctx->fbr, "informed client about instance %lu", iid);
	return 0;
}

static void client_informer_loop_iter(ME_P_ struct fbr_buffer *buffer,
	struct connection_fiber_arg *arg)
{
	int retval;
	struct lea_instance_info instance, *instance_ptr;
	instance_ptr = fbr_buffer_read_address(&mctx->fbr,
			buffer, sizeof(instance));
	memcpy(&instance, instance_ptr, sizeof(instance));
	fbr_buffer_read_advance(&mctx->fbr, buffer);

	retval = inform_client(ME_A_ arg, instance.iid, instance.buffer);

	sm_free(instance.buffer);

	if (retval)
		fbr_log_w(&mctx->fbr, "informing of a client has"
				" failed");
}

static void client_informer_tcp_loop_iter(ME_P_ struct fbr_buffer *buffer,
		struct connection_fiber_arg *arg)
{
	int retval;
	size_t count, i;
	size_t sz = sizeof(struct lea_instance_info);
	fbr_buffer_wait_read(&mctx->fbr, buffer, sz);
	count = fbr_buffer_bytes(&mctx->fbr, buffer) / sz;

	retval = tcp_cork(ME_A_ arg->fd);
	if (retval)
		fbr_log_w(&mctx->fbr, "enabling of TCP_CORK has failed: %s",
				strerror(errno));
	for (i = 0; i < count; i++) {
		client_informer_loop_iter(ME_A_ buffer, arg);
	}
	if (0 == retval) {
		retval = tcp_nocork(ME_A_ arg->fd);
		if (retval)
			fbr_log_w(&mctx->fbr,
					"disabling of TCP_CORK has failed: %s",
					strerror(errno));
	}
}

static void client_informer_fiber(struct fbr_context *fiber_context, void *_arg)
{
	struct me_context *mctx;
	fbr_id_t learner;
	struct connection_fiber_arg *arg_p = _arg;
	struct connection_fiber_arg arg = *arg_p;
	int retval;
	struct lea_fiber_arg lea_arg;
	struct fbr_buffer lea_buffer;

	mctx = container_of(fiber_context, struct me_context, fbr);

	fbr_buffer_init(&mctx->fbr, &lea_buffer, 1);
	lea_arg.buffer = &lea_buffer;
	lea_arg.starting_iid = arg.starting_iid;
	learner = fbr_create(&mctx->fbr, "informer/learner", lea_local_fiber,
			&lea_arg, 0);
	retval = fbr_transfer(&mctx->fbr, learner);
	assert(0 == retval);

	for (;;)
		client_informer_tcp_loop_iter(ME_A_ lea_arg.buffer, &arg);
}

static int redirect_client(ME_P_ struct connection_fiber_arg *arg)
{
	msgpack_sbuffer *buf = msgpack_sbuffer_new();
	msgpack_packer pk;
	union me_cli_any me_msg;
	int retval;
	char *addr_str;
	struct me_peer *leader;

	me_msg.m_type = ME_CMT_REDIRECT;
	leader = ldr_get_or_wait_for_leader(ME_A);
	if (leader == mctx->me)
		return 1;
	addr_str = inet_ntoa(leader->addr.sin_addr);
	strncpy(me_msg.redirect.ip, addr_str, ME_CMT_IP_SIZE);
	msgpack_packer_init(&pk, buf, msgpack_sbuffer_write);
	retval = me_cli_msg_pack(&pk, &me_msg);
	if (retval)
		return retval;

	fbr_mutex_lock(&mctx->fbr, &arg->mutex);
	retval = fbr_write_all(&mctx->fbr, arg->fd, buf->data, buf->size);
	fbr_mutex_unlock(&mctx->fbr, &arg->mutex);
	if (retval < buf->size) {
		fbr_log_w(&mctx->fbr, "fbr_write_all: %s",
				strerror(errno));
		return -1;
	}
	msgpack_sbuffer_free(buf);
	fbr_log_i(&mctx->fbr, "redirected client to %s", me_msg.redirect.ip);
	return 0;
}

void leadership_change_fiber(struct fbr_context *fiber_context, void *_arg)
{
	struct me_context *mctx;
	struct connection_fiber_arg *arg_p = _arg;
	struct connection_fiber_arg arg = *arg_p;

	mctx = container_of(fiber_context, struct me_context, fbr);

	for (;;) {
		ldr_wait_for_leader_change(ME_A);
		if (!ldr_is_leader(ME_A))
			redirect_client(ME_A_ &arg);
	}
}

int process_message(ME_P_ struct me_cli_new_value *nv,
		struct fbr_buffer *pro_buf)
{
	struct buffer *value;
	char *buf;
	value = buf_sm_copy(nv->buf, nv->size);
	if (fbr_need_log(&mctx->fbr, FBR_LOG_DEBUG)) {
		buf = malloc(value->size1 + 1);
		memcpy(buf, value->ptr, value->size1);
		buf[value->size1] = '\0';
		fbr_log_d(&mctx->fbr, "Got new value: ``%s'' size=(%zd)", buf,
				nv->size);
		free(buf);
	}
	pro_push_value(ME_A_ value);
	sm_free(value);
	return 0;
}

static int send_server_hello(ME_P_ struct connection_fiber_arg *arg)
{
	msgpack_sbuffer *buf = msgpack_sbuffer_new();
	msgpack_packer pk;
	union me_cli_any me_msg;
	int retval;
	char **peers;
	unsigned int count, i;
	struct me_peer *p;

	count = HASH_COUNT(mctx->peers);
	peers = calloc(count, sizeof(void *));
	for (i = 0, p = mctx->peers; NULL != p; p = p->hh.next, i++)
		peers[i] = strdup(inet_ntoa(p->addr.sin_addr));

	me_msg.m_type = ME_CMT_SERVER_HELLO;
	me_msg.server_hello.count = count;
	me_msg.server_hello.peers = peers;
	msgpack_packer_init(&pk, buf, msgpack_sbuffer_write);
	retval = me_cli_msg_pack(&pk, &me_msg);
	for (i = 0; i < count; i++) {
		free(peers[i]);
	}
	free(peers);
	if (retval)
		return retval;

	fbr_mutex_lock(&mctx->fbr, &arg->mutex);
	fbr_log_d(&mctx->fbr, "writing server hello...");
	retval = fbr_write_all(&mctx->fbr, arg->fd, buf->data, buf->size);
	fbr_log_d(&mctx->fbr, "finished writing server hello");
	fbr_mutex_unlock(&mctx->fbr, &arg->mutex);
	if (retval < buf->size) {
		msgpack_sbuffer_free(buf);
		fbr_log_w(&mctx->fbr, "fbr_write_all: %s",
				strerror(errno));
		return -1;
	}
	msgpack_sbuffer_free(buf);
	return 0;
}

static int conn_new_value(ME_P_ struct connection_fiber_arg *arg,
		struct fbr_buffer *pro_buf, union me_cli_any *u)
{
	ssize_t retval;
	if (!ldr_is_leader(ME_A))
		redirect_client(ME_A_ arg);

	retval = process_message(ME_A_ &u->new_value, pro_buf);
	if (retval)
		return -1;
	return 0;
}

static int send_error(ME_P_ struct connection_fiber_arg *arg,
		enum me_cli_error_code code)
{
	msgpack_sbuffer *buf = msgpack_sbuffer_new();
	msgpack_packer pk;
	union me_cli_any me_msg;
	ssize_t retval;

	me_msg.m_type = ME_CMT_ERROR;
	me_msg.error.code = code;
	msgpack_packer_init(&pk, buf, msgpack_sbuffer_write);
	retval = me_cli_msg_pack(&pk, &me_msg);
	if (retval)
		return retval;

	fbr_mutex_lock(&mctx->fbr, &arg->mutex);
	fbr_log_d(&mctx->fbr, "writing error message to the client, code %d",
			code);
	retval = fbr_write_all(&mctx->fbr, arg->fd, buf->data, buf->size);
	fbr_log_d(&mctx->fbr, "finished writing error message");
	fbr_mutex_unlock(&mctx->fbr, &arg->mutex);
	if (retval < buf->size) {
		msgpack_sbuffer_free(buf);
		fbr_log_w(&mctx->fbr, "fbr_write_all: %s",
				strerror(errno));
		return -1;
	}
	msgpack_sbuffer_free(buf);
	return 0;
}

struct informer_thread_func_arg {
	int fd;
	uint64_t starting_iid;
	struct me_context *mctx;
};

void *informer_thread_func(void *_arg)
{
	struct informer_thread_func_arg *arg = _arg;
	struct me_context *mctx = arg->mctx;
	uint64_t i;
	char *buf;
	size_t size;
	msgpack_sbuffer *sbuf = msgpack_sbuffer_new();
	msgpack_packer pk;
	union me_cli_any me_msg;
	int retval;
	for (i = arg->starting_iid;;i++) {
		printf("informer waiting for instance %zd\n", i);
		lea_get_or_wait_for_instance(ME_A_ i, &buf, &size);
		printf("informer got instance %zd !\n", i);

		me_msg.m_type = ME_CMT_ARRIVED_VALUE;
		me_msg.arrived_value.buf = buf;
		me_msg.arrived_value.size = size;
		me_msg.arrived_value.iid = i;
		msgpack_sbuffer_clear(sbuf);
		msgpack_packer_init(&pk, sbuf, msgpack_sbuffer_write);
		retval = me_cli_msg_pack(&pk, &me_msg);
		if (retval) {
			printf("informer: me_cli_msg_pack() failed\n");
			break;
		}
		do {
			retval = write(arg->fd, sbuf->data, sbuf->size);
		} while (-1 == retval && EINTR == errno);
		if (retval < sbuf->size) {
			printf("informer: wrote less than expected: %d\n",
					retval);
			break;
		}
	}
	msgpack_sbuffer_free(sbuf);
	free(_arg);
	return NULL;
}

static int conn_client_hello(ME_P_ struct connection_fiber_arg *arg,
		union me_cli_any *u)
{
	fbr_id_t informer;
	//pthread_t tid;
	arg->starting_iid = u->client_hello.starting_iid;
	int retval;
	//struct informer_thread_func_arg *targ = malloc(sizeof(*arg));
	if (0 == arg->starting_iid) {
		fbr_log_i(&mctx->fbr, "requested start from recent instance");
		arg->starting_iid = acs_get_highest_finalized(ME_A) + 1;
		fbr_log_i(&mctx->fbr, "picked latest iid %lu",
				arg->starting_iid);
	}
	if (arg->starting_iid < acs_get_lowest_available(ME_A)) {
		fbr_log_i(&mctx->fbr, "requested instance is unavailable");
		fbr_log_i(&mctx->fbr, "lowest available is %lu",
				acs_get_lowest_available(ME_A));
		retval = send_error(ME_A_ arg, ME_CME_IID_UNAVAILABLE);
		if (retval)
			fbr_log_w(&mctx->fbr, "failed to send error message to"
					" the client");
		return -1;
	}
	//targ->fd = arg->fd;
	//targ->starting_iid = arg->starting_iid;
	//targ->mctx = mctx;
	//retval = pthread_create(&tid, NULL, informer_thread_func, targ);
	//if (retval)
	//	err(EXIT_FAILURE, "pthread_create() failed");
	fbr_log_i(&mctx->fbr, "started informer thread at instance %ld",
			arg->starting_iid);
	informer = fbr_create(&mctx->fbr, "client/informer",
			client_informer_fiber, arg, 0);
	fbr_transfer(&mctx->fbr, informer);
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
	fbr_id_t leader_change;
	int informer_started = 0;
	char *error = NULL;
	char client_ip_str[INET_ADDRSTRLEN] = {0};

	mctx = container_of(fiber_context, struct me_context, fbr);
	fbr_mutex_init(&mctx->fbr, &arg.mutex);
	inet_ntop(AF_INET, &arg.addr.sin_addr, client_ip_str, INET_ADDRSTRLEN);
	fbr_log_i(&mctx->fbr, "Connection fiber has started, client %s",
			client_ip_str);

	msgpack_unpacker_init(&pac, MSGPACK_UNPACKER_INIT_BUFFER_SIZE);
	msgpack_unpacked_init(&result);

	retval = fbr_fd_nonblock(&mctx->fbr, fd);
	if (retval) {
		fbr_log_e(&mctx->fbr, "fbr_fd_nonblock: %s", strerror(errno));
		goto conn_finish;
	}

	fbr_log_d(&mctx->fbr, "sending server hello");

	retval = send_server_hello(ME_A_ &arg);
	if (retval)
		goto conn_finish;

	fbr_log_d(&mctx->fbr, "sent server hello");

	if (!ldr_is_leader(ME_A)) {
		retval = redirect_client(ME_A_ &arg);
		if (0 >= retval)
			/* Either we failed, or we told the client about
			 * the current leader
			 */
			goto conn_finish;
		/* We gained leadership */
		fbr_log_d(&mctx->fbr, "proceeding with the client");
	}

	pro_buf = fbr_get_user_data(&mctx->fbr, mctx->fiber_proposer);
	assert(NULL != pro_buf);

	leader_change = fbr_create(&mctx->fbr, "client/leader_change",
			leadership_change_fiber, &arg, 0);
	fbr_transfer(&mctx->fbr, leader_change);

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

			switch(u.m_type) {
			case ME_CMT_NEW_VALUE:
				retval = conn_new_value(ME_A_ &arg, pro_buf,
						&u);
				if (retval)
					goto conn_finish;
				break;
			case ME_CMT_CLIENT_HELLO:
				if (informer_started)
					continue;
				retval = conn_client_hello(ME_A_ &arg, &u);
				if (retval)
					goto conn_finish;
				informer_started = 1;
				break;
			default:
				fbr_log_w(&mctx->fbr, "bad m_type: %d",
						u.m_type);
				goto conn_finish;
			}
		}
	}
conn_finish:
	msgpack_unpacker_destroy(&pac);
	fbr_log_i(&mctx->fbr, "Connection fiber has finished, client %s",
			client_ip_str);
	close(fd);
}

void clt_tcp_fiber(struct fbr_context *fiber_context, void *_arg)
{
	struct me_context *mctx;
	int sockfd;
	fbr_id_t fiber;
	struct connection_fiber_arg arg;
	socklen_t addrlen = sizeof(arg.addr);

	mctx = container_of(fiber_context, struct me_context, fbr);

	for (;;) {
		sockfd = fbr_accept(&mctx->fbr, mctx->client_tcp_fd,
				(struct	sockaddr *)&arg.addr, &addrlen);
		if (-1 == sockfd)
			err(EXIT_FAILURE, "fbr_accept failed on tcp socket");
		arg.fd = sockfd;
		fbr_log_d(&mctx->fbr, "accepted client socket %d", sockfd);
		fiber = fbr_create(&mctx->fbr, "tcp_client",
				connection_fiber, &arg, 0);
		fbr_transfer(&mctx->fbr, fiber);
	}
}
