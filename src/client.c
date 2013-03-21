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
#include <errno.h>

#include <mersenne/client.h>
#include <mersenne/context.h>
#include <mersenne/cl_protocol.h>
#include <mersenne/fiber_args.h>
#include <mersenne/util.h>
#include <mersenne/sharedmem.h>

static int inform_client(ME_P_ int fd, uint64_t iid, struct buffer *buffer)
{
	struct cl_message msg;
	XDR xdrs;
	char buf[ME_MAX_XDR_MESSAGE_LEN];
	uint16_t size, send_size;
	ssize_t retval;

	buffer = sm_in_use(buffer);
	msg.type = CL_LEARNED_VALUE;
	msg.cl_message_u.learned_value.i = iid;
	msg.cl_message_u.learned_value.value = buffer;
	xdrmem_create(&xdrs, buf, ME_MAX_XDR_MESSAGE_LEN, XDR_ENCODE);
	if(!xdr_cl_message(&xdrs, &msg)) {
		fbr_log_w(&mctx->fbr, "xdr_cl_message: unable to encode");
		retval = -1;
		goto finish;
	}
	size = xdr_getpos(&xdrs);
	xdr_destroy(&xdrs);

	send_size = htons(size);
	retval = fbr_write_all(&mctx->fbr, fd, &send_size, sizeof(uint16_t));
	if(-1 == retval) {
		fbr_log_w(&mctx->fbr, "fbr_write_all: %s",
				strerror(errno));
		goto finish;
	}
	retval = fbr_write_all(&mctx->fbr, fd, buf, size);
	if(-1 == retval) {
		fbr_log_w(&mctx->fbr, "fbr_write_all: %s",
				strerror(errno));
		goto finish;
	}
	fbr_log_d(&mctx->fbr, "informed client about instance %lu", iid);
	retval = 0;
finish:
	sm_free(buffer);
	return retval;
}

void client_informer_fiber(struct fbr_context *fiber_context, void *_arg)
{
	struct me_context *mctx;
	fbr_id_t learner;
	int fd = *(int *)_arg;
	int retval;
	struct lea_fiber_arg lea_arg;
	struct lea_instance_info *instance;
	struct fbr_buffer lea_buffer;

	mctx = container_of(fiber_context, struct me_context, fbr);

	fbr_buffer_init(&mctx->fbr, &lea_buffer, 1);
	lea_arg.buffer = &lea_buffer;
	lea_arg.starting_iid = 0;
	learner = fbr_create(&mctx->fbr, "informer/learner", lea_fiber,
			&lea_arg, 0);
	retval = fbr_transfer(&mctx->fbr, learner);
	assert(0 == retval);

	for (;;) {
		instance = fbr_buffer_read_address(&mctx->fbr, lea_arg.buffer,
				sizeof(struct lea_instance_info));

		retval = inform_client(ME_A_ fd, instance->iid, instance->buffer);

		sm_free(instance->buffer);
		fbr_buffer_read_advance(&mctx->fbr, lea_arg.buffer);

		if(-1 == retval)
			fbr_log_w(&mctx->fbr, "informing of a client has"
					" failed");
	}
}

static void connection_fiber(struct fbr_context *fiber_context, void *_arg)
{
	struct me_context *mctx;
	int fd = *(int *)_arg;
	XDR xdrs;
	char buf[ME_MAX_XDR_MESSAGE_LEN];
	struct cl_message msg;
	ssize_t retval;
	uint16_t size;
	struct buffer *value;
	fbr_id_t informer;
	struct fbr_buffer *pro_buf;
	struct pro_msg_client_value *msg_client_value;

       	mctx = container_of(fiber_context, struct me_context, fbr);

	informer = fbr_create(&mctx->fbr, "informer",
			client_informer_fiber, &fd, 0);
	fbr_transfer(&mctx->fbr, informer);

	fbr_log_i(&mctx->fbr, "Connection fiber has started");
	for(;;) {
		retval = fbr_read_all(&mctx->fbr, fd, &size, sizeof(uint16_t));
		if(-1 == retval) {
			fbr_log_w(&mctx->fbr, "fbr_read_all: %s", strerror(errno));
			goto conn_finish;
		}
		if(retval < sizeof(uint16_t)) {
			fbr_log_d(&mctx->fbr, "fbr_read_all returned less "
					"bytes (%lu) than expected (%lu)",
					retval, sizeof(uint16_t));
			goto conn_finish;
		}
		size = ntohs(size);
		retval = fbr_read_all(&mctx->fbr, fd, buf, size);
		if(-1 == retval) {
			fbr_log_w(&mctx->fbr, "fbr_read_all: %s", strerror(errno));
			goto conn_finish;
		}
		if(retval < size) {
			fbr_log_d(&mctx->fbr, "fbr_read_all returned less "
					"bytes (%lu) than expected (%hu)",
					retval, size);
			goto conn_finish;
		}

		xdrmem_create(&xdrs, buf, size, XDR_DECODE);
		memset(&msg, 0, sizeof(msg));
		if(!xdr_cl_message(&xdrs, &msg)) {
			fbr_log_w(&mctx->fbr, "xdr_cl_message: unable to decode");
			xdr_destroy(&xdrs);
			goto conn_finish;
		}
		if(CL_NEW_VALUE != msg.type) {
			fbr_log_d(&mctx->fbr, "msg.type (%d) != CL_NEW_VALUE", msg.type);
			goto conn_finish;
		}
		value = buf_sm_steal(msg.cl_message_u.new_value.value);
		if(fbr_need_log(&mctx->fbr, FBR_LOG_DEBUG)) {
			memcpy(buf, value->ptr, value->size1);
			buf[value->size1] = '\0';
			fbr_log_d(&mctx->fbr, "Got value: ``%s''", buf);
		}
		pro_buf = fbr_get_user_data(&mctx->fbr, mctx->fiber_proposer);
		assert(NULL != pro_buf);
		msg_client_value = fbr_buffer_alloc_prepare(&mctx->fbr,
				pro_buf, sizeof(struct pro_msg_client_value));
		msg_client_value->base.type = PRO_MSG_CLIENT_VALUE;
		msg_client_value->value = sm_in_use(value);
		fbr_buffer_alloc_commit(&mctx->fbr, pro_buf);
		sm_free(value);
		xdr_free((xdrproc_t)xdr_cl_message, (caddr_t)&msg);
		xdr_destroy(&xdrs);
	}
conn_finish:
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

	mctx = container_of(fiber_context, struct me_context, fbr);

	for(;;) {
		sockfd = fbr_accept(&mctx->fbr, mctx->client_fd,
				(struct	sockaddr *)&addr, &addrlen);
		if(-1 == sockfd)
			err(EXIT_FAILURE, "fbr_accept failed");
		fiber = fbr_create(&mctx->fbr, "client",
				connection_fiber, &sockfd, 0);
		fbr_transfer(&mctx->fbr, fiber);
	}
}
