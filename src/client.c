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
#include <mersenne/log.h>

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
		log(LL_WARNING, "[CLIENT] xdr_cl_message: unable to encode\n");
		retval = -1;
		goto finish;
	}
	size = xdr_getpos(&xdrs);
	xdr_destroy(&xdrs);

	send_size = htons(size);
	retval = fbr_write_all(&mctx->fbr, fd, &send_size, sizeof(uint16_t));
	if(-1 == retval) {
		log(LL_WARNING, "[CLIENT] fbr_write_all: %s\n",
				strerror(errno));
		goto finish;
	}
	retval = fbr_write_all(&mctx->fbr, fd, buf, size);
	if(-1 == retval) {
		log(LL_WARNING, "[CLIENT] fbr_write_all: %s\n",
				strerror(errno));
		goto finish;
	}
	retval = 0;
finish:
	sm_free(buffer);
	return retval;
}

void client_informer_fiber(struct fbr_context *fiber_context)
{
	struct me_context *mctx;
	struct fbr_call_info *info = NULL;
	struct fbr_fiber *learner;
	int fd;
	struct buffer *buf;
	uint64_t iid;
	int retval;

	mctx = container_of(fiber_context, struct me_context, fbr);
	fbr_assert(&mctx->fbr, 1 == fbr_next_call_info(&mctx->fbr, &info));
	fbr_assert(&mctx->fbr, 1 == info->argc);
	fd = info->argv[0].i;

	learner = fbr_create(&mctx->fbr, "client/informer/learner", lea_fiber, 0);
	fbr_call(&mctx->fbr, learner, 1, fbr_arg_i(0));

start:
	fbr_yield(&mctx->fbr);
	while(fbr_next_call_info(&mctx->fbr, &info)) {

		switch(info->argv[0].i) {
			case FAT_PXS_DELIVERED_VALUE:
				fbr_assert(&mctx->fbr, 3 == info->argc);
				iid = info->argv[1].i;
				buf = info->argv[2].v;

				retval = inform_client(ME_A_ fd, iid, buf);
				sm_free(buf);
				if(-1 == retval)
					log(LL_WARNING, "[CLIENT] informing of "
							"a client has failed");
				break;
			case FAT_QUIT:
				goto quit;
		}
	}
	goto start;
quit:
	fbr_reclaim(&mctx->fbr, learner);
}

static void connection_fiber(struct fbr_context *fiber_context)
{
	struct me_context *mctx;
	int fd;
	struct fbr_call_info *info = NULL;
	XDR xdrs;
	char buf[ME_MAX_XDR_MESSAGE_LEN];
	struct cl_message msg;
	ssize_t retval;
	uint16_t size;
	struct buffer *value;
	struct fbr_fiber *informer;

       	mctx = container_of(fiber_context, struct me_context, fbr);
	
	fbr_assert(&mctx->fbr, 1 == fbr_next_call_info(&mctx->fbr, &info));
	fbr_assert(&mctx->fbr, 1 == info->argc);
	fd = info->argv[0].i;
	
	informer = fbr_create(&mctx->fbr, "client/informer", client_informer_fiber, 0);
	fbr_call(&mctx->fbr, informer, 1, fbr_arg_i(fd));

	log(LL_INFO, "[CLIENT] Connection fiber has started");
	for(;;) {
		retval = fbr_read_all(&mctx->fbr, fd, &size, sizeof(uint16_t));
		if(-1 == retval) {
			log(LL_WARNING, "fbr_read_all: %s\n", strerror(errno));
			goto conn_finish;
		}
		if(retval < sizeof(uint16_t)) {
			log(LL_DEBUG, "[CLIENT] fbr_read_all returned less "
					"bytes (%lu) than expected (%lu)\n",
					retval, sizeof(uint16_t));
			goto conn_finish;
		}
		size = ntohs(size);
		retval = fbr_read_all(&mctx->fbr, fd, buf, size);
		if(-1 == retval) {
			log(LL_WARNING, "[CLIENT] fbr_read_all: %s\n", strerror(errno));
			goto conn_finish;
		}
		if(retval < size) {
			log(LL_DEBUG, "[CLIENT] fbr_read_all returned less "
					"bytes (%lu) than expected (%hu)\n",
					retval, size);
			goto conn_finish;
		}

		xdrmem_create(&xdrs, buf, size, XDR_DECODE);
		memset(&msg, 0, sizeof(msg));
		if(!xdr_cl_message(&xdrs, &msg)) {
			log(LL_WARNING, "[CLIENT] xdr_cl_message: unable to decode\n");
			xdr_destroy(&xdrs);
			goto conn_finish;
		}
		if(CL_NEW_VALUE != msg.type) {
			log(LL_DEBUG, "[CLIENT] msg.type (%d) != CL_NEW_VALUE\n", msg.type);
			goto conn_finish;
		}
		value = buf_sm_steal(msg.cl_message_u.new_value.value);
		if(log_level_match(LL_DEBUG)) {
			memcpy(buf, value->ptr, value->size1);
			buf[value->size1] = '\0';
			log(LL_DEBUG, "[CLIENT] Got value: ``%s''\n", buf);
		}
		fbr_call(&mctx->fbr, mctx->fiber_proposer, 2,
				fbr_arg_i(FAT_PXS_CLIENT_VALUE),
				fiber_arg_vsm(value)
			);
		sm_free(value);
		xdr_free((xdrproc_t)xdr_cl_message, (caddr_t)&msg);
		xdr_destroy(&xdrs);
	}
conn_finish:
	log(LL_INFO, "[CLIENT] Connection fiber has finished\n");
	close(fd);
	fbr_call(&mctx->fbr, informer, 1, fbr_arg_i(FAT_QUIT));
	fbr_reclaim(&mctx->fbr, informer);
}

void clt_fiber(struct fbr_context *fiber_context)
{
	struct me_context *mctx;
	int sockfd;
	struct sockaddr_un addr;
	socklen_t addrlen = sizeof(addr);
	struct fbr_fiber *fiber;

	mctx = container_of(fiber_context, struct me_context, fbr);
	fbr_next_call_info(&mctx->fbr, NULL);

	for(;;) {
		sockfd = fbr_accept(&mctx->fbr, mctx->client_fd, (struct sockaddr *)&addr, &addrlen);
		if(-1 == sockfd)
			err(EXIT_FAILURE, "fbr_accept failed");
		fiber = fbr_create(&mctx->fbr, "client_fiber", connection_fiber, 0);
		fbr_call(&mctx->fbr, fiber, 1, fbr_arg_i(sockfd));
	}
}
