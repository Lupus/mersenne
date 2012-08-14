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

#include <mersenne/client.h>
#include <mersenne/context.h>
#include <mersenne/cl_protocol.h>
#include <mersenne/fiber_args.h>

struct read_context {
	int fd;
	struct me_context *mctx;
};

static int readit(char *ptr, char *buf, int size)
{
	struct read_context *context = (struct read_context *)ptr;
	struct me_context *mctx = context->mctx;
	int retval;
	retval = fbr_read(ME_A_ context->fd, buf, size);
	if(-1 == retval)
		err(EXIT_FAILURE, "fbr_read failed");
	if(0 == retval)
		return -1;
	//fprintf(stderr, "retval = %d\n", retval);
	return retval;
}

static void connection_fiber(ME_P)
{
	int fd;
	struct fbr_call_info *info = NULL;
	XDR xdrs;
	struct read_context context;
	struct cl_message msg;
	char buf[1000];

	assert(1 == fbr_next_call_info(ME_A_ &info));
	assert(1 == info->argc);
	fd = info->argv[0].i;
	context.fd = fd;
	context.mctx = mctx;
	puts("[CLIENT] Connection fiber has started");
	for(;;) {
		xdrrec_create(&xdrs, 0, 0, (char *)&context, readit, NULL);
		xdrs.x_op = XDR_DECODE;
		if (!xdrrec_skiprecord(&xdrs))
			errx(EXIT_FAILURE, "unable to skip initial record");
		//do {
		for(;;) {
			memset(&msg, 0, sizeof(msg));
			if(!xdr_cl_message(&xdrs, &msg)) {
				if (!xdrrec_skiprecord(&xdrs))
					goto conn_finish;
				continue;
			}
			snprintf(buf, msg.value.size1 + 1, "%s", msg.value.ptr);
			printf("[CLIENT] Got value: ``%s''\n", buf);
			fbr_call(ME_A_ mctx->fiber_proposer, 2,
					fbr_arg_i(FAT_PXS_CLIENT_VALUE),
					fbr_arg_v(&msg.value)
				);
			//} while(!xdrrec_eof(&xdrs));
		}
		xdr_free((xdrproc_t)xdr_cl_message, (caddr_t)&msg);
		xdr_destroy(&xdrs);
	}
conn_finish:
	puts("[CLIENT] Connection fiber has finished");
	close(fd);
}

void clt_fiber(ME_P)
{
	int sockfd;
	struct sockaddr_un addr;
	socklen_t addrlen = sizeof(addr);
	fbr_next_call_info(ME_A_ NULL);
	struct fbr_fiber *fiber;

	for(;;) {
		sockfd = fbr_accept(ME_A_ mctx->client_fd, (struct sockaddr *)&addr, &addrlen);
		if(-1 == sockfd)
			err(EXIT_FAILURE, "fbr_accept failed");
		fiber = fbr_create(ME_A_ "client_fiber", connection_fiber);
		fbr_call(ME_A_ fiber, 1, fbr_arg_i(sockfd));
	}
}
