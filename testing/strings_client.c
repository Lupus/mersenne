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

#include <evfibers/fiber.h>
#include <mersenne/cl_protocol.h>
#include <mersenne/util.h>

struct client_context {
	int fd;
	struct fbr_context fbr;
	struct ev_loop *loop;
	struct fbr_fiber *mersenne_read;
	struct fbr_fiber *line_feed;
};

static int readit(char *ptr, char *buf, int size)
{
	struct client_context *cc = (struct client_context *)ptr;
	int retval;
	retval = fbr_read(&cc->fbr, cc->fd, buf, size);
	if(-1 == retval)
		err(EXIT_FAILURE, "fbr_read failed");
	if(0 == retval)
		return -1;
	return retval;
}

static int writeit(char *ptr, char *buf, int size)
{
	struct client_context *cc = (struct client_context *)ptr;
	return fbr_write(&cc->fbr, cc->fd, buf, size, NULL);
}

void fiber_linefeed(struct fbr_context *fiber_context)
{
	struct client_context *cc;
	char buf[1000];
	char buf2[1200];
	XDR xdrs;
	struct cl_message msg;
	int i = 0;

	cc = container_of(fiber_context, struct client_context, fbr);
	fbr_next_call_info(&cc->fbr, NULL);

	while(fbr_readline(&cc->fbr, STDIN_FILENO, buf, 1000)) {
		snprintf(buf2, 1200, "%03d:%s", i++, buf);
		xdrrec_create(&xdrs, 0, 0, (char *)cc, NULL, writeit);
		xdrs.x_op = XDR_ENCODE;
		buf_init(&msg.value, buf2, strlen(buf2) - 1);
		if(!xdr_cl_message(&xdrs, &msg))
			err(EXIT_FAILURE, "unable to encode a client message");
		xdrrec_endofrecord(&xdrs, TRUE);
		xdr_destroy(&xdrs);
	}
}

void fiber_reader(struct fbr_context *fiber_context)
{
	struct client_context *cc;
	XDR xdrs;
	struct cl_message msg;
	char buf[1000];

	cc = container_of(fiber_context, struct client_context, fbr);
	fbr_next_call_info(&cc->fbr, NULL);

	for(;;) {
		xdrrec_create(&xdrs, 0, 0, (char *)cc, readit, NULL);
		xdrs.x_op = XDR_DECODE;
		if (!xdrrec_skiprecord(&xdrs))
			errx(EXIT_FAILURE, "unable to skip initial record");
		for(;;) {
			memset(&msg, 0, sizeof(msg));
			if(!xdr_cl_message(&xdrs, &msg)) {
				if (!xdrrec_skiprecord(&xdrs))
					goto conn_finish;
				continue;
			}
			snprintf(buf, msg.value.size1 + 1, "%s", msg.value.ptr);
			printf("Mersenne has closed an instance with value: ``%s''\n", buf);
		}
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

int main(int argc, char *argv[]) {
	struct client_context cc;

	cc.loop = EV_DEFAULT;

	set_up_socket(&cc);

	fbr_init(&cc.fbr, cc.loop);

	cc.mersenne_read = fbr_create(&cc.fbr, "mersenne_read", fiber_reader);
	cc.line_feed = fbr_create(&cc.fbr, "linefeed", fiber_linefeed);

	fbr_call(&cc.fbr, cc.line_feed, 0);
	fbr_call(&cc.fbr, cc.mersenne_read, 0);
	
	ev_loop(cc.loop, 0);

	return 0;
}
