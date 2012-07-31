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

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <time.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/epoll.h>
#include <err.h>
#include <errno.h>
#include <bitmask.h>
#include <ev.h>

#include <mersenne/me_protocol.h>
#include <mersenne/context.h>
#include <mersenne/peers.h>
#include <mersenne/vars.h>
#include <mersenne/paxos.h>
#include <mersenne/util.h>
#include <mersenne/fiber.h>

static void process_message(ME_P_ char* buf, int buf_size, const struct sockaddr *addr,
		socklen_t addrlen)
{
	struct me_message msg;
	struct me_peer *p;
	XDR xdrs;
	
	if (addr->sa_family != AF_INET) {
		warnx("unsupported address family: %d", (addr->sa_family));
		return;
	}

	p = find_peer(ME_A_ (struct sockaddr_in *)addr);
	if(!p) {
		warnx("got message from unknown peer --- ignoring");
		return;
	}

	memset(&msg, 0, sizeof(msg));
	xdrmem_create(&xdrs, buf, buf_size, XDR_DECODE);
	if(!xdr_me_message(&xdrs, &msg))
		err(EXIT_FAILURE, "unable to decode a message");
	switch(msg.super_type) {
		case ME_LEADER:
			ldr_do_message(ME_A_ &msg, p);
			break;
		case ME_PAXOS:
			pxs_do_message(ME_A_ &msg, p);
			break;
	}
	xdr_free((xdrproc_t)xdr_me_message, (caddr_t)&msg);
}

static void fiber_main(ME_P)
{
	int nbytes;
	struct sockaddr client_addr;
	socklen_t client_addrlen = sizeof(client_addr);
	char msgbuf[MSGBUFSIZE];
	
	for(;;) {
		nbytes = fbr_recvfrom(ME_A_ mctx->fd, msgbuf, MSGBUFSIZE, 0,
				&client_addr, &client_addrlen);
		if (nbytes < 0 && errno != EINTR)
				err(1, "recvfrom");
		process_message(ME_A_ msgbuf, nbytes, &client_addr, client_addrlen);
	}
}

int main(int argc, char *argv[])
{
	struct me_context context = ME_CONTEXT_INITIALIZER;
	struct me_context *mctx = &context;
	int yes = 1;
	struct fbr_fiber *fiber;

	fiber = fbr_create(ME_A_ fiber_main);

	if(argc != 2) {
		puts("Please enter peer number!");
		abort();
	}

	setenv("TZ", "UTC", 1); // We're operating in UTC

	load_peer_list(ME_A_ atoi(argv[1]));

	/* create what looks like an ordinary UDP socket */
	if ((context.fd = socket(AF_INET, SOCK_DGRAM, 0)) < 0)
		err(EXIT_FAILURE, "failed to create a socket");

	if (-1 == make_socket_non_blocking(context.fd))
		abort();

	/* allow multiple sockets to use the same PORT number */
	if (setsockopt(context.fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0)
		err(EXIT_FAILURE, "reusing of address failed");

	/* bind to receive address */
	if (bind(context.fd, (struct sockaddr *) &context.me->addr, sizeof(context.me->addr)) < 0)
		err(EXIT_FAILURE, "bind failed");

	// use the default event loop unless you have special needs
	context.loop = EV_DEFAULT;

	fbr_init(ME_A);
	ldr_fiber_init(ME_A);
	pxs_fiber_init(ME_A);

	fbr_call(ME_A_ fiber, 0);

	// now wait for events to arrive
	printf("Starting main loop\n");
	ev_loop(context.loop, 0);
	printf("Exiting\n");

	// break was called, so exit
	return 0;
}
