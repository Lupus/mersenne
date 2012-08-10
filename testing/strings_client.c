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

#include <mersenne/cl_protocol.h>
#include <mersenne/util.h>

static int writeit(char *ptr, char *buf, int size)
{
	int fd = *(int *)ptr;
	return write(fd, buf, size);
}

int main(int argc, char *argv[]) {
	struct sockaddr_un addr;
	char *rendezvous = getenv("UNIX_SOCKET");
	char buf[1000];
	int fd;
	XDR xdrs;
	struct cl_message msg;

	if ((fd = socket(AF_UNIX, SOCK_STREAM, 0)) < 0)
		err(EXIT_FAILURE, "failed to create a client socket");

	make_socket_non_blocking(fd);

	addr.sun_family = AF_UNIX;
	strcpy(addr.sun_path, rendezvous);
	if (-1 == connect(fd, (struct sockaddr *)&addr, sizeof(addr)))
		err(EXIT_FAILURE, "connect to unix socket failed");

	while(fgets(buf, 1000, stdin)) {
		if(feof(stdin))
			break;
		xdrrec_create(&xdrs, 0, 0, (char *)&fd, NULL, writeit);
		xdrs.x_op = XDR_ENCODE;
		msg.value.value_val = buf;
		msg.value.value_len = strlen(buf) - 1;
		if(!xdr_cl_message(&xdrs, &msg))
			err(EXIT_FAILURE, "unable to encode a client message");
		xdrrec_endofrecord(&xdrs, TRUE);
		xdr_destroy(&xdrs);
	}
	return 0;
}
