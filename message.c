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
#include <message.h>
#include <context.h>

void msg_send_to(ME_P_ struct me_message *msg, const int peer_num)
{
	int p(struct me_peer *peer) { return peer->index == peer_num; }
	msg_send_matching(ME_A_ msg, &p);
}

void msg_send_all(ME_P_ struct me_message *msg)
{
	int p(struct me_peer *peer) { return 1; }
	msg_send_matching(ME_A_ msg, &p);
}

void msg_send_matching(ME_P_ struct me_message *msg, int (*predicate)(struct me_peer *)) {
	struct me_peer *p;
	int size;
	XDR xdrs;
	char buf[ME_MAX_XDR_MESSAGE_LEN];

	xdrmem_create(&xdrs, buf, ME_MAX_XDR_MESSAGE_LEN, XDR_ENCODE);
	if(!xdr_me_message(&xdrs, msg))
		err(EXIT_FAILURE, "failed to encode a message");
	size = xdr_getpos(&xdrs);

	for(p=mctx->peers; p != NULL; p=p->hh.next) {
		if(!predicate(p))
			continue;
		//printf("Sending predicated msg to %d\n", p->index);
		if (sendto(mctx->fd, buf, size, 0, (struct sockaddr *) &p->addr, sizeof(p->addr)) < 0)
			err(EXIT_FAILURE, "failed to send message");
	}
	xdr_destroy(&xdrs);
}


