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
#include <mersenne/message.h>
#include <mersenne/context.h>
#include <mersenne/sharedmem.h>
#include <mersenne/log.h>

static inline int p_peer(struct me_peer *peer, void *context)
{
	return peer->index == *(int *)context;
}

void msg_send_to(ME_P_ struct me_message *msg, const int peer_num)
{
	msg_send_matching(ME_A_ msg, &p_peer, (int *)&peer_num);
}

static inline int p_all(struct me_peer *peer, void *context)
{
	return 1;
}

void msg_send_all(ME_P_ struct me_message *msg)
{
	msg_send_matching(ME_A_ msg, &p_all, NULL);
}

/*static void send_matching_now(ME_P_ struct me_message *msg,
		int (*predicate)(struct me_peer *, void *context),
		void *context) {
	struct me_peer *p;
	int size;
	int retval;
	XDR xdrs;
	char buf[ME_MAX_XDR_MESSAGE_LEN];

	xdrmem_create(&xdrs, buf, ME_MAX_XDR_MESSAGE_LEN, XDR_ENCODE);
	if(!xdr_me_message(&xdrs, msg))
		errx(EXIT_FAILURE, "xdr_me_message: failed to encode a message");
	size = xdr_getpos(&xdrs);

	for(p=mctx->peers; p != NULL; p=p->hh.next) {
		if(!predicate(p, context))
			continue;
		retval = sendto(mctx->fd, buf, size, 0, (struct sockaddr *) &p->addr, sizeof(p->addr));
		if (-1 == retval)
			err(EXIT_FAILURE, "failed to send message");
		if (retval < size)
			log(LL_NOTICE, "message got truncated from %d to %d while sending", size, retval);
	}
	xdr_destroy(&xdrs);
}*/

void msg_send_matching(ME_P_ struct me_message *msg, int (*predicate)(struct me_peer *, void *context), void *context) {
	struct me_peer *p;
	int size;
	XDR xdrs;
	char* buf;

	//if(ME_LEADER == msg->super_type) {
	//	send_matching_now(ME_A_ msg, predicate, context);
	//	return;
	//}

	buf = sm_alloc(ME_MAX_XDR_MESSAGE_LEN);

	xdrmem_create(&xdrs, buf, ME_MAX_XDR_MESSAGE_LEN, XDR_ENCODE);
	if(!xdr_me_message(&xdrs, msg))
		errx(EXIT_FAILURE, "xdr_me_message: failed to encode a message");
	size = xdr_getpos(&xdrs);

	for(p=mctx->peers; p != NULL; p=p->hh.next) {
		if(!predicate(p, context))
			continue;
		peer_enque_message(ME_A_ p, buf, size);
	}
	sm_free(buf);
	xdr_destroy(&xdrs);
}

