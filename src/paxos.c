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

#include <mersenne/paxos.h>
#include <mersenne/context.h>
#include <mersenne/vars.h>
#include <mersenne/util.h>
#include <mersenne/message.h>
#include <mersenne/peers.h>
#include <mersenne/proposer.h>


inline static int acceptor_predicate(struct me_peer *peer)
{
	//if(peer->pxs.is_acceptor)
		//printf("%d is acceptor\n", peer->pxs.is_acceptor);
	return peer->pxs.is_acceptor;
}

void pxs_send_acceptors(ME_P_ struct me_message *msg)
{
	msg_send_matching(ME_A_ msg, &acceptor_predicate);
}

int pxs_acceptors_count(ME_P)
{
	return peer_count_matching(ME_A_ &acceptor_predicate);
}

void pxs_do_message(ME_P_ struct me_message *msg, struct me_peer *from)
{
	struct me_paxos_message *pmsg;

	pmsg = &msg->me_message_u.paxos_message;
	switch(pmsg->data.type) {
		case ME_PAXOS_PREPARE:
		case ME_PAXOS_ACCEPT:
			if(mctx->me->pxs.is_acceptor)
				acc_do_message(ME_A_ msg, from);
			break;
		case ME_PAXOS_LEARN:
			lea_do_message(ME_A_ msg, from);
		case ME_PAXOS_PROMISE:
			if(mctx->ldr.leader == mctx->me->index)
				pro_do_message(ME_A_ msg, from);
			break;
	}
}

void pxs_fiber_init(ME_P)
{
}
