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
#include <mersenne/util.h>
#include <mersenne/message.h>
#include <mersenne/peers.h>
#include <mersenne/leader.h>
#include <mersenne/proposer.h>
#include <mersenne/fiber_args.h>


static inline int acceptor_predicate(struct me_peer *peer, void *context)
{
	return peer->pxs.is_acceptor;
}

void pxs_send_acceptors(ME_P_ struct me_message *msg)
{
	msg_send_matching(ME_A_ msg, &acceptor_predicate, NULL);
}

int pxs_acceptors_count(ME_P)
{
	return peer_count_matching(ME_A_ &acceptor_predicate, NULL);
}

int pxs_is_acc_majority(ME_P_ int acc_num)
{
	int acc_maj = pxs_acceptors_count(ME_A) / 2 + 1;
	return acc_num >= acc_maj;
}

void pxs_do_message(ME_P_ struct me_message *msg, struct me_peer *from)
{
	struct me_paxos_message *pmsg;
	pmsg = &msg->me_message_u.paxos_message;
	switch(pmsg->data.type) {
		case ME_PAXOS_PREPARE:
		case ME_PAXOS_ACCEPT:
		case ME_PAXOS_RETRANSMIT:
			if(mctx->me->pxs.is_acceptor)
				fbr_call(&mctx->fbr, mctx->fiber_acceptor, 3,
						fbr_arg_i(FAT_ME_MESSAGE),
						fiber_arg_vsm(msg),
						fbr_arg_v(from)
					);
			break;
		case ME_PAXOS_LEARN:
		case ME_PAXOS_LAST_ACCEPTED:
			fbr_multicall(&mctx->fbr, FMT_LEARNER, 3,
					fbr_arg_i(FAT_ME_MESSAGE),
					fiber_arg_vsm(msg),
					fbr_arg_v(from)
				);
			break;
		case ME_PAXOS_CLIENT_VALUE:
		case ME_PAXOS_PROMISE:
		case ME_PAXOS_REJECT:
			fbr_call(&mctx->fbr, mctx->fiber_proposer, 3,
					fbr_arg_i(FAT_ME_MESSAGE),
					fiber_arg_vsm(msg),
					fbr_arg_v(from)
				);
			break;
	}
}

void pxs_fiber_init(ME_P)
{
	acc_init_storage(ME_A);
	mctx->fiber_acceptor = fbr_create(&mctx->fbr, "acceptor", acc_fiber);
	mctx->fiber_proposer = NULL;

	fbr_call(&mctx->fbr, mctx->fiber_acceptor, 0);
	pro_stop(ME_A);
}
