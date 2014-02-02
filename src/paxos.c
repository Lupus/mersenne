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
#include <mersenne/sharedmem.h>

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
	struct me_paxos_learn_data *ldata;
	struct buffer *buf;
	struct msg_info *info;
	struct fbr_buffer *fb;
	struct fiber_tailq_i *item;
	struct pro_msg_me_message *pro_msg;

	pmsg = &msg->me_message_u.paxos_message;

	switch(pmsg->data.type) {
	case ME_PAXOS_PREPARE:
	case ME_PAXOS_ACCEPT:
	case ME_PAXOS_RETRANSMIT:
		if(!mctx->me->pxs.is_acceptor)
			break;
		fb = fbr_get_user_data(&mctx->fbr, mctx->fiber_acceptor);
		if (NULL == fb)
			/* Acceptor is not yet ready to handle anything */
			break;
		buffer_ensure_writable(ME_A_ fb, sizeof(struct msg_info));
		info = fbr_buffer_alloc_prepare(&mctx->fbr, fb,
				sizeof(struct msg_info));
		info->msg = sm_in_use(msg);
		info->from = from;
		fbr_buffer_alloc_commit(&mctx->fbr, fb);
		break;

	case ME_PAXOS_LEARN:
	case ME_PAXOS_RELEARN:
		ldata = &pmsg->data.me_paxos_msg_data_u.learn;
		buf = buf_sm_steal(ldata->v);
		TAILQ_FOREACH(item, &mctx->learners, entries) {
			fb = fbr_get_user_data(&mctx->fbr, item->id);
			assert(NULL != fb);
			buffer_ensure_writable(ME_A_ fb,
					sizeof(struct msg_info));
			info = fbr_buffer_alloc_prepare(&mctx->fbr, fb,
					sizeof(struct msg_info));
			info->msg = sm_in_use(msg);
			info->buf = sm_in_use(buf);
			info->from = from;
			fbr_buffer_alloc_commit(&mctx->fbr, fb);
		}
		sm_free(buf);
		break;

	case ME_PAXOS_LAST_ACCEPTED:
		TAILQ_FOREACH(item, &mctx->learners, entries) {
			fb = fbr_get_user_data(&mctx->fbr, item->id);
			assert(NULL != fb);
			buffer_ensure_writable(ME_A_ fb,
					sizeof(struct msg_info));
			info = fbr_buffer_alloc_prepare(&mctx->fbr, fb,
					sizeof(struct msg_info));
			info->msg = sm_in_use(msg);
			info->from = from;
			fbr_buffer_alloc_commit(&mctx->fbr, fb);
		}
		break;

	case ME_PAXOS_CLIENT_VALUE:
	case ME_PAXOS_PROMISE:
	case ME_PAXOS_REJECT:
		fb = fbr_get_user_data(&mctx->fbr, mctx->fiber_proposer);
		if (NULL == fb)
			/* Proposer is not available */
			break;
		buffer_ensure_writable(ME_A_ fb, sizeof(*pro_msg));
		pro_msg = fbr_buffer_alloc_prepare(&mctx->fbr, fb,
				sizeof(*pro_msg));
		pro_msg->base.type = PRO_MSG_ME_MESSAGE;
		pro_msg->info.msg = sm_in_use(msg);
		pro_msg->info.from = from;
		fbr_buffer_alloc_commit(&mctx->fbr, fb);
		break;
	}
}

void pxs_fiber_init(ME_P)
{
	acs_initialize(ME_A);
	mctx->fiber_acceptor = fbr_create(&mctx->fbr, "acceptor", acc_fiber,
			NULL, 0);
	mctx->fiber_proposer = FBR_ID_NULL;

	fbr_transfer(&mctx->fbr, mctx->fiber_acceptor);
	pro_stop(ME_A);
}

void pxs_fiber_shutdown(ME_P)
{
	pro_stop(ME_A);
	fbr_reclaim(&mctx->fbr, mctx->fiber_proposer);
	fbr_reclaim(&mctx->fbr, mctx->fiber_acceptor);
}
