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
#include <assert.h>
#include <err.h>

#include <mersenne/acceptor.h>
#include <mersenne/paxos.h>
#include <mersenne/context.h>
#include <mersenne/message.h>
#include <mersenne/fiber_args.h>
#include <mersenne/util.h>
#include <mersenne/me_protocol.strenum.h>

#define REPEAT_INTERVAL 1.

static void send_promise(ME_P_ struct acc_instance_record *r, struct me_peer
		*to)
{
	struct me_message msg;
	struct me_paxos_msg_data *data = &msg.me_message_u.paxos_message.data;
	msg.super_type = ME_PAXOS;
	data->type = ME_PAXOS_PROMISE;
	data->me_paxos_msg_data_u.promise.i = r->iid;
	data->me_paxos_msg_data_u.promise.b = r->b;
	buf_share(&data->me_paxos_msg_data_u.promise.v, &r->v);
	data->me_paxos_msg_data_u.promise.vb = r->vb;
	msg_send_to(ME_A_ &msg, to->index);
}

static void send_learn(ME_P_ struct acc_instance_record *r, struct me_peer *to)
{
	struct me_message msg;
	struct me_paxos_msg_data *data = &msg.me_message_u.paxos_message.data;

	assert(r->v.size1 > 0);

	msg.super_type = ME_PAXOS;
	data->type = ME_PAXOS_LEARN;
	data->me_paxos_msg_data_u.learn.i = r->iid;
	data->me_paxos_msg_data_u.learn.b = r->b;
	buf_share(&data->me_paxos_msg_data_u.learn.v, &r->v);
	if(NULL == to)
		msg_send_all(ME_A_ &msg);
	else
		msg_send_to(ME_A_ &msg, to->index);
}

static void send_highest_accepted(ME_P)
{
	struct me_message msg;
	struct me_paxos_msg_data *data = &msg.me_message_u.paxos_message.data;
	msg.super_type = ME_PAXOS;
	data->type = ME_PAXOS_LAST_ACCEPTED;
	data->me_paxos_msg_data_u.last_accepted.i = mctx->pxs.acc.highest_accepted;
	msg_send_all(ME_A_ &msg);
}

static void do_prepare(ME_P_ struct me_paxos_message *pmsg, struct me_peer
		*from)
{
	struct acc_instance_record *r;
	struct me_paxos_prepare_data *data;

	data = &pmsg->data.me_paxos_msg_data_u.prepare;
	HASH_FIND_IID(mctx->pxs.acc.records, &data->i, r);
	if(NULL == r) {
		r = calloc(sizeof(struct acc_instance_record), 1);
		r->iid = data->i;
		r->b = data->b;
		buf_init(&r->v, NULL, 0);
		HASH_ADD_IID(mctx->pxs.acc.records, iid, r);
	}
	if(data->b < r->b) {
		//TODO: Add REJECT message here for speedup
		return;
	}
	r->b = data->b;
	//printf("[ACCEPTOR] Promised to not accept ballots lower that %ld\n", data->b);
	send_promise(ME_A_ r, from);
}

static void do_accept(ME_P_ struct me_paxos_message *pmsg, struct me_peer
		*from)
{
	struct acc_instance_record *r = NULL;
	struct me_paxos_accept_data *data;
	char *ptr;

	data = &pmsg->data.me_paxos_msg_data_u.accept;
	HASH_FIND_IID(mctx->pxs.acc.records, &data->i, r);
	if(NULL == r) {
		//TODO: Add support for automatic accept of ballot 1
		return;
	}
	if(data->b < r->b) {
		//TODO: Add REJECT message here for speedup
		return;
	}
	r->b = data->b;
	if(r->v.empty) {
		ptr = malloc(data->v.size1);
		buf_init(&r->v, ptr, data->v.size1);
		buf_copy(&r->v, &data->v);
		assert(r->v.size1 > 0);
	} else
		assert(0 == buf_cmp(&r->v, &data->v));
	if(r->iid > mctx->pxs.acc.highest_accepted)
		 mctx->pxs.acc.highest_accepted = r->iid;
	//printf("[ACCEPTOR] Accepted instance #%lu at ballot #%lu, bound to "
	//		"0x%lx\n", data->i, data->b, (unsigned long)r);
	send_learn(ME_A_ r, NULL);
}

static void do_retransmit(ME_P_ struct me_paxos_message *pmsg, struct me_peer
		*from)
{
	uint64_t iid;
	struct acc_instance_record *r = NULL;
	struct me_paxos_retransmit_data *data;

	data = &pmsg->data.me_paxos_msg_data_u.retransmit;
	for(iid = data->from; iid <= data->to; iid++) {
		HASH_FIND_IID(mctx->pxs.acc.records, &iid, r);
		if(NULL == r)
			continue;
		if(r->v.empty)
			//FIXME: Not absolutely sure about this...
			continue;
		send_learn(ME_A_ r, from);
	}
}

static void repeater_fiber(struct fbr_context *fiber_context)
{
	struct me_context *mctx;
	mctx = container_of(fiber_context, struct me_context, fbr);
	
	fbr_next_call_info(&mctx->fbr, NULL);

	for(;;) {
		send_highest_accepted(ME_A);
		fbr_sleep(&mctx->fbr, REPEAT_INTERVAL);
	}
}

void acc_fiber(struct fbr_context *fiber_context)
{
	struct me_context *mctx;
	struct me_message *msg;
	struct me_peer *from;
	struct me_paxos_message *pmsg;
	struct fbr_call_info *info = NULL;
	struct fbr_fiber *repeater;

	mctx = container_of(fiber_context, struct me_context, fbr);
	fbr_next_call_info(&mctx->fbr, NULL);

	repeater = fbr_create(&mctx->fbr, "acceptor_repeater", repeater_fiber);
	fbr_call(&mctx->fbr, repeater, 0);
start:
	fbr_yield(&mctx->fbr);
	while(fbr_next_call_info(&mctx->fbr, &info)) {
		fbr_assert(&mctx->fbr, FAT_ME_MESSAGE == info->argv[0].i);
		msg = info->argv[1].v;
		from = info->argv[2].v;

		pmsg = &msg->me_message_u.paxos_message;
		switch(pmsg->data.type) {
			case ME_PAXOS_PREPARE:
				do_prepare(ME_A_ pmsg, from);
				break;
			case ME_PAXOS_ACCEPT:
				do_accept(ME_A_ pmsg, from);
				break;
			case ME_PAXOS_RETRANSMIT:
				do_retransmit(ME_A_ pmsg, from);
				break;
			default:
				errx(EXIT_FAILURE,
						"wrong message type for acceptor: %s",
						strval_me_paxos_message_type(pmsg->data.type));
		}
	}
	goto start;
}

