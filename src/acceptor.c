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

#include <mersenne/acceptor.h>
#include <mersenne/paxos.h>
#include <mersenne/context.h>
#include <mersenne/message.h>
#include <mersenne/fiber_args.h>

static void send_promise(ME_P_ struct acc_instance_record *r, struct me_peer
		*to)
{
	struct me_message msg;
	struct me_paxos_msg_data *data = &msg.me_message_u.paxos_message.data;
	msg.super_type = ME_PAXOS;
	data->type = ME_PAXOS_PROMISE;
	data->me_paxos_msg_data_u.promise.i = r->iid;
	data->me_paxos_msg_data_u.promise.b = r->b;
	data->me_paxos_msg_data_u.promise.v.v_val = r->v;
	data->me_paxos_msg_data_u.promise.v.v_len = r->v_size;
	data->me_paxos_msg_data_u.promise.vb = r->vb;
	msg_send_to(ME_A_ &msg, to->index);
}

static void send_learn(ME_P_ struct acc_instance_record *r)
{
	struct me_message msg;
	struct me_paxos_msg_data *data = &msg.me_message_u.paxos_message.data;
	msg.super_type = ME_PAXOS;
	data->type = ME_PAXOS_LEARN;
	data->me_paxos_msg_data_u.learn.i = r->iid;
	data->me_paxos_msg_data_u.learn.b = r->b;
	data->me_paxos_msg_data_u.learn.v.v_val = r->v;
	data->me_paxos_msg_data_u.learn.v.v_len = r->v_size;
	msg_send_all(ME_A_ &msg);
}

static void do_prepare(ME_P_ struct me_paxos_message *pmsg, struct me_peer
		*from)
{
	struct acc_instance_record *r;
	struct me_paxos_msg_prepare_data *data;

	data = &pmsg->data.me_paxos_msg_data_u.prepare;
	HASH_FIND_IID(mctx->pxs.acc.records, &data->i, r);
	if(NULL == r) {
		r = calloc(sizeof(struct acc_instance_record), 1);
		r->iid = data->i;
		r->b = data->b;
	}
	if(data->b < r->b) {
		//TODO: Add REJECT message here for speedup
		return;
	}
	r->b = data->b;
	printf("[ACCEPTOR] Promised to not accept ballots lower that %ld\n", data->b);
	send_promise(ME_A_ r, from);
}

static void do_accept(ME_P_ struct me_paxos_message *pmsg, struct me_peer
		*from)
{
	struct acc_instance_record *r;
	struct me_paxos_accept_data *data;

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
	r->v = malloc(data->v.v_len);
	memcpy(r->v, data->v.v_val, data->v.v_len);
	r->v_size = data->v.v_len;
	send_learn(ME_A_ r);
}

void acc_fiber(ME_P)
{
	struct me_message *msg;
	struct me_peer *from;
	struct me_paxos_message *pmsg;
	struct fbr_call_info *info;

	fbr_next_call_info(ME_A_ NULL);

start:
	fbr_yield(ME_A);
	while(fbr_next_call_info(ME_A_ &info)) {
		assert(FAT_ME_MESSAGE == info->argv[0].i);
		msg = info->argv[1].v;
		from = info->argv[2].v;

		pmsg = &msg->me_message_u.paxos_message;
		if(ME_PAXOS_PREPARE == pmsg->data.type) {
			do_prepare(ME_A_ pmsg, from);
		} else if(ME_PAXOS_ACCEPT == pmsg->data.type) {
			do_accept(ME_A_ pmsg, from);
		}
	}
	goto start;
}

