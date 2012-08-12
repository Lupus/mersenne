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

#include <mersenne/learner.h>
#include <mersenne/fiber.h>
#include <mersenne/paxos.h>
#include <mersenne/context.h>
#include <mersenne/message.h>
#include <mersenne/me_protocol.h>
#include <mersenne/fiber_args.h>

static void do_deliver(ME_P_ struct lea_instance *instance)
{
	char buf[1000];

	snprintf(buf, instance->v_size + 1, "%s", instance->v);
	fprintf(stderr, "[LEARNER] Instance #%ld is delivered at ballot #%ld vith value ``%s''\n", instance->iid, instance->b, buf);
	if(ldr_is_leader(ME_A))
		fbr_call(ME_A_ mctx->fiber_proposer, 3,
				fbr_arg_i(FAT_PXS_DELIVERED_VALUE),
				fbr_arg_i(instance->iid),
				fbr_arg_v(instance->v),
				fbr_arg_i(instance->v_size)
			);
}

static void try_deliver(ME_P)
{
	int i, j;
	int start = mctx->pxs.lea.first_non_delivered;
	struct lea_instance *instance;
	for(j = 0, i = start; j < LEA_INSTANCE_WINDOW; j++, i++) {
		instance = mctx->pxs.lea.instances + (i % LEA_INSTANCE_WINDOW);
		if(!instance->closed)
			return;
		do_deliver(ME_A_ instance);
		mctx->pxs.lea.first_non_delivered = i + 1;

		instance->v_size = 0;
		instance->closed = 0;
		bm_init(instance->acks, peer_count(ME_A));

	}
}

static void do_learn(ME_P_ struct me_paxos_message *pmsg, struct me_peer
		*from)
{
	struct lea_instance *instance;
	struct me_paxos_learn_data *data;
	int num;

	data = &pmsg->data.me_paxos_msg_data_u.learn;
	if(data->i < mctx->pxs.lea.first_non_delivered)
		return;
	if(data->i > mctx->pxs.lea.first_non_delivered + LEA_INSTANCE_WINDOW) {
		warnx("instance windows is full, discarding next record");
		return;
	}
	instance = mctx->pxs.lea.instances + (data->i % LEA_INSTANCE_WINDOW);
	if(instance->closed)
		return;
	if(!instance->v_size) {
		instance->iid = data->i;
		instance->b = data->b;
		memcpy(instance->v, data->v.v_val, data->v.v_len);
		instance->v_size = data->v.v_len;
	} else {
		//TODO: Wrap in some debug #ifdef?
		assert(instance->iid == data->i);
		assert(instance->b == data->b);
		assert(instance->v_size == data->v.v_len);
		assert(0 == memcmp(instance->v, data->v.v_val, data->v.v_len));
	}
	bm_set_bit(instance->acks, from->index, 1);
	num = bm_hweight(instance->acks);
	if(pxs_is_acc_majority(ME_A_ num)) {
		instance->closed = 1;
		try_deliver(ME_A);
	}
}


void lea_fiber(ME_P)
{
	struct me_message *msg;
	struct me_peer *from;
	struct me_paxos_message *pmsg;
	struct fbr_call_info *info;
	int i;
	struct lea_instance *instance;
	int nbits = peer_count(ME_A);

	fbr_next_call_info(ME_A_ NULL);
	for(i = 0; i < LEA_INSTANCE_WINDOW; i++) {
		instance = mctx->pxs.lea.instances + i;
		instance->acks = fbr_alloc(ME_A_ bm_size(nbits));
		bm_init(instance->acks, nbits);
	}

start:
	fbr_yield(ME_A);
	while(fbr_next_call_info(ME_A_ &info)) {
		assert(FAT_ME_MESSAGE == info->argv[0].i);
		msg = info->argv[1].v;
		from = info->argv[2].v;

		pmsg = &msg->me_message_u.paxos_message;
		assert(ME_PAXOS_LEARN == pmsg->data.type);
		do_learn(ME_A_ pmsg, from);
	}
	goto start;
}
