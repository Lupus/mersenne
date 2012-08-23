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
#include <mersenne/paxos.h>
#include <mersenne/context.h>
#include <mersenne/message.h>
#include <mersenne/me_protocol.h>
#include <mersenne/fiber_args.h>
#include <mersenne/fiber_args.strenum.h>
#include <mersenne/util.h>
#include <mersenne/me_protocol.strenum.h>

#define LEA_INSTANCE_WINDOW mctx->args_info.learner_instance_window_arg

struct lea_instance {
	uint64_t iid;
	uint64_t b;
	struct buffer v;
	char v_data[ME_MAX_XDR_MESSAGE_LEN];
	struct bm_mask *acks;
	int closed;
};

struct learner_context {
	uint64_t first_non_delivered;
	uint64_t highest_seen;
	uint64_t next_retransmit;
	struct lea_instance *instances;
	struct fbr_fiber *owner;
	struct me_context *mctx;
};

static inline struct lea_instance * get_instance(struct learner_context *context, uint64_t iid)
{
	struct me_context *mctx = context->mctx;
	return context->instances + (iid % LEA_INSTANCE_WINDOW);
}

static void do_deliver(ME_P_ struct learner_context *context, struct lea_instance *instance)
{
	//char buf[ME_MAX_XDR_MESSAGE_LEN];

	//snprintf(buf, instance->v.size1 + 1, "%s", instance->v.ptr);
	//fprintf(stderr, "[LEARNER] Instance #%ld is delivered at ballot #%ld vith value ``%s''\n",
	//		instance->iid, instance->b, buf);
	fbr_call(&mctx->fbr, context->owner, 3,
			fbr_arg_i(FAT_PXS_DELIVERED_VALUE),
			fbr_arg_i(instance->iid),
			fbr_arg_v(&instance->v)
		);
}

static void send_retransmit(ME_P_ uint64_t from, uint64_t to)
{
	struct me_message msg;
	struct me_paxos_msg_data *data = &msg.me_message_u.paxos_message.data;
	msg.super_type = ME_PAXOS;
	data->type = ME_PAXOS_RETRANSMIT;
	data->me_paxos_msg_data_u.retransmit.from = from;
	data->me_paxos_msg_data_u.retransmit.to = to;
	pxs_send_acceptors(ME_A_ &msg);
}

static void retransmit_next_window(ME_P_ struct learner_context *context)
{
	context->next_retransmit = min(
		context->first_non_delivered + LEA_INSTANCE_WINDOW,
		context->highest_seen);
	send_retransmit(ME_A_ context->first_non_delivered,
			min(context->highest_seen, context->next_retransmit));
	printf("[LEARNER] requesting retransmits from %lu to %lu, highest seen is %lu\n",
		context->first_non_delivered, context->next_retransmit,
		context->highest_seen);
}

static void try_deliver(ME_P_ struct learner_context *context)
{
	int i, j;
	int start = context->first_non_delivered;
	struct lea_instance *instance;
	for(j = 0, i = start; j < LEA_INSTANCE_WINDOW; j++, i++) {
		instance = get_instance(context, i);
		if(!instance->closed)
			return;
		do_deliver(ME_A_ context, instance);
		context->first_non_delivered = i + 1;

		instance->v.empty = 1;
		instance->closed = 0;
		bm_init(instance->acks, peer_count(ME_A));
		buf_init(&instance->v, instance->v_data, ME_MAX_XDR_MESSAGE_LEN);
		if(context->first_non_delivered == context->next_retransmit)
			retransmit_next_window(ME_A_ context);
	}
}

static void do_learn(ME_P_ struct learner_context *context, struct
		me_paxos_message *pmsg, struct me_peer *from)
{
	struct lea_instance *instance;
	struct me_paxos_learn_data *data;
	int num;

	data = &pmsg->data.me_paxos_msg_data_u.learn;

	if(data->i < context->first_non_delivered)
		return;
	if(data->i >= context->first_non_delivered + LEA_INSTANCE_WINDOW) {
		if(data->i > context->highest_seen)
			context->highest_seen = data->i;
		warnx("[LEARNER] value out of instance windows, discarding");
		warnx("[LEARNER] data->i == %lu while "
				"context->first_non_delivered == %lu", data->i,
				context->first_non_delivered);
		return;
	}

	instance = get_instance(context, data->i);
	if(instance->closed)
		return;
	if(instance->v.empty) {
		instance->iid = data->i;
		instance->b = data->b;
		assert(data->v.size1 > 0);
		buf_copy(&instance->v, &data->v);
	} else {
		assert(instance->iid == data->i);
		//FIXME: Find out why does it fails the following:
		//assert(instance->b == data->b);
		assert(0 == buf_cmp(&instance->v, &data->v));
	}
	bm_set_bit(instance->acks, from->index, 1);
	num = bm_hweight(instance->acks);
	if(pxs_is_acc_majority(ME_A_ num)) {
		instance->closed = 1;
		try_deliver(ME_A_ context);
	}
}

static void do_last_accepted(ME_P_ struct learner_context *context, struct
		me_paxos_message *pmsg, struct me_peer *from)
{
	struct me_paxos_last_accepted_data *data;

	data = &pmsg->data.me_paxos_msg_data_u.last_accepted;
	if(data->i < context->first_non_delivered)
		return;
	if(data->i > context->first_non_delivered) {
		if(data->i > context->highest_seen)
			context->highest_seen = data->i;
		retransmit_next_window(ME_A_ context);
	}
}

void lea_fiber(struct fbr_context *fiber_context)
{
	struct me_context *mctx;
	struct me_message *msg;
	struct me_peer *from;
	struct me_paxos_message *pmsg;
	struct fbr_call_info *info = NULL;
	int i;
	struct lea_instance *instance;
	int nbits;
	struct learner_context context;

	mctx = container_of(fiber_context, struct me_context, fbr);
	nbits = peer_count(ME_A);
	fbr_next_call_info(&mctx->fbr, &info);
	fbr_assert(&mctx->fbr, 1 == info->argc);
	context.first_non_delivered = info->argv[0].i;
	context.owner = info->caller;
	context.mctx = mctx;

	fbr_subscribe(&mctx->fbr, FMT_LEARNER);

	context.instances = fbr_alloc(&mctx->fbr, LEA_INSTANCE_WINDOW * sizeof(struct lea_instance));
	context.highest_seen = context.first_non_delivered;
	context.next_retransmit = ~0UL; // "infinity"

	for(i = 0; i < LEA_INSTANCE_WINDOW; i++) {
		instance = context.instances + i;
		instance->acks = fbr_alloc(&mctx->fbr, bm_size(nbits));
		instance->closed = 0;
		bm_init(instance->acks, nbits);
		buf_init(&instance->v, instance->v_data, ME_MAX_XDR_MESSAGE_LEN);
		memset(instance->v_data, 0x00, ME_MAX_XDR_MESSAGE_LEN);
	}

start:
	fbr_yield(&mctx->fbr);
	while(fbr_next_call_info(&mctx->fbr, &info)) {
		fbr_assert(&mctx->fbr, FAT_ME_MESSAGE == info->argv[0].i);
		msg = info->argv[1].v;
		from = info->argv[2].v;

		pmsg = &msg->me_message_u.paxos_message;
		switch(pmsg->data.type) {
			case ME_PAXOS_LEARN:
				do_learn(ME_A_ &context, pmsg, from);
				break;
			case ME_PAXOS_LAST_ACCEPTED:
				do_last_accepted(ME_A_ &context, pmsg, from);
				break;
			default:
				errx(EXIT_FAILURE,
						"wrong message type for learner: %s",
						strval_me_paxos_message_type(pmsg->data.type));
		}
	}
	goto start;
}
