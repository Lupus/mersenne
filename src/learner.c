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

#define LEA_INSTANCE_WINDOW 5

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
	struct lea_instance *instances;
	struct fbr_fiber *owner;
};

static void do_deliver(ME_P_ struct learner_context *context, struct lea_instance *instance)
{
	char buf[1000];

	snprintf(buf, instance->v.size1 + 1, "%s", instance->v.ptr);
	fprintf(stderr, "[LEARNER] Instance #%ld is delivered at ballot #%ld vith value ``%s''\n",
			instance->iid, instance->b, buf);
	fbr_call(&mctx->fbr, context->owner, 3,
			fbr_arg_i(FAT_PXS_DELIVERED_VALUE),
			fbr_arg_i(instance->iid),
			fbr_arg_v(&instance->v)
		);
}

static void try_deliver(ME_P_ struct learner_context *context)
{
	int i, j;
	int start = context->first_non_delivered;
	struct lea_instance *instance;
	for(j = 0, i = start; j < LEA_INSTANCE_WINDOW; j++, i++) {
		instance = context->instances + (i % LEA_INSTANCE_WINDOW);
		if(!instance->closed)
			return;
		do_deliver(ME_A_ context, instance);
		context->first_non_delivered = i + 1;

		instance->v.empty = 1;
		instance->closed = 0;
		bm_init(instance->acks, peer_count(ME_A));
		buf_init(&instance->v, instance->v_data, ME_MAX_XDR_MESSAGE_LEN);
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
	if(data->i > context->first_non_delivered + LEA_INSTANCE_WINDOW) {
		warnx("instance windows is full, discarding next record");
		warnx("data->i == %lu while context->first_non_delivered == %lu", data->i, context->first_non_delivered);
		return;
	}
	instance = context->instances + (data->i % LEA_INSTANCE_WINDOW);
	if(instance->closed)
		return;
	if(instance->v.empty) {
		instance->iid = data->i;
		instance->b = data->b;
		buf_copy(&instance->v, &data->v);
	} else {
		//TODO: Wrap in some debug #ifdef?
		assert(instance->iid == data->i);
		assert(instance->b == data->b);
		assert(0 == buf_cmp(&instance->v, &data->v));
	}
	bm_set_bit(instance->acks, from->index, 1);
	num = bm_hweight(instance->acks);
	if(pxs_is_acc_majority(ME_A_ num)) {
		instance->closed = 1;
		try_deliver(ME_A_ context);
	}
}


void lea_fiber(struct fbr_context *fiber_context)
{
	struct me_context *mctx;
	struct me_message *msg;
	struct me_peer *from;
	struct me_paxos_message *pmsg;
	struct fbr_call_info *info;
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
	fbr_free_call_info(&mctx->fbr, info);

	fbr_subscribe(&mctx->fbr, FMT_LEARNER);

	context.instances = fbr_alloc(&mctx->fbr, LEA_INSTANCE_WINDOW * sizeof(struct lea_instance));

	for(i = 0; i < LEA_INSTANCE_WINDOW; i++) {
		instance = context.instances + i;
		instance->acks = fbr_alloc(&mctx->fbr, bm_size(nbits));
		bm_init(instance->acks, nbits);
		buf_init(&instance->v, instance->v_data, ME_MAX_XDR_MESSAGE_LEN);
	}

start:
	fbr_yield(&mctx->fbr);
	while(fbr_next_call_info(&mctx->fbr, &info)) {
		printf("call type: %s\n", strval_fiber_args_type(info->argv[0].i));
		fbr_assert(&mctx->fbr, FAT_ME_MESSAGE == info->argv[0].i);
		msg = info->argv[1].v;
		from = info->argv[2].v;

		pmsg = &msg->me_message_u.paxos_message;
		fbr_assert(&mctx->fbr, ME_PAXOS_LEARN == pmsg->data.type);
		do_learn(ME_A_ &context, pmsg, from);
		fbr_free_call_info(&mctx->fbr, info);
	}
	goto start;
}
