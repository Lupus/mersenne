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
#include <mersenne/sharedmem.h>
#include <mersenne/log.h>

#define LEA_INSTANCE_WINDOW mctx->args_info.learner_instance_window_arg
//#define WINDOW_DUMP

enum lea_instance_state {
	LIS_WAITING = 0,
	LIS_CLOSED,
	LIS_DELIVERED,
};

struct lea_instance {
	uint64_t iid;
	uint64_t b;
	struct buffer *v;
	struct bm_mask *acks;
	enum lea_instance_state state;
};

struct learner_context {
	uint64_t first_non_delivered;
	uint64_t highest_seen;
	struct lea_instance *instances;
	struct fbr_fiber *owner;
	struct me_context *mctx;
};

static inline struct lea_instance * get_instance(struct learner_context *context, uint64_t iid)
{
	struct me_context *mctx = context->mctx;
	return context->instances + (iid % LEA_INSTANCE_WINDOW);
}

#ifdef WINDOW_DUMP
static void print_window(ME_P_ struct learner_context *context)
{
       int j;
       int hw;
       struct lea_instance *instance;
       char buf[LEA_INSTANCE_WINDOW + 1];
       char *ptr = buf;
       fprintf(stderr, "%lu\t", context->first_non_delivered);
       for(j = 0; j < LEA_INSTANCE_WINDOW; j++) {
               instance = context->instances + j;
	       switch(instance->state) {
		       case LIS_WAITING:
			       hw = bm_hweight(instance->acks);
			       if(hw)
				       snprintf(ptr++, 2, "%d", hw);
			       else
				       *ptr++ = '.';
			       break;
		       case LIS_CLOSED:
			       *ptr++ = 'X';
			       break;
		       case LIS_DELIVERED:
			       *ptr++ = 'D';
			       break;
	       }
       }
       *ptr++ = '\0';
       fprintf(stderr, "%s\n", buf);
}
#endif

static void do_deliver(ME_P_ struct learner_context *context, struct
               lea_instance *instance)
{
	char buf[ME_MAX_XDR_MESSAGE_LEN];
	assert(LIS_CLOSED == instance->state);
	if(log_level_match(LL_DEBUG)) {
		snprintf(buf, instance->v->size1 + 1, "%s", instance->v->ptr);
		log(LL_DEBUG, "[LEARNER] Instance #%ld is delivered at ballot "
				"#%ld vith value ``%s'' size %d\n",
				instance->iid, instance->b, buf,
				instance->v->size1);
	}
       fbr_call(&mctx->fbr, context->owner, 3,
		       fbr_arg_i(FAT_PXS_DELIVERED_VALUE),
		       fbr_arg_i(instance->iid),
		       fiber_arg_vsm(instance->v)
	       );
       instance->state = LIS_DELIVERED;
#ifdef WINDOW_DUMP
       print_window(ME_A_ context);
#endif
}

static void send_retransmit(ME_P_ struct learner_context *context, uint64_t
		from, uint64_t to)
{
	struct me_message msg;
	struct me_paxos_msg_data *data = &msg.me_message_u.paxos_message.data;
	msg.super_type = ME_PAXOS;
	data->type = ME_PAXOS_RETRANSMIT;
	data->me_paxos_msg_data_u.retransmit.from = from;
	data->me_paxos_msg_data_u.retransmit.to = to;
	pxs_send_acceptors(ME_A_ &msg);
	log(LL_DEBUG, "[LEARNER] requesting retransmits from %lu to %lu,"
			" highest seen is %lu\n", from, to,
			context->highest_seen);
}

static void retransmit_window(ME_P_ struct learner_context *context)
{
	uint64_t i, j;
	uint64_t start = context->first_non_delivered;
	uint64_t from = 0;
	struct lea_instance *instance;
	int collecting_gap = 0;
	for(i = start, j = 0; j < LEA_INSTANCE_WINDOW; i++, j++) {
		instance = get_instance(context, i);
		if(collecting_gap) {
			if(LIS_WAITING != instance->state) {
				send_retransmit(ME_A_ context, from, i - 1);
				collecting_gap = 0;
			}

		} else if(LIS_WAITING == instance->state) {
			from = i;
			collecting_gap = 1;
		}
		if(i == context->highest_seen)
			break;
	}
}

static void try_deliver(ME_P_ struct learner_context *context)
{
	uint64_t i;
	int j;
	int start = context->first_non_delivered;
	struct lea_instance *instance;
	for(j = 0, i = start; j < LEA_INSTANCE_WINDOW; j++, i++) {
		instance = get_instance(context, i);
		assert(LIS_DELIVERED != instance->state);
		if(LIS_WAITING == instance->state)
			break;
		do_deliver(ME_A_ context, instance);
		instance->state = LIS_WAITING;
		bm_init(instance->acks, peer_count(ME_A));
		sm_free(instance->v);
		instance->v = NULL;
		instance->iid = i + LEA_INSTANCE_WINDOW;
		context->first_non_delivered = i + 1;
	}
	return;
	for(j = 0; j < LEA_INSTANCE_WINDOW; j++) {
		if(LIS_WAITING != context->instances[j].state) {
			break;
		}
	}
	if(j < LEA_INSTANCE_WINDOW)
		return;
	log(LL_DEBUG, "[LEARNER] while window is in wait, requesting retransmit\n");
	retransmit_window(ME_A_ context);
}

static void do_learn(ME_P_ struct learner_context *context, struct
		me_paxos_message *pmsg, struct buffer *buf, struct me_peer
		*from)
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
		log(LL_DEBUG, "[LEARNER] value out of instance windows, discarding\n");
		log(LL_DEBUG, "[LEARNER] data->i == %lu while "
				"context->first_non_delivered == %lu\n", data->i,
				context->first_non_delivered);
		return;
	}

	instance = get_instance(context, data->i);
	assert(LIS_DELIVERED != instance->state);
	if(LIS_CLOSED == instance->state)
		return;
	if(NULL == instance->v) {
		instance->iid = data->i;
		instance->b = data->b;
		assert(buf->size1 > 0);
		instance->v = sm_in_use(buf);
	} else {
		assert(instance->iid == data->i);
		//FIXME: Find out why does it fails the following:
		//assert(instance->b == data->b);
		assert(0 == buf_cmp(instance->v, buf));
	}
	bm_set_bit(instance->acks, from->index, 1);
	num = bm_hweight(instance->acks);
	if(pxs_is_acc_majority(ME_A_ num)) {
		instance->state = LIS_CLOSED;
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
	if(data->i > context->highest_seen)
		context->highest_seen = data->i;
	retransmit_window(ME_A_ context);
}

void lea_fiber(struct fbr_context *fiber_context)
{
	struct me_context *mctx;
	struct me_message *msg;
	struct me_peer *from;
	struct me_paxos_message *pmsg;
	struct fbr_call_info *info = NULL;
	struct buffer *buf;
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

	for(i = 0; i < LEA_INSTANCE_WINDOW; i++) {
		instance = context.instances + i;
		instance->acks = fbr_alloc(&mctx->fbr, bm_size(nbits));
		instance->state = LIS_WAITING;
		bm_init(instance->acks, nbits);
		instance->v = NULL;
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
				buf = info->argv[3].v;
				do_learn(ME_A_ &context, pmsg, buf, from);
				sm_free(buf);
				break;
			case ME_PAXOS_LAST_ACCEPTED:
				do_last_accepted(ME_A_ &context, pmsg, from);
				break;
			default:
				errx(EXIT_FAILURE,
						"wrong message type for learner: %s",
						strval_me_paxos_message_type(pmsg->data.type));
		}
		sm_free(msg);
	}
	goto start;
}
