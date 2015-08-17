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
#include <dlfcn.h>
#include <wordexp.h>

#include <mersenne/acceptor.h>
#include <mersenne/paxos.h>
#include <mersenne/context.h>
#include <mersenne/message.h>
#include <mersenne/fiber_args.h>
#include <mersenne/util.h>
#include <mersenne/strenum.h>
#include <mersenne/sharedmem.h>

static void record_send_promise(ME_P_ struct acc_instance_record *r,
		struct me_peer *to)
{
	struct me_message *msg;
	struct me_paxos_msg_data *data;
	msg = r->msg;
	if (NULL == msg)
		msg = malloc(sizeof(*msg));
	data = &msg->me_message_u.paxos_message.data;
	msg->super_type = ME_PAXOS;
	data->type = ME_PAXOS_PROMISE;
	data->me_paxos_msg_data_u.promise.i = r->iid;
	data->me_paxos_msg_data_u.promise.b = r->b;
	data->me_paxos_msg_data_u.promise.v = r->v;
	data->me_paxos_msg_data_u.promise.vb = r->vb;
	r->msg = msg;
	r->msg_to_index = to->index;
	fbr_log_d(&mctx->fbr, "Recorded promise sending for instance %lu with"
			" value size %u", r->iid, r->v ? r->v->size1 : 0);
}

static void record_send_learn(ME_P_ struct acc_instance_record *r,
		struct me_peer *to)
{
	struct me_message *msg;
	struct me_paxos_msg_data *data;

	assert(r->v->size1 > 0);

	msg = r->msg;
	if (NULL == msg)
		msg = malloc(sizeof(*msg));
	data = &msg->me_message_u.paxos_message.data;

	msg->super_type = ME_PAXOS;
	data->type = ME_PAXOS_LEARN;
	data->me_paxos_msg_data_u.learn.i = r->iid;
	data->me_paxos_msg_data_u.learn.b = r->vb;
	data->me_paxos_msg_data_u.learn.v = r->v;
	data->me_paxos_msg_data_u.learn.final = 0;
	r->msg = msg;
	if(NULL == to)
		r->msg_to_index = -1;
	else
		r->msg_to_index = to->index;
	fbr_log_d(&mctx->fbr, "Recorded learn sending for instance %lu"
			" ballot %lu", r->iid, r->vb);
}

static void send_relearn(ME_P_ struct acc_instance_record *r, struct me_peer *to)
{
	struct me_message msg;
	struct me_paxos_msg_data *data;

	assert(r->v->size1 > 0);

	data = &msg.me_message_u.paxos_message.data;
	msg.super_type = ME_PAXOS;
	data->type = ME_PAXOS_RELEARN;
	data->me_paxos_msg_data_u.learn.i = r->iid;
	data->me_paxos_msg_data_u.learn.b = r->vb;
	data->me_paxos_msg_data_u.learn.v = r->v;
	data->me_paxos_msg_data_u.learn.final = 0;
	if(NULL == to)
		msg_send_all(ME_A_ &msg);
	else
		msg_send_to(ME_A_ &msg, to->index);
}

static void send_state(ME_P)
{
	struct me_message msg;
	struct me_paxos_msg_data *data = &msg.me_message_u.paxos_message.data;
	uint64_t iid;
	msg.super_type = ME_PAXOS;
	data->type = ME_PAXOS_ACCEPTOR_STATE;

	iid = acs_get_highest_accepted(ME_A);
	data->me_paxos_msg_data_u.acceptor_state.highest_accepted = iid;

	iid = mctx->pxs.lea.first_non_delivered - 1;
	data->me_paxos_msg_data_u.acceptor_state.highest_finalized = iid;

	iid = acs_get_lowest_available(ME_A);
	data->me_paxos_msg_data_u.acceptor_state.lowest_available = iid;
	msg_send_all(ME_A_ &msg);
}

static void send_reject(ME_P_ struct acc_instance_record *r, struct me_peer *to)
{
	struct me_message msg;
	struct me_paxos_msg_data *data = &msg.me_message_u.paxos_message.data;
	msg.super_type = ME_PAXOS;
	data->type = ME_PAXOS_REJECT;
	data->me_paxos_msg_data_u.reject.i = r->iid;
	data->me_paxos_msg_data_u.reject.b = r->b;
	msg_send_to(ME_A_ &msg, to->index);
	fbr_log_d(&mctx->fbr, "Sent reject for instance %lu at ballot %lu", r->iid, r->b);
}

static void do_prepare(ME_P_ struct me_paxos_message *pmsg, struct me_peer
		*from)
{
	struct acc_instance_record *r = NULL;
	struct me_paxos_prepare_data *data;

	data = &pmsg->data.me_paxos_msg_data_u.prepare;
	if (data->i <= mctx->pxs.lea.lowest_synced) {
		// Learner already delivered this instance and it is finalized,
		// so we will ignore this message altogether.
		fbr_log_d(&mctx->fbr, "Ignoring prepare for instance %lu"
				" as it's finalized", data->i);
		return;
	}
	if (0 == acs_find_record(ME_A_ &r, data->i, ACS_FM_CREATE)) {
		r->iid = data->i;
		r->b = data->b;
		r->v = NULL;
		r->vb = 0;
		acs_store_record(ME_A_ r);
	}
	if (data->b < r->b) {
		send_reject(ME_A_ r, from);
		goto cleanup;
	}
	r->b = data->b;
	acs_store_record(ME_A_ r);
	fbr_log_d(&mctx->fbr, "Promised not to accept ballots lower than %lu"
			" for instance %lu", data->b, data->i);
	record_send_promise(ME_A_ r, from);
cleanup:
	acs_free_record(ME_A_ r);
}

static void do_accept(ME_P_ struct me_paxos_message *pmsg, struct me_peer
		*from)
{
	struct acc_instance_record *r = NULL;
	struct me_paxos_accept_data *data;

	data = &pmsg->data.me_paxos_msg_data_u.accept;
	assert(data->v->size1 > 0);
	if (data->i <= mctx->pxs.lea.lowest_synced) {
		// Learner already delivered this instance and it is finalized,
		// so we will ignore this message altogether.
		fbr_log_d(&mctx->fbr, "Ignoring accept for instance %lu"
				" as it's finalized", data->i);
		return;
	}
	assert(data->i > mctx->pxs.lea.lowest_synced);
	if (0 == acs_find_record(ME_A_ &r, data->i, ACS_FM_CREATE)) {
		// We got an accept for an instance we know nothing about
		// without prior prepare.
		// Since we have not promised anything, we can just accept it.
		r->iid = data->i;
		r->b = data->b;
		r->v = NULL;
		r->vb = 0;
	}
	assert(r->iid == data->i);
	if (data->b < r->b) {
		send_reject(ME_A_ r, from);
		goto cleanup;
	}
	r->b = data->b;
	r->vb = data->b;
	if (NULL == r->v) {
		r->v = buf_sm_steal(data->v);
		assert(r->v->size1 > 0);
	} else if (0 != buf_cmp(r->v, data->v)) {
		fbr_log_d(&mctx->fbr, "Replacing value for instance %lu"
				" ballot %lu", r->iid, r->b);
		fbr_log_d(&mctx->fbr, "old value for instance %lu"
				" was ``%.*s'', size %d", r->iid,
				(unsigned)r->v->size1, r->v->ptr, r->v->size1);
		sm_free(r->v);
		r->v = buf_sm_steal(data->v);
		assert(r->v->size1 > 0);
		fbr_log_d(&mctx->fbr, "new value for instance %lu"
				" is ``%.*s'', size %d", r->iid,
				(unsigned)r->v->size1, r->v->ptr, r->v->size1);
	}
	acs_store_record(ME_A_ r);
	if (r->iid > acs_get_highest_accepted(ME_A))
		acs_set_highest_accepted(ME_A_ r->iid);
	record_send_learn(ME_A_ r, NULL);
cleanup:
	acs_free_record(ME_A_ r);
}

static void do_retransmit(ME_P_ struct me_paxos_message *pmsg, struct me_peer
		*from)
{
	uint64_t iid;
	struct acc_instance_record *r = NULL;
	struct me_paxos_retransmit_data *data;

	data = &pmsg->data.me_paxos_msg_data_u.retransmit;

	if (data->to < mctx->pxs.lea.first_non_delivered) {
		lea_resend(ME_A_ data->from, data->to, from);
		return;
	}

	for (iid = data->from; iid <= data->to; iid++) {
		if (0 == acs_find_record(ME_A_ &r, iid, ACS_FM_JUST_FIND)) {
			fbr_log_d(&mctx->fbr, "unable to find record %lu for"
					" a retransmit", iid);
			continue;
		}
		if (NULL == r->v) {
			//FIXME: Not absolutely sure about this...
			fbr_log_d(&mctx->fbr, "found record %lu with no value "
					" for a retransmit", iid);
			acs_free_record(ME_A_ r);
			continue;
		}
		send_relearn(ME_A_ r, from);
		acs_free_record(ME_A_ r);
	}
}

static void repeater_fiber(struct fbr_context *fiber_context, void *_arg)
{
	struct me_context *mctx;
	mctx = container_of(fiber_context, struct me_context, fbr);

	for(;;) {
		send_state(ME_A);
		fbr_sleep(&mctx->fbr,
				mctx->args_info.acceptor_repeat_interval_arg);
	}
}

static void do_acceptor_msg(ME_P_ struct msg_info *info)
{
	struct me_paxos_message *pmsg;
	const char *s;
	pmsg = &info->msg->me_message_u.paxos_message;
	switch(pmsg->data.type) {
	case ME_PAXOS_PREPARE:
		do_prepare(ME_A_ pmsg, info->from);
		break;
	case ME_PAXOS_ACCEPT:
		do_accept(ME_A_ pmsg, info->from);
		break;
	case ME_PAXOS_RETRANSMIT:
		do_retransmit(ME_A_ pmsg, info->from);
		break;
	default:
		s = strval_me_paxos_message_type(pmsg->data.type);
		errx(EXIT_FAILURE, "wrong message type for acceptor:"
				" %s", s);
	}
	sm_free(info->msg);
}

void acc_fiber(struct fbr_context *fiber_context, void *_arg)
{
	struct me_context *mctx;
	fbr_id_t repeater;
	struct fbr_buffer fb;
	struct msg_info info, *ptr;
	const size_t msg_size = sizeof(struct msg_info);
	size_t count, i;

	mctx = container_of(fiber_context, struct me_context, fbr);

	fbr_buffer_init(&mctx->fbr, &fb, 0);
	fbr_set_user_data(&mctx->fbr, fbr_self(&mctx->fbr), &fb);

	repeater = fbr_create(&mctx->fbr, "acceptor/repeater", repeater_fiber,
			NULL, 0);
	fbr_transfer(&mctx->fbr, repeater);

	fbr_log_d(&mctx->fbr, "acceptor started");

	while (fbr_buffer_wait_read(&mctx->fbr, &fb, msg_size)) {
		fbr_set_noreclaim(&mctx->fbr, fbr_self(&mctx->fbr));

		acs_batch_start(ME_A);
		count = fbr_buffer_bytes(&mctx->fbr, &fb) / msg_size;
		fbr_log_d(&mctx->fbr, "reading %ld messages from fb", count);
		for (i = 0; i < count; i++) {
			ptr = fbr_buffer_read_address(&mctx->fbr, &fb,
					msg_size);
			memcpy(&info, ptr, msg_size);
			fbr_buffer_read_advance(&mctx->fbr, &fb);
			do_acceptor_msg(ME_A_ &info);
		}
		acs_batch_finish(ME_A);

		fbr_set_reclaim(&mctx->fbr, fbr_self(&mctx->fbr));
		if (fbr_want_reclaim(&mctx->fbr, fbr_self(&mctx->fbr)))
			break;
	}
}
