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
#include <mersenne/me_protocol.strenum.h>
#include <mersenne/sharedmem.h>

#define DL_CALL(func,...) (*mctx->pxs.acc.func)(mctx->pxs.acc.context, ##__VA_ARGS__)

static void send_promise(ME_P_ struct acc_instance_record *r, struct me_peer
		*to)
{
	struct me_message msg;
	struct me_paxos_msg_data *data = &msg.me_message_u.paxos_message.data;
	msg.super_type = ME_PAXOS;
	data->type = ME_PAXOS_PROMISE;
	data->me_paxos_msg_data_u.promise.i = r->iid;
	data->me_paxos_msg_data_u.promise.b = r->b;
	data->me_paxos_msg_data_u.promise.v = r->v;
	data->me_paxos_msg_data_u.promise.vb = r->vb;
	msg_send_to(ME_A_ &msg, to->index);
	fbr_log_d(&mctx->fbr, "Sent promise for instance %lu with value size %u", r->iid, r->v ? r->v->size1 : 0);
}

static void send_learn(ME_P_ struct acc_instance_record *r, struct me_peer *to)
{
	struct me_message msg;
	struct me_paxos_msg_data *data = &msg.me_message_u.paxos_message.data;

	assert(r->v->size1 > 0);

	msg.super_type = ME_PAXOS;
	data->type = ME_PAXOS_LEARN;
	data->me_paxos_msg_data_u.learn.i = r->iid;
	data->me_paxos_msg_data_u.learn.b = r->b;
	data->me_paxos_msg_data_u.learn.v = r->v;
	if(NULL == to)
		msg_send_all(ME_A_ &msg);
	else
		msg_send_to(ME_A_ &msg, to->index);
}

static void send_highest_accepted(ME_P)
{
	struct me_message msg;
	struct me_paxos_msg_data *data = &msg.me_message_u.paxos_message.data;
	uint64_t iid;
	iid = DL_CALL(get_highest_accepted_func);
	msg.super_type = ME_PAXOS;
	data->type = ME_PAXOS_LAST_ACCEPTED;
	data->me_paxos_msg_data_u.last_accepted.i = iid;
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
	if(0 == DL_CALL(find_record_func, &r, data->i, ACS_FM_CREATE)) {
		r->iid = data->i;
		r->b = data->b;
		r->v = NULL;
		r->vb = 0;
		DL_CALL(store_record_func, r);
	}
	if(data->b < r->b) {
		send_reject(ME_A_ r, from);
		goto cleanup;
	}
	r->b = data->b;
	fbr_log_d(&mctx->fbr, "Promised not to accept ballots lower that %lu for instance %lu", data->b, data->i);
	send_promise(ME_A_ r, from);
cleanup:
	DL_CALL(free_record_func, r);
}

static void do_accept(ME_P_ struct me_paxos_message *pmsg, struct me_peer
		*from)
{
	struct acc_instance_record *r = NULL;
	struct me_paxos_accept_data *data;
	char buf[ME_MAX_XDR_MESSAGE_LEN];

	data = &pmsg->data.me_paxos_msg_data_u.accept;
	assert(data->v->size1 > 0);
	if(0 == DL_CALL(find_record_func, &r, data->i, ACS_FM_JUST_FIND)) {
		//TODO: Add automatic accept of ballot 0
		return;
	}
	if(data->b < r->b) {
		//TODO: Add REJECT message here for speedup
		goto cleanup;
	}
	r->b = data->b;
	r->vb = data->b;
	if(NULL == r->v) {
		r->v = buf_sm_steal(data->v);
		assert(r->v->size1 > 0);
	} else
		//FIXME: Find the cause of this as it's a violation of the
		//FIXME: crutial safety property
		if(0 != buf_cmp(r->v, data->v)) {
			fbr_log_e(&mctx->fbr, "Conflict while accepting a value for instance %lu ballot %lu", r->iid, r->b);
			snprintf(buf, data->v->size1 + 1, "%s", data->v->ptr);
			fbr_log_e(&mctx->fbr, "Suggested value sized %d is: ``%s''", data->v->size1, buf);
			snprintf(buf, r->v->size1 + 1, "%s", r->v->ptr);
			fbr_log_e(&mctx->fbr, "Current value sized %d is: ``%s''", r->v->size1, buf);
			abort();
		}
	if(r->iid > DL_CALL(get_highest_accepted_func))
		 DL_CALL(set_highest_accepted_func, r->iid);
	DL_CALL(store_record_func, r);
	send_learn(ME_A_ r, NULL);
cleanup:
	DL_CALL(free_record_func, r);
}

static void do_retransmit(ME_P_ struct me_paxos_message *pmsg, struct me_peer
		*from)
{
	uint64_t iid;
	struct acc_instance_record *r = NULL;
	struct me_paxos_retransmit_data *data;

	data = &pmsg->data.me_paxos_msg_data_u.retransmit;
	for(iid = data->from; iid <= data->to; iid++) {
		if(0 == DL_CALL(find_record_func, &r, iid, ACS_FM_JUST_FIND))
			continue;
		if(NULL == r->v) {
			//FIXME: Not absolutely sure about this...
			DL_CALL(free_record_func, r);
			continue;
		}
		send_learn(ME_A_ r, from);
		DL_CALL(free_record_func, r);
	}
}

static void repeater_fiber(struct fbr_context *fiber_context, void *_arg)
{
	struct me_context *mctx;
	mctx = container_of(fiber_context, struct me_context, fbr);

	for(;;) {
		send_highest_accepted(ME_A);
		fbr_sleep(&mctx->fbr, mctx->args_info.acceptor_repeat_interval_arg);
	}
}

void acc_fiber(struct fbr_context *fiber_context, void *_arg)
{
	struct me_context *mctx;
	struct msg_info *info;
	struct me_paxos_message *pmsg;
	fbr_id_t repeater;
	struct fbr_buffer *buffer;

	mctx = container_of(fiber_context, struct me_context, fbr);

	buffer = fbr_buffer_create(&mctx->fbr, 0);
	fbr_set_user_data(&mctx->fbr, fbr_self(&mctx->fbr), buffer);

	repeater = fbr_create(&mctx->fbr, "acceptor_repeater", repeater_fiber,
			NULL, 0);
	fbr_transfer(&mctx->fbr, repeater);

	for(;;) {
		info = fbr_buffer_read_address(&mctx->fbr, buffer,
				sizeof(struct msg_info));

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
				errx(EXIT_FAILURE,
						"wrong message type for acceptor: %s",
						strval_me_paxos_message_type(pmsg->data.type));
		}
		sm_free(info->msg);

		fbr_buffer_read_advance(&mctx->fbr, buffer);
	}
}

static void attach_symbol(ME_P_ void **vptr, const char *symbol)
{
	char *error;
	*vptr = dlsym(mctx->pxs.acc.handle, symbol);
	if (NULL != (error = dlerror()))
		errx(EXIT_FAILURE, "dlsym: %s", error);
}

void acc_init_storage(ME_P)
{
	wordexp_t we;
	const int buf_size = 1024;
	char buf[buf_size];
	mctx->pxs.acc.handle = dlopen(mctx->args_info.acceptor_storage_module_arg, RTLD_NOW);
	if (!mctx->pxs.acc.handle)
               errx(EXIT_FAILURE, "dlopen: %s", dlerror());
	dlerror();
	attach_symbol(ME_A_ (void **)&mctx->pxs.acc.destroy_func, "destroy");
	attach_symbol(ME_A_ (void **)&mctx->pxs.acc.find_record_func, "find_record");
	attach_symbol(ME_A_ (void **)&mctx->pxs.acc.get_highest_accepted_func, "get_highest_accepted");
	attach_symbol(ME_A_ (void **)&mctx->pxs.acc.initialize_func, "initialize");
	attach_symbol(ME_A_ (void **)&mctx->pxs.acc.set_highest_accepted_func, "set_highest_accepted");
	attach_symbol(ME_A_ (void **)&mctx->pxs.acc.store_record_func, "store_record");
	attach_symbol(ME_A_ (void **)&mctx->pxs.acc.free_record_func, "free_record");
	if(mctx->args_info.acceptor_storage_options_given) {
		snprintf(buf, buf_size, "%s %s",
				mctx->args_info.acceptor_storage_module_arg,
				mctx->args_info.acceptor_storage_options_arg);
		if(0 != wordexp(buf, &we, WRDE_NOCMD))
			errx(EXIT_FAILURE, "wordexp failed to parse acceptor storage command line");
		mctx->pxs.acc.context = (*mctx->pxs.acc.initialize_func)(we.we_wordc, we.we_wordv);
		wordfree(&we);
	} else {
		mctx->pxs.acc.context = (*mctx->pxs.acc.initialize_func)(0, NULL);
	}
}

void acc_free_storage(ME_P)
{
	DL_CALL(destroy_func);
	if(0 != dlclose(mctx->pxs.acc.handle))
               errx(EXIT_FAILURE, "dlclose: %s", dlerror());
}
