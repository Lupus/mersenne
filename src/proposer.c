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

#include <ev.h>
#include <err.h>
#include <assert.h>
#include <utlist.h>

#include <mersenne/proposer.h>
#include <mersenne/proposer_prv.h>
#include <mersenne/proposer_prv.strenum.h>
#include <mersenne/paxos.h>
#include <mersenne/context.h>
#include <mersenne/message.h>
#include <mersenne/peers.h>
#include <mersenne/util.h>
#include <mersenne/fiber_args.h>
#include <mersenne/bitmask.h>
#include <mersenne/me_protocol.strenum.h>
#include <mersenne/sharedmem.h>

static is_func_t * const state_table[IS_MAX] = {
	do_is_empty,
	do_is_p1_pending,
	do_is_p1_ready_no_value,
	do_is_p1_ready_with_value,
	do_is_p2_pending,
	do_is_delivered,
};

static int veql(struct pro_instance *instance)
{
	if(buf_cmp(instance->p1.v, instance->p2.v))
		return 0;
	return 1;
}

static int v2_eql_to(struct pro_instance *instance, struct buffer *buf)
{
	if(buf_cmp(instance->p2.v, buf))
		return 0;
	return 1;
}

static void v1_to_v2(struct pro_instance *instance)
{
	instance->p2.v = instance->p1.v;
	instance->p1.v = NULL;
}

static void set_client_v2(struct pro_instance *instance, struct buffer *buffer)
{
	if(instance->p2.v) sm_free(instance->p2.v);
	instance->p2.v = buffer;
	instance->client_value = 1;
}

static int pending_append(ME_P_ struct buffer *from)
{
	struct pending_value *pv;
	if(mctx->pxs.pro.pending_size > mctx->args_info.proposer_queue_size_arg)
		return -1;
	pv = fbr_calloc(&mctx->fbr, 1, sizeof(struct pending_value));
	pv->v = sm_in_use(from);
	DL_APPEND(mctx->pxs.pro.pending, pv);
	mctx->pxs.pro.pending_size++;
	return 0;
}

static int pending_shift(ME_P_ struct buffer **pptr)
{
	struct pending_value *pv = mctx->pxs.pro.pending;
	if(NULL == pv)
		return 0;
	assert(mctx->pxs.pro.pending_size > 0);
	DL_DELETE(mctx->pxs.pro.pending, pv);
	*pptr = pv->v;
	mctx->pxs.pro.pending_size--;
	fbr_free(&mctx->fbr, pv);
	return 1;
}

static void pending_unshift(ME_P_ struct buffer *from)
{
	struct pending_value *pv;
	pv = fbr_calloc(&mctx->fbr, 1, sizeof(struct pending_value));
	pv->v = sm_in_use(from);
	DL_PREPEND(mctx->pxs.pro.pending, pv);
	mctx->pxs.pro.pending_size++;
}

#ifdef WINDOW_DUMP
static void print_window(ME_P)
{
	int j;
	int hw;
	struct pro_instance *instance;
	char buf[PRO_INSTANCE_WINDOW + 1];
	char *ptr = buf;
	for(j = 0; j < PRO_INSTANCE_WINDOW; j++) {
		instance = mctx->pxs.pro.instances +j;
		switch(instance->state) {
			case IS_CLOSED:
				*ptr++ = 'C';
				break;
			case IS_DELIVERED:
				*ptr++ = 'D';
				break;
			case IS_EMPTY:
				*ptr++ = '.';
				break;
			case IS_P1_PENDING:
				hw = bm_hweight(instance->p1.acks);
				snprintf(ptr++, 2, "%d", hw);
				break;
			case IS_P1_READY_NO_VALUE:
				*ptr++ = '?';
				break;
			case IS_P1_READY_WITH_VALUE:
				*ptr++ = 'V';
				break;
			case IS_P2_PENDING:
				*ptr++ = 'P';
				break;
			case IS_MAX:
				*ptr++ = '!';
				break;
		}
	}
	*ptr++ = '\0';
	fprintf(stderr, "%s\n", buf);
}
#endif

static void run_instance(ME_P_ struct pro_instance *instance, struct ie_base *base)
{
	if(IE_D == base->type)
		do_is_delivered(ME_A_ instance, base);
	else
		state_table[instance->state](ME_A_ instance, base);
}

static void switch_instance(ME_P_ struct pro_instance *instance, enum
		instance_state state, struct ie_base *base)
{
	fbr_log_d(&mctx->fbr, "Switching instance %ld ballot %ld from state "
			"%s to state %s transition %s",
			instance->iid,
			instance->b,
			strval_instance_state(instance->state),
			strval_instance_state(state),
			strval_instance_event_type(base->type));
	instance->state = state;
#ifdef WINDOW_DUMP
	print_window(ME_A);
#endif
	run_instance(ME_A_ instance, base);
}

static void reclaim_instance(ME_P_ struct pro_instance *instance)
{
	struct ie_base base;
	struct bm_mask *mask = instance->p1.acks;
	base.type = IE_I;
	ev_timer timer = instance->timer;
	if(instance->p1.v)
		sm_free(instance->p1.v);
	if(instance->p2.v && instance->p2.v != instance->p1.v)
		sm_free(instance->p2.v);
	memset(instance, 0x00, sizeof(struct pro_instance));
	bm_init(mask, peer_count(ME_A));
	instance->p1.acks = mask;
	instance->timer = timer;
	instance->iid = mctx->pxs.pro.max_iid++;
	run_instance(ME_A_ instance, &base);
}

static void adjust_window(ME_P)
{
	struct pro_instance *instance;
	int i, j;
	int start = mctx->pxs.pro.lowest_non_closed;
	for(j = 0, i = start; j < PRO_INSTANCE_WINDOW; j++, i++) {
		instance = mctx->pxs.pro.instances + (i % PRO_INSTANCE_WINDOW);
		if(IS_DELIVERED != instance->state)
			return;
		mctx->pxs.pro.lowest_non_closed = i + 1;
		reclaim_instance(ME_A_ instance);
	}
}

static uint64_t encode_ballot(ME_P_ uint64_t ballot)
{
	return ballot * mctx->args_info.max_instances_arg + mctx->me->index;
}

static uint64_t decode_ballot(ME_P_ uint64_t ballot)
{
	return ballot / mctx->args_info.max_instances_arg;
}

static void send_prepare(ME_P_ struct pro_instance *instance)
{
	struct me_message msg;
	struct me_paxos_msg_data *data = &msg.me_message_u.paxos_message.data;
	msg.super_type = ME_PAXOS;
	data->type = ME_PAXOS_PREPARE;
	data->me_paxos_msg_data_u.prepare.i = instance->iid;
	data->me_paxos_msg_data_u.prepare.b = instance->b;
	pxs_send_acceptors(ME_A_ &msg);
}

static void send_accept(ME_P_ struct pro_instance *instance)
{
	struct me_message msg;
	struct me_paxos_msg_data *data = &msg.me_message_u.paxos_message.data;
	msg.super_type = ME_PAXOS;
	data->type = ME_PAXOS_ACCEPT;
	data->me_paxos_msg_data_u.accept.i = instance->iid;
	data->me_paxos_msg_data_u.accept.b = instance->b;
	data->me_paxos_msg_data_u.accept.v = instance->p2.v;
	assert(data->me_paxos_msg_data_u.accept.v->size1 > 0);
	fbr_log_d(&mctx->fbr, "Sending accepts for instance #%lu at ballot "
			"#%lu", instance->iid, instance->b);
	pxs_send_acceptors(ME_A_ &msg);
}

void do_is_empty(ME_P_ struct pro_instance *instance, struct ie_base *base)
{
	struct ie_base b;
	b.type = IE_S;
	switch_instance(ME_A_ instance, IS_P1_PENDING, &b);
}

void do_is_p1_pending(ME_P_ struct pro_instance *instance, struct ie_base *base)
{
	int b;
	struct ie_p *p;
	struct ie_base new_base;
	int num;
	switch(base->type) {
		case IE_S:
			instance->b = encode_ballot(ME_A_ 1);
			instance->p1.v = NULL;
			instance->p2.v = NULL;
			instance->p1.vb = 0;
			instance->client_value = 0;
			send_prepare(ME_A_ instance);
			ev_timer_set(&instance->timer, 0., TO1);
			ev_timer_again(mctx->loop, &instance->timer);
			break;
		case IE_P:
			p = container_of(base, struct ie_p, b);
			fbr_log_d(&mctx->fbr, "Processing promise for instance %lu at vb %lu "
					"from peer #%d", p->data->i,
					p->data->vb, p->from->index);
			if(p->data->v && p->data->vb > instance->p1.vb) {
				if(instance->p1.v) sm_free(instance->p1.v);
				instance->p1.v = buf_sm_steal(p->data->v);
				instance->p1.vb = p->data->vb;
				fbr_log_d(&mctx->fbr, "Found safe value for instance "
						"%lu at vb %lu", p->data->i,
						p->data->vb);
			}
			bm_set_bit(instance->p1.acks, p->from->index, 1);
			num = bm_hweight(instance->p1.acks);
			if(pxs_is_acc_majority(ME_A_ num)) {
				ev_timer_stop(mctx->loop, &instance->timer);
				if(NULL != instance->p1.v) {
					new_base.type = IE_R1;
					switch_instance(ME_A_ instance,
							IS_P1_READY_WITH_VALUE,
							&new_base);
				} else {
					new_base.type = IE_R0;
					switch_instance(ME_A_ instance,
							IS_P1_READY_NO_VALUE,
							&new_base);
				}
			}
			break;
		case IE_TO:
			fbr_log_d(&mctx->fbr, "Phase 1 timeout for instance "
					"#%lu at ballot #%lu", instance->iid,
					instance->b);
			bm_clear_all(instance->p1.acks);
			b = decode_ballot(ME_A_ instance->b);
			instance->b = encode_ballot(ME_A_ ++b);
			send_prepare(ME_A_ instance);
			ev_timer_set(&instance->timer, 0., TO1);
			ev_timer_again(mctx->loop, &instance->timer);
			break;
		default:
			errx(EXIT_FAILURE, "inappropriate transition %s for "
					"state %s",
					strval_instance_event_type(base->type),
					strval_instance_state(instance->state));
	}
}

void do_is_p1_ready_no_value(ME_P_ struct pro_instance *instance, struct ie_base *base)
{
	struct ie_base new_base;
	struct ie_nv *nv;
	switch(base->type) {
		case IE_R0:
			if(NULL == instance->p2.v) {
				if(!pending_shift(ME_A_ &instance->p2.v))
					break;
				instance->client_value = 1;

			}
			new_base.type = IE_A;
			switch_instance(ME_A_ instance, IS_P1_READY_WITH_VALUE,
					&new_base);
			break;
		case IE_NV:
			nv = container_of(base, struct ie_nv, b);
			set_client_v2(instance, nv->buffer);
			new_base.type = IE_A;
			switch_instance(ME_A_ instance,
					IS_P1_READY_WITH_VALUE,
					&new_base);
			break;
		default:
			errx(EXIT_FAILURE, "inappropriate transition %s for "
					"state %s",
					strval_instance_event_type(base->type),
					strval_instance_state(instance->state));
	}
}

void do_is_p1_ready_with_value(ME_P_ struct pro_instance *instance, struct ie_base *base)
{
	struct ie_base new_base;
	switch(base->type) {
		case IE_R1:
			if(NULL == instance->p2.v) {
				v1_to_v2(instance);
				instance->client_value = 0;
			} else if(veql(instance)) {
				//Do nothing here?
			} else if(!veql(instance)) {
				if(instance->client_value)
					pending_unshift(ME_A_ instance->p2.v);
				v1_to_v2(instance);
				instance->client_value = 0;
			}
			//Fall through
		case IE_A:
			new_base.type = IE_E;
			switch_instance(ME_A_ instance,
					IS_P2_PENDING,
					&new_base);
			break;
		default:
			errx(EXIT_FAILURE, "inappropriate transition %s for "
					"state %s",
					strval_instance_event_type(base->type),
					strval_instance_state(instance->state));
	}
}

void do_is_p2_pending(ME_P_ struct pro_instance *instance, struct ie_base *base)
{
	switch(base->type) {
		case IE_E:
			send_accept(ME_A_ instance);
			instance->timer.repeat = TO2;
			ev_timer_again(mctx->loop, &instance->timer);
			break;
		case IE_TO:
			fbr_log_d(&mctx->fbr, "Phase 2 timeout for instance #%lu at ballot #%lu", instance->iid, instance->b);
			switch_instance(ME_A_ instance,
					IS_P1_PENDING,
					base);
			break;

		default:
			errx(EXIT_FAILURE, "inappropriate transition %s for "
					"state %s",
					strval_instance_event_type(base->type),
					strval_instance_state(instance->state));
	}
}

void do_is_delivered(ME_P_ struct pro_instance *instance, struct ie_base *base)
{
	struct ie_d *d;
	switch(base->type) {
		case IE_D:
			d = container_of(base, struct ie_d, b);
			if(instance->client_value) {
				if(v2_eql_to(instance, d->buffer)) {
					//TODO: Client value delivered,
					//TODO: inform it about this
				} else
					pending_unshift(ME_A_ instance->p2.v);
			}
			ev_timer_stop(mctx->loop, &instance->timer);
			adjust_window(ME_A);
			break;
		default:
			errx(EXIT_FAILURE, "inappropriate transition %s for "
					"state %s",
					strval_instance_event_type(base->type),
					strval_instance_state(instance->state));
	}
}

static void do_promise(ME_P_ struct me_paxos_message *pmsg, struct me_peer
		*from)
{
	struct pro_instance *instance;
	struct me_paxos_promise_data *data;
	struct ie_p p;
	data = &pmsg->data.me_paxos_msg_data_u.promise;
	fbr_log_d(&mctx->fbr, "Got promise for instance %lu at vb %lu "
			"from peer #%d", data->i,
			data->vb, from->index);
	if(data->i < mctx->pxs.pro.lowest_non_closed) {
		fbr_log_d(&mctx->fbr, "Promise discarded as %lu < %lu "
				"(lowest non closed)",
				data->i,
				mctx->pxs.pro.lowest_non_closed);
		return;
	}
	p.b.type = IE_P;
	p.from = from;
	p.data = data;
	instance = mctx->pxs.pro.instances + (data->i % PRO_INSTANCE_WINDOW);
	assert(instance->iid == data->i);
	if(IS_P1_PENDING != instance->state)
		return;
	if(instance->b == data->b)
		run_instance(ME_A_ instance, &p.b);
}

static void instance_timeout_cb (EV_P_ ev_timer *w, int revents)
{
	struct me_context *mctx = (struct me_context *)w->data;
	struct pro_instance *instance;
	struct ie_base base;
	instance = container_of(w, struct pro_instance, timer);
	base.type = IE_TO;
	run_instance(ME_A_ instance, &base);
}

static void init_instance(ME_P_ struct pro_instance *instance)
{
	int nbits = peer_count(ME_A);
	instance->p1.acks = fbr_alloc(&mctx->fbr, bm_size(nbits));
	bm_init(instance->p1.acks, nbits);
	instance->p1.v = NULL;
	instance->p2.v = NULL;
	ev_timer_init(&instance->timer, instance_timeout_cb, 0., 0.);
	instance->timer.data = ME_A;
}

static void free_instance(ME_P_ struct pro_instance *instance)
{
	ev_timer_stop(mctx->loop, &instance->timer);
}

static void do_reject(ME_P_ struct me_paxos_message *pmsg, struct me_peer
		*from)
{
	struct pro_instance *instance;
	struct me_paxos_reject_data *data;
	struct ie_base base;
	base.type = IE_TO;
	data = &pmsg->data.me_paxos_msg_data_u.reject;
	if(data->i < mctx->pxs.pro.lowest_non_closed)
		return;
	instance = mctx->pxs.pro.instances + (data->i % PRO_INSTANCE_WINDOW);
	if(IS_P1_PENDING == instance->state || IS_P2_PENDING == instance->state) {
		instance->b = data->b;
		run_instance(ME_A_ instance, &base);
	}
}

static void instances_destructor(struct fbr_context *fiber_context, void *ptr,
		void *context)
{
	int i;
	struct pro_instance *instances = ptr;
	struct me_context *mctx = context;
	for(i = 0; i < PRO_INSTANCE_WINDOW; i++) {
		free_instance(ME_A_ instances + i);
	}
}


static void proposer_init(ME_P)
{
	int i;
	struct ie_base base;
	size_t size = sizeof(struct pro_instance) * PRO_INSTANCE_WINDOW;
	base.type = IE_I;
	mctx->pxs.pro.max_iid = 0;
	mctx->pxs.pro.lowest_non_closed = 0;
	mctx->pxs.pro.instances = fbr_alloc(&mctx->fbr, size);
	fbr_alloc_set_destructor(&mctx->fbr, mctx->pxs.pro.instances,
			instances_destructor, mctx);
	mctx->pxs.pro.pending = NULL;
	mctx->pxs.pro.pending_size = 0;
	memset(mctx->pxs.pro.instances, 0, size);
	for(i = 0; i < PRO_INSTANCE_WINDOW; i++) {
		init_instance(ME_A_ mctx->pxs.pro.instances + i);
		mctx->pxs.pro.instances[i].iid = mctx->pxs.pro.max_iid++;
		run_instance(ME_A_ mctx->pxs.pro.instances + i, &base);
	}
}

static void do_client_value(ME_P_ struct buffer *buf)
{
	struct ie_nv nv;
	struct pro_instance *instance;
	int i, j;
	int start = mctx->pxs.pro.lowest_non_closed;
	for(j = 0, i = start; j < PRO_INSTANCE_WINDOW; j++, i++) {
		instance = mctx->pxs.pro.instances + (i % PRO_INSTANCE_WINDOW);
		if(IS_P1_READY_NO_VALUE == instance->state) {
			nv.b.type = IE_NV;
			nv.buffer = sm_in_use(buf);
			run_instance(ME_A_ instance, &nv.b);
			return;
		}
	}
	pending_append(ME_A_ buf);
}

static void do_message(ME_P_ struct me_message *msg, struct me_peer *from)
{
	struct me_paxos_message *pmsg;
	struct buffer *buf;

	pmsg = &msg->me_message_u.paxos_message;

	switch(pmsg->data.type) {
		case ME_PAXOS_PROMISE:
			do_promise(ME_A_ pmsg, from);
			break;
		case ME_PAXOS_REJECT:
			do_reject(ME_A_ pmsg, from);
			break;
		case ME_PAXOS_CLIENT_VALUE:
			buf = buf_sm_steal(pmsg->data.me_paxos_msg_data_u.client_value.v);
			do_client_value(ME_A_ buf);
			sm_free(buf);
			break;
		default:
			errx(EXIT_FAILURE, "invalid paxos message type for "
					"proposer: %s",
					strval_me_paxos_message_type(pmsg->data.type));
	}
}

static void do_delivered_value(ME_P_ uint64_t iid, struct buffer *buffer)
{
	struct ie_d d;
	struct pro_instance *instance;
	d.b.type = IE_D;
	d.buffer = buffer;
	instance = mctx->pxs.pro.instances + (iid % PRO_INSTANCE_WINDOW);
	switch_instance(ME_A_ instance, IS_DELIVERED, &d.b);
}

struct proposer_context {
	struct fbr_buffer *fb;
	struct fbr_buffer *lea_fb;
	struct fbr_mutex *fb_mutex;
	struct fbr_mutex *lea_fb_mutex;
};

static void context_destructor(struct fbr_context *fiber_context, void *ptr,
		void *context)
{
	struct proposer_context *proposer_context = ptr;
	struct me_context *mctx = context;
	fbr_buffer_free(&mctx->fbr, proposer_context->fb);
	if(proposer_context->lea_fb)
		fbr_buffer_free(&mctx->fbr, proposer_context->lea_fb);
	if(proposer_context->fb_mutex)
		fbr_mutex_destroy(&mctx->fbr, proposer_context->fb_mutex);
	if(proposer_context->lea_fb_mutex)
		fbr_mutex_destroy(&mctx->fbr, proposer_context->lea_fb_mutex);
}

static void process_lea_fb(ME_P_ struct fbr_buffer *lea_fb)
{
	struct lea_instance_info *instance_info;
	instance_info = fbr_buffer_read_address(&mctx->fbr, lea_fb,
			sizeof(struct lea_instance_info));
	do_delivered_value(ME_A_ instance_info->iid, instance_info->buffer);
	sm_free(instance_info->buffer);
	fbr_buffer_read_advance(&mctx->fbr, lea_fb);
}

static void process_fb(ME_P_ struct fbr_buffer *fb)
{
	struct pro_msg_base *base;
	enum pro_msg_type type;
	struct pro_msg_me_message *pro_msg;
	struct msg_info *msg_info;
	struct pro_msg_client_value *pro_client;
	base = fbr_buffer_read_address(&mctx->fbr, fb,
			sizeof(struct pro_msg_base));
	type = base->type;
	fbr_buffer_read_discard(&mctx->fbr, fb);
	switch(type) {
		case PRO_MSG_ME_MESSAGE:
			pro_msg = fbr_buffer_read_address(&mctx->fbr, fb,
					sizeof(struct pro_msg_me_message));
			msg_info = &pro_msg->info;
			do_message(ME_A_ msg_info->msg, msg_info->from);
			sm_free(msg_info->msg);
			fbr_buffer_read_advance(&mctx->fbr, fb);
			break;
		case PRO_MSG_CLIENT_VALUE:
			pro_client = fbr_buffer_read_address(&mctx->fbr, fb,
					sizeof(struct pro_msg_client_value));
			do_client_value(ME_A_ pro_client->value);
			sm_free(pro_client->value);
			fbr_buffer_read_advance(&mctx->fbr, fb);
			break;
		default:
			abort();
	}
}

void pro_fiber(struct fbr_context *fiber_context, void *_arg)
{
	struct me_context *mctx;
	fbr_id_t learner;
	struct fbr_buffer *fb, *lea_fb;
	struct lea_fiber_arg lea_arg;
	struct fbr_ev_cond_var ev_fb;
	struct fbr_ev_cond_var ev_lea_fb;
	struct fbr_ev_base *ev;
	struct proposer_context *proposer_context;
	struct fbr_mutex *fb_mutex;
	struct fbr_mutex *lea_fb_mutex;

	mctx = container_of(fiber_context, struct me_context, fbr);

	proposer_context = fbr_alloc(&mctx->fbr, sizeof(struct pro_context));
	fbr_alloc_set_destructor(&mctx->fbr, proposer_context,
			context_destructor, mctx);

	fb = fbr_buffer_create(&mctx->fbr, 0);
	assert(NULL != fb);
	fbr_set_user_data(&mctx->fbr, fbr_self(&mctx->fbr), fb);
	fb_mutex = fbr_mutex_create(&mctx->fbr);
	lea_fb = fbr_buffer_create(&mctx->fbr, 0);
	assert(NULL != lea_fb);
	lea_fb_mutex = fbr_mutex_create(&mctx->fbr);
	proposer_context->fb = fb;
	proposer_context->lea_fb = lea_fb;
	proposer_context->fb_mutex = fb_mutex;
	proposer_context->lea_fb_mutex = lea_fb_mutex;

	fbr_ev_cond_var_init(&mctx->fbr, &ev_fb,
			fbr_buffer_cond_read(&mctx->fbr, fb),
			fb_mutex);
	fbr_ev_cond_var_init(&mctx->fbr, &ev_lea_fb,
			fbr_buffer_cond_read(&mctx->fbr, lea_fb),
			lea_fb_mutex);

	proposer_init(ME_A);

	lea_arg.buffer = lea_fb;
	lea_arg.starting_iid = 1;
	learner = fbr_create(&mctx->fbr, "proposer/learner", lea_fiber, &lea_arg, 0);
	fbr_transfer(&mctx->fbr, learner);

	for(;;) {
		fbr_mutex_lock(&mctx->fbr, fb_mutex);
		fbr_mutex_lock(&mctx->fbr, lea_fb_mutex);
		ev = fbr_ev_wait(&mctx->fbr, (struct fbr_ev_base*[]){
				&ev_fb.ev_base,
				&ev_lea_fb.ev_base,
				NULL
				});
		if(ev == &ev_fb.ev_base) {
			fbr_mutex_unlock(&mctx->fbr, fb_mutex);
			process_fb(ME_A_ fb);
		} else {
			assert(ev == &ev_lea_fb.ev_base);
			fbr_mutex_unlock(&mctx->fbr, lea_fb_mutex);
			process_lea_fb(ME_A_ lea_fb);
		}


	}
}

static void send_proxy_value(ME_P_ struct buffer *buf)
{
	struct me_message msg;
	struct me_paxos_msg_data *data = &msg.me_message_u.paxos_message.data;
	msg.super_type = ME_PAXOS;
	data->type = ME_PAXOS_CLIENT_VALUE;
	data->me_paxos_msg_data_u.client_value.v = buf;
	msg_send_to(ME_A_ &msg, mctx->ldr.leader);
}

static void proxy_fiber(struct fbr_context *fiber_context, void *_arg)
{
	struct me_context *mctx;
	struct fbr_buffer *fb;
	struct proposer_context *proposer_context;
	struct pro_msg_base *base;
	enum pro_msg_type type;
	struct pro_msg_me_message *pro_msg;
	struct msg_info *msg_info;
	struct pro_msg_client_value *pro_client;

	mctx = container_of(fiber_context, struct me_context, fbr);

	proposer_context = fbr_alloc(&mctx->fbr, sizeof(struct pro_context));
	fbr_alloc_set_destructor(&mctx->fbr, proposer_context,
			context_destructor, mctx);

	fb = fbr_buffer_create(&mctx->fbr, 0);
	fbr_set_user_data(&mctx->fbr, fbr_self(&mctx->fbr), fb);
	memset(proposer_context, 0x00, sizeof(struct proposer_context));
	proposer_context->fb = fb;

	for(;;) {
		base = fbr_buffer_read_address(&mctx->fbr, fb,
				sizeof(struct pro_msg_base));
		type = base->type;
		fbr_buffer_read_discard(&mctx->fbr, fb);
		switch(type) {
			case PRO_MSG_ME_MESSAGE:
				pro_msg = fbr_buffer_read_address(&mctx->fbr, fb,
						sizeof(struct pro_msg_me_message));
				msg_info = &pro_msg->info;
				sm_free(msg_info->msg);
				fbr_buffer_read_advance(&mctx->fbr, fb);
				break;
			case PRO_MSG_CLIENT_VALUE:
				pro_client = fbr_buffer_read_address(&mctx->fbr, fb,
						sizeof(struct pro_msg_client_value));
				send_proxy_value(ME_A_ pro_client->value);
				sm_free(pro_client->value);
				fbr_buffer_read_advance(&mctx->fbr, fb);
				break;
		}
	}
}

void pro_start(ME_P)
{
	if(0 != mctx->fiber_proposer)
		fbr_reclaim(&mctx->fbr, mctx->fiber_proposer);
	mctx->fiber_proposer = fbr_create(&mctx->fbr, "proposer", pro_fiber, NULL, 0);
	assert(0 != mctx->fiber_proposer);
	fbr_transfer(&mctx->fbr, mctx->fiber_proposer);
}

void pro_stop(ME_P)
{
	if(0 != mctx->fiber_proposer)
		fbr_reclaim(&mctx->fbr, mctx->fiber_proposer);
	mctx->fiber_proposer = fbr_create(&mctx->fbr, "proposer_proxy",
			proxy_fiber, NULL, 0);
	assert(0 != mctx->fiber_proposer);
	fbr_transfer(&mctx->fbr, mctx->fiber_proposer);
}
