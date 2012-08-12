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

static is_func_t * const state_table[IS_MAX] = {
	do_is_empty,
	do_is_p1_pending,
	do_is_p1_ready_no_value,
	do_is_p1_ready_with_value,
	do_is_p2_pending,
	do_is_closed,
	do_is_delivered,
};

static int veql(struct pro_instance *instance)
{
	if(instance->p1.v_size != instance->p2.v_size)
		return 0;
	if(memcmp(instance->p1.v, instance->p2.v, instance->p1.v_size))
		return 0;
	return 1;
}

static int v2_eql_to(struct pro_instance *instance, char *data, int len)
{
	if(instance->p2.v_size != len)
		return 0;
	if(memcmp(instance->p2.v, data, len))
		return 0;
	return 1;
}

static void v1_to_v2(struct pro_instance *instance)
{
	memcpy(instance->p2.v, instance->p1.v, instance->p1.v_size);
	instance->p2.v_size = instance->p1.v_size;
}

static void set_client_v2(struct pro_instance *instance, char *data, int len)
{
	memcpy(instance->p2.v, data, len);
	instance->p2.v_size = len;
	instance->client_value = 1;
}

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
	printf("[PROPOSER] Switching instance %ld ballot %ld from state %s to state %s transition %s\n",
			instance->iid,
			instance->b,
			strval_instance_state(instance->state),
			strval_instance_state(state),
			strval_instance_event_type(base->type));
	instance->state = state;
	run_instance(ME_A_ instance, base);
}

static void reclaim_instance(ME_P_ struct pro_instance *instance)
{
	struct ie_base base;
	struct bm_mask *mask = instance->p1.acks;
	base.type = IE_I;
	ev_timer timer = instance->timer;
	memset(instance, 0, sizeof(struct pro_instance));
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
	//printf("%ld * %d + %d == %ld\n", ballot, MAX_PROC, mctx->me->index, ballot * MAX_PROC + mctx->me->index);
	return ballot * MAX_PROC + mctx->me->index;
}

static uint64_t decode_ballot(ME_P_ uint64_t ballot)
{
	return ballot / MAX_PROC;
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
	data->me_paxos_msg_data_u.accept.v.v_len = instance->p2.v_size;
	data->me_paxos_msg_data_u.accept.v.v_val = instance->p2.v;
	printf("[PROPOSER] Sending accepts for instance %ld\n", instance->iid);
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
	int p_num;
	int acc_maj;
	switch(base->type) {
		case IE_S:
			instance->b = encode_ballot(ME_A_ 0);
			send_prepare(ME_A_ instance);
			ev_timer_set(&instance->timer, 0., TO1);
			ev_timer_again(mctx->loop, &instance->timer);
			break;
		case IE_P:
			p = container_of(base, struct ie_p, b);
			bm_set_bit(instance->p1.acks, p->from->index, 1);
			if(p->data->vb > instance->p1.vb) {
				instance->p1.v_size = p->data->v.v_len;
				memcpy(instance->p1.v, p->data->v.v_val,
						p->data->v.v_len);
			}
			p_num = bm_hweight(instance->p1.acks);
			//puts("[PROPOSER] Got promise!");
			acc_maj = pxs_acceptors_count(ME_A) / 2 + 1;
			if(p_num >= acc_maj) {
				ev_timer_stop(mctx->loop, &instance->timer);
				if(instance->p1.v_size) {
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
			if(instance->p2.v_size) {
				new_base.type = IE_A;
				switch_instance(ME_A_ instance,
						IS_P1_READY_WITH_VALUE,
						&new_base);
			}
			break;
		case IE_NV:
			nv = container_of(base, struct ie_nv, b);
			set_client_v2(instance, nv->data, nv->len);
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
			if(0 == instance->p2.v_size) {
				v1_to_v2(instance);
				instance->client_value = 0;
			} else if(veql(instance)) {
				//Do nothing here?
			} else if(!veql(instance)) {
				if(instance->client_value) {
					//TODO: Push v2 back to pending list
				}
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

void do_is_closed(ME_P_ struct pro_instance *instance, struct ie_base *base)
{
	//TODO: Do we really need this?
}

void do_is_delivered(ME_P_ struct pro_instance *instance, struct ie_base *base)
{
	struct ie_d *d;
	switch(base->type) {
		case IE_D:
			d = container_of(base, struct ie_d, b);
			if(instance->client_value) {
				if(v2_eql_to(instance, d->data, d->len)) {
					//TODO: Client value delivered,
					//TODO: inform it about this
				} else {
					//TODO: Push v2 back to pending list
				}
			}
			ev_timer_stop(mctx->loop, &instance->timer);
			adjust_window(ME_A);
			break;
			fprintf(stderr, "Adjusted window!\n");
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
	p.b.type = IE_P;
	p.from = from;
	p.data = data;
	instance = mctx->pxs.pro.instances + (data->i % PRO_INSTANCE_WINDOW);
	if(instance->b == data->b && IS_P1_PENDING == instance->state)
		run_instance(ME_A_ instance, &p.b);
}

static void do_learn(ME_P_ struct me_paxos_message *pmsg, struct me_peer
		*from)
{
	struct pro_instance *instance;
	struct me_paxos_promise_data *data;
	struct ie_p p;
	data = &pmsg->data.me_paxos_msg_data_u.promise;
	p.b.type = IE_P;
	p.from = from;
	p.data = data;
	instance = mctx->pxs.pro.instances + (data->i % PRO_INSTANCE_WINDOW);
	if(instance->b == data->b && IS_P1_PENDING == instance->state)
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
	instance->p1.acks = fbr_alloc(ME_A_ bm_size(nbits));
	bm_init(instance->p1.acks, nbits);
	ev_timer_init(&instance->timer, instance_timeout_cb, 0., 0.);
	instance->timer.data = ME_A;
	//ev_timer_start(mctx->loop, &instance->timer);
	//puts("timer started");
}

static void free_instance(ME_P_ struct pro_instance *instance)
{
	ev_timer_stop(mctx->loop, &instance->timer);
}


static void proposer_init(ME_P)
{
	int i;
	struct ie_base base;
	size_t size = sizeof(struct pro_instance) * PRO_INSTANCE_WINDOW;
	base.type = IE_I;
	mctx->pxs.pro.max_iid = 0;
	mctx->pxs.pro.instances = fbr_alloc(ME_A_ size);
	memset(mctx->pxs.pro.instances, 0, size);
	for(i = 0; i < PRO_INSTANCE_WINDOW; i++) {
		init_instance(ME_A_ mctx->pxs.pro.instances + i);
		mctx->pxs.pro.instances[i].iid = mctx->pxs.pro.max_iid++;
		run_instance(ME_A_ mctx->pxs.pro.instances + i, &base);
	}
}

static void proposer_shutdown(ME_P)
{
	int i;
	for(i = 0; i < PRO_INSTANCE_WINDOW; i++) {
		free_instance(ME_A_ mctx->pxs.pro.instances + i);
	}
}

static void do_message(ME_P_ struct me_message *msg, struct me_peer *from)
{
	struct me_paxos_message *pmsg;

	pmsg = &msg->me_message_u.paxos_message;

	if(ME_PAXOS_PROMISE == pmsg->data.type) {
		do_promise(ME_A_ pmsg, from);
	} else if(ME_PAXOS_LEARN == pmsg->data.type) {
		do_learn(ME_A_ pmsg, from);
	}
}

static void do_client_value(ME_P_ char *data, int len)
{
	struct ie_nv nv;
	struct pro_instance *instance;
	int i, j;
	int start = mctx->pxs.pro.lowest_non_closed;
	nv.b.type = IE_NV;
	nv.data = data;
	nv.len = len;
	for(j = 0, i = start; j < PRO_INSTANCE_WINDOW; j++, i++) {
		instance = mctx->pxs.pro.instances + (i % PRO_INSTANCE_WINDOW);
		if(IS_P1_READY_NO_VALUE == instance->state) {
			run_instance(ME_A_ instance, &nv.b);
			return;
		}
	}
	warnx("client value is discarded");
	//TODO: add value to pending queue here
}

static void do_delivered_value(ME_P_ uint64_t iid, char *data, int len)
{
	struct ie_d d;
	struct pro_instance *instance;
	d.b.type = IE_D;
	d.data = data;
	d.len = len;
	instance = mctx->pxs.pro.instances + (iid % PRO_INSTANCE_WINDOW);
	switch_instance(ME_A_ instance, IS_DELIVERED, &d.b);
}

void pro_fiber(ME_P)
{
	struct me_message *msg;
	struct me_peer *from;
	struct fbr_call_info *info;
	char *data;
	int len;
	uint64_t iid;

	fbr_next_call_info(ME_A_ NULL);

	proposer_init(ME_A);

start:
	fbr_yield(ME_A);
	while(fbr_next_call_info(ME_A_ &info)) {

		switch(info->argv[0].i) {
			case FAT_ME_MESSAGE:
				msg = info->argv[1].v;
				from = info->argv[2].v;

				do_message(ME_A_ msg, from);
				break;
			case FAT_PXS_CLIENT_VALUE:
				data = info->argv[1].v;
				len = info->argv[2].i;

				do_client_value(ME_A_ data, len);
				break;
			case FAT_PXS_DELIVERED_VALUE:
				iid = info->argv[1].i;
				data = info->argv[2].v;
				len = info->argv[3].i;

				do_delivered_value(ME_A_ iid, data, len);
				break;
			case FAT_QUIT:
				goto fiber_exit;
		}

		fbr_free_call_info(ME_A_ info);
	}
	goto start;
fiber_exit:
	proposer_shutdown(ME_A);
}

void pro_start(ME_P)
{
	fbr_reset(ME_A_ mctx->fiber_proposer);
	fbr_call(ME_A_ mctx->fiber_proposer, 0);
}

void pro_stop(ME_P)
{
	fbr_call(ME_A_ mctx->fiber_proposer, 1, fbr_arg_i(FAT_QUIT));
}
