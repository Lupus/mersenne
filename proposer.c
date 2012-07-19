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
#include <bitmask.h>

#include <proposer.h>
#include <paxos.h>
#include <context.h>
#include <message.h>
#include <peers.h>
#include <util.h>

#define INSTANCE_WINDOW 100
#define MAX_PROC 100
#define TO1 0.1
#define TO2 0.1

enum instance_state {
    IS_EMPTY = 0,
    IS_P1_PENDING,
    IS_P1_READY_NO_VALUE,
    IS_P1_READY_WITH_VALUE,
    IS_P2_PENDING,
    IS_CLOSED,
    IS_DELIVERED,
    IS_MAX
};

struct pro_instance {
	uint64_t iid;
	uint64_t b;
	enum instance_state state;
	struct {
		char *v;
		uint32_t v_size;
		uint64_t vb;
		struct bitmask *acks;
	} p1;
	struct {
		char *v;
		uint32_t v_size;
	} p2;
	int client_value;
	ev_timer timer;

	struct me_context *mctx;
};

enum instance_event_type {
	IE_S,
	IE_TO,
	IE_P,
	IE_R0,
	IE_R1,
	IE_A,
	IE_E,
	IE_C,
	IE_D,
};

struct ie_base {
	enum instance_event_type type;
};

struct ie_p {
	struct me_peer *from;
	struct ie_base b;
};

typedef void is_func_t(ME_P_ struct pro_instance *instance, struct ie_base *base);

void do_is_empty(ME_P_ struct pro_instance *instance, struct ie_base *base);
void do_is_p1_pending(ME_P_ struct pro_instance *instance, struct ie_base *base);
void do_is_p1_ready_no_value(ME_P_ struct pro_instance *instance, struct ie_base *base);
void do_is_p1_ready_with_value(ME_P_ struct pro_instance *instance, struct ie_base *base);
void do_is_p2_pending(ME_P_ struct pro_instance *instance, struct ie_base *base);
void do_is_closed(ME_P_ struct pro_instance *instance, struct ie_base *base);
void do_is_delivered(ME_P_ struct pro_instance *instance, struct ie_base *base);

static is_func_t * const state_table[IS_MAX] = {
	do_is_empty,
	do_is_p1_pending,
	do_is_p1_ready_no_value,
	do_is_p1_ready_with_value,
	do_is_p2_pending,
	do_is_closed,
	do_is_delivered,
};

static void run_instance(ME_P_ struct pro_instance *instance, struct ie_base *base)
{
	state_table[instance->state](ME_A_ instance, base);
}

static void switch_instance(ME_P_ struct pro_instance *instance, enum
		instance_state state, struct ie_base *base)
{
	instance->state = state;
	run_instance(ME_A_ instance, base);
}

static uint64_t encode_ballot(ME_P_ uint64_t ballot)
{
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
	switch(base->type) {
		case IE_S:
			instance->b = encode_ballot(ME_A_ 1);
			send_prepare(ME_A_ instance);
			instance->timer.repeat = TO1;
			ev_timer_again(mctx->loop, &instance->timer);
			break;
		case IE_P:
			p = container_of(base, struct ie_p, b);
			bitmask_setbit(instance->p1.acks, p->from->index);
			//TODO: Continue here and change acks from bitmask to
			//something reasonable
		case IE_TO:
			bitmask_clearall(instance->p1.acks);
			b = decode_ballot(ME_A_ instance->b);
			instance->b = encode_ballot(ME_A_ ++b);
			send_prepare(ME_A_ instance);
			instance->timer.repeat = TO1;
			ev_timer_again(mctx->loop, &instance->timer);
			break;
		default:
			errx(EXIT_FAILURE, "inappropriate transition %d for "
					"state %d", base->type,
					instance->state);
	}
}

void do_is_p1_ready_no_value(ME_P_ struct pro_instance *instance, struct ie_base *base)
{
}

void do_is_p1_ready_with_value(ME_P_ struct pro_instance *instance, struct ie_base *base)
{
}

void do_is_p2_pending(ME_P_ struct pro_instance *instance, struct ie_base *base)
{
}

void do_is_closed(ME_P_ struct pro_instance *instance, struct ie_base *base)
{
}

void do_is_delivered(ME_P_ struct pro_instance *instance, struct ie_base *base)
{
}

static void do_promise(ME_P_ struct me_paxos_message *pmsg, struct me_peer
		*from)
{
	struct pro_instance *instance;
	struct me_paxos_promise_data *data;
	struct ie_p p;
	p.b.type = IE_P;
	p.from = from;
	data = &pmsg->data.me_paxos_msg_data_u.promise;
	instance = mctx->pxs.pro.instances + (data->i % INSTANCE_WINDOW);
	run_instance(ME_A_ instance, &p.b);
}

static void do_learn(ME_P_ struct me_paxos_message *pmsg, struct me_peer
		*from)
{
}

void pro_do_message(ME_P_ struct me_message *msg, struct me_peer *from)
{
	struct me_paxos_message *pmsg;

	pmsg = &msg->me_message_u.paxos_message;
	if(ME_PAXOS_PROMISE == pmsg->data.type) {
		do_promise(ME_A_ pmsg, from);
	} else if(ME_PAXOS_LEARN == pmsg->data.type) {
		do_learn(ME_A_ pmsg, from);
	}
}

static void instance_timeout_cb (EV_P_ ev_timer *w, int revents)
{
	struct pro_instance *instance;
	struct ie_base base;
	base.type = IE_TO;
	instance = container_of(w, struct pro_instance, timer);
	run_instance(instance->mctx, instance, &base);
}

static void init_instance(ME_P_ struct pro_instance *instance)
{
	instance->p1.acks = bitmask_alloc(peer_count(ME_A));
	instance->mctx = ME_A;
	ev_timer_init(&instance->timer, instance_timeout_cb, 0., 0.);
}

void pro_init(ME_P)
{
	int i;
	mctx->pxs.pro.instances = calloc(sizeof(struct pro_instance),
			INSTANCE_WINDOW);
	for(i = 0; i < INSTANCE_WINDOW; i++) {
		init_instance(ME_A_ mctx->pxs.pro.instances + i);
		mctx->pxs.pro.instances[i].iid = i + 1;
		run_instance(ME_A_ mctx->pxs.pro.instances + i, NULL);
	}
}
