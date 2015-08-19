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

#ifndef _PROPOSER_PRV_H_
#define _PROPOSER_PRV_H_

#include <ev.h>

#include <mersenne/context.h>
#include <mersenne/me_protocol.h>
#include <mersenne/bitmask.h>
#include <mersenne/buffer.h>
#include <mersenne/util.h>

#define PRO_INSTANCE_WINDOW mctx->args_info.proposer_instance_window_arg
#define TO1 mctx->args_info.proposer_timeout_1_arg
#define TO2 mctx->args_info.proposer_timeout_2_arg

enum instance_state {
    IS_EMPTY = 0,
    IS_P1_PENDING,
    IS_P1_READY_NO_VALUE,
    IS_P1_READY_WITH_VALUE,
    IS_P2_PENDING,
    IS_DELIVERED,
    IS_MAX
};

struct pro_instance {
	uint64_t iid;
	uint64_t b;
	enum instance_state state;
	struct {
		struct buffer *v;
		uint64_t vb;
		struct bm_mask *acks;
	} p1;
	struct {
		struct buffer *v;
	} p2;
	int client_value;
	ev_timer timer;
	int timed_out;
	int nfails;
	struct perf_snap state_snaps[IS_MAX];
};

enum instance_event_type {
	IE_I = 0,
	IE_S,
	IE_TO,
	IE_TO21,
	IE_P,
	IE_R0,
	IE_R1,
	IE_NV,
	IE_A,
	IE_E,
	IE_D,
};

struct ie_base {
	enum instance_event_type type;
};

struct ie_p {
	struct me_peer *from;
	struct me_paxos_promise_data *data;
	struct ie_base b;
};

struct ie_nv {
	struct buffer *buffer;
	struct ie_base b;
};

struct ie_d {
	struct buffer *buffer;
	struct ie_base b;
};

struct ie_to {
	int propagated_from_p2;
	struct ie_base b;
};

typedef void is_func_t(ME_P_ struct pro_instance *instance, struct ie_base *base);

void do_is_empty(ME_P_ struct pro_instance *instance, struct ie_base *base);
void do_is_p1_pending(ME_P_ struct pro_instance *instance, struct ie_base *base);
void do_is_p1_ready_no_value(ME_P_ struct pro_instance *instance, struct ie_base *base);
void do_is_p1_ready_with_value(ME_P_ struct pro_instance *instance, struct ie_base *base);
void do_is_p2_pending(ME_P_ struct pro_instance *instance, struct ie_base *base);
void do_is_delivered(ME_P_ struct pro_instance *instance, struct ie_base *base);

#endif
