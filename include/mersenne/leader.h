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

#ifndef _LEADER_H_
#define _LEADER_H_

#include <evfibers/fiber.h>
#include <mersenne/me_protocol.h>
#include <mersenne/context_fwd.h>
#include <mersenne/peers.h>
#include <mersenne/message.h>

struct ldr_context {
	int r;
	int leader;
	int delta_count;
	struct fbr_cond_var timeout_cond;
	struct fbr_mutex mutex;
	struct fbr_cond_var changed_cond;
	int timed_out;
	struct ev_timer delta_timer;
};

#define LDR_CONTEXT_INITIALIZER { \
	.r = 0, \
	.leader = 0, \
	.delta_count = 0, \
	.timed_out = 0, \
}

void ldr_fiber(struct fbr_context *fiber_context, void *_arg);
int ldr_is_leader(ME_P);
struct me_peer *ldr_get_or_wait_for_leader(ME_P);
void ldr_wait_for_leader_change(ME_P);
int ldr_round_length(ME_P);

#endif
