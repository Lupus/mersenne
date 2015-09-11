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

#ifndef _LEARNER_H_
#define _LEARNER_H_

#include <stdint.h>

#include <evfibers/fiber.h>
#include <mersenne/context_fwd.h>

struct lea_fiber_arg {
	struct fbr_buffer *buffer;
	uint64_t starting_iid;
};

struct lea_instance_info {
	uint64_t iid;
	uint64_t vb;
	struct buffer *buffer;
};

struct lea_local_state_i {
	const char *name;
	uint64_t i;
	const char *state;
	TAILQ_ENTRY(lea_local_state_i) entries;
};

TAILQ_HEAD(lea_local_state_tailq, lea_local_state_i);

struct lea_context {
	uint64_t first_non_delivered;
	uint64_t highest_seen;
	uint64_t next_retransmit;
	struct lea_instance *instances;
	struct me_context *mctx;
	struct lea_acc_state *acc_states;
	struct fbr_cond_var delivered;
	struct lea_fiber_arg *arg;
	struct fbr_buffer buffer;
	struct lea_local_state_tailq local_states;
};

#define LEA_CONTEXT_INITIALIZER {              \
	.first_non_delivered = 0,              \
	.highest_seen = 0,                     \
	.next_retransmit = 0,                  \
	.instances = NULL,                     \
	.acc_states = NULL,                    \
}

struct JsonNode;

void lea_fiber(struct fbr_context *fiber_context, void *_arg);
void lea_local_fiber(struct fbr_context *fiber_context, void *_arg);

struct JsonNode *lea_get_state_dump(ME_P);

#endif
