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
#include <mersenne/context_fwd.h>
#include <mersenne/bitmask.h>
#include <mersenne/me_protocol.h>

#define LEA_INSTANCE_WINDOW 5

struct lea_instance {
	uint64_t iid;
	uint64_t b;
	struct buffer v;
	char v_data[ME_MAX_XDR_MESSAGE_LEN];
	struct bm_mask *acks;
	int closed;
};

struct me_peer;
struct me_message;

struct lea_context {
	uint64_t first_non_delivered;
	struct lea_instance instances[LEA_INSTANCE_WINDOW];
};

#define LEA_CONTEXT_INITIALIZER { \
	.first_non_delivered = 0, \
	.instances = {{0}}, \
}

void lea_fiber(ME_P);

#endif
