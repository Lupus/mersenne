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
	struct buffer *buffer;
};

void lea_fiber(struct fbr_context *fiber_context, void *_arg);
void lea_local_fiber(struct fbr_context *fiber_context, void *_arg);

#endif
