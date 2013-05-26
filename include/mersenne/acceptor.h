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

#ifndef _ACCEPTOR_H_
#define _ACCEPTOR_H_

#include <uthash.h>

#include <evfibers/fiber.h>
#include <mersenne/acc_storage.h>
#include <mersenne/context_fwd.h>
#include <mersenne/buffer.h>

struct me_peer;
struct me_message;

struct acc_context {
	struct acs_context acs;
};

#define ACC_CONTEXT_INITIALIZER {           \
	.acs = ACS_CONTEXT_INITIALIZER,     \
}

void acc_fiber(struct fbr_context *fiber_context, void *_arg);

#endif
