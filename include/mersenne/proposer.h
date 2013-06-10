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

#ifndef _PROPOSER_H_
#define _PROPOSER_H_

#include <stdint.h>
#include <evfibers/fiber.h>
#include <mersenne/context_fwd.h>
#include <mersenne/buffer.h>
#include <mersenne/message.h>

struct me_peer;
struct me_message;
struct pro_instance;

enum pro_msg_type {
	PRO_MSG_ME_MESSAGE = 1,
	PRO_MSG_CLIENT_VALUE,
};

struct pro_msg_base {
	enum pro_msg_type type;
};

struct pro_msg_me_message {
	struct pro_msg_base base;
	struct msg_info info;
};

struct pro_msg_client_value {
	struct pro_msg_base base;
	struct buffer *value;
};

struct pending_value {
	struct buffer *v;
	struct pending_value *next, *prev;
};

struct pro_context {
	struct pro_instance *instances;
	uint64_t lowest_non_closed;
	struct pending_value *pending;
	int pending_size;
};

#define PRO_CONTEXT_INITIALIZER { \
	.instances = NULL, \
	.pending = NULL, \
	.pending_size = 0, \
}

void pro_fiber(struct fbr_context *fiber_context, void *_arg);
void pro_start(ME_P);
void pro_stop(ME_P);

#endif
