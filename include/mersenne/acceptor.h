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
	void *handle;
	void *context;
	acs_initialize_func initialize_func;
	acs_get_highest_accepted_func get_highest_accepted_func;
	acs_set_highest_accepted_func set_highest_accepted_func;
	acs_find_record_func find_record_func;
	acs_set_record_value_func set_record_value_func;
	acs_store_record_func store_record_func;
	acs_free_record_func free_record_func;
	acs_destroy_func destroy_func;
};

#define ACC_CONTEXT_INITIALIZER { \
	.handle = NULL, \
	.context = NULL, \
	.initialize_func = NULL, \
	.get_highest_accepted_func = NULL, \
	.set_highest_accepted_func = NULL, \
	.find_record_func = NULL, \
	.store_record_func = NULL, \
	.free_record_func = NULL, \
	.destroy_func = NULL, \
}

void acc_fiber(struct fbr_context *fiber_context);
void acc_init_storage(ME_P);
void acc_free_storage(ME_P);

#endif
