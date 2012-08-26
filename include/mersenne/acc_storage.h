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

#ifndef _ACC_STORAGE_H_
#define _ACC_STORAGE_H_

#include <stdint.h>
#include <mersenne/buffer.h>

struct acc_instance_record {
	uint64_t iid;
	uint64_t b;
	struct buffer v;
	uint64_t vb;
};

enum acs_find_mode {
	ACS_FM_CREATE = 0,
	ACS_FM_JUST_FIND
};

typedef void * (*acs_initialize_func)();
typedef uint64_t (*acs_get_highest_accepted_func)(void *context);
typedef void (*acs_set_highest_accepted_func)(void *context, uint64_t iid);
typedef int (*acs_find_record_func)(void *context, struct acc_instance_record **record_ptr, uint64_t iid, enum acs_find_mode mode);
typedef void (*acs_store_record_func)(void *context, struct acc_instance_record *record);
typedef void (*acs_free_record_func)(void *context, struct acc_instance_record *record);
typedef void (*acs_destroy_func)(void *context);

#endif
