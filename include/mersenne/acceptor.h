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

#include <mersenne/context_fwd.h>

struct me_peer;
struct me_message;

struct acc_instance_record {
	uint64_t iid;
	uint64_t b;
	char *v;
	uint32_t v_size;
	uint64_t vb;

	UT_hash_handle hh;
};

struct acc_context {
	struct acc_instance_record *records;
};

#define ACC_CONTEXT_INITIALIZER { \
	.records = NULL, \
}

void acc_do_message(ME_P_ struct me_message *msg, struct me_peer *from);

#endif
