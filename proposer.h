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
#include <context_fwd.h>

struct me_peer;
struct me_message;
struct pro_instance;

struct pro_context {
	struct pro_instance *instances;
};

#define PRO_CONTEXT_INITIALIZER { \
	.instances = NULL, \
}

void pro_do_message(ME_P_ struct me_message *msg, struct me_peer *from);
void pro_init(ME_P);

#endif
