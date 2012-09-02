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

%#include <mersenne/xdr.h>

enum cl_message_type {
	CL_NEW_VALUE,
	CL_LEARNED_VALUE
};

struct cl_new_value_data {
	struct buffer *value;
};

struct cl_learned_value_data {
	uint64_t i;
	struct buffer *value;
};

union cl_message switch(cl_message_type type) {
	case CL_NEW_VALUE:
	struct cl_new_value_data new_value;
	case CL_LEARNED_VALUE:
	struct cl_learned_value_data learned_value;

	default:
	void;
};
