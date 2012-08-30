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
#ifndef _BUFFER_H_
#define _BUFFER_H_

#include <string.h>

struct buffer {
	char *ptr;
	unsigned int size1;

	struct buffer *next, *prev;
};

void buf_init(struct buffer *buf, size_t size);
int buf_cmp(struct buffer *a, struct buffer *b);
void buffer_sm_destructor(void *context, void *ptr);

#endif
