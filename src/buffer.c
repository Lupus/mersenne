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
#include <stdlib.h>
#include <assert.h>
#include <mersenne/buffer.h>

void buf_init(struct buffer *buf, char *ptr, size_t size, enum buffer_state
		state)
{
	buf->ptr = ptr;
	buf->size1 = size;
	buf->state = state;
	buf->prev = NULL;
	buf->next = NULL;
}

void buf_copy(struct buffer *to, struct buffer *from)
{
	to->size1 = from->size1;
	memcpy(to->ptr, from->ptr, from->size1);
	to->state = from->state;
}

void buf_share(struct buffer *to, struct buffer *from)
{
	to->size1 = from->size1;
	to->ptr = from->ptr;
	to->state = from->state;
}

int buf_cmp(struct buffer *a, struct buffer *b)
{
	if(a->size1 != b->size1)
		return a->size1 - b->size1;
	return(memcmp(a->ptr, b->ptr, b->size1));
}

struct buffer * buf_deep_clone(struct buffer *from)
{
	struct buffer *bn = malloc(sizeof(struct buffer));
	buf_init(bn, malloc(from->size1), from->size1, BS_EMPTY);
	buf_copy(bn, from);
	return bn;
}

void buf_free(struct buffer *buffer)
{
	free(buffer->ptr);
	free(buffer);
}
