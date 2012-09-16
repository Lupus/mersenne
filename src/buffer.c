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
#include <ctype.h>
#include <stdlib.h>
#include <assert.h>
#include <mersenne/buffer.h>
#include <mersenne/sharedmem.h>

void buf_init(struct buffer *buf, size_t size)
{
	if(size)
		buf->ptr = malloc(size);
	else
		buf->ptr = NULL;
	buf->size1 = size;
}

struct buffer * buf_sm_steal(struct buffer *xdr_buf)
{
	struct buffer *buf;
	buf = sm_alloc_ext(sizeof(struct buffer), buffer_sm_destructor, NULL);
	buf->ptr = xdr_buf->ptr;
	xdr_buf->ptr = NULL;
	buf->size1 = xdr_buf->size1;
	xdr_buf->size1 = 0;
	return buf;
}

int buf_cmp(struct buffer *a, struct buffer *b)
{
	if(a->size1 != b->size1)
		return a->size1 - b->size1;
	return(memcmp(a->ptr, b->ptr, b->size1));
}

void buffer_sm_destructor(void *context, void *ptr)
{
	struct buffer *buffer = ptr;
	free(buffer->ptr);
}

void buffer_print(FILE *stream, struct buffer *buf)
{
       int i;
       for(i = 0; i < buf->size1; i++) {
               if(isprint(buf->ptr[i]) || isspace(buf->ptr[i])) {
                       fprintf(stream, "%c", buf->ptr[i]);
               } else {
                       fprintf(stream, "\\x%02x", (unsigned char)buf->ptr[i]);
               }
       }
}
