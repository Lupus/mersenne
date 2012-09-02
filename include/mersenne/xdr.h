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

#ifndef _XDR_H_
#define _XDR_H_

#include <mersenne/bitmask.h>
#include <mersenne/buffer.h>

typedef struct bm_mask * bm_mask_ptr;
typedef struct buffer * buffer_ptr;

bool_t xdr_timeval(XDR *xdrs, struct timeval *tv);
bool_t xdr_bm_mask_ptr(XDR *xdrs, struct bm_mask **pptr);
bool_t xdr_buffer(XDR *xdrs, struct buffer *buf);

#endif
