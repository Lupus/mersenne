
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

#include <rpc/xdr.h>
#include <xdr.h>

bool_t xdr_timeval(XDR *xdrs, struct timeval *tv)
{
	return (
			xdr_uint64_t(xdrs, (uint64_t *)&tv->tv_sec) &&
			xdr_uint64_t(xdrs, (uint64_t *)&tv->tv_usec)
	       );
}

bool_t xdr_bitmask_ptr(XDR *xdrs, struct bitmask **pptr)
{
	uint32_t size;
	if(XDR_ENCODE == xdrs->x_op)
		size = bitmask_nbits(*pptr);
	if(!xdr_uint32_t(xdrs, &size))
		return FALSE;
	if(XDR_DECODE == xdrs->x_op && NULL == *pptr)
		*pptr = bitmask_alloc(size);
	if(XDR_FREE == xdrs->x_op)
		bitmask_free(*pptr);
	size = bitmask_nbytes(*pptr);
	if(!xdr_opaque(xdrs, (caddr_t)bitmask_mask(*pptr), size))
		return FALSE;
	return TRUE;
}
