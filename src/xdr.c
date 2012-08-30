
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
#include <mersenne/xdr.h>
#include <mersenne/me_protocol.h>
#include <mersenne/sharedmem.h>

bool_t xdr_timeval(XDR *xdrs, struct timeval *tv)
{
	return (
			xdr_uint64_t(xdrs, (uint64_t *)&tv->tv_sec) &&
			xdr_uint64_t(xdrs, (uint64_t *)&tv->tv_usec)
	       );
}

bool_t xdr_bm_mask_ptr(XDR *xdrs, struct bm_mask **pptr)
{
	unsigned int nbits;
	if(XDR_DECODE == xdrs->x_op && NULL == *pptr) {
		if(!xdr_u_int(xdrs, &nbits))
			return FALSE;
		*pptr = malloc(bm_size(nbits));
		bm_init(*pptr, nbits);
	} else if(XDR_ENCODE == xdrs->x_op) {
		if(!xdr_u_int(xdrs, &((*pptr)->nbits)))
			return FALSE;
	}
	if(!xdr_opaque(xdrs, (caddr_t)(*pptr)->mask, (*pptr)->size * sizeof(unsigned long)))
		return FALSE;
	if(XDR_FREE == xdrs->x_op)
		free(*pptr);
	return TRUE;
}

bool_t xdr_sm_bytes (XDR *xdrs, char **cpp, unsigned int *sizep, size_t maxsize)
{
	char *sp = *cpp;
	size_t nodesize;

	if (!xdr_u_int (xdrs, sizep))
		return FALSE;

	nodesize = *sizep;
	if ((nodesize > maxsize) && (xdrs->x_op != XDR_FREE))
		return FALSE;

	switch (xdrs->x_op) {
		case XDR_DECODE:
			if (nodesize == 0)
				return TRUE;
			if (sp == NULL)
				*cpp = sp = (char *) sm_alloc (nodesize);
			// Fallthrough
		case XDR_ENCODE:
			return xdr_opaque (xdrs, sp, nodesize);

		case XDR_FREE:
			if (sp != NULL)
				sm_free (sp);
			return TRUE;
	}
	return FALSE;
}

bool_t xdr_sm_reference (XDR *xdrs, caddr_t *pp, size_t size, xdrproc_t proc)
{
	caddr_t loc = *pp;
	bool_t stat;

	if (loc == NULL)
		switch (xdrs->x_op) {
			case XDR_FREE:
				return TRUE;

			case XDR_DECODE:
				*pp = loc = (caddr_t) sm_calloc (1, size);
				break;
			default:
				break;
		}

	stat = (*proc) (xdrs, loc, ~0U);

	if (xdrs->x_op == XDR_FREE) {
		sm_free (loc);
		*pp = NULL;
	}
	return stat;
}

bool_t xdr_sm_pointer (XDR *xdrs, char **objpp, size_t obj_size, xdrproc_t xdr_obj)
{
	bool_t more_data;

	more_data = (*objpp != NULL);
	if (!xdr_bool (xdrs, &more_data)){
		return FALSE;
	}
	if (!more_data) {
		*objpp = NULL;
		return TRUE;
	}
	return xdr_sm_reference (xdrs, objpp, obj_size, xdr_obj);
}

bool_t xdr_buffer(XDR *xdrs, struct buffer *buf)
{
	if(!xdr_sm_bytes(xdrs, &buf->ptr, &buf->size1, ME_MAX_XDR_MESSAGE_LEN))
		return FALSE;
	if(xdrs->x_op == XDR_DECODE)
		sm_in_use(buf->ptr);
	return TRUE;
}

bool_t xdr_buffer_ptr(XDR *xdrs, struct buffer **pptr)
{
	if(!xdr_sm_pointer(xdrs, (char **)pptr, sizeof(struct buffer), (xdrproc_t)xdr_buffer))
		return FALSE;
	if(NULL != *pptr && xdrs->x_op == XDR_DECODE)
		sm_set_destructor(*pptr, buffer_sm_destructor, NULL);
	return TRUE;
}
