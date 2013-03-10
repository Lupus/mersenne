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
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <assert.h>
#include <err.h>
#include <rpc/xdr.h>
#include <tcutil.h>
#include <tchdb.h>

#include <mersenne/acc_storage.h>
#include <mersenne/me_protocol.h>
#include <mersenne/sharedmem.h>

static const char key[] = "highest_accepted";
static const char key2[] = "highest_finalized";

struct context {
	TCHDB *db;
};

static bool_t xdr_instance_record(XDR *xdrs, struct acc_instance_record *record)
{
	return xdr_uint64_t(xdrs, &record->iid) &&
		xdr_uint64_t(xdrs, &record->b) &&
		xdr_pointer(xdrs, (char **)&record->v, sizeof(struct buffer),
				(xdrproc_t)xdr_buffer) &&
		xdr_uint64_t(xdrs, &record->vb) &&
		xdr_int(xdrs, &record->is_final);
}


void *initialize(int argc, char **argv)
{
	char *opstring = "i:";
	char db_filename[64];
	int opt = 0;
	int result;
	struct context *ctx;
	int index;

	optind = 0; //forces the invocation of an internal initialization routine
	while(-1 != (opt = getopt(argc, argv, opstring))) {
		switch(opt) {
			case 'i':
				index = atoi(optarg);
				break;
			case '?':
				errx(EXIT_FAILURE, "invalid options for redis "
						"storage module");
		}
	}
	snprintf(db_filename, sizeof(db_filename), "acceptor_storage_%d.tchdb",
			index);
	ctx = malloc(sizeof(struct context));
	ctx->db = tchdbnew();
	result = tchdbopen(ctx->db, db_filename, HDBOWRITER | HDBOCREAT);
	if (!result)
		errx(EXIT_FAILURE, "failed to create TCHDB");
	return ctx;
}

uint64_t get_highest_accepted(void *context)
{
	struct context *ctx = (struct context *)context;
	uint64_t iid;
	int result;

	result = tchdbget3(ctx->db, key, sizeof(key), &iid, sizeof(iid));
	if (-1 == result)
		return 0ULL;

	assert(sizeof(iid) == result);
	return iid;
}

void set_highest_accepted(void *context, uint64_t iid)
{
	struct context *ctx = (struct context *)context;
	int result;
	result = tchdbput(ctx->db, key, sizeof(key), &iid, sizeof(iid));
	if (!result)
		errx(EXIT_FAILURE, "failed to set highest accepted");
}

uint64_t get_highest_finalized(void *context)
{
	struct context *ctx = (struct context *)context;
	uint64_t iid;
	int result;

	result = tchdbget3(ctx->db, key2, sizeof(key2), &iid, sizeof(iid));
	if (-1 == result)
		return 0ULL;

	assert(sizeof(iid) == result);
	return iid;
}

void set_highest_finalized(void *context, uint64_t iid)
{
	struct context *ctx = (struct context *)context;
	int result;
	result = tchdbput(ctx->db, key2, sizeof(key2), &iid, sizeof(iid));
	if (!result)
		errx(EXIT_FAILURE, "failed to set highest finalized");
}

int find_record(void *context, struct acc_instance_record **rptr, uint64_t iid,
		enum acs_find_mode mode)
{
	struct context *ctx = (struct context *)context;
	struct acc_instance_record *r = NULL;
	struct buffer *tmp;
	int found = 1;
	XDR xdrs;
	void *record;
	int size;

	record = tchdbget(ctx->db, &iid, sizeof(iid), &size);
	if (NULL == record) {
		found = 0;
		if (mode == ACS_FM_CREATE) {
			r = malloc(sizeof(struct acc_instance_record));
			memset(r, 0x00, sizeof(struct acc_instance_record));
		}
	} else {
			r = malloc(sizeof(struct acc_instance_record));
			memset(r, 0x00, sizeof(struct acc_instance_record));
			xdrmem_create(&xdrs, record, size, XDR_DECODE);
			if (!xdr_instance_record(&xdrs, r))
				errx(EXIT_FAILURE, "xdr_instance_record: unable "
						"to decode value from tokyo cabinet");
			xdr_destroy(&xdrs);
			if(r->v) {
				tmp = r->v;
				r->v = buf_sm_steal(r->v);
				free(tmp);
			}
			free(record);
	}
	*rptr = r;
	return found;
}

void store_record(void *context, struct acc_instance_record *record)
{
	struct context *ctx = (struct context *)context;
	XDR xdrs;
	uint64_t size;
	char buf[ME_MAX_XDR_MESSAGE_LEN];
	int result;

	xdrmem_create(&xdrs, buf, ME_MAX_XDR_MESSAGE_LEN, XDR_ENCODE);
	if (!xdr_instance_record(&xdrs, record))
		errx(EXIT_FAILURE, "xdr_instance_record: unable "
				"to encode value for tokyo cabinet");
	size = xdr_getpos(&xdrs);
	xdr_destroy(&xdrs);

	result = tchdbput(ctx->db, &record->iid, sizeof(record->iid), buf, size);
	if (!result)
		errx(EXIT_FAILURE, "failed to store a record");
}

void free_record(void *context, struct acc_instance_record *record)
{
	if(record->v)
		sm_free(record->v);
	free(record);
}

void destroy(void *context)
{
	struct context *ctx = (struct context *)context;
	int result;

	result = tchdbclose(ctx->db);
	tchdbdel(ctx->db);
	if (!result)
		errx(EXIT_FAILURE, "failed to close TCHDB");
	free(ctx);
}
