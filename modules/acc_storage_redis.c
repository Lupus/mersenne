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
#include <hiredis/hiredis.h>

#include <mersenne/acc_storage.h>
#include <mersenne/me_protocol.h>
#include <mersenne/sharedmem.h>

struct context {
	redisContext *rctx;
};

static bool_t xdr_instance_record(XDR *xdrs, struct acc_instance_record *record)
{
	return xdr_uint64_t(xdrs, &record->iid) &&
		xdr_uint64_t(xdrs, &record->b) &&
		xdr_pointer(xdrs, (char **)&record->v, sizeof(struct buffer), (xdrproc_t)xdr_buffer) &&
		xdr_uint64_t(xdrs, &record->vb);
}


void * initialize(int argc, char **argv)
{
	char *opstring = "s:i:";
	char *socket = "/var/run/redis/redis.sock";
	unsigned int db_index = 0;
	int opt = 0;
	redisReply *reply;
	optind = 0; //forces the invocation of an internal initialization routine
	while(-1 != (opt = getopt(argc, argv, opstring))) {
		switch(opt) {
			case 's':
				socket = optarg;
				break;
			case 'i':
				db_index = atoi(optarg);
				break;
			case '?':
				errx(EXIT_FAILURE, "invalid options for redis "
						"storage module");
		}
	}
	struct context *ctx = malloc(sizeof(struct context));
	ctx->rctx = redisConnectUnix(socket);
	if (ctx->rctx->err)
		errx(EXIT_FAILURE, "redis connect failed: %s", ctx->rctx->errstr);
	
	reply = redisCommand(ctx->rctx, "SELECT %d", db_index);
	switch(reply->type) {
		case REDIS_REPLY_STATUS:
			//OK
			freeReplyObject(reply);
			break;
		case REDIS_REPLY_ERROR:
			errx(EXIT_FAILURE, "redis reply error: %s", reply->str);
		default:
			errx(EXIT_FAILURE, "unexpected redis reply type: %d", reply->type);
	}
	return ctx;
}

uint64_t get_highest_accepted(void *context)
{
	struct context *ctx = (struct context *)context;
	redisReply *reply;
	reply = redisCommand(ctx->rctx, "GET highest_accepted");
	uint64_t result;
	switch(reply->type) {
		case REDIS_REPLY_NIL:
			freeReplyObject(reply);
			return 0ULL;
		case REDIS_REPLY_STRING:
			assert(reply->len == sizeof(uint64_t));
			memcpy(&result, reply->str, sizeof(uint64_t));
			freeReplyObject(reply);
			return result;
		case REDIS_REPLY_ERROR:
			errx(EXIT_FAILURE, "redis reply error: %s", reply->str);
		default:
			errx(EXIT_FAILURE, "unexpected redis reply type: %d", reply->type);
	}
}

void set_highest_accepted(void *context, uint64_t iid)
{
	struct context *ctx = (struct context *)context;
	redisReply *reply;
	reply = redisCommand(ctx->rctx, "SET highest_accepted %b", &iid, sizeof(iid));
	assert(NULL != reply);
	switch(reply->type) {
		case REDIS_REPLY_STATUS:
			//OK
			freeReplyObject(reply);
			return;
		case REDIS_REPLY_ERROR:
			errx(EXIT_FAILURE, "redis reply error: %s", reply->str);
		default:
			errx(EXIT_FAILURE, "unexpected redis reply type: %d", reply->type);
	}
}

int find_record(void *context, struct acc_instance_record **rptr, uint64_t iid,
		enum acs_find_mode mode)
{
	struct context *ctx = (struct context *)context;
	struct acc_instance_record *r = NULL;
	int found = 1;
	redisReply *reply;
	XDR xdrs;
	
	reply = redisCommand(ctx->rctx, "GET rec:%d", iid);
	switch(reply->type) {
		case REDIS_REPLY_NIL:
			found = 0;
			if(mode == ACS_FM_CREATE) {
				r = malloc(sizeof(struct acc_instance_record));
				memset(r, 0x00, sizeof(struct acc_instance_record));
			}
			break;
		case REDIS_REPLY_STRING:
			r = malloc(sizeof(struct acc_instance_record));
			memset(r, 0x00, sizeof(struct acc_instance_record));
			xdrmem_create(&xdrs, reply->str, reply->len, XDR_DECODE);
			if(!xdr_instance_record(&xdrs, r))
				errx(EXIT_FAILURE, "xdr_instance_record: unable "
						"to decode value from redis");
			xdr_destroy(&xdrs);
			break;
		case REDIS_REPLY_ERROR:
			errx(EXIT_FAILURE, "redis reply error: %s", reply->str);
		default:
			errx(EXIT_FAILURE, "unexpected redis reply type: %d", reply->type);
	}
	freeReplyObject(reply);
	*rptr = r;
	return found;
}

void set_record_value(void *context, struct acc_instance_record *record, struct buffer *v)
{
	if(record->v)
		sm_free(record->v);
	record->v = v;
	if(v)
		sm_in_use(v);
}

void store_record(void *context, struct acc_instance_record *record)
{
	struct context *ctx = (struct context *)context;
	redisReply *reply;
	XDR xdrs;
	uint64_t size;
	char buf[ME_MAX_XDR_MESSAGE_LEN];
	
	xdrmem_create(&xdrs, buf, ME_MAX_XDR_MESSAGE_LEN, XDR_ENCODE);
	if(!xdr_instance_record(&xdrs, record))
		errx(EXIT_FAILURE, "xdr_instance_record: unable "
				"to encode value for redis");
	size = xdr_getpos(&xdrs);
	xdr_destroy(&xdrs);

	reply = redisCommand(ctx->rctx, "SET rec:%d %b", record->iid, buf, size);
	switch(reply->type) {
		case REDIS_REPLY_STATUS:
			//OK
			freeReplyObject(reply);
			return;
		case REDIS_REPLY_ERROR:
			errx(EXIT_FAILURE, "redis reply error: %s", reply->str);
		default:
			errx(EXIT_FAILURE, "unexpected redis reply type: %d", reply->type);
	}
}

void free_record(void *context, struct acc_instance_record *record)
{
	if(record->v)
		xdr_free((xdrproc_t)xdr_instance_record, (char *)record);
	free(record);
}

void destroy(void *context)
{
	struct context *ctx = (struct context *)context;
	redisFree(ctx->rctx);
	free(ctx);
}
