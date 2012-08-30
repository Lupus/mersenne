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
#include <assert.h>
#include <err.h>
#include <uthash.h>

#include <mersenne/acc_storage.h>
#include <mersenne/sharedmem.h>

#define HASH_FIND_WIID(head,findiid,out) \
	HASH_FIND(hh,head,findiid,sizeof(uint64_t),out)
#define HASH_ADD_WIID(head,iidfield,add) \
	HASH_ADD(hh,head,record.iidfield,sizeof(uint64_t),add)

struct acc_instance_wrapper {
	struct acc_instance_record record;
	int stored;
	struct buffer *stored_v;
	UT_hash_handle hh;
};

struct context {
	struct acc_instance_wrapper *instances;
	uint64_t highest_accepted;
};

void * initialize(int argc, char *argv[])
{
	struct context *ctx = malloc(sizeof(struct context));
	ctx->instances = NULL;
	ctx->highest_accepted = 0;
	return ctx;
}

uint64_t get_highest_accepted(void *context)
{
	struct context *ctx = (struct context *)context;
	return ctx->highest_accepted;
}

void set_highest_accepted(void *context, uint64_t iid)
{
	struct context *ctx = (struct context *)context;
	ctx->highest_accepted = iid;
}

int find_record(void *context, struct acc_instance_record **rptr, uint64_t iid,
		enum acs_find_mode mode)
{
	struct context *ctx = (struct context *)context;
	struct acc_instance_wrapper *w = NULL;
	int found = 1;
	
	HASH_FIND_WIID(ctx->instances, &iid, w);
	if(NULL == w) {
		found = 0;
		if(mode == ACS_FM_CREATE) {
			w = malloc(sizeof(struct acc_instance_wrapper));
			w->stored = 0;
			w->stored_v = NULL;
		}
	}
	w->record.v = w->stored_v;
	*rptr = (struct acc_instance_record *)w;
	return found;
}

void store_record(void *context, struct acc_instance_record *record)
{
	struct context *ctx = (struct context *)context;
	struct acc_instance_wrapper *w = NULL;

	HASH_FIND_WIID(ctx->instances, &record->iid, w);
	if(NULL == w) {
		w = (struct acc_instance_wrapper *)record;
		w->stored = 1;
		HASH_ADD_WIID(ctx->instances, iid, w);
	}
	if(NULL == w->stored_v && NULL != record->v)
		w->stored_v = sm_in_use(record->v);
}

void free_record(void *context, struct acc_instance_record *record)
{
	struct acc_instance_wrapper *w;
	w = (struct acc_instance_wrapper *)record;
	if(!w->stored)
		free(record);
}

void destroy(void *context)
{
	struct context *ctx = (struct context *)context;
	free(ctx);
}
