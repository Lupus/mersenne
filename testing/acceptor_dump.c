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
#include <dlfcn.h>
#include <rpc/xdr.h>
#include <hiredis/hiredis.h>

#include <mersenne/acc_storage.h>
#include <mersenne/me_protocol.h>

static void *handle;
static void *context;
static acs_initialize_func initialize_func;
static acs_get_highest_accepted_func get_highest_accepted_func;
static acs_set_highest_accepted_func set_highest_accepted_func;
static acs_find_record_func find_record_func;
static acs_store_record_func store_record_func;
static acs_free_record_func free_record_func;
static acs_destroy_func destroy_func;

static void attach_symbol(void **vptr, const char *symbol)
{
	char *error;
	*vptr = dlsym(handle, symbol);
	if (NULL != (error = dlerror()))
		errx(EXIT_FAILURE, "dlsym: %s", error);
}

static void dump_record(struct acc_instance_record *r)
{
	char buf[r->v->size1 + 1];
	if(r->v->size1 > 64) {
		snprintf(buf, 64 + 1, "%s", r->v->ptr);
		snprintf(buf + 64, 15, "...(truncated)");
	} else
		snprintf(buf, r->v->size1 + 1, "%s", r->v->ptr);
	printf("iid: %lu, b: %lu, vb: %lu, value size: %u, value: ``%s''\n", r->iid, r->b, r->vb, r->v->size1, buf);
}

int main(int argc, char *argv[]) {
	uint64_t highest;
	uint64_t i;
	struct acc_instance_record *r;
	handle = dlopen(argv[1], RTLD_NOW);
	if (!handle)
               errx(EXIT_FAILURE, "dlopen: %s", dlerror());
	dlerror();
	attach_symbol((void **)&destroy_func, "destroy");
	attach_symbol((void **)&find_record_func, "find_record");
	attach_symbol((void **)&get_highest_accepted_func, "get_highest_accepted");
	attach_symbol((void **)&initialize_func, "initialize");
	attach_symbol((void **)&set_highest_accepted_func, "set_highest_accepted");
	attach_symbol((void **)&store_record_func, "store_record");
	attach_symbol((void **)&free_record_func, "free_record");
	argv[1] = argv[0];
	context = (*initialize_func)(argc - 1, argv + 1);
	highest = (*get_highest_accepted_func)(context);
	printf("Highest accepted is %lu\n", highest);
	printf("================  records  ==================\n");
	for(i = 0; i <= highest; i++) {
		if(0 == (*find_record_func)(context, &r, i, ACS_FM_JUST_FIND))
			continue;
		dump_record(r);
		(*free_record_func)(context, r);
	}
	printf("=============================================\n");
	printf("Dumped %lu records\n", i - 1);
}
