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

#ifndef _ACC_STORAGE_H_
#define _ACC_STORAGE_H_

#include <stdio.h>
#include <uthash.h>

#include <stdint.h>
#include <mersenne/context_fwd.h>
#include <mersenne/buffer.h>

struct acc_instance_record {
	uint64_t iid;
	uint64_t b;
	struct buffer *v;
	uint64_t vb;
	int is_final;
	int stored;
	UT_hash_handle hh;
};

struct acs_log_dir {
	char *dirname;
	char *ext;
	uint64_t *lsn_arr;
	size_t lsn_arr_len;
	size_t lsn_arr_size;
	uint64_t max_lsn;
};

struct wal_log;

struct acs_context {
	struct acs_log_dir wal_dir;
	struct acs_log_dir snap_dir;
	uint64_t confirmed_lsn;
	struct wal_log *wal;
	struct acc_instance_record *instances;
	uint64_t highest_accepted;
	uint64_t highest_finalized;
};

#define ACS_CONTEXT_INITIALIZER {  \
	.wal_dir = {               \
		.ext = ".wal",     \
		.lsn_arr = NULL,   \
		.lsn_arr_len = 0,  \
		.lsn_arr_size = 0, \
		.max_lsn = 0,      \
	},                         \
	.snap_dir = {              \
		.ext = ".snap",    \
		.lsn_arr = NULL,   \
		.lsn_arr_len = 0,  \
		.lsn_arr_size = 0, \
		.max_lsn = 0,      \
	},                         \
	.confirmed_lsn = 0,        \
	.wal = NULL,               \
	.instances = NULL,         \
	.highest_accepted = 0,     \
	.highest_finalized = 0,    \
}

enum acs_find_mode {
	ACS_FM_CREATE = 0,
	ACS_FM_JUST_FIND
};

void acs_initialize(ME_P);
void acs_batch_start(ME_P);
void acs_batch_finish(ME_P);
uint64_t acs_get_highest_accepted(ME_P);
void acs_set_highest_accepted(ME_P_ uint64_t iid);
uint64_t acs_get_highest_finalized(ME_P);
void acs_set_highest_finalized(ME_P_ uint64_t iid);
int acs_find_record(ME_P_ struct acc_instance_record **record_ptr, uint64_t iid,
		enum acs_find_mode mode);
void acs_store_record(ME_P_ struct acc_instance_record *record);
void acs_free_record(ME_P_ struct acc_instance_record *record);
void acs_destroy(ME_P);

#endif
