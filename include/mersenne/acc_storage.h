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
#include <sys/queue.h>
#include <uthash.h>
#include <evfibers/fiber.h>

#include <stdint.h>
#include <mersenne/kvec.h>
#include <mersenne/context_fwd.h>
#include <mersenne/buffer.h>

struct acc_instance_record {
	uint64_t iid;
	uint64_t b;
	struct buffer *v;
	uint64_t vb;
	int stored;
	int dirty;
	struct me_message *msg;
	int msg_to_index;
	struct acc_instance_record *r_copy;
	UT_hash_handle hh;
	SLIST_ENTRY(acc_instance_record) entries;
	SLIST_ENTRY(acc_instance_record) dirty_entries;
};

SLIST_HEAD(acc_instance_record_slist, acc_instance_record);

enum acs_log_kind {
	ALK_WAL,
	ALK_SNAP,
};

struct acs_log_dir {
	char *dirname;
	char *ext;
	enum acs_log_kind kind;
	uint64_t *lsn_arr;
	size_t lsn_arr_len;
	size_t lsn_arr_size;
	uint64_t max_lsn;
};

struct wal_log;

struct acs_iov_stat {
	unsigned n_rows;
	unsigned n_buffers;
	unsigned n_bytes;
	unsigned n_useconds;
	unsigned n_flushes;
	unsigned n_sync_useconds;
};

struct acs_context {
	struct acs_log_dir wal_dir;
	struct acs_log_dir snap_dir;
	uint64_t confirmed_lsn;
	struct wal_log *wal;
	size_t writes_per_sync;
	int in_batch;
	uint64_t batch_start_lsn;
	fbr_id_t snapshot_fiber;
	struct acc_instance_record *instances;
	struct acc_instance_record_slist snap_instances;
	struct acc_instance_record_slist dirty_instances;
	uint64_t highest_accepted;
	uint64_t highest_finalized;
	uint64_t lowest_available;
	struct fbr_cond_var highest_finalized_changed;
	struct fbr_mutex snapshot_mutex;
	struct fbr_mutex batch_mutex;
	int dirty;
	kvec_t(struct acs_iov_stat) stats;
	kvec_t(struct acs_iov_stat) stats2;
	uint64_t highest_stored;
	uint64_t cow_min;
	uint64_t cow_max;
};

#define ACS_CONTEXT_INITIALIZER {      \
	.wal_dir = {                   \
		.ext = ".wal",         \
		.kind = ALK_WAL,       \
		.lsn_arr = NULL,       \
		.lsn_arr_len = 0,      \
		.lsn_arr_size = 0,     \
		.max_lsn = 0,          \
	},                             \
	.snap_dir = {                  \
		.ext = ".snap",        \
		.kind = ALK_SNAP,      \
		.lsn_arr = NULL,       \
		.lsn_arr_len = 0,      \
		.lsn_arr_size = 0,     \
		.max_lsn = 0,          \
	},                             \
	.confirmed_lsn = 0,            \
	.wal = NULL,                   \
	.writes_per_sync = 0,          \
	.in_batch = 0,                 \
	.snapshot_fiber = FBR_ID_NULL, \
	.instances = NULL,             \
	.highest_accepted = 0,         \
	.highest_finalized = 0,        \
	.lowest_available = 0,         \
	.dirty = 0,                    \
	.highest_stored = 0,           \
	.cow_min = 0,                  \
	.cow_max = 0,                  \
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
uint64_t acs_get_lowest_available(ME_P);
void acs_set_highest_finalized(ME_P_ uint64_t iid);
void acs_set_highest_finalized_async(ME_P_ uint64_t iid);
void acs_vacuum(ME_P);
int acs_record(ME_P_ struct acc_instance_record **record_ptr, uint64_t iid,
		enum acs_find_mode mode);
int acs_find_record(ME_P_ struct acc_instance_record **record_ptr, uint64_t iid,
		enum acs_find_mode mode);
const struct acc_instance_record *acs_find_record_ro(ME_P_ uint64_t iid);
void acs_store_record(ME_P_ struct acc_instance_record *record);
void acs_free_record(ME_P_ struct acc_instance_record *record);
void acs_destroy(ME_P);

#endif
