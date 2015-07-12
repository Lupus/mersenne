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

#ifndef _LEARNER_H_
#define _LEARNER_H_

#include <stdint.h>
#include <leveldb/c.h>
#include <pthread.h>

#include <evfibers/fiber.h>
#include <mersenne/context_fwd.h>

struct lea_fiber_arg {
	struct fbr_buffer *buffer;
	uint64_t starting_iid;
};

struct lea_instance_info {
	uint64_t iid;
	struct buffer *buffer;
};

struct lea_context {
	uint64_t first_non_delivered;
	uint64_t highest_seen;
	uint64_t next_retransmit;
	uint64_t lowest_cached;
	struct lea_instance *instances;
	struct lea_instance_info *cache;
	struct me_context *mctx;
	struct lea_acc_state *acc_states;
	struct fbr_cond_var delivered;
	struct fbr_buffer buffer;
	leveldb_t *ldb;
	leveldb_options_t *ldb_options;
	leveldb_cache_t *ldb_cache;
	leveldb_writebatch_t *ldb_batch;
	leveldb_writeoptions_t *ldb_write_options_sync;
	leveldb_writeoptions_t *ldb_write_options_async;
	leveldb_readoptions_t *ldb_read_options_cache;
	pthread_cond_t fnd_pcond;
	uint8_t last_checksum[16];
};

#define LEA_CONTEXT_INITIALIZER {              \
	.first_non_delivered = 0,              \
	.highest_seen = 0,                     \
	.next_retransmit = 0,                  \
	.instances = NULL,                     \
	.acc_states = NULL,                    \
	.ldb = NULL,                           \
	.ldb_write_options_async = NULL,       \
	.fnd_pcond = PTHREAD_COND_INITIALIZER, \
}

void lea_fiber(struct fbr_context *fiber_context, void *_arg);
void lea_local_fiber(struct fbr_context *fiber_context, void *_arg);
void lea_get_or_wait_for_instance(ME_P_ uint64_t iid, char **value, size_t *sz);

#endif
