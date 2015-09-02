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
#include <sys/types.h>
#include <sys/uio.h>
#include <dirent.h>
#include <err.h>
#include <errno.h>
#include <zlib.h>
#include <evfibers/eio.h>

#include <mersenne/kvec.h>
#include <mersenne/acc_storage.h>
#include <mersenne/sharedmem.h>
#include <mersenne/message.h>
#include <mersenne/context.h>
#include <mersenne/util.h>
#include <mersenne/statd.h>
#include <mersenne/wal_obj.h>

#define HASH_FIND_WIID(head,findiid,out) \
	HASH_FIND(hh,head,findiid,sizeof(uint64_t),out)
#define HASH_ADD_WIID(head,iidfield,add) \
	HASH_ADD(hh,head,iidfield,sizeof(uint64_t),add)

static double now()
{
	struct timeval t;
	int retval;
	retval = gettimeofday(&t, NULL);
	if (retval)
		err(EXIT_FAILURE, "gettimeofday");
	return t.tv_sec + t.tv_usec * 1e-6;
}

static void wal_replay_state(ME_P_ struct wal_state *w_state)
{
	struct acs_context *ctx = &mctx->pxs.acc.acs;
	ctx->highest_accepted = w_state->highest_accepted;
	ctx->highest_finalized = w_state->highest_finalized;
	ctx->lowest_available = w_state->lowest_available;
}

static void store_record(ME_P_ struct acc_instance_record *record)
{
	struct acs_context *ctx = &mctx->pxs.acc.acs;
	struct acc_instance_record *w = NULL;

	HASH_FIND_WIID(ctx->instances, &record->iid, w);
	if (NULL == w) {
		record->stored = 1;
		HASH_ADD_WIID(ctx->instances, iid, record);
	}
}

static void wal_replay_value(ME_P_ struct wal_value *w_value)
{
	struct acc_instance_record *r;

	//Both return values of acs_find_record are okay in this context
	acs_find_record(ME_A_ &r, w_value->iid, ACS_FM_CREATE);
	r->iid = w_value->iid;
	r->b = w_value->b;
	if (NULL == r->v) {
		r->v = buf_sm_copy(w_value->content.data,
				w_value->content.len);
	} else {
		if (memcmp(r->v->ptr, w_value->content.data,
					r->v->size1)) {
			sm_free(r->v);
			r->v = buf_sm_copy(w_value->content.data,
					w_value->content.len);
		}
	}
	r->vb = w_value->vb;
	store_record(ME_A_ r);
}

static void wal_replay_promise(ME_P_ struct wal_promise *w_promise)
{
	struct acc_instance_record *r;

	//Both return values of acs_find_record are okay in this context
	acs_find_record(ME_A_ &r, w_promise->iid, ACS_FM_CREATE);
	r->iid = w_promise->iid;
	r->b = w_promise->b;
	store_record(ME_A_ r);
}

static void ldb_write_state(ME_P_ struct acs_context *ctx)
{
	msgpack_packer pk;
	msgpack_sbuffer sbuf;
	union wal_rec_any wal_rec;
	int retval;
	char *tmp_data;
	size_t tmp_size;
	const char *key = "acc_state";
	const size_t klen = strlen(key);

	wal_rec.w_type = WAL_REC_TYPE_STATE;
	wal_rec.state.highest_accepted = ctx->highest_accepted;
	wal_rec.state.highest_finalized = ctx->highest_finalized;
	wal_rec.state.lowest_available = ctx->lowest_available;

	msgpack_sbuffer_init(&sbuf);

	msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);
	retval = wal_msg_pack(&pk, &wal_rec);
	if(retval){
		errx(EXIT_FAILURE, "unable to pack wal_state");
	}
	tmp_size = sbuf.size;
	tmp_data = sbuf.data;
	leveldb_writebatch_put(ctx->ldb_batch, key, klen, tmp_data, tmp_size);
	msgpack_sbuffer_destroy(&sbuf);
	ctx->writes_per_sync++;
}

static char *record_iid_to_key(uint64_t iid)
{
	static __thread char buf[256];
	snprintf(buf, sizeof(buf), "acc_rec/%020lld", (long long int)iid);
	return buf;
}

static void ldb_write_value(ME_P_ struct acc_instance_record *r)
{
	struct acs_context *ctx = &mctx->pxs.acc.acs;
	msgpack_packer pk;
	msgpack_sbuffer sbuf;
	union wal_rec_any wal_rec;
	int retval;
	char *tmp_data;
	size_t tmp_size;
	char *key;

	if (r->v) {
		wal_rec.w_type = WAL_REC_TYPE_VALUE;
		wal_rec.value.iid = r->iid;
		wal_rec.value.b = r->b;
		wal_rec.value.vb = r->vb;
		wal_rec.value.content.data = (uint8_t *)r->v->ptr;
		wal_rec.value.content.len = r->v->size1;
	}
	else {
		wal_rec.w_type = WAL_REC_TYPE_PROMISE;
		wal_rec.promise.iid = r->iid;
		wal_rec.promise.b = r->b;
	}

	msgpack_sbuffer_init(&sbuf);

	msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);
	retval = wal_msg_pack(&pk, &wal_rec);
	if(retval){
		errx(EXIT_FAILURE, "unable to pack wal_value");
	}
	tmp_size = sbuf.size;
	tmp_data = sbuf.data;
	key = record_iid_to_key(r->iid);
	leveldb_writebatch_put(ctx->ldb_batch, key, strlen(key), tmp_data,
			tmp_size);

	msgpack_sbuffer_destroy(&sbuf);
	ctx->writes_per_sync++;
}

int find_record(ME_P_ struct acc_instance_record **rptr, uint64_t iid,
		enum acs_find_mode mode);

static void recover_rec(ME_P_ const void *ptr, size_t size)
{
	int retval;
	msgpack_unpacked result;
	union wal_rec_any wal_rec;
	char *error = NULL;

	msgpack_unpacked_init(&result);

	retval = msgpack_unpack_next(&result, ptr, size, NULL);
	if(!retval) {
		errx(EXIT_FAILURE, "unable to deserialize rec for recover");
	}

	retval = wal_msg_unpack(&result.data, &wal_rec, 0, &error);
	if(retval){
		errx(EXIT_FAILURE, "unable to unpack record for recover: %s",
				error);
	}
	switch (wal_rec.w_type) {
	case WAL_REC_TYPE_STATE:
		wal_replay_state(ME_A_ &wal_rec.state);
		break;
	case WAL_REC_TYPE_VALUE:
		wal_replay_value(ME_A_ &wal_rec.value);
		break;
	case WAL_REC_TYPE_PROMISE:
		wal_replay_promise(ME_A_ &wal_rec.promise);
		break;
	default:
		errx(EXIT_FAILURE, "unknown WAL message type: %d",
				wal_rec.w_type);
	}
	msgpack_unpacked_destroy(&result);
}

static void recover(ME_P)
{
	struct acs_context *ctx = &mctx->pxs.acc.acs;
	char *buf;
	size_t len;
	char *error = NULL;
	leveldb_readoptions_t *options = leveldb_readoptions_create();
	leveldb_iterator_t* iter;
	const char *key;
	size_t klen;
	const char *value;
	size_t vlen;

	ev_now_update(mctx->loop);
	fbr_log_i(&mctx->fbr, "Starting recovery of acceptor state");

	leveldb_readoptions_set_verify_checksums(options, 1);
	leveldb_readoptions_set_fill_cache(options, 0);

	key = "acc_state";
	klen = strlen(key);
	buf = leveldb_get(ctx->ldb, options, key, klen, &len, &error);
	if (error)
		errx(EXIT_FAILURE, "leveldb_get() failed: %s", error);
	if (NULL == buf) {
		ctx->highest_accepted = 0;
		ctx->highest_finalized = 0;
		ctx->lowest_available = 0;
	} else {
		recover_rec(ME_A_ buf, len);
		free(buf);
	}

	iter = leveldb_create_iterator(ctx->ldb, options);
	key = record_iid_to_key(ctx->highest_finalized + 1);
	leveldb_iter_seek(iter, key, strlen(key));
	while (0 != leveldb_iter_valid(iter)) {
		key = leveldb_iter_key(iter, &klen);
		if (klen < 8 || memcmp(key, "acc_rec/", 8))
			break;
		value = leveldb_iter_value(iter, &vlen);
		fbr_log_d(&mctx->fbr, "recovering record %.*s, len %zd",
				(unsigned)klen, key, vlen);
		recover_rec(ME_A_ value, vlen);
		leveldb_iter_next(iter);
	}

	ev_now_update(mctx->loop);
	fbr_log_i(&mctx->fbr, "Recovered local state, highest accepted = %zd,"
			" highest finalized = %zd, lowest available = %zd",
			ctx->highest_accepted, ctx->highest_finalized,
			ctx->lowest_available);
	leveldb_iter_destroy(iter);
	leveldb_readoptions_destroy(options);
}

struct stats_summary {
	double min;
	double p25;
	double mean;
	double p50;
	double p75;
	double p99;
	double max;
};

struct stats_calc_arg {
	struct acs_context *ctx;
	struct stats_summary write_io;
	struct stats_summary sync_io;
	double write_speed;
	unsigned bytes_written;
	unsigned count;
};

void acs_initialize(ME_P)
{
	struct acs_context *ctx = &mctx->pxs.acc.acs;
	char *error = NULL;
	char path[PATH_MAX];
	ctx->ldb_options = leveldb_options_create();
	leveldb_options_set_create_if_missing(ctx->ldb_options, 1);
	leveldb_options_set_write_buffer_size(ctx->ldb_options,
			mctx->args_info.ldb_write_buffer_arg);
	leveldb_options_set_max_open_files(ctx->ldb_options,
			mctx->args_info.ldb_max_open_files_arg);
	ctx->ldb_cache =
		leveldb_cache_create_lru(mctx->args_info.ldb_cache_arg);
	leveldb_options_set_cache(ctx->ldb_options, ctx->ldb_cache);
	leveldb_options_set_block_size(ctx->ldb_options,
			mctx->args_info.ldb_block_size_arg);
	if (mctx->args_info.ldb_compression_flag) {
		leveldb_options_set_compression(ctx->ldb_options,
				leveldb_snappy_compression);
	} else {
		leveldb_options_set_compression(ctx->ldb_options,
				leveldb_no_compression);
	}
	snprintf(path, sizeof(path), "%s/acceptor", mctx->args_info.db_dir_arg);
	ctx->ldb = leveldb_open(ctx->ldb_options, path, &error);
	if (error) {
		fbr_log_e(&mctx->fbr, "leveldb error: %s", error);
		exit(EXIT_FAILURE);
	}
	ctx->ldb_batch = leveldb_writebatch_create();
	ctx->ldb_write_options_sync = leveldb_writeoptions_create();
	leveldb_writeoptions_set_sync(ctx->ldb_write_options_sync, 1);
	fbr_mutex_init(&mctx->fbr, &ctx->batch_mutex);
	fbr_cond_init(&mctx->fbr, &ctx->highest_finalized_changed);

	recover(ME_A);
}

void acs_batch_start(ME_P)
{
	struct acs_context *ctx = &mctx->pxs.acc.acs;
	fbr_mutex_lock(&mctx->fbr, &ctx->batch_mutex);
	ctx->writes_per_sync = 0;
	ctx->in_batch = 1;
	SLIST_INIT(&ctx->dirty_instances);
	ctx->dirty = 0;
}

struct ldb_write_custom_cb_arg {
	char *error;
	struct acs_context *ctx;
	double write_time;
};

static eio_ssize_t ldb_write_custom_cb(void *data)
{
	struct ldb_write_custom_cb_arg *arg = data;
	struct acs_context *ctx = arg->ctx;
	double t1, t2;
	t1 = now();
	leveldb_write(ctx->ldb, ctx->ldb_write_options_sync, ctx->ldb_batch,
			&arg->error);
	t2 = now();
	arg->write_time = t2 - t1;
	return 0;
}

static void vacuum_state(ME_P)
{
	struct acs_context *ctx = &mctx->pxs.acc.acs;
	struct acc_instance_record *r;
	uint64_t max_truncated = ctx->lowest_available;
	unsigned n_truncated = 0;
	uint64_t i;
	int found;
	ev_tstamp t1, t2;
	const int trunc = mctx->args_info.acceptor_truncate_arg;

	if (ctx->highest_finalized < trunc)
		return;
	if (0 == max_truncated)
		max_truncated = 1;
	ev_now_update(mctx->loop);
	t1 = ev_now(mctx->loop);
	for (i = max_truncated; i < ctx->highest_finalized - trunc; i++) {
		found = find_record(ME_A_ &r, i, ACS_FM_JUST_FIND);
		if (!found)
			continue;
		HASH_DEL(ctx->instances, r);
		sm_free(r->v);
		free(r);
		n_truncated++;
	}
	if (0 == n_truncated)
		return;
	ctx->lowest_available = i;
	if (ctx->highest_finalized > 1)
		assert(ctx->highest_finalized > ctx->lowest_available);
	ev_now_update(mctx->loop);
	t2 = ev_now(mctx->loop);
	fbr_log_d(&mctx->fbr, "vacuumed the state: highest accepted = %zd,"
			"highest finalized = %zd, lowest available = %zd",
			ctx->highest_accepted, ctx->highest_finalized,
			ctx->lowest_available);
	fbr_log_d(&mctx->fbr, "vacuuming took %f seconds", t2 - t1);
	fbr_log_d(&mctx->fbr, "%d instances disposed", n_truncated);
	statd_send_timer(ME_A_ "acc_storage.vacuum_time", t2 - t1);
	statd_send_gauge(ME_A_ "acc_storage.lowest_available",
			ctx->lowest_available);
}

void acs_batch_finish(ME_P)
{
	struct acs_context *ctx = &mctx->pxs.acc.acs;
	struct acc_instance_record *r, *x;
	int retval;
	struct ldb_write_custom_cb_arg arg;

	ctx->in_batch = 0;

	arg.ctx = ctx;
	arg.error = NULL;
	arg.write_time = 0;
	SLIST_FOREACH_SAFE(r, &ctx->dirty_instances, dirty_entries, x) {
		if (!r->stored) {
			SLIST_REMOVE(&ctx->dirty_instances, r,
					acc_instance_record, dirty_entries);
			if (r->v)
				sm_free(r->v);
			free(r);
			continue;
		}
		ldb_write_value(ME_A_ r);
		r->dirty = 0;
	}

	if (ctx->dirty)
		ldb_write_state(ME_A_ ctx);

	if (0 == ctx->writes_per_sync) {
		fbr_mutex_unlock(&mctx->fbr, &ctx->batch_mutex);
		return;
	}
	retval = fbr_eio_custom(&mctx->fbr, ldb_write_custom_cb, &arg, 0);
	if (retval)
		err(EXIT_FAILURE, "ldb_write_custom_cb failed");

	fbr_log_d(&mctx->fbr, "leveldb_write() finished in %f", arg.write_time);
	leveldb_writebatch_clear(ctx->ldb_batch);

	SLIST_FOREACH(r, &ctx->dirty_instances, dirty_entries) {
		assert(r->stored);
		if (r->msg) {
			fbr_log_d(&mctx->fbr, "flushing pending message for"
					" instance %ld", r->iid);
			if (r->msg_to_index >= 0) {
				msg_send_to(ME_A_ r->msg, r->msg_to_index);
			} else {
				msg_send_all(ME_A_ r->msg);
			}
			free(r->msg);
			r->msg = NULL;
		}
	}

	vacuum_state(ME_A);

	fbr_mutex_unlock(&mctx->fbr, &ctx->batch_mutex);
}

uint64_t acs_get_highest_accepted(ME_P)
{
	return mctx->pxs.acc.acs.highest_accepted;
}

void acs_set_highest_accepted(ME_P_ uint64_t iid)
{
	struct acs_context *ctx = &mctx->pxs.acc.acs;
	assert(1 == ctx->in_batch);
	ctx->highest_accepted = iid;
	ctx->dirty = 1;
}

uint64_t acs_get_highest_finalized(ME_P)
{
	return mctx->pxs.acc.acs.highest_finalized;
}

uint64_t acs_get_lowest_available(ME_P)
{
	struct acs_context *ctx = &mctx->pxs.acc.acs;
	return ctx->lowest_available ?: 1;
}

void acs_set_highest_finalized(ME_P_ uint64_t iid)
{
	struct acs_context *ctx = &mctx->pxs.acc.acs;
	assert(1 == ctx->in_batch);
	ctx->highest_finalized = iid;
	ctx->dirty = 1;
	fbr_cond_broadcast(&mctx->fbr, &ctx->highest_finalized_changed);
}

void acs_set_highest_finalized_async(ME_P_ uint64_t iid)
{
	struct acs_context *ctx = &mctx->pxs.acc.acs;
	ctx->highest_finalized = iid;
	fbr_cond_broadcast(&mctx->fbr, &ctx->highest_finalized_changed);
}

struct ldb_read_custom_cb_arg {
	char *error;
	struct me_context *mctx;
	unsigned max;
	struct iovec *buffers;
	uint64_t i;
	size_t count;
};

static eio_ssize_t ldb_read_custom_cb(void *data)
{
	struct ldb_read_custom_cb_arg *arg = data;
	struct me_context *mctx = arg->mctx;
	leveldb_readoptions_t *options = leveldb_readoptions_create();
	const char *key;
	unsigned x = 0;

	struct acs_context *ctx = &mctx->pxs.acc.acs;
	char *error = NULL;
	char *value;



	arg->buffers = calloc(arg->max, sizeof(struct iovec));
	arg->count = 0;
	for (x = 0; x < arg->max; x++) {
		key = record_iid_to_key(arg->i + x);
		value = leveldb_get(ctx->ldb, options, key, strlen(key),
				&arg->buffers[x].iov_len, &error);
		if (error)
			errx(EXIT_FAILURE, "leveldb_get failed: %s", error);
		if (!value)
			break;

		arg->buffers[x].iov_base = value;
		arg->count++;
	}

	assert(arg->count <= arg->max);
	leveldb_readoptions_destroy(options);
	return 0;
}

struct acc_archive_record *acs_get_archive_records(ME_P_ uint64_t iid,
		unsigned *count)
{
	struct ldb_read_custom_cb_arg arg = { NULL };
	unsigned x;
	int retval;
	msgpack_unpacked result;
	union wal_rec_any wal_rec;
	char *error = NULL;
	struct acc_archive_record *records;

	memset(&arg, 0x00, sizeof(arg));
	arg.mctx = mctx;
	arg.i = iid;
	arg.max = *count;

	assert(iid + *count - 1 < acs_get_lowest_available(ME_A));
	fbr_log_d(&mctx->fbr, "ldb read starting from %lu, count %d", iid,
			*count);
	fbr_eio_custom(&mctx->fbr, ldb_read_custom_cb, &arg, 0);
	*count = arg.count;
	records = calloc(arg.count, sizeof(struct acc_archive_record));
	for (x = 0; x < arg.count; x++) {

		msgpack_unpacked_init(&result);

		retval = msgpack_unpack_next(&result, arg.buffers[x].iov_base,
				arg.buffers[x].iov_len, NULL);
		if (!retval)
			errx(EXIT_FAILURE, "unable to deserialize record");

		retval = wal_msg_unpack(&result.data, &wal_rec, 0, &error);
		if(retval){
			errx(EXIT_FAILURE, "unable to unpack record: %s",
					error);
		}
		assert(WAL_REC_TYPE_VALUE == wal_rec.w_type);

		assert(iid + x == wal_rec.value.iid);
		records[x].iid = wal_rec.value.iid;
		records[x].vb = wal_rec.value.vb;
		records[x].v = buf_sm_copy(wal_rec.value.content.data,
				wal_rec.value.content.len);
		msgpack_unpacked_destroy(&result);
		leveldb_free(arg.buffers[x].iov_base);
	}
	free(arg.buffers);
	return records;
}

void acs_free_archive_records(ME_P_ struct acc_archive_record *records,
		unsigned count)
{
	unsigned i;
	for (i = 0; i < count; i++) {
		sm_free(records[i].v);
	}
	free(records);
}

int find_record(ME_P_ struct acc_instance_record **rptr, uint64_t iid,
		enum acs_find_mode mode)
{
	struct acs_context *ctx = &mctx->pxs.acc.acs;
	struct acc_instance_record *w;
	int found = 1;

	HASH_FIND_WIID(ctx->instances, &iid, w);
	if (NULL == w) {
		found = 0;
		if(mode == ACS_FM_CREATE) {
			w = malloc(sizeof(*w));
			memset(w, 0x00, sizeof(*w));
		}
	}
	*rptr = w;
	return found;
}

int acs_find_record(ME_P_ struct acc_instance_record **rptr, uint64_t iid,
		enum acs_find_mode mode)
{
	struct acc_instance_record *r = NULL;
	int result;
	result = find_record(ME_A_ &r, iid, mode);
	*rptr = r;
	return result;
}

const struct acc_instance_record *acs_find_record_ro(ME_P_ uint64_t iid)
{
	struct acc_instance_record *r;
	int result = find_record(ME_A_ &r, iid, ACS_FM_JUST_FIND);
	if (1 == result)
		return r;
	return NULL;
}

void acs_store_record(ME_P_ struct acc_instance_record *record)
{
	struct acs_context *ctx = &mctx->pxs.acc.acs;
	assert(record->iid > ctx->highest_finalized);
	store_record(ME_A_ record);
	assert(1 == ctx->in_batch);
	if (!record->dirty) {
		SLIST_INSERT_HEAD(&ctx->dirty_instances, record,
				dirty_entries);
		record->dirty = 1;
	}
}

void acs_free_record(ME_P_ struct acc_instance_record *record)
{
}

void acs_destroy(ME_P)
{
	struct acs_context *ctx = &mctx->pxs.acc.acs;
	struct acc_instance_record *r, *x;
	leveldb_writeoptions_destroy(ctx->ldb_write_options_sync);
	leveldb_writebatch_destroy(ctx->ldb_batch);
	leveldb_options_destroy(ctx->ldb_options);
	leveldb_close(ctx->ldb);
	HASH_ITER(hh, ctx->instances, r, x) {
		HASH_DEL(ctx->instances, r);
		sm_free(r->v);
		free(r);
	}

	fbr_cond_destroy(&mctx->fbr, &ctx->highest_finalized_changed);
	fbr_mutex_destroy(&mctx->fbr, &ctx->batch_mutex);
}
