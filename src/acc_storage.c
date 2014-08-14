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
#include <mersenne/wal_obj.h>

static const uint32_t WAL_MAGIC = 0xcf52754d;
static const uint32_t REC_MAGIC = 0xc548d94d;
static const uint32_t EOF_MAGIC = 0x2478c945;

enum wal_log_mode {
	WLM_RO,
	WLM_RW,
};

struct wal_log {
	struct acs_log_dir *dir;
	int fd;
	char filename[PATH_MAX + 1];
	int in_progress;
	size_t rows;
	enum wal_log_mode mode;
	kvec_t(struct iovec) iov;
	int collecting_iov;
	int iov_rows;
	struct acs_context *write_context;
};

struct wal_iter {
	struct wal_log *log;
	size_t row_count;
	size_t good_offt;
	int eof;
};

struct wal_rec_header {
	uint32_t header_checksum;
	uint64_t lsn;
	ev_tstamp tstamp;
	uint16_t size;
	uint32_t checksum;
} __attribute__((packed));

#define HASH_FIND_WIID(head,findiid,out) \
	HASH_FIND(hh,head,findiid,sizeof(uint64_t),out)
#define HASH_ADD_WIID(head,iidfield,add) \
	HASH_ADD(hh,head,iidfield,sizeof(uint64_t),add)

const char in_progress_ext[] = ".in_progress";

static int cmp_uint64_t(const void *_a, const void *_b)
{
	const uint64_t *a = _a, *b = _b;
	if (*a == *b)
		return 0;
	return (*a > *b) ? 1 : -1;
}

static void scan_log_dir(ME_P_ struct acs_log_dir *dir)
{
	size_t i = 0;
	const ssize_t ext_len = strlen(dir->ext);
	DIR *dh = opendir(dir->dirname);
	struct dirent *dent;
	int ext_is_ok;
	char *ext, *suffix;

	if (NULL == dh)
		err(EXIT_FAILURE, "unable to open %s", dir->dirname);

	if (NULL == dir->lsn_arr) {
		dir->lsn_arr_size = 128;
		dir->lsn_arr = malloc(dir->lsn_arr_size * sizeof(uint64_t));
		if (NULL == dir->lsn_arr)
			err(EXIT_FAILURE, "malloc");
	}
	dir->lsn_arr_len = 0;

	errno = 0;
	while (NULL != (dent = readdir(dh))) {

		ext = strchr(dent->d_name, '.');
		if (NULL == ext)
			continue;

		suffix = strchr(ext + 1, '.');
		if (NULL == suffix)
			ext_is_ok = !strcmp(ext, dir->ext);
		else
			ext_is_ok = (!strncmp(ext, dir->ext, ext_len) &&
				     !strcmp(suffix, in_progress_ext));
		if (!ext_is_ok)
			continue;

		if (i >= dir->lsn_arr_size) {
			while (i >= dir->lsn_arr_size)
				dir->lsn_arr_size *= 2;
			dir->lsn_arr = realloc(dir->lsn_arr,
					dir->lsn_arr_size * sizeof(uint64_t));
			if (NULL == dir->lsn_arr)
				err(EXIT_FAILURE, "realloc");
		}
		dir->lsn_arr[i] = strtoll(dent->d_name, &ext, 10);
		if (strncmp(ext, dir->ext, ext_len) != 0) {
			fbr_log_w(&mctx->fbr, "strtoll can't parse `%s'",
					dent->d_name);
			continue;
		}

		if (dir->lsn_arr[i] == LLONG_MAX ||
				dir->lsn_arr[i] == LLONG_MIN) {
			fbr_log_w(&mctx->fbr, "strtoll can't parse `%s'",
					dent->d_name);
			continue;
		}

		if (dir->lsn_arr[i] > dir->max_lsn)
			dir->max_lsn = dir->lsn_arr[i];
		dir->lsn_arr_len = ++i;
	}

	qsort(dir->lsn_arr, dir->lsn_arr_len, sizeof(uint64_t), cmp_uint64_t);

	if (errno != 0)
		err(EXIT_FAILURE, "error reading directory `%s'", dir->dirname);

	if (dh != NULL)
		closedir(dh);
}

static char *wal_lsn_to_filename(struct acs_log_dir *dir, uint64_t lsn,
		int in_progress)
{
	static char filename[PATH_MAX + 1];
	const char *suffix_str = in_progress ? in_progress_ext : "";
	snprintf(filename, PATH_MAX, "%s/%020lld%s%s",
		 dir->dirname, (long long)lsn, dir->ext, suffix_str);
	return filename;
}

static void wal_iter_open(ME_P_ struct wal_iter *iter, struct wal_log *log)
{
	ssize_t retval;
	iter->log = log;
	retval = fbr_eio_seek(&mctx->fbr, log->fd, 0, EIO_SEEK_CUR, 0);
	if (-1 == retval)
		err(EXIT_FAILURE, "seek failed");
	iter->good_offt = retval;
	iter->row_count = 0;
	iter->eof = 0;
}

static void wal_iter_close(ME_P_ struct wal_iter *iter)
{
	ssize_t retval;
	iter->log->rows += iter->row_count;
	retval = fbr_eio_seek(&mctx->fbr, iter->log->fd, iter->good_offt,
			EIO_SEEK_SET, 0);
	if (-1 == retval)
		err(EXIT_FAILURE, "seek failed");
}

static inline uint32_t calc_header_checksum(struct wal_rec_header *header)
{
	uint32_t crc;
	crc = crc32(0L, Z_NULL, 0);
	crc = crc32(crc, (void *)&header->lsn, sizeof(header->lsn));
	crc = crc32(crc, (void *)&header->tstamp, sizeof(header->tstamp));
	crc = crc32(crc, (void *)&header->size, sizeof(header->size));
	crc = crc32(crc, (void *)&header->checksum, sizeof(header->checksum));
	return crc;
}

size_t wal_rec_read(ME_P_ struct wal_iter *iter, uint64_t *lsn_ptr, void **pptr,
		size_t *size_ptr)
{
	struct wal_log *log = iter->log;
	struct wal_rec_header header;
	uint32_t header_checksum, checksum;
	ssize_t retval;

	retval = fbr_eio_read(&mctx->fbr, log->fd, &header, sizeof(header), -1,
			0);
	if (0 == retval)
		return 0;
	if (0 > retval)
		err(EXIT_FAILURE, "WAL read failed");
	if (retval < (ssize_t)sizeof(header))
		errx(EXIT_FAILURE, "unable to read a header: unexpected eof");

	header_checksum = calc_header_checksum(&header);

	if (header.header_checksum != header_checksum)
		errx(EXIT_FAILURE, "header checksum mismatch");

	if (NULL != *pptr) {
		if (*size_ptr != header.size) {
			*pptr = realloc(*pptr, header.size);
			if (NULL == *pptr)
				err(EXIT_FAILURE, "realloc");
			*size_ptr = header.size;
		}
	} else {
			*pptr = malloc(header.size);
			if (NULL == *pptr)
				err(EXIT_FAILURE, "malloc");
			*size_ptr = header.size;
	}

	retval = fbr_eio_read(&mctx->fbr, log->fd, *pptr, header.size, -1, 0);
	if (0 == retval)
		return 0;
	if (0 > retval)
		err(EXIT_FAILURE, "WAL read failed");
	if (retval < (ssize_t)header.size)
		errx(EXIT_FAILURE, "unable to read a record: unexpected eof");

	checksum = crc32(0L, Z_NULL, 0);
	checksum = crc32(checksum, *pptr, header.size);
	if (header.checksum != checksum)
		errx(EXIT_FAILURE, "data checksum mismatch");

	*lsn_ptr = header.lsn;
	/* fbr_log_d(&mctx->fbr, "read wal rec with lsn %zd", header.lsn); */
	return header.size;
}

static size_t wal_iter_read(ME_P_ struct wal_iter *iter, uint64_t *lsn_ptr,
		void **pptr, size_t *size_ptr)
{
	struct wal_log *log = iter->log;
	uint32_t magic;
	uint64_t magic_offset = 0;
	ssize_t retval;
	size_t size;
	char c;

	assert(0 == iter->eof);
	if (magic_offset > 0) {
		retval = fbr_eio_seek(&mctx->fbr, log->fd, magic_offset + 1,
				EIO_SEEK_SET, 0);
		if (-1 == retval)
			err(EXIT_FAILURE, "seek failed");
	}

	retval = fbr_eio_read(&mctx->fbr, log->fd, &magic, sizeof(magic), -1,
			0);
	if (-1 == retval)
		err(EXIT_FAILURE, "read failed");
	if (retval < sizeof(magic))
		goto eof;

	while (REC_MAGIC != magic) {
		retval = fbr_eio_read(&mctx->fbr, log->fd, &c, sizeof(c), -1,
				0);
		if (-1 == retval)
			err(EXIT_FAILURE, "read failed");
		if (retval < sizeof(c)) {
			/* fbr_log_d(&mctx->fbr, "got eof while looking for rec"
					" magic"); */
			goto eof;
		}
		magic = magic >> 8 |
			((uint32_t)c & 0xff) << (sizeof(magic) * 8 - 8);
	}
	retval = fbr_eio_seek(&mctx->fbr, log->fd, 0, EIO_SEEK_CUR, 0);
	if (-1 == retval)
		err(EXIT_FAILURE, "seek failed");
	magic_offset = retval - sizeof(REC_MAGIC);
	if (iter->good_offt != magic_offset)
		fbr_log_d(&mctx->fbr, "skipped %jd bytes after 0x%08jx offset",
			(intmax_t)(magic_offset - iter->good_offt),
			(uintmax_t)iter->good_offt);
	/* fbr_log_d(&mctx->fbr, "magic found at 0x%08jx",
			(uintmax_t)magic_offset); */

	size = wal_rec_read(ME_A_ iter, lsn_ptr, pptr, size_ptr);
	if (0 == size)
		goto eof;

	retval = fbr_eio_seek(&mctx->fbr, log->fd, 0, EIO_SEEK_CUR, 0);
	if (-1 == retval)
		err(EXIT_FAILURE, "seek failed");
	iter->good_offt = retval;
	iter->row_count++;

	if (iter->row_count % 100000 == 0)
		fbr_log_d(&mctx->fbr, "%.1fM wal recs processed",
				iter->row_count / 1000000.);

	return size;
eof:
	retval = fbr_eio_seek(&mctx->fbr, log->fd, 0, EIO_SEEK_CUR, 0);
	if (-1 == retval)
		err(EXIT_FAILURE, "seek failed");
	if (retval == iter->good_offt + sizeof(EOF_MAGIC)) {
		retval = fbr_eio_seek(&mctx->fbr, log->fd, iter->good_offt,
				EIO_SEEK_SET, 0);
		if (-1 == retval)
			err(EXIT_FAILURE, "seek failed");

		retval = fbr_eio_read(&mctx->fbr, log->fd, &magic,
				sizeof(magic), -1, 0);
		if (-1 == retval)
			err(EXIT_FAILURE, "read failed");
		if (retval < sizeof(magic)) {
			fbr_log_e(&mctx->fbr, "unable to read eof magic");
		} else if (magic == EOF_MAGIC) {
			retval = fbr_eio_seek(&mctx->fbr, log->fd,
					iter->good_offt, EIO_SEEK_SET, 0);
			if (-1 == retval)
				err(EXIT_FAILURE, "seek failed");
			iter->good_offt = retval;
			iter->eof = 1;
		} else {
			fbr_log_e(&mctx->fbr, "eof magic is corrupt: %lu",
				  (unsigned long) magic);
		}
	}

	return 0;
}

void in_progress_rename(struct wal_log *log)
{
	char *filename = log->filename;
	char *new_filename;
	char *suffix = strrchr(filename, '.');

	assert(log->in_progress);
	assert(suffix);
	assert(!strcmp(suffix, in_progress_ext));

	new_filename = alloca(suffix - filename + 1);
	memcpy(new_filename, filename, suffix - filename);
	new_filename[suffix - filename] = '\0';

	if (0 != rename(filename, new_filename))
		err(EXIT_FAILURE, "rename of %s to %s failed", filename,
				new_filename);
	log->in_progress = 0;
}

void in_progress_unlink(char *filename)
{
	char *suffix = strrchr(filename, '.');
	assert(suffix);
	assert(!strcmp(suffix, in_progress_ext));
	(void)suffix;

	if (0 != unlink(filename) && ENOENT != errno)
		err(EXIT_FAILURE, "unlink");
}

int wal_log_open(ME_P_ struct wal_log *log, struct acs_log_dir *dir,
		enum wal_log_mode mode, uint64_t lsn, int in_progress)
{
	uint32_t magic;
	char *formatted_filename = wal_lsn_to_filename(dir, lsn, in_progress);
	int open_flags;
	ssize_t retval;
	memset(log, 0x00, sizeof(*log));
	strncpy(log->filename, formatted_filename, PATH_MAX);
	log->mode = mode;
	log->dir = dir;
	log->in_progress = in_progress;
	kv_init(log->iov);
	kv_resize(struct iovec, log->iov, UIO_MAXIOV);
	if (WLM_RO == mode)
		open_flags = O_RDONLY;
	else
		open_flags = O_CREAT | O_WRONLY | O_EXCL;
	log->fd = fbr_eio_open(&mctx->fbr, log->filename, open_flags, 0640, 0);
	if (0 > log->fd)
		err(EXIT_FAILURE, "open failed");
	if (WLM_RO == mode) {
		retval = fbr_eio_read(&mctx->fbr, log->fd, &magic,
				sizeof(magic), -1, 0);
		if (-1 == retval)
			err(EXIT_FAILURE, "read failed");
		if (retval < sizeof(magic)) {
			fbr_log_w(&mctx->fbr, "eof while looking for wal"
					" header for %s", log->filename);
			goto error;
		}
		if (WAL_MAGIC != magic) {
			fbr_log_w(&mctx->fbr, "wal file magic mismatch for %s",
					log->filename);
			goto error;
		}
	} else {
		magic = WAL_MAGIC;
		retval = fbr_eio_write(&mctx->fbr, log->fd, &magic,
				sizeof(magic), -1, 0);
		if (retval < sizeof(magic))
			err(EXIT_FAILURE, "write failed");
	}
	return 0;
error:

	retval = fbr_eio_close(&mctx->fbr, log->fd, 0);
	if (0 > retval)
		fbr_log_e(&mctx->fbr, "close failed: %s", strerror(errno));
	return -1;
}

int wal_log_open_ro(ME_P_ struct wal_log *log, struct acs_log_dir *dir,
		 uint64_t lsn, int in_progress)
{
	return wal_log_open(ME_A_ log, dir, WLM_RO, lsn, in_progress);
}

int wal_log_open_rw(ME_P_ struct wal_log *log, struct acs_log_dir *dir,
		 uint64_t lsn, int in_progress)
{
	char *filename;

	if (in_progress) {
		filename = wal_lsn_to_filename(dir, lsn, 0);
		if (0 == access(filename, F_OK)) {
			fbr_log_w(&mctx->fbr, "attempt to create in-progress"
					" file for already existing WAL");
			return -1;
		}
	}
	return wal_log_open(ME_A_ log, dir, WLM_RW, lsn, in_progress);
}

void wal_log_iov_collect(ME_P_ struct wal_log *log)
{
	assert(0 == log->collecting_iov);
	log->collecting_iov = 1;
	log->iov_rows = 0;
}

int wal_log_iov_need_flush(ME_P_ struct wal_log *log)
{
	assert(1 == log->collecting_iov);
	return kv_size(log->iov) > UIO_MAXIOV - 10;
}

static eio_ssize_t log_flush_custom_cb(void *data)
{
	struct wal_log *log = data;
	ssize_t rv;
	do {
		rv = writev(log->fd, &kv_A(log->iov, 0), kv_size(log->iov));
	} while (-1 == rv && EINTR == errno);
	return rv;
}

void wal_log_iov_flush(ME_P_ struct wal_log *log)
{
	struct acs_context *ctx = &mctx->pxs.acc.acs;
	int i;
	ssize_t retval;
	size_t total = 0;
	if (0 == kv_size(log->iov))
		return;
	for (i = 0; i < kv_size(log->iov); i++)
		total += kv_A(log->iov, i).iov_len;
	retval = fbr_eio_custom(&mctx->fbr, log_flush_custom_cb, log, 0);
	if (retval < total)
		err(EXIT_FAILURE, "writev failed");
	fbr_log_d(&mctx->fbr, "wrotev %d rows containing %zd buffers with %zd"
			" bytes", log->iov_rows, kv_size(log->iov), total);
	log->rows += log->iov_rows;
	for (i = 0; i < kv_size(log->iov); i++)
		free(kv_A(log->iov, i).iov_base);
	kv_size(log->iov) = 0;
	if (ALK_WAL == log->dir->kind) {
		ctx->confirmed_lsn += log->iov_rows;
	}
	log->iov_rows = 0;
}

void wal_log_iov_stop(ME_P_ struct wal_log *log)
{
	assert(1 == log->collecting_iov);
	log->collecting_iov = 0;
}

void wal_log_write(ME_P_ struct wal_log *log, void *data, size_t size)
{
	struct acs_context *ctx = &mctx->pxs.acc.acs;
	struct wal_rec_header header;
	uint32_t magic = REC_MAGIC;
	struct iovec iovec;
	assert(WLM_RW == log->mode);
	assert(1 == log->collecting_iov);
	if (ALK_WAL == log->dir->kind)
		header.lsn = ctx->confirmed_lsn + log->iov_rows;
	else
		header.lsn = 0;
	header.size = size;
	header.tstamp = ev_now(mctx->loop);
	header.checksum = crc32(0L, Z_NULL, 0);
	header.checksum = crc32(header.checksum, data, size);
	header.header_checksum = calc_header_checksum(&header);
	assert(kv_size(log->iov) + 3 <= UIO_MAXIOV);

	iovec.iov_base = malloc(sizeof(magic));
	memcpy(iovec.iov_base, &magic, sizeof(magic));
	iovec.iov_len = sizeof(magic);
	kv_push(struct iovec, log->iov, iovec);

	iovec.iov_base = malloc(sizeof(header));
	memcpy(iovec.iov_base, &header, sizeof(header));
	iovec.iov_len = sizeof(header);
	kv_push(struct iovec, log->iov, iovec);

	iovec.iov_base = data;
	iovec.iov_len = size;
	kv_push(struct iovec, log->iov, iovec);

	log->iov_rows++;
	if (0 == log->rows && 1 == log->iov_rows && ALK_WAL == log->dir->kind) {
		fbr_log_d(&mctx->fbr, "flushed first row");
		wal_log_iov_flush(ME_A_ log);
		in_progress_rename(log);
	}
}

void wal_log_close(ME_P_ struct wal_log *log)
{
	uint32_t magic = EOF_MAGIC;
	ssize_t retval;

	kv_destroy(log->iov);

	if (log->mode == WLM_RW) {
		retval = fbr_eio_write(&mctx->fbr, log->fd, &magic,
				sizeof(magic), -1, 0);
		if (retval < sizeof(magic))
			err(EXIT_FAILURE, "write failed");
		retval = fbr_eio_fsync(&mctx->fbr, log->fd, 0);
		if (-1 == retval)
			err(EXIT_FAILURE, "fsync failed");
	}

	if (log->in_progress) {
		if (1 == log->rows)
			in_progress_rename(log);
		else if(0 == log->rows)
			in_progress_unlink(log->filename);
		else
			errx(EXIT_FAILURE, "In-progress log with %zd"
					" rows", log->rows);
	}

	retval = fbr_eio_close(&mctx->fbr, log->fd, 0);
	if (-1 == retval)
		fbr_log_e(&mctx->fbr, "close failed: %s", strerror(errno));
}

void wal_log_sync(ME_P_ struct wal_log *log)
{
	ssize_t retval;
	assert(WLM_RW == log->mode);
	retval = fbr_eio_fdatasync(&mctx->fbr, log->fd, 0);
	if (-1 == retval)
		err(EXIT_FAILURE, "fdatasync failed");
}

static void wal_replay_state(ME_P_ struct wal_state *w_state)
{
	struct acs_context *ctx = &mctx->pxs.acc.acs;
	ctx->highest_accepted = w_state->highest_accepted;
	ctx->highest_finalized = w_state->highest_finalized;
}

static void store_record(ME_P_ struct acc_instance_record *record)
{
	struct acs_context *ctx = &mctx->pxs.acc.acs;
	struct acc_instance_record *w = NULL;

	HASH_FIND_WIID(ctx->instances, &record->iid, w);
	if(NULL == w) {
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

static void wal_replay_rec(ME_P_ union wal_rec_any *wal_rec)
{

	switch (wal_rec->w_type) {
	case WAL_REC_TYPE_STATE:
		wal_replay_state(ME_A_ &wal_rec->state);
		break;
	case WAL_REC_TYPE_VALUE:
		wal_replay_value(ME_A_ &wal_rec->value);
		break;
	case WAL_REC_TYPE_PROMISE:
		wal_replay_promise(ME_A_ &wal_rec->promise);
		break;
	default:
		errx(EXIT_FAILURE, "unknown WAL message type: %d",
				wal_rec->w_type);
	}
}

static void replay_rec(ME_P_ void *ptr, size_t size, int alloc)
{
	int retval;
	msgpack_unpacked result;
	union wal_rec_any wal_rec;
	char *error = NULL;

	msgpack_unpacked_init(&result);

	retval = msgpack_unpack_next(&result, ptr, size, NULL);
	if(!retval) {
		errx(EXIT_FAILURE, "unable to deserialize WAL rec for replay");
	}

	retval = wal_msg_unpack(&result.data, &wal_rec, alloc, &error);
	if(retval){
		errx(EXIT_FAILURE, "unable to unpack WAL record for replay: %s", error);
	}
	wal_replay_rec(ME_A_ &wal_rec);
	if(alloc) {
		wal_msg_free(&wal_rec);
	}
	msgpack_unpacked_destroy(&result);
}

static void recover_wal(ME_P_ struct wal_log *log)
{
	struct wal_iter iter;
	void *ptr = NULL;
	size_t buf_size = 0, size = 0;
	uint64_t lsn;
	struct acs_context *ctx = &mctx->pxs.acc.acs;
	int alloc_mem = 0;

	wal_iter_open(ME_A_ &iter, log);
	fbr_log_i(&mctx->fbr, "recovering %s", log->filename);

	while ((size = wal_iter_read(ME_A_ &iter, &lsn, &ptr, &buf_size))) {
		if (ALK_SNAP == log->dir->kind) {
			replay_rec(ME_A_ ptr, size, alloc_mem);
			continue;
		}
		if (lsn > 0 && lsn <= ctx->confirmed_lsn) {
			/* fbr_log_d(&mctx->fbr, "skipping lsn less than current,"
					" (%zd <= %zd)", lsn,
					ctx->confirmed_lsn); */
			continue;
		}
		if (lsn > ctx->confirmed_lsn + 1) {
			fbr_log_e(&mctx->fbr, "Gap found in log, lsn is %zd"
					" while last confirmed lsn is %zd",
					lsn, ctx->confirmed_lsn);
			abort();
		}
		replay_rec(ME_A_ ptr, size, alloc_mem);
		ctx->confirmed_lsn = lsn;
	}
	wal_iter_close(ME_A_ &iter);
}

static void recover_remaining_wals(ME_P)
{
	struct acs_context *ctx = &mctx->pxs.acc.acs;
	struct acs_log_dir *dir = &ctx->wal_dir;
	uint64_t lsn = 0, last_lsn;
	size_t i;
	struct wal_log log;
	ssize_t retval;
	char *last_filename;

	if (0 == dir->lsn_arr_len) {
		retval = wal_log_open_rw(ME_A_ ctx->wal, &ctx->wal_dir, 0, 1);
		if (-1 == retval)
			errx(EXIT_FAILURE, "unable to open initial log for"
					" writing");
		return;
	}
	if (1 == dir->lsn_arr_len) {
		last_lsn = dir->lsn_arr[0];
		goto last_wal;
	}
	lsn = ctx->confirmed_lsn;

	for (i = 0; i < dir->lsn_arr_len - 1; i++) {
		if (dir->lsn_arr[i] <= lsn && lsn < dir->lsn_arr[i + 1])
			break;
	}
	for (; i < dir->lsn_arr_len - 1; i++) {
		lsn = dir->lsn_arr[i];
		retval = wal_log_open_ro(ME_A_ &log, dir, lsn, 0);
		if (-1 == retval)
			errx(EXIT_FAILURE, "unable to read log for %zd", lsn);
		recover_wal(ME_A_ &log);
		wal_log_close(ME_A_ &log);
	}
	last_lsn = dir->lsn_arr[i];
last_wal:
	last_filename = wal_lsn_to_filename(dir, last_lsn, 1);
	if (0 == access(last_filename, F_OK)) {
		retval = wal_log_open_ro(ME_A_ &log, dir, last_lsn, 1);
		if (-1 == retval) {
			fbr_log_w(&mctx->fbr, "unable to read in-progress log"
				" for %zd, unlinking it", last_lsn);
			in_progress_unlink(last_filename);
			return;
		}
		recover_wal(ME_A_ &log);
		wal_log_close(ME_A_ &log);
		fbr_log_i(&mctx->fbr, "Recovered in-progress WAL");
	} else {
		retval = wal_log_open_ro(ME_A_ &log, dir, last_lsn, 0);
		if (-1 == retval)
			errx(EXIT_FAILURE, "unable to read log for %zd", lsn);
		recover_wal(ME_A_ &log);
		wal_log_close(ME_A_ &log);
	}
	/* Switch confirmed_lsn semantics to write mode, i.e. hold not yet used
	 * lsn number for the next wal record
	 */
	ctx->confirmed_lsn++;
	retval = wal_log_open_rw(ME_A_ ctx->wal, &ctx->wal_dir,
			ctx->confirmed_lsn, 1);
	if (-1 == retval)
		errx(EXIT_FAILURE, "unable to open last log for writing");
}

void wal_rotate(ME_P_ struct wal_log *log, struct acs_log_dir *dir)
{
	struct wal_log new_log;
	struct acs_context *ctx = &mctx->pxs.acc.acs;
	uint64_t lsn = ctx->confirmed_lsn;
	ssize_t retval;

	retval = wal_log_open_rw(ME_A_ &new_log, dir, lsn, 1);
	if (-1 == retval)
		errx(EXIT_FAILURE, "unable to rotate log at lsn %zd", lsn);
	wal_log_close(ME_A_ log);
	*log = new_log;
}

static void wal_write_state_to(ME_P_ struct acs_context *ctx,
		struct wal_log *log)
{
	msgpack_packer pk;
	msgpack_sbuffer sbuf;
	union wal_rec_any wal_rec;
	int retval;
	char *tmp_data;
	size_t tmp_size;

	wal_rec.w_type = WAL_REC_TYPE_STATE;
	wal_rec.state.highest_accepted = ctx->highest_accepted;
	wal_rec.state.highest_finalized = ctx->highest_finalized;

	msgpack_sbuffer_init(&sbuf);

	msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);
	retval = wal_msg_pack(&pk, &wal_rec);
	if(retval){
		errx(EXIT_FAILURE, "unable to pack wal_state");
	}
	tmp_size = sbuf.size;
	tmp_data = msgpack_sbuffer_release(&sbuf);
	wal_log_write(ME_A_ log, tmp_data, tmp_size);
	ctx->writes_per_sync++;
}

static void wal_write_state(ME_P_ struct acs_context *ctx)
{
	struct acs_context *me_ctx = &mctx->pxs.acc.acs;
	wal_write_state_to(ME_A_ ctx, me_ctx->wal);
}

static void wal_write_value_to(ME_P_ struct acc_instance_record *r,
		struct wal_log *log)
{
	struct acs_context *ctx = &mctx->pxs.acc.acs;
	msgpack_packer pk;
	msgpack_sbuffer sbuf;
	union wal_rec_any wal_rec;
	int retval;
	char *tmp_data;
	size_t tmp_size;

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
	tmp_data = msgpack_sbuffer_release(&sbuf);
	wal_log_write(ME_A_ log, tmp_data, tmp_size);
	ctx->writes_per_sync++;
}

static void wal_write_value(ME_P_ struct acc_instance_record *r)
{
	struct acs_context *ctx = &mctx->pxs.acc.acs;
	wal_write_value_to(ME_A_ r, ctx->wal);
}

static void snapshot_fiber(struct fbr_context *fiber_context, void *_arg)
{
	struct me_context *mctx;
	struct acs_context *ctx;
	uint64_t lsn;
	struct acc_instance_record *r, *x;
	struct wal_log snap;
	ssize_t retval;
	struct acs_context snap_acs_ctx;

	mctx = container_of(fiber_context, struct me_context, fbr);
	ctx = &mctx->pxs.acc.acs;
	lsn = ctx->confirmed_lsn - 1;

	/* Take the snapshot */
	//fbr_mutex_lock(&mctx->fbr, &ctx->snapshot_mutex);
	SLIST_INIT(&ctx->snap_instances);
	for (r = ctx->instances; r != NULL; r = r->hh.next) {
		r->is_cow = 1;
		SLIST_INSERT_HEAD(&ctx->snap_instances, r, entries);
	}
	memcpy(&snap_acs_ctx, ctx, sizeof(snap_acs_ctx));
	//fbr_mutex_unlock(&mctx->fbr, &ctx->snapshot_mutex);

	/* Write the snapshot */
	retval = wal_log_open_rw(ME_A_ &snap, &ctx->snap_dir, lsn, 1);
	if (-1 == retval)
		errx(EXIT_FAILURE, "unable to create a new snashot");
	wal_log_iov_collect(ME_A_ &snap);
	wal_write_state_to(ME_A_ &snap_acs_ctx, &snap);
	SLIST_FOREACH_SAFE(r, &ctx->snap_instances, entries, x) {
		wal_write_value_to(ME_A_ r, &snap);
		if (0 == r->is_cow) {
			sm_free(r->v);
			free(r);
		}
		if (wal_log_iov_need_flush(ME_A_ &snap))
			wal_log_iov_flush(ME_A_ &snap);
	}
	wal_log_iov_flush(ME_A_ &snap);
	wal_log_iov_stop(ME_A_ &snap);
	for (r = ctx->instances; r != NULL; r = r->hh.next) {
		r->is_cow = 0;
	}
	in_progress_rename(&snap);
	wal_log_close(ME_A_ &snap);
	ctx->snap_dir.max_lsn = lsn;
	ctx->snapshot_fiber = FBR_ID_NULL;
}

static void create_snapshot(ME_P)
{
	struct acs_context* ctx = &mctx->pxs.acc.acs;
	if (!fbr_id_isnull(ctx->snapshot_fiber))
		return;
	ctx->snapshot_fiber = fbr_create(&mctx->fbr, "acceptor/take_snapshot",
			snapshot_fiber, NULL, 0);
	fbr_transfer(&mctx->fbr, ctx->snapshot_fiber);
}

static void recover_snapshot(ME_P)
{
	struct acs_context *ctx = &mctx->pxs.acc.acs;
	struct wal_log log;
	ssize_t retval;
	char *test_filename;
	if (0 == ctx->snap_dir.max_lsn)
		return;

	test_filename = wal_lsn_to_filename(&ctx->snap_dir,
			ctx->snap_dir.max_lsn, 1);
	if (0 == access(test_filename, F_OK)) {
		in_progress_unlink(test_filename);
		ctx->snap_dir.lsn_arr_len--;
		ctx->snap_dir.max_lsn =
			ctx->snap_dir.lsn_arr[ctx->snap_dir.lsn_arr_len - 1];
	}
	retval = wal_log_open(ME_A_ &log, &ctx->snap_dir, WLM_RO,
			ctx->snap_dir.max_lsn, 0);
	if (-1 == retval)
		errx(EXIT_FAILURE, "unable to recover from snapshot at %zd",
				ctx->snap_dir.max_lsn);
	recover_wal(ME_A_ &log);
	wal_log_close(ME_A_ &log);
	ctx->confirmed_lsn = ctx->snap_dir.max_lsn;
}

static void recover(ME_P)
{
	struct acs_context *ctx = &mctx->pxs.acc.acs;
	ctx->snap_dir.dirname = mctx->args_info.acceptor_wal_dir_arg;
	scan_log_dir(ME_A_ &ctx->snap_dir);
	ctx->wal_dir.dirname = mctx->args_info.acceptor_wal_dir_arg;
	scan_log_dir(ME_A_ &ctx->wal_dir);
	recover_snapshot(ME_A);
	recover_remaining_wals(ME_A);
	fbr_log_i(&mctx->fbr, "Recovered local state, highest accepter = %zd,"
			" highest finalized = %zd", ctx->highest_accepted,
			ctx->highest_finalized);
}

void acs_initialize(ME_P)
{
	struct acs_context *ctx = &mctx->pxs.acc.acs;
	fbr_mutex_init(&mctx->fbr, &ctx->snapshot_mutex);
	fbr_mutex_init(&mctx->fbr, &ctx->batch_mutex);
	fbr_cond_init(&mctx->fbr, &ctx->highest_finalized_changed);
	ctx->wal = malloc(sizeof(*ctx->wal));
	if (NULL == ctx->wal)
		err(EXIT_FAILURE, "malloc");
	recover(ME_A);
}


void acs_batch_start(ME_P)
{
	struct acs_context *ctx = &mctx->pxs.acc.acs;
	//fbr_mutex_lock(&mctx->fbr, &ctx->snapshot_mutex);
	fbr_mutex_lock(&mctx->fbr, &ctx->batch_mutex);
	ctx->writes_per_sync = 0;
	ctx->in_batch = 1;
	SLIST_INIT(&ctx->dirty_instances);
	ctx->dirty = 0;
}

void acs_batch_finish(ME_P)
{
	struct acs_context *ctx = &mctx->pxs.acc.acs;
	uint64_t rows_per_snap;
	struct acc_instance_record *r, *x;
	ctx->in_batch = 0;

	wal_log_iov_collect(ME_A_ ctx->wal);
	SLIST_FOREACH_SAFE(r, &ctx->dirty_instances, dirty_entries, x) {
		if (!r->stored) {
			SLIST_REMOVE(&ctx->dirty_instances, r,
					acc_instance_record, dirty_entries);
			if (r->v)
				sm_free(r->v);
			free(r);
			continue;
		}
		if (wal_log_iov_need_flush(ME_A_ ctx->wal))
			wal_log_iov_flush(ME_A_ ctx->wal);
		wal_write_value(ME_A_ r);
		r->dirty = 0;
	}
	if (ctx->dirty)
		wal_write_state(ME_A_ ctx);
	wal_log_iov_flush(ME_A_ ctx->wal);
	wal_log_iov_stop(ME_A_ ctx->wal);

	if (0 == ctx->writes_per_sync) {
		fbr_mutex_unlock(&mctx->fbr, &ctx->batch_mutex);
		return;
	}
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
	if (ctx->wal->rows > mctx->args_info.acceptor_wal_rotate_arg)
		wal_rotate(ME_A_ ctx->wal, &ctx->wal_dir);
	else
		wal_log_sync(ME_A_ ctx->wal);

	acs_vacuum(ME_A);

	//fbr_mutex_unlock(&mctx->fbr, &ctx->snapshot_mutex);
	rows_per_snap = ctx->confirmed_lsn - ctx->snap_dir.max_lsn;
	if (rows_per_snap > mctx->args_info.acceptor_wal_snap_arg)
		create_snapshot(ME_A);
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

void acs_vacuum(ME_P)
{
	struct acs_context *ctx = &mctx->pxs.acc.acs;
	struct acc_instance_record *r, *x;
	int trunc, threshold;
	trunc = mctx->args_info.acceptor_truncate_arg;
	threshold = trunc * (1 + mctx->args_info.acceptor_truncate_margin_arg);

	/*
	printf("%ld <= %d == %d\n", ctx->highest_finalized, trunc,
			ctx->highest_finalized <= trunc);
			*/
	if (HASH_COUNT(ctx->instances) <= threshold)
			return;
	HASH_ITER(hh, ctx->instances, r, x) {
		if (r->iid >= ctx->highest_finalized - trunc)
			continue;
		fbr_log_d(&mctx->fbr, "vacuuming instance %ld...", r->iid);
		HASH_DEL(ctx->instances, r);
		if (!r->is_cow) {
			sm_free(r->v);
			free(r);
		}
	}
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
	struct acs_context *ctx = &mctx->pxs.acc.acs;
	struct acc_instance_record *r, *r_copy;
	int result;
	result = find_record(ME_A_ &r, iid, mode);
	if (1 == result && r->is_cow) {
		assert(0 == r->dirty);
		r_copy = malloc(sizeof(*r_copy));
		if (NULL == r_copy)
			err(EXIT_FAILURE, "malloc");
		memcpy(r_copy, r, sizeof(*r));
		if (r_copy->v)
			r->v = buf_sm_copy(r_copy->v->ptr, r_copy->v->size1);
		HASH_DEL(ctx->instances, r);
		r->is_cow = 0;
		HASH_ADD_WIID(ctx->instances, iid, r_copy);
		r_copy->is_cow = 0;
		r = r_copy;
	}
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
	assert(0 == record->is_cow);
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
	wal_log_close(ME_A_ ctx->wal);
	HASH_ITER(hh, ctx->instances, r, x) {
		HASH_DEL(ctx->instances, r);
		sm_free(r->v);
		free(r);
	}

	fbr_cond_destroy(&mctx->fbr, &ctx->highest_finalized_changed);
	fbr_mutex_destroy(&mctx->fbr, &ctx->snapshot_mutex);
	fbr_mutex_destroy(&mctx->fbr, &ctx->batch_mutex);
	free(ctx->wal);
}
