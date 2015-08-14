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
	double flush_io;
	struct acs_iov_stat stat;
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
	retval = lseek(log->fd, 0, EIO_SEEK_CUR);
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
	retval = lseek(iter->log->fd, iter->good_offt,
			EIO_SEEK_SET);
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

	retval = read(log->fd, &header, sizeof(header));
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

	retval = read(log->fd, *pptr, header.size);
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
		retval = lseek(log->fd, magic_offset + 1, EIO_SEEK_SET);
		if (-1 == retval)
			err(EXIT_FAILURE, "seek failed");
	}

	retval = read(log->fd, &magic, sizeof(magic));
	if (-1 == retval)
		err(EXIT_FAILURE, "read failed");
	if (retval < sizeof(magic))
		goto eof;

	while (REC_MAGIC != magic) {
		retval = read(log->fd, &c, sizeof(c));
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
	retval = lseek(log->fd, 0, EIO_SEEK_CUR);
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

	retval = lseek(log->fd, 0, EIO_SEEK_CUR);
	if (-1 == retval)
		err(EXIT_FAILURE, "seek failed");
	iter->good_offt = retval;
	iter->row_count++;

	if (iter->row_count % 100000 == 0)
		fbr_log_d(&mctx->fbr, "%.1fM wal recs processed",
				iter->row_count / 1000000.);

	return size;
eof:
	retval = lseek(log->fd, 0, EIO_SEEK_CUR);
	if (-1 == retval)
		err(EXIT_FAILURE, "seek failed");
	if (retval == iter->good_offt + sizeof(EOF_MAGIC)) {
		retval = lseek(log->fd, iter->good_offt, EIO_SEEK_SET);
		if (-1 == retval)
			err(EXIT_FAILURE, "seek failed");

		retval = read(log->fd, &magic, sizeof(magic));
		if (-1 == retval)
			err(EXIT_FAILURE, "read failed");
		if (retval < sizeof(magic)) {
			fbr_log_e(&mctx->fbr, "unable to read eof magic");
		} else if (magic == EOF_MAGIC) {
			retval = lseek(log->fd, iter->good_offt, EIO_SEEK_SET);
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
	strncpy(log->filename, new_filename, PATH_MAX);
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
	if (WLM_RO == mode) {
		open_flags = O_RDONLY;
		log->fd = open(log->filename, open_flags, 0640);
	} else {
		open_flags = O_CREAT | O_WRONLY | O_EXCL;
		log->fd = fbr_eio_open(&mctx->fbr, log->filename, open_flags,
				0640, 0);
	}
	if (0 > log->fd)
		err(EXIT_FAILURE, "open failed");
	if (WLM_RO == mode) {
		retval = read(log->fd, &magic, sizeof(magic));
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

	if (WLM_RW == mode) {
		retval = fbr_eio_close(&mctx->fbr, log->fd, 0);
	} else {
		retval = close(log->fd);
	}
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

static double now()
{
	struct timeval t;
	int retval;
	retval = gettimeofday(&t, NULL);
	if (retval)
		err(EXIT_FAILURE, "gettimeofday");
	return t.tv_sec + t.tv_usec * 1e-6;
}

static eio_ssize_t log_flush_custom_cb(void *data)
{
	struct wal_log *log = data;
	ssize_t rv;
	double t1, t2;
	t1 = now();
	do {
		rv = writev(log->fd, &kv_A(log->iov, 0), kv_size(log->iov));
	} while (-1 == rv && EINTR == errno);
	t2 = now();
	log->flush_io = t2 - t1;
	return rv;
}

void wal_log_iov_flush(ME_P_ struct wal_log *log)
{
	struct acs_context *ctx = &mctx->pxs.acc.acs;
	int i;
	ssize_t retval;
	size_t total = 0;
	char *name;
	char buf[64];
	if (0 == kv_size(log->iov))
		return;
	for (i = 0; i < kv_size(log->iov); i++)
		total += kv_A(log->iov, i).iov_len;
	retval = fbr_eio_custom(&mctx->fbr, log_flush_custom_cb, log, 0);
	if (retval < total)
		err(EXIT_FAILURE, "writev failed");
	if (log->dir->kind == ALK_WAL)
		name = "wal";
	else
		name = "snap";
	snprintf(buf, sizeof(buf), "acc_storage.%s.rows", name);
	statd_send_counter(ME_A_ buf, log->iov_rows);
	log->stat.n_rows += log->iov_rows;
	snprintf(buf, sizeof(buf), "acc_storage.%s.buffers", name);
	statd_send_counter(ME_A_ buf, kv_size(log->iov));
	log->stat.n_buffers += kv_size(log->iov);
	snprintf(buf, sizeof(buf), "acc_storage.%s.bytes", name);
	statd_send_counter(ME_A_ buf, total);
	log->stat.n_bytes += total;
	snprintf(buf, sizeof(buf), "acc_storage.%s.flush_time", name);
	statd_send_timer(ME_A_ buf, log->flush_io);
	log->stat.n_useconds += (unsigned)(log->flush_io * 1e6);
	log->stat.n_flushes++;
	fbr_log_d(&mctx->fbr, "wrotev %d rows containing %zd buffers with %zd"
			" bytes in %f", log->iov_rows, kv_size(log->iov),
			total, log->flush_io);
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

	if (WLM_RW == log->mode)
		retval = fbr_eio_close(&mctx->fbr, log->fd, 0);
	else
		retval = close(log->fd);
	if (-1 == retval)
		fbr_log_e(&mctx->fbr, "close failed: %s", strerror(errno));
}

static eio_ssize_t log_sync_custom_cb(void *data)
{
	struct wal_log *log = data;
	ssize_t rv;
	double t1, t2;
	t1 = now();
	rv = fdatasync(log->fd);
	t2 = now();
	log->flush_io = t2 - t1;
	return rv;
}

static void sync_fiber(struct fbr_context *fiber_context, void *_arg)
{
	struct me_context *mctx;
	struct acs_context *ctx;
	struct wal_log *log = _arg;
	ssize_t retval;
	char buf[64];
	char *name;

	mctx = container_of(fiber_context, struct me_context, fbr);
	ctx = &mctx->pxs.acc.acs;
	fbr_set_noreclaim(&mctx->fbr, fbr_self(&mctx->fbr));
	assert(WLM_RW == log->mode);
	log->stat.n_rows += log->iov_rows;
	retval = fbr_eio_custom(&mctx->fbr, log_sync_custom_cb, log, 0);
	if (-1 == retval)
		err(EXIT_FAILURE, "fdatasync failed");

	if (log->dir->kind == ALK_WAL)
		name = "wal";
	else
		name = "snap";
	snprintf(buf, sizeof(buf), "acc_storage.%s.sync_time", name);
	statd_send_timer(ME_A_ buf, log->flush_io);
	log->stat.n_sync_useconds = (unsigned)(log->flush_io * 1e6);
	kv_push(struct acs_iov_stat, ctx->stats, log->stat);
	memset(&log->stat, 0x00, sizeof(log->stat));
	fbr_set_reclaim(&mctx->fbr, fbr_self(&mctx->fbr));
}

void wal_log_sync(ME_P_ struct wal_log *log)
{
	fbr_id_t id;
	id = fbr_create(&mctx->fbr, "acceptor/sync_log", sync_fiber, log, 0);
	fbr_transfer(&mctx->fbr, id);
	acs_vacuum(ME_A);
	fbr_reclaim(&mctx->fbr, id);
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
	if (NULL == w) {
		record->stored = 1;
		HASH_ADD_WIID(ctx->instances, iid, record);
		if (record->iid > ctx->highest_stored)
			ctx->highest_stored = record->iid;
	}
}

static void wal_replay_value(ME_P_ struct wal_value *w_value)
{
	struct acs_context *ctx = &mctx->pxs.acc.acs;
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
	if (r->iid < ctx->lowest_available || 0 == ctx->lowest_available) {
		ctx->lowest_available = r->iid;
	}
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
	ev_now_update(mctx->loop);
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

int find_record(ME_P_ struct acc_instance_record **rptr, uint64_t iid,
		enum acs_find_mode mode);

static void snapshot_fiber(struct fbr_context *fiber_context, void *_arg)
{
	struct me_context *mctx =
		container_of(fiber_context, struct me_context, fbr);
	struct acs_context *ctx;
	uint64_t lsn;
	struct acc_instance_record *r;
	struct wal_log snap;
	ssize_t retval;
	struct acs_context snap_acs_ctx;
	ev_tstamp t1, t2;
	static char wdfn[PATH_MAX + 1];
	static char tmp[PATH_MAX + 1] = {0};
	uint64_t i;
	ev_tstamp x, delta;
	const ev_tstamp time_to_throttle =
		mctx->args_info.acceptor_snap_throttle_time_arg;
	const unsigned trottle_check =
		mctx->args_info.acceptor_snap_throttle_arg;

	ctx = &mctx->pxs.acc.acs;
	lsn = ctx->confirmed_lsn - 1;
	ctx->snap_min = ctx->lowest_available;
	ctx->snap_max = ctx->highest_stored;

	memcpy(&snap_acs_ctx, ctx, sizeof(snap_acs_ctx));

	/* Write the snapshot */
	t1 = ev_now(mctx->loop);
	fbr_log_i(&mctx->fbr, "started writing snapshot for %lu", lsn);
	retval = wal_log_open_rw(ME_A_ &snap, &ctx->snap_dir, lsn, 1);
	if (-1 == retval)
		errx(EXIT_FAILURE, "unable to create a new snashot");
	wal_log_iov_collect(ME_A_ &snap);
	wal_write_state_to(ME_A_ &snap_acs_ctx, &snap);

	x = ev_now(mctx->loop);
	for (i = ctx->snap_min; i <= ctx->snap_max; i++) {
		if (!find_record(ME_A_ &r, i, ACS_FM_JUST_FIND)) {
			ctx->snap_min++;
			continue;
		}
		wal_write_value_to(ME_A_ r, &snap);
		ctx->snap_min++;
		if (wal_log_iov_need_flush(ME_A_ &snap))
			wal_log_iov_flush(ME_A_ &snap);
		if (0 == i % trottle_check) {
			delta = ev_now(mctx->loop) - x;
			if (delta < time_to_throttle)
				fbr_sleep(&mctx->fbr, time_to_throttle - delta);
			x = ev_now(mctx->loop);
		}
	}
	ctx->snap_min = 0;
	ctx->snap_max = 0;
	wal_log_iov_flush(ME_A_ &snap);
	wal_log_iov_stop(ME_A_ &snap);
	in_progress_rename(&snap);
	wal_log_close(ME_A_ &snap);
	snprintf(wdfn, PATH_MAX, "%s/%020lld%s", ctx->wal_dir.dirname,
			(long long)lsn, ctx->snap_dir.ext);
	if (access(wdfn, R_OK) && ENOENT == errno) {
		retval = fbr_eio_realpath(&mctx->fbr, snap.filename, tmp,
				PATH_MAX, 0);
		if (-1 == retval)
			fbr_log_w(&mctx->fbr, "failed to realpath %s: %s",
					snap.filename, strerror(errno));
		retval = fbr_eio_symlink(&mctx->fbr, tmp, wdfn, 0);
		if (retval)
			fbr_log_w(&mctx->fbr, "failed to symlink %s to %s: %s",
					tmp, wdfn, strerror(errno));
	}
	ctx->snap_dir.max_lsn = lsn;
	ctx->snapshot_fiber = FBR_ID_NULL;
	fbr_log_i(&mctx->fbr, "finished writing snapshot for %lu", lsn);
	t2 = ev_now(mctx->loop);
	statd_send_keyval(ME_A_ "acc_storage.snapshot.write_time", t2 - t1);
}

static void create_snapshot(ME_P)
{
	struct acs_context* ctx = &mctx->pxs.acc.acs;
	if (!fbr_id_isnull(ctx->snapshot_fiber))
		return;
	ctx->snapshot_fiber = fbr_create(&mctx->fbr, "acceptor/snap",
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
	ctx->snap_dir.dirname = mctx->args_info.acceptor_snap_dir_arg;
	scan_log_dir(ME_A_ &ctx->snap_dir);
	ctx->wal_dir.dirname = mctx->args_info.acceptor_wal_dir_arg;
	scan_log_dir(ME_A_ &ctx->wal_dir);
	recover_snapshot(ME_A);
	recover_remaining_wals(ME_A);
	fbr_log_i(&mctx->fbr, "Recovered local state, highest accepted = %zd,"
			" highest finalized = %zd, lowest available = %zd",
			ctx->highest_accepted, ctx->highest_finalized,
			ctx->lowest_available);
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

static int stat_useconds_cmp(const void *_a, const void *_b)
{
	const struct acs_iov_stat *a, *b;
	a = *((struct acs_iov_stat **)_a);
	b = *((struct acs_iov_stat **)_b);
	return a->n_useconds - b->n_useconds;
}

static int stat_sync_useconds_cmp(const void *_a, const void *_b)
{
	const struct acs_iov_stat *a, *b;
	a = *((struct acs_iov_stat **)_a);
	b = *((struct acs_iov_stat **)_b);
	return a->n_sync_useconds - b->n_sync_useconds;
}

static eio_ssize_t stats_calculate_cb(void *data)
{
	struct stats_calc_arg *arg = data;
	struct acs_context *ctx = arg->ctx;
	unsigned i;
	unsigned total_bytes = 0;
	double total_io = 0;
	double total_sync_io = 0;
	const unsigned n = kv_size(ctx->stats2);
	struct acs_iov_stat **ptr_arr;

	assert(n > 0);
	ptr_arr = calloc(n, sizeof(void *));
	if (NULL == ptr_arr)
		err(EXIT_FAILURE, "malloc");
	for (i = 0; i < n; i++)
		ptr_arr[i] = &kv_A(ctx->stats2, i);

	qsort(ptr_arr, n, sizeof(void *), stat_useconds_cmp);
	arg->write_io.min = ptr_arr[0]->n_useconds / 1e6;
	arg->write_io.p25 = ptr_arr[(int)(0.25 * n)]->n_useconds / 1e6;
	arg->write_io.p50 = ptr_arr[(int)(0.50 * n)]->n_useconds / 1e6;
	arg->write_io.p75 = ptr_arr[(int)(0.75 * n)]->n_useconds / 1e6;
	arg->write_io.p99 = ptr_arr[(int)(0.99 * n)]->n_useconds / 1e6;
	arg->write_io.max = ptr_arr[n - 1]->n_useconds / 1e6;

	qsort(ptr_arr, n, sizeof(void *), stat_sync_useconds_cmp);
	arg->sync_io.min = ptr_arr[0]->n_sync_useconds / 1e6;
	arg->sync_io.p25 = ptr_arr[(int)(0.25 * n)]->n_sync_useconds / 1e6;
	arg->sync_io.p50 = ptr_arr[(int)(0.50 * n)]->n_sync_useconds / 1e6;
	arg->sync_io.p75 = ptr_arr[(int)(0.75 * n)]->n_sync_useconds / 1e6;
	arg->sync_io.p99 = ptr_arr[(int)(0.99 * n)]->n_sync_useconds / 1e6;
	arg->sync_io.max = ptr_arr[n - 1]->n_sync_useconds / 1e6;

	for (i = 0; i < n; i++) {
		total_bytes += kv_A(ctx->stats2, i).n_bytes;
		total_io += kv_A(ctx->stats2, i).n_useconds / 1e6;
		total_sync_io += kv_A(ctx->stats2, i).n_sync_useconds / 1e6;
	}
	arg->write_io.mean = total_io / n;
	arg->sync_io.mean = total_sync_io / n;
	arg->bytes_written = total_bytes;
	arg->write_speed = total_bytes / (total_io + total_sync_io);
	arg->count = n;
	free(ptr_arr);
	return 0;
}

static inline void report_stats(ME_P_ struct stats_calc_arg *arg)
{
	fbr_log_d(&mctx->fbr, "collected %d data points", arg->count);
	fbr_log_i(&mctx->fbr, "base io min=%f p25=%f mean=%f p50=%f"
			" p75=%f p99=%f max=%f",
			arg->write_io.min, arg->write_io.p25,
			arg->write_io.mean, arg->write_io.p50,
			arg->write_io.p75, arg->write_io.p99,
			arg->write_io.max);
	fbr_log_i(&mctx->fbr, "sync io min=%f p25=%f mean=%f p50=%f"
			" p75=%f p99=%f max=%f",
			arg->sync_io.min, arg->sync_io.p25,
			arg->sync_io.mean, arg->sync_io.p50,
			arg->sync_io.p75, arg->sync_io.p99,
			arg->sync_io.max);
	fbr_log_i(&mctx->fbr, "sync write speed %f MB/s, %d bytes written",
			arg->write_speed / 1e6, arg->bytes_written);
}

static void stats_fiber(struct fbr_context *fiber_context, void *_arg)
{
	struct me_context *mctx;
	struct acs_context *ctx;
	ssize_t retval;
	kvec_t(struct acs_iov_stat) stats_tmp;
	struct stats_calc_arg arg;

	mctx = container_of(fiber_context, struct me_context, fbr);
	ctx = &mctx->pxs.acc.acs;

	arg.ctx = ctx;
	for (;;) {
		fbr_sleep(&mctx->fbr, 60);
		memcpy(&stats_tmp, &ctx->stats, sizeof(stats_tmp));
		memcpy(&ctx->stats, &ctx->stats2, sizeof(stats_tmp));
		memcpy(&ctx->stats2, &stats_tmp, sizeof(stats_tmp));
		if (0 == kv_size(ctx->stats2))
			goto skip;
		retval = fbr_eio_custom(&mctx->fbr, stats_calculate_cb, &arg,
				0);
		if (retval)
			err(EXIT_FAILURE, "stats failed");
		report_stats(ME_A_ &arg);
		fbr_log_i(&mctx->fbr, "acceptor state: highest accepted = %zd,"
			" highest finalized = %zd, lowest available = %zd",
			ctx->highest_accepted, ctx->highest_finalized,
			ctx->lowest_available);
skip:
		kv_size(ctx->stats2) = 0;
	}
}

void acs_initialize(ME_P)
{
	struct acs_context *ctx = &mctx->pxs.acc.acs;
	fbr_id_t stats_fiber_id;
	fbr_mutex_init(&mctx->fbr, &ctx->snapshot_mutex);
	fbr_mutex_init(&mctx->fbr, &ctx->batch_mutex);
	fbr_cond_init(&mctx->fbr, &ctx->highest_finalized_changed);
	ctx->wal = malloc(sizeof(*ctx->wal));
	if (NULL == ctx->wal)
		err(EXIT_FAILURE, "malloc");
	kv_init(ctx->stats);
	kv_resize(struct acs_iov_stat, ctx->stats, 1024);
	kv_init(ctx->stats2);
	kv_resize(struct acs_iov_stat, ctx->stats2, 1024);
	recover(ME_A);
	stats_fiber_id = fbr_create(&mctx->fbr, "acceptor/stats",
			stats_fiber, NULL, 0);
	fbr_transfer(&mctx->fbr, stats_fiber_id);
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
	if (ctx->wal->rows > mctx->args_info.acceptor_wal_rotate_arg)
		wal_rotate(ME_A_ ctx->wal, &ctx->wal_dir);
	else
		wal_log_sync(ME_A_ ctx->wal);
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

	//fbr_mutex_unlock(&mctx->fbr, &ctx->snapshot_mutex);
	rows_per_snap = ctx->confirmed_lsn - ctx->snap_dir.max_lsn;
	if (rows_per_snap > mctx->args_info.acceptor_snap_rows_arg)
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

void acs_vacuum(ME_P)
{
	struct acs_context *ctx = &mctx->pxs.acc.acs;
	struct acc_instance_record *r;
	int trunc;
	uint64_t max_truncated = ctx->lowest_available;
	unsigned n_truncated = 0;
	uint64_t i;
	int found;
	trunc = mctx->args_info.acceptor_truncate_arg;
	ev_tstamp t1, t2;

	if (ctx->highest_finalized > 1)
		assert(ctx->highest_finalized > ctx->lowest_available);

	if (ctx->highest_finalized < trunc)
		return;
	if (0 == max_truncated)
		max_truncated = 1;
	ev_now_update(mctx->loop);
	t1 = ev_now(mctx->loop);
	for (i = max_truncated; i < ctx->highest_finalized - trunc; i++) {
		if (i == ctx->snap_min)
			break;
		found = find_record(ME_A_ &r, i, ACS_FM_JUST_FIND);
		assert(found);
		fbr_log_d(&mctx->fbr, "vacuuming instance %ld...", r->iid);
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
			" highest finalized = %zd, lowest available = %zd",
			ctx->highest_accepted, ctx->highest_finalized,
			ctx->lowest_available);
	fbr_log_d(&mctx->fbr, "vacuuming took %f seconds", t2 - t1);
	fbr_log_d(&mctx->fbr, "%d instances disposed", n_truncated);
	statd_send_timer(ME_A_ "acc_storage.vacuum_time", t2 - t1);
	statd_send_gauge(ME_A_ "acc_storage.lowest_available",
			ctx->lowest_available);
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
	assert(record->b > 0);
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
