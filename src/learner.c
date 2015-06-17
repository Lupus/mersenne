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
#include <assert.h>
#include <err.h>
#include <evfibers/eio.h>

#include <mersenne/learner.h>
#include <mersenne/paxos.h>
#include <mersenne/context.h>
#include <mersenne/message.h>
#include <mersenne/me_protocol.h>
#include <mersenne/fiber_args.h>
#include <mersenne/strenum.h>
#include <mersenne/util.h>
#include <mersenne/sharedmem.h>
#include <mersenne/acc_storage.h>

#define LEA_INSTANCE_WINDOW mctx->args_info.learner_instance_window_arg
#define LEA_CACHE_WINDOW (LEA_INSTANCE_WINDOW * 10)
//#define WINDOW_DUMP

struct lea_ack {
	uint64_t b;
	struct buffer *v;
};

struct lea_acc_state {
	uint64_t highest_accepted;
	uint64_t highest_finalized;
	uint64_t lowest_available;
	struct me_peer *peer;
};

struct lea_instance {
	uint64_t iid;
	struct lea_ack *acks;
	struct lea_ack *chosen;
	int closed;
	ev_tstamp modified;
};

struct lea_packed_instance {
	uint64_t iid;
	char value[0];
} __attribute__((packed));

struct lea_packed_state {
	uint64_t lowest_available;
	uint64_t first_non_delivered;
} __attribute__((packed));


static inline struct lea_instance *get_instance(ME_P_ uint64_t iid)
{
	struct lea_context *context = &mctx->pxs.lea;
	return context->instances + (iid % LEA_INSTANCE_WINDOW);
}

static inline struct lea_instance_info *get_cache_instance(ME_P_ uint64_t iid)
{
	struct lea_context *context = &mctx->pxs.lea;
	return context->cache + (iid % LEA_CACHE_WINDOW);
}

static inline int count_acks(ME_P_ struct lea_ack *acks)
{
	int i;
	int count = 0;
	for (i = 0; i < pxs_acceptors_count(ME_A); i++)
		count += (NULL != acks[i].v);
	return count;
}

#ifdef WINDOW_DUMP
static void print_window(ME_P)
{
	struct lea_context *context = &mctx->pxs.lea;
	int j;
	struct lea_instance *instance;
	char buf[LEA_INSTANCE_WINDOW + 1];
	char *ptr = buf;
	int num;
	for (j = 0; j < LEA_INSTANCE_WINDOW; j++) {
		instance = context->instances +j;
		if(instance->closed) {
			*ptr++ = 'X';
		} else {
			num = count_acks(ME_A_ instance->acks);
			if (!pxs_is_acc_majority(ME_A_ num)) {
				if (num) {
					snprintf(ptr++, 2, "%d", num);
				} else {
					*ptr++ = '.';
				}
			} else {
				*ptr++ = '.';
			}
		}
	}
	*ptr++ = '\0';
	fbr_log_d(&mctx->fbr, "%lu\t%s", context->first_non_delivered, buf);
}
#endif

static char *instance_iid_to_key(uint64_t iid)
{
	static char buf[256];
	snprintf(buf, sizeof(buf), "lea_instance/%020lld", (long long int)iid);
	return buf;
}

void lea_get_or_wait_for_instance(ME_P_ uint64_t iid, char **value,
		size_t *size)
{
	char buf[256];
	char *error = NULL;
	struct lea_packed_instance *pinstance;
	size_t sz;
	pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
	while (iid >= mctx->pxs.lea.first_non_delivered) {
		pthread_mutex_lock(&mutex);
		pthread_cond_wait(&mctx->pxs.lea.fnd_pcond, &mutex);
		pthread_mutex_unlock(&mutex);
	}
	snprintf(buf, sizeof(buf), "lea_instance/%020lld", (long long int)iid);
	printf("retrieving %s\n", buf);
	pinstance = (void *)leveldb_get(mctx->pxs.lea.ldb,
			mctx->pxs.lea.ldb_read_options_cache, buf, strlen(buf),
			&sz, &error);
	if (error)
		errx(EXIT_FAILURE, "wfi: leveldb_get() failed: %s", error);
	assert(pinstance);
	fprintf(stderr, "%zd == %zd\n", pinstance->iid, iid);
	assert(pinstance->iid == iid);
	*value = pinstance->value;
	*size = sz - sizeof(*pinstance);
}

static void deliver_v(ME_P_ struct fbr_buffer *buffer, uint64_t iid,
		uint64_t b, struct buffer *v)
{
	struct lea_instance_info *info;
	fbr_log_d(&mctx->fbr, "Instance #%ld is delivered at ballot "
			"#%ld vith value ``%.*s'' size %d",
			iid, b, (unsigned)v->size1, v->ptr, v->size1);
	buffer_ensure_writable(ME_A_ buffer, sizeof(struct lea_instance_info));
	info = fbr_buffer_alloc_prepare(&mctx->fbr, buffer,
			sizeof(struct lea_instance_info));
	info->iid = iid;
	info->vb = b;
	info->buffer = sm_in_use(v);
	fbr_buffer_alloc_commit(&mctx->fbr, buffer);
}

static inline void do_deliver(ME_P_ struct lea_instance *instance)
{
	struct lea_context *context = &mctx->pxs.lea;
	static char msg_buf[ME_MAX_XDR_MESSAGE_LEN];
	struct lea_packed_instance *pinstance = (void *)msg_buf;
	struct lea_instance_info *info;
	char *key;
	pinstance->iid = instance->iid;
	memcpy(pinstance->value, instance->chosen->v->ptr,
			instance->chosen->v->size1);
	key = instance_iid_to_key(instance->iid);
	leveldb_writebatch_put(context->ldb_batch, key, strlen(key),
			(void *)pinstance,
			sizeof(pinstance) + instance->chosen->v->size1);
	if (mctx->pxs.pro.lea_fb) {
		fbr_log_d(&mctx->fbr, "Delivering instance #%ld to proposer",
			instance->iid);
		deliver_v(ME_A_ mctx->pxs.pro.lea_fb, instance->iid,
				instance->chosen->b, instance->chosen->v);
	}
	info = get_cache_instance(ME_A_ instance->iid);
	if (0 == info->iid) {
	       if (0 == context->lowest_cached) {
			context->lowest_cached = instance->iid;
			fbr_cond_signal(&mctx->fbr, &context->delivered);
			fbr_log_d(&mctx->fbr,
					"initialized lowest cached to %zd",
				       instance->iid);
	       }
	} else {
		context->lowest_cached = info->iid + 1;
		fbr_log_d(&mctx->fbr,
				"slided lowest cached to %zd",
			       info->iid + 1);
		sm_free(info->buffer);
	}
	fbr_log_d(&mctx->fbr, "assigning %zd to slot with old iid %zd",
			instance->iid, info->iid);
	info->iid = instance->iid;
	info->vb = instance->chosen->b;
	info->buffer = sm_in_use(instance->chosen->v);
	fbr_log_d(&mctx->fbr, "Instance #%ld is delivered at ballot "
			"#%ld vith value ``%.*s'' size %d", instance->iid,
			instance->chosen->b,
			(unsigned)instance->chosen->v->size1,
			instance->chosen->v->ptr,
			(unsigned)instance->chosen->v->size1);
}

void shuffle(struct lea_acc_state **array, size_t n)
{
	size_t i, j;
	struct lea_acc_state* t;
	if (n <= 1)
		return;
	for (i = 0; i < n - 1; i++)
	{
		j = i + rand() / (RAND_MAX / (n - i) + 1);
		t = array[j];
		array[j] = array[i];
		array[i] = t;
	}
}

static void send_retransmit(ME_P_ uint64_t from, uint64_t to)
{
	struct lea_context *context = &mctx->pxs.lea;
	uint64_t i;
	struct me_message msg;
	struct me_paxos_msg_data *data = &msg.me_message_u.paxos_message.data;
	struct lea_instance *instance;
	struct lea_acc_state **acc_states;
	unsigned x;
	int found_acceptor = 0;
	msg.super_type = ME_PAXOS;
	data->type = ME_PAXOS_RETRANSMIT;
	data->me_paxos_msg_data_u.retransmit.from = from;
	data->me_paxos_msg_data_u.retransmit.to = to;
	acc_states = alloca(pxs_acceptors_count(ME_A) * sizeof(void *));
	for (x = 0; x < pxs_acceptors_count(ME_A); x++)
		acc_states[x] = context->acc_states + x;
	shuffle(acc_states, pxs_acceptors_count(ME_A));
	for (x = 0; x < pxs_acceptors_count(ME_A); x++) {
		if (acc_states[x]->highest_finalized >= to) {
			msg_send_to(ME_A_ &msg, acc_states[x]->peer->index);
			mctx->delayed_stats.learner_unicast_requests++;
			found_acceptor = 1;
			break;
		}
	}
	if (!found_acceptor) {
		pxs_send_acceptors(ME_A_ &msg);
		mctx->delayed_stats.learner_broadcast_requests++;
	}
	for (i = from; i < to; i++) {
		instance = get_instance(ME_A_ i);
		instance->modified = ev_now(mctx->loop);
	}
	mctx->delayed_stats.learner_retransmits += to - from + 1;
	fbr_log_d(&mctx->fbr, "requested retransmits from %ld to %ld",
			from, to);
}

static ev_tstamp age(ME_P_ struct lea_instance *instance)
{
	ev_tstamp old_age = mctx->args_info.learner_retransmit_age_arg;
	ev_tstamp age = ev_now(mctx->loop) - instance->modified + old_age;
	if (age < 0)
		return 0;
	return 1;
}

static void retransmit_window(ME_P)
{
	struct lea_context *context = &mctx->pxs.lea;
	uint64_t i, j;
	uint64_t start = context->first_non_delivered;
	uint64_t from = 0;
	struct lea_instance *instance;
	int collecting_gap = 0;
	for (i = start, j = 0; j < LEA_INSTANCE_WINDOW; i++, j++) {
		if (i > context->highest_seen) {
			if (collecting_gap)
				send_retransmit(ME_A_ from, i - 1);
			break;
		}
		instance = get_instance(ME_A_ i);
		if (collecting_gap) {
			if (instance->closed || !age(ME_A_ instance)) {
				send_retransmit(ME_A_ from, i - 1);
				collecting_gap = 0;
			}
		} else if (!instance->closed && age(ME_A_ instance)) {
			from = i;
			collecting_gap = 1;
		}
	}
}

static void retransmit_next_window(ME_P)
{
	struct lea_context *context = &mctx->pxs.lea;
	context->next_retransmit = min(
			context->first_non_delivered + LEA_INSTANCE_WINDOW,
			context->highest_seen);
	send_retransmit(ME_A_ context->first_non_delivered,
			min(context->highest_seen, context->next_retransmit));
	fbr_log_d(&mctx->fbr, "requesting retransmits from %lu to %lu, highest seen is %lu",
			context->first_non_delivered, context->next_retransmit,
			context->highest_seen);
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

struct ldb_write_custom_cb_arg {
	char *error;
	struct lea_context *ctx;
	double write_time;
};

static eio_ssize_t ldb_write_custom_cb(void *data)
{
	struct ldb_write_custom_cb_arg *arg = data;
	struct lea_context *ctx = arg->ctx;
	double t1, t2;
	t1 = now();
	leveldb_write(ctx->ldb, ctx->ldb_write_options_async, ctx->ldb_batch,
			&arg->error);
	t2 = now();
	arg->write_time = t2 - t1;
	return 0;
}


static void try_deliver(ME_P)
{
	struct lea_context *context = &mctx->pxs.lea;
	int i, j;
	int k;
	int start = context->first_non_delivered;
	struct lea_instance *instance;
	struct lea_packed_state pstate;
	struct ldb_write_custom_cb_arg arg;
	char *error = NULL;
	const char *key = "lea_state";
	const size_t klen = strlen(key);
	int retval;

	arg.ctx = context;
	arg.error = NULL;
	arg.write_time = 0;

	for (j = 0, i = start; j < LEA_INSTANCE_WINDOW; j++, i++) {
		instance = get_instance(ME_A_ i);
		if (!instance->closed)
			break;
		do_deliver(ME_A_ instance);
		context->first_non_delivered = i + 1;

		instance->closed = 0;
		for (k = 0; k < pxs_acceptors_count(ME_A); k++) {
			sm_free(instance->acks[k].v);
			instance->acks[k].v = NULL;
			instance->acks[k].b = 0;
		}
		instance->chosen = NULL;
		instance->modified = 0;
		instance->iid = i + LEA_INSTANCE_WINDOW;
	}

	if (context->first_non_delivered == start)
		return;

	fbr_cond_signal(&mctx->fbr, &context->delivered);

	pthread_cond_broadcast(&context->fnd_pcond);
	pstate.first_non_delivered = context->first_non_delivered;
	pstate.lowest_available = 0; /* FIXME */
	leveldb_writebatch_put(context->ldb_batch, key,	klen,
			(void *)&pstate, sizeof(pstate));

	retval = fbr_eio_custom(&mctx->fbr, ldb_write_custom_cb, &arg, 0);
	if (retval)
		err(EXIT_FAILURE, "ldb_write_custom_cb failed");

	if (error)
		errx(EXIT_FAILURE,
				"try_deliver: leveldb_write() failed: %s",
				error);
	fbr_log_d(&mctx->fbr, "leveldb_write() finished in %f", arg.write_time);
	leveldb_writebatch_clear(context->ldb_batch);
}

static int ack_eq(void *_a, void *_b) {
	struct lea_ack *a = _a;
	struct lea_ack *b = _b;
	if (a == b)
		return 1;
	if (a->b != b->b)
		return 0;
	if (!a->v ^ !b->v)
		return 0;
	if (NULL == a->v && NULL == b->v)
		return 1;
	if (buf_cmp(a->v, b->v))
		return 0;
	return 1;
}

static void do_learn(ME_P_ struct me_paxos_message *pmsg, struct buffer *buf,
		struct me_peer *from)
{
	struct lea_context *context = &mctx->pxs.lea;
	struct lea_instance *instance;
	struct me_paxos_learn_data *data;
	int num;
	struct lea_ack *maj_ack;
	struct lea_ack *ack;

	data = &pmsg->data.me_paxos_msg_data_u.learn;

	if (data->i < context->first_non_delivered) {
		fbr_log_d(&mctx->fbr, "data->i (%lu) < first_non_delivered"
				" (%lu), discarding",
				data->i, context->first_non_delivered);
		return;
	}
	if (data->i >= context->first_non_delivered + LEA_INSTANCE_WINDOW) {
		if (data->i > context->highest_seen)
			context->highest_seen = data->i;
		fbr_log_d(&mctx->fbr, "value out of instance windows, discarding");
		fbr_log_d(&mctx->fbr, "data->i == %lu while "
				"context->first_non_delivered == %lu", data->i,
				context->first_non_delivered);
		return;
	}

	instance = get_instance(ME_A_ data->i);
	assert(instance->iid == data->i);
	if (instance->closed) {
		fbr_log_d(&mctx->fbr, "instance %lu is already closed,"
				" discarding", data->i);
		return;
	}
	instance->modified = ev_now(mctx->loop);
	ack = instance->acks + from->acc_index;
	if (1 == data->final) {
		fbr_log_d(&mctx->fbr, "got final value for instance %lu",
				data->i);
		ack->b = data->b;
		sm_free(ack->v);
		assert(buf->size1 > 0);
		ack->v = sm_in_use(buf);
		maj_ack = ack;
		goto close_instance;
	}
	if (NULL == ack->v) {
		ack->b = data->b;
		assert(buf->size1 > 0);
		ack->v = sm_in_use(buf);
		fbr_log_d(&mctx->fbr, "assigning initial value for this ack in"
				" instance %lu", instance->iid);
	} else {
		if (data->b <= ack->b) {
			fbr_log_d(&mctx->fbr, "data->b (%lu) is <= ack->b"
					" (%lu) in instance %lu, discarding",
					data->b, ack->b, instance->iid);
			return;
		}
		ack->b = data->b;
		sm_free(ack->v);
		assert(buf->size1 > 0);
		ack->v = sm_in_use(buf);
		fbr_log_d(&mctx->fbr, "replacing an old value for this ack in"
				" instance %lu", instance->iid);
	}
	num = count_acks(ME_A_ instance->acks);
	if (!pxs_is_acc_majority(ME_A_ num)) {
		fbr_log_d(&mctx->fbr, "instance %lu has less acks (%d) than"
				" majority", data->i, num);
		return;
	}
	maj_ack = find_majority_element(instance->acks,
			pxs_acceptors_count(ME_A),
			sizeof(struct lea_ack), ack_eq);
	if (NULL == maj_ack) {
		fbr_log_d(&mctx->fbr, "unable to find majority element among"
				" acks for instance %lu", instance->iid);
		return;
	}
close_instance:
	instance->closed = 1;
	instance->chosen = maj_ack;
	fbr_log_d(&mctx->fbr, "instance %lu is now closed at ballot %lu with"
			" value ``%.*s'' size %d", instance->iid,
			instance->chosen->b,
			(unsigned)instance->chosen->v->size1,
			instance->chosen->v->ptr, instance->chosen->v->size1);
	try_deliver(ME_A);
}

static void do_acceptor_state(ME_P_ struct me_paxos_message *pmsg,
		struct me_peer *from)
{
	struct lea_context *context = &mctx->pxs.lea;
	struct me_paxos_acceptor_state *data;
	struct lea_acc_state *acc_state;
	data = &pmsg->data.me_paxos_msg_data_u.acceptor_state;
	if (data->highest_accepted > context->highest_seen) {
		fbr_log_d(&mctx->fbr, "updating highest seen to %ld",
				data->highest_accepted);
		context->highest_seen = data->highest_accepted;
	}
	acc_state = context->acc_states + from->acc_index;
	acc_state->highest_accepted = data->highest_accepted;
	acc_state->highest_finalized = data->highest_finalized;
	acc_state->lowest_available = data->lowest_available;
}

static ev_tstamp min_window_age(ME_P)
{
	struct lea_context *context = &mctx->pxs.lea;
	uint64_t i, j;
	uint64_t start = context->first_non_delivered;
	struct lea_instance *instance;
	ev_tstamp m = mctx->args_info.learner_retransmit_age_arg;
	ev_tstamp instance_age;
	for (i = start, j = 0; j < LEA_INSTANCE_WINDOW; i++, j++) {
		if (i == context->highest_seen)
			break;
		instance = get_instance(ME_A_ i);
		instance_age = age(ME_A_ instance);
		if (instance_age < m)
			m = instance_age;
	}
	return m;
}

static void lea_hole_checker_fiber(struct fbr_context *fiber_context,
		void *_arg)
{
	struct me_context *mctx;
	struct lea_context *context;
	ev_tstamp next_aged;
	struct fbr_ev_cond_var ev;
	struct fbr_ev_base *events[] = {NULL, NULL};
	int retval;

	mctx = container_of(fiber_context, struct me_context, fbr);
	context = &mctx->pxs.lea;

	fbr_ev_cond_var_init(&mctx->fbr, &ev, &context->delivered, NULL);
	events[0] = &ev.ev_base;
	for (;;) {
		next_aged = min_window_age(ME_A);
		retval = fbr_ev_wait_to(&mctx->fbr, events, next_aged);
		assert(retval >= 0);
		if (context->highest_seen >= context->first_non_delivered +
				LEA_INSTANCE_WINDOW) {
			fbr_log_d(&mctx->fbr, "learner is lagging behind"
					" highest seen: %zu, first non"
					" delivered: %zu",
					context->highest_seen,
					context->first_non_delivered);
			retransmit_window(ME_A);
			retransmit_next_window(ME_A);
		} else if(context->highest_seen >= context->first_non_delivered) {
			fbr_log_d(&mctx->fbr, "learner is out of sync, highest"
					" seen: %zu, first non delivered: %zu",
					context->highest_seen,
					context->first_non_delivered);
			retransmit_window(ME_A);
		}
	}
}

struct lea_context_destructor_arg {
	struct me_context *mctx;
	struct lea_context *lcontext;
};

void lea_context_destructor(struct fbr_context *fiber_context, void *_arg)
{
	struct lea_context_destructor_arg *arg = _arg;
	struct me_context *mctx = arg->mctx;
	struct lea_context *lcontext = arg->lcontext;
	uint64_t j;
	struct lea_instance *instance;

	for (j = 0; j < LEA_INSTANCE_WINDOW; j++) {
		instance = lcontext->instances + j;
		free(instance->acks);
	}
	free(lcontext->instances);
}

static void init_context(ME_P)
{
	struct lea_context *context = &mctx->pxs.lea;
	unsigned i;
	char *error = NULL;
	const char *key = "lea_state";
	const size_t klen = strlen(key);
	struct lea_packed_state *pstate;
	size_t sz;
	char path[PATH_MAX];

	context->ldb_options = leveldb_options_create();
	leveldb_options_set_create_if_missing(context->ldb_options, 1);
	snprintf(path, sizeof(path), "%s/learner", mctx->args_info.db_dir_arg);
	context->ldb = leveldb_open(context->ldb_options, path, &error);
	if (error) {
		fbr_log_e(&mctx->fbr, "leveldb error: %s", error);
		exit(EXIT_FAILURE);
	}
	context->ldb_batch = leveldb_writebatch_create();
	context->ldb_write_options_sync = leveldb_writeoptions_create();
	leveldb_writeoptions_set_sync(context->ldb_write_options_sync, 1);
	context->ldb_write_options_async = leveldb_writeoptions_create();
	leveldb_writeoptions_set_sync(context->ldb_write_options_async, 0);

	context->ldb_read_options_cache = leveldb_readoptions_create();
	leveldb_readoptions_set_verify_checksums(
			context->ldb_read_options_cache, 1);
	leveldb_readoptions_set_fill_cache(context->ldb_read_options_cache, 1);
	pstate = (void *)leveldb_get(context->ldb,
			context->ldb_read_options_cache, key, klen, &sz,
			&error);
	if (error)
		errx(EXIT_FAILURE, "leveldb_get() failed: %s", error);
	if (pstate) {
		context->first_non_delivered = pstate->first_non_delivered;
	} else {
		context->first_non_delivered = 1ULL;
	}
	context->mctx = mctx;
	fbr_cond_init(&mctx->fbr, &context->delivered);

	fbr_buffer_init(&mctx->fbr, &context->buffer, 0);

	context->ldb_write_options_async = leveldb_writeoptions_create();
	leveldb_writeoptions_set_sync(context->ldb_write_options_async, 0);

	context->next_retransmit = ~0ULL; // "infinity"
	context->instances = calloc(LEA_INSTANCE_WINDOW,
			sizeof(struct lea_instance));
	context->cache = calloc(LEA_CACHE_WINDOW,
			sizeof(struct lea_instance_info));
	context->lowest_cached = 0;
	context->highest_seen = context->first_non_delivered;
	fbr_log_d(&mctx->fbr, "highest_seen = %ld, first_non_delivered = %ld",
			context->highest_seen, context->first_non_delivered);
	context->acc_states = calloc(pxs_acceptors_count(ME_A),
				sizeof(struct lea_acc_state));
	assert(context->acc_states);
	for (i = 0; i < pxs_acceptors_count(ME_A); i++)
		context->acc_states[i].peer = find_peer_by_acc_index(ME_A_ i);
}

static void init_window(ME_P)
{
	struct lea_context *context = &mctx->pxs.lea;
	uint64_t i, j;
	struct lea_instance *instance;
	uint64_t start;
	start = round_down(context->first_non_delivered, LEA_INSTANCE_WINDOW);
	for (j = 0, i = start; j < LEA_INSTANCE_WINDOW; j++, i++) {
		instance = context->instances + j;
		instance->closed = 0;
		instance->acks = calloc(pxs_acceptors_count(ME_A),
				sizeof(struct lea_ack));
		instance->modified = ev_now(mctx->loop);
		instance->iid = i;
		if (i < context->first_non_delivered)
			instance->iid += LEA_INSTANCE_WINDOW;
	}
}

void lea_fiber(struct fbr_context *fiber_context, void *_arg)
{
	struct me_context *mctx;
	struct me_paxos_message *pmsg;
	struct buffer *buf;
	struct msg_info info, *ptr;
	struct fbr_buffer *fb;
	fbr_id_t hole_checker;
	struct fbr_destructor dtor = FBR_DESTRUCTOR_INITIALIZER;
	struct lea_context_destructor_arg dtor_arg;
	struct lea_context *context;

	mctx = container_of(fiber_context, struct me_context, fbr);
	context = &mctx->pxs.lea;
	init_context(ME_A);
	dtor_arg.mctx = mctx;
	dtor_arg.lcontext = context;
	dtor.func = lea_context_destructor;
	dtor.arg = &dtor_arg;
	fbr_destructor_add(&mctx->fbr, &dtor);
	fb = &context->buffer;

	init_window(ME_A);

	hole_checker = fbr_create(&mctx->fbr, "learner/hole_checker",
			lea_hole_checker_fiber, NULL, 0);
	fbr_transfer(&mctx->fbr, hole_checker);

	for (;;) {
		ptr = fbr_buffer_read_address(&mctx->fbr, fb,
				sizeof(struct msg_info));
		memcpy(&info, ptr, sizeof(struct msg_info));
		fbr_buffer_read_advance(&mctx->fbr, fb);

		pmsg = &info.msg->me_message_u.paxos_message;
		switch(pmsg->data.type) {
		case ME_PAXOS_LEARN:
		case ME_PAXOS_RELEARN:
			buf = info.buf;
			do_learn(ME_A_ pmsg, buf, info.from);
			sm_free(info.buf);
			break;
		case ME_PAXOS_ACCEPTOR_STATE:
			do_acceptor_state(ME_A_ pmsg, info.from);
			break;
		default:
			errx(EXIT_FAILURE,
					"wrong message type for learner: %s",
					strval_me_paxos_message_type(
						pmsg->data.type));
		}
		sm_free(info.msg);
	}
}

void lea_local_fiber(struct fbr_context *fiber_context, void *_arg)
{
	struct me_context *mctx;
	struct lea_fiber_arg *arg = _arg;
	struct lea_instance_info *info;
	uint64_t i = arg->starting_iid > 0 ? arg->starting_iid : 1;
	struct fbr_cond_var *cond;

	mctx = container_of(fiber_context, struct me_context, fbr);
	cond = &mctx->pxs.lea.delivered;

	fbr_log_d(&mctx->fbr, "local learner starting at %ld", i);
	while (0 == mctx->pxs.lea.lowest_cached) {
		fbr_log_d(&mctx->fbr, "waiting for cache to populate");
		fbr_cond_wait(&mctx->fbr, cond, NULL);
	}

	for (;;i++) {
		while (i >= mctx->pxs.lea.first_non_delivered) {
			fbr_log_d(&mctx->fbr, "waiting for first non delivered to change");
			fbr_cond_wait(&mctx->fbr, cond, NULL);
		}
		info = get_cache_instance(ME_A_ i);
		assert(info->iid == i);
		fbr_log_d(&mctx->fbr, "delivering %ld from cache",
				info->iid);
		deliver_v(ME_A_ arg->buffer, info->iid, info->vb,
				info->buffer);
	}
}
