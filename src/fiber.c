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
#include <errno.h>
#include <utlist.h>

#include <mersenne/fiber.h>
#include <mersenne/context.h>
#include <mersenne/util.h>

#define ENSURE_ROOT_FIBER do { assert(mctx->fbr.sp->fiber == &mctx->fbr.root); } while(0);
#define CURRENT_FIBER mctx->fbr.sp->fiber
#define CALLED_BY_ROOT ((mctx->fbr.sp - 1)->fiber == &mctx->fbr.root)

void fbr_init(ME_P)
{
	coro_create(&mctx->fbr.root.ctx, NULL, NULL, NULL, 0);
	mctx->fbr.sp = mctx->fbr.stack;
	mctx->fbr.sp->fiber = &mctx->fbr.root;
	fill_trace_info(&mctx->fbr.sp->tinfo);
}

static void ev_wakeup_io(EV_P_ ev_io *w, int event)
{
	struct me_context *mctx;
	struct fbr_fiber *fiber;
	fiber = container_of(w, struct fbr_fiber, w_io);
	mctx = (struct me_context *)w->data;

	ENSURE_ROOT_FIBER;

	fbr_call(ME_A_ fiber, 0);
}

static void ev_wakeup_timer(EV_P_ ev_timer *w, int event)
{
	struct me_context *mctx;
	struct fbr_fiber *fiber;
	fiber = container_of(w, struct fbr_fiber, w_timer);
	mctx = (struct me_context *)w->data;

	ENSURE_ROOT_FIBER;

	fbr_call(ME_A_ fiber, 0);
}

static inline int ptrcmp(void *p1, void *p2)
{
	return !(p1 == p2);
}

static void unsubscribe_all(ME_P_ struct fbr_fiber *fiber)
{
	struct fbr_multicall *call;
	struct fbr_fiber *tmp;
	for(call=mctx->fbr.multicalls; call != NULL; call=call->hh.next) {
		DL_SEARCH(call->fibers, tmp, fiber, ptrcmp);
		if(tmp == fiber)
			DL_DELETE(call->fibers, fiber);
	}
}

static void fiber_cleanup(ME_P_ struct fbr_fiber *fiber)
{
	struct fbr_call_info *elt, *tmp;
	//coro_destroy(&fiber->ctx);
	unsubscribe_all(ME_A_ fiber);
	ev_io_stop(mctx->loop, &fiber->w_io);
	ev_timer_stop(mctx->loop, &fiber->w_timer);
	obstack_free(&fiber->obstack, NULL);
	DL_FOREACH_SAFE(fiber->call_list, elt, tmp) {
		DL_DELETE(fiber->call_list, elt);
		fbr_free_call_info(ME_A_ elt);
	}
}

void fbr_reclaim(ME_P_ struct fbr_fiber *fiber)
{
	if(fiber->reclaimed)
		return;
	fiber_cleanup(ME_A_ fiber);
	fiber->reclaimed = 1;
	LL_PREPEND(mctx->fbr.reclaimed, fiber);
}

int fbr_is_reclaimed(ME_P_ struct fbr_fiber *fiber)
{
	return fiber->reclaimed;
}

static void fiber_prepare(ME_P_ struct fbr_fiber *fiber)
{
	obstack_init(&fiber->obstack);
	ev_init(&fiber->w_io, ev_wakeup_io);
	ev_init(&fiber->w_timer, ev_wakeup_timer);
	fiber->w_io.data = ME_A;
	fiber->w_timer.data = ME_A;
	fiber->reclaimed = 0;
}

static void call_wrapper(ME_P_ void (*func) (ME_P))
{
	struct fbr_fiber *fiber = CURRENT_FIBER;
	fiber_prepare(ME_A_ fiber);

	fiber->func(ME_A);
	
	fbr_reclaim(ME_A_ fiber);
	fbr_yield(ME_A);
}

struct fbr_fiber_arg fbr_arg_i(int i)
{
	struct fbr_fiber_arg arg;
	arg.i = i;
	return arg;
}

struct fbr_fiber_arg fbr_arg_v(void *v)
{
	struct fbr_fiber_arg arg;
	arg.v = v;
	return arg;
}

static struct fbr_multicall * get_multicall(ME_P_ int mid)
{
	struct fbr_multicall *call;
	HASH_FIND_INT(mctx->fbr.multicalls, &mid, call);
	if(NULL == call) {
		call = malloc(sizeof(struct fbr_multicall));
		call->fibers = NULL;
		call->mid = mid;
		HASH_ADD_INT(mctx->fbr.multicalls, mid, call);
	}
	return call;
}

void fbr_subscribe(ME_P_ int mid)
{
	struct fbr_multicall *call = get_multicall(ME_A_ mid);
	DL_APPEND(call->fibers, CURRENT_FIBER);
}

void fbr_unsubscribe(ME_P_ int mid)
{
	struct fbr_multicall *call = get_multicall(ME_A_ mid);
	struct fbr_fiber *fiber = CURRENT_FIBER;
	struct fbr_fiber *tmp;
	DL_SEARCH(call->fibers, tmp, fiber, ptrcmp);
	if(tmp == fiber)
		DL_DELETE(call->fibers, fiber);
}

void fbr_unsubscribe_all(ME_P)
{
	unsubscribe_all(ME_A_ CURRENT_FIBER);
}

void fbr_vcall(ME_P_ struct fbr_fiber *callee, int argnum, va_list ap)
{
	struct fbr_fiber *caller = mctx->fbr.sp->fiber;
	int i;
	struct fbr_call_info *info;
	
	assert(argnum < FBR_MAX_ARG_NUM);

	mctx->fbr.sp++;

	mctx->fbr.sp->fiber = callee;
	fill_trace_info(&mctx->fbr.sp->tinfo);

	info = malloc(sizeof(struct fbr_call_info));
	info->caller = caller;
	info->argc = argnum;
	for(i = 0; i < argnum; i++)
		info->argv[i] = va_arg(ap, struct fbr_fiber_arg);

	DL_APPEND(callee->call_list, info);

	coro_transfer(&caller->ctx, &callee->ctx);
}

void fbr_call(ME_P_ struct fbr_fiber *callee, int argnum, ...)
{
	va_list ap;
	va_start(ap, argnum);
	fbr_vcall(ME_A_ callee, argnum, ap);
	va_end(ap);
}

void fbr_multicall(ME_P_ int mid, int argnum, ...)
{
	struct fbr_multicall *call = get_multicall(ME_A_ mid);
	struct fbr_fiber *fiber;
	va_list ap;
	va_start(ap, argnum);
	DL_FOREACH(call->fibers,fiber)
		fbr_vcall(ME_A_ fiber, argnum, ap);
	va_end(ap);
}

int fbr_next_call_info(ME_P_ struct fbr_call_info **info_ptr)
{
	struct fbr_fiber *fiber = CURRENT_FIBER;
	struct fbr_call_info *tmp;

	if(NULL == fiber->call_list)
		return 0;
	tmp = fiber->call_list;
	DL_DELETE(fiber->call_list, fiber->call_list);
	if(NULL == info_ptr)
		fbr_free_call_info(ME_A_ tmp);
	else
		*info_ptr = tmp;
	return 1;
}

void fbr_free_call_info(ME_P_ struct fbr_call_info *info)
{
	free(info);
}

void fbr_yield(ME_P)
{
	struct fbr_fiber *callee = mctx->fbr.sp->fiber;
	struct fbr_fiber *caller = (--mctx->fbr.sp)->fiber;
	coro_transfer(&callee->ctx, &caller->ctx);
}

ssize_t fbr_read(ME_P_ int fd, void *buf, size_t count)
{
	struct fbr_fiber *fiber = CURRENT_FIBER;
	ssize_t r;

	ev_io_set(&fiber->w_io, fd, EV_READ);
	ev_io_start(mctx->loop, &fiber->w_io);
		fbr_yield(ME_A);
		if(!CALLED_BY_ROOT) {
			errno = EINTR;
			r = -1;
			goto finish;
		}
		do {
			r = read(fd, buf, count);
		} while(-1 == r && EINTR == errno);

finish:
		ev_io_stop(mctx->loop, &fiber->w_io);
		return r;
}

ssize_t fbr_read_all(ME_P_ int fd, void *buf, size_t count, ssize_t *done)
{
	struct fbr_fiber *fiber = CURRENT_FIBER;
	ssize_t r;
	ssize_t local_done = 0;
	int uninterruptable = (NULL == done);
	if(uninterruptable) done = &local_done;

	ev_io_set(&fiber->w_io, fd, EV_READ);
	ev_io_start(mctx->loop, &fiber->w_io);
	while (count != *done) {
next:
		fbr_yield(ME_A);
		if(!CALLED_BY_ROOT) {
			if(uninterruptable)
				continue;
			ev_io_stop(mctx->loop, &fiber->w_io);
			errno = EINTR;
			return -1;
		}
		for(;;) {
			r = read(fd, buf + *done, count - *done);
			if (-1 == r) {
				switch(errno) {
					case EINTR:
						continue;
					case EAGAIN:
						goto next;
					default:
						goto error;
				}
			}
			break;
		}
		if(0 == r)
			break;
		*done += r;
	}
	ev_io_stop(mctx->loop, &fiber->w_io);

	return *done;

error:
	ev_io_stop(mctx->loop, &fiber->w_io);
	return -1;
}

ssize_t fbr_write(ME_P_ int fd, const void *buf, size_t count, ssize_t *done)
{
	struct fbr_fiber *fiber = CURRENT_FIBER;
	ssize_t r;
	ssize_t local_done = 0;
	int uninterruptable = (NULL == done);
	if(uninterruptable) done = &local_done;

	ev_io_set(&fiber->w_io, fd, EV_WRITE);
	ev_io_start(mctx->loop, &fiber->w_io);
	while (count != *done) {
next:
		fbr_yield(ME_A);
		if(!CALLED_BY_ROOT) {
			if(uninterruptable)
				continue;
			ev_io_stop(mctx->loop, &fiber->w_io);
			errno = EINTR;
			return -1;
		}
		for(;;) {
			r = write(fd, buf + *done, count - *done);
			if (-1 == r) {
				switch(errno) {
					case EINTR:
						continue;
					case EAGAIN:
						goto next;
					default:
						goto error;
				}
			}
			break;
		}
		*done += r;
	}
	ev_io_stop(mctx->loop, &fiber->w_io);

	return *done;

error:
	ev_io_stop(mctx->loop, &fiber->w_io);
	return -1;
}

ssize_t fbr_recvfrom(ME_P_ int sockfd, void *buf, size_t len, int flags, struct
		sockaddr *src_addr, socklen_t *addrlen)
{
	struct fbr_fiber *fiber = CURRENT_FIBER;
	int nbytes;

	ev_io_set(&fiber->w_io, sockfd, EV_READ);
	ev_io_start(mctx->loop, &fiber->w_io);
	fbr_yield(ME_A);
	ev_io_stop(mctx->loop, &fiber->w_io);
	if(CALLED_BY_ROOT) {
		nbytes = recvfrom(sockfd, buf, len, flags, src_addr, addrlen);
		return nbytes;
	}
	errno = EINTR;
	return -1;
}

ssize_t fbr_sendto(ME_P_ int sockfd, const void *buf, size_t len, int flags, const
		struct sockaddr *dest_addr, socklen_t addrlen)
{
	struct fbr_fiber *fiber = CURRENT_FIBER;
	int nbytes;

	ev_io_set(&fiber->w_io, sockfd, EV_WRITE);
	ev_io_start(mctx->loop, &fiber->w_io);
	fbr_yield(ME_A);
	ev_io_stop(mctx->loop, &fiber->w_io);
	if(CALLED_BY_ROOT) {
		nbytes = sendto(sockfd, buf, len, flags, dest_addr, addrlen);
		return nbytes;
	}
	errno = EINTR;
	return -1;
}

int fbr_accept(ME_P_ int sockfd, struct sockaddr *addr, socklen_t *addrlen)
{
	struct fbr_fiber *fiber = CURRENT_FIBER;
	int r;

	ev_io_set(&fiber->w_io, sockfd, EV_READ);
	ev_io_start(mctx->loop, &fiber->w_io);
	fbr_yield(ME_A);
	if(!CALLED_BY_ROOT) {
		ev_io_stop(mctx->loop, &fiber->w_io);
		errno = EINTR;
		return -1;
	}
	do {
		r = accept(sockfd, addr, addrlen);
	} while(-1 == r && EINTR == errno);
	ev_io_stop(mctx->loop, &fiber->w_io);

	return r;
}

ev_tstamp fbr_sleep(ME_P_ ev_tstamp seconds)
{
	struct fbr_fiber *fiber = CURRENT_FIBER;
	ev_tstamp expected = ev_now(mctx->loop) + seconds;
	ev_timer_set(&fiber->w_timer, seconds, 0.);
	ev_timer_start(mctx->loop, &fiber->w_timer);
	fbr_yield(ME_A);
	ev_timer_stop(mctx->loop, &fiber->w_timer);
	if(CALLED_BY_ROOT)
		return 0.;
	return expected - ev_now(mctx->loop);
}

struct fbr_fiber * fbr_create(ME_P_ const char *name, void (*func) (ME_P))
{
	struct fbr_fiber *fiber;
	if(mctx->fbr.reclaimed) {
		fiber = mctx->fbr.reclaimed;
		LL_DELETE(mctx->fbr.reclaimed, mctx->fbr.reclaimed);
	} else {
		fiber = malloc(sizeof(struct fbr_fiber));
		fiber->stack = malloc(FBR_STACK_SIZE);
	}
	coro_create(&fiber->ctx, (coro_func)call_wrapper, ME_A, fiber->stack,
			FBR_STACK_SIZE);
	fiber->call_list = NULL;
	fiber->name = name;
	fiber->func = func;
	return fiber;
}

void * fbr_alloc(ME_P_ size_t size)
{
	return obstack_alloc(&CURRENT_FIBER->obstack, size);
}

void fbr_dump_stack(ME_P)
{
	struct fbr_stack_item *ptr = mctx->fbr.sp;
		fprintf(stderr, "%s\n%s\n", "Fiber call stack:",
				"-------------------------------");
	while(ptr >= mctx->fbr.stack) {
		fprintf(stderr, "fiber_call: 0x%lx\t%s\n",
				(long unsigned int)ptr->fiber,
				ptr->fiber->name);
		print_trace_info(&ptr->tinfo);
		fprintf(stderr, "%s\n", "-------------------------------");
		ptr--;
	}
}
