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

#include <mersenne/fiber.h>
#include <mersenne/context.h>
#include <mersenne/util.h>

#define ENSURE_ROOT_FIBER do { assert(*mctx->fbr.sp == &mctx->fbr.root); } while(0);
#define CURRENT_FIBER (*mctx->fbr.sp)
#define CALLED_BY_ROOT (*(mctx->fbr.sp - 1) == &mctx->fbr.root)

void fbr_init(ME_P)
{
	coro_create(&mctx->fbr.root.ctx, NULL, NULL, NULL, 0);
	mctx->fbr.sp = mctx->fbr.stack;
	*mctx->fbr.sp = &mctx->fbr.root;
}

static void call_wrapper(ME_P_ void (*func) (ME_P))
{
	(*mctx->fbr.sp)->func(ME_A);
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

void fbr_call(ME_P_ struct fbr_fiber *callee, int argnum, ...)
{
	struct fbr_fiber *caller = *mctx->fbr.sp++;
	va_list ap;
	int i;
	
	assert(argnum < FBR_MAX_ARG_NUM);

	*mctx->fbr.sp = callee;
	callee->argc = argnum;
	va_start(ap, argnum);
	for(i = 0; i < argnum; i++)
		callee->argv[i] = va_arg(ap, struct fbr_fiber_arg);
	va_end(ap);

	coro_transfer(&caller->ctx, &callee->ctx);
}

void fbr_yield(ME_P)
{
	struct fbr_fiber *callee = *mctx->fbr.sp--;
	struct fbr_fiber *caller = *mctx->fbr.sp;
	coro_transfer(&callee->ctx, &caller->ctx);
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

ssize_t fbr_read(ME_P_ int fd, void *buf, size_t count, ssize_t *done)
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

struct fbr_fiber * fbr_create(ME_P_ void (*func) (ME_P))
{
	struct fbr_fiber *fiber;
	fiber = malloc(sizeof(struct fbr_fiber));
	fiber->stack = malloc(FBR_STACK_SIZE);
	fiber->func = func;
	coro_create(&fiber->ctx, (coro_func)call_wrapper, ME_A, fiber->stack, FBR_STACK_SIZE);
	ev_init(&fiber->w_io, ev_wakeup_io);
	ev_init(&fiber->w_timer, ev_wakeup_timer);
	fiber->w_io.data = ME_A;
	fiber->w_timer.data = ME_A;
	return fiber;
}

void fbr_destroy(ME_P_ struct fbr_fiber *fiber)
{
}
