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
#include <stdio.h>
#include <valgrind/valgrind.h>

#include <evfibers_private/fiber.h>

#define ENSURE_ROOT_FIBER do { assert(fctx->sp->fiber == &fctx->root); } while(0);
#define CURRENT_FIBER fctx->sp->fiber
#define CALLED_BY_ROOT ((fctx->sp - 1)->fiber == &fctx->root)

int fbr_context_size()
{
	return sizeof(struct fbr_context);
}

void fbr_init(FBR_P_ struct ev_loop *loop)
{
	memset(fctx, 0x00, sizeof(struct fbr_context));
	fctx->root.name = "root";
	coro_create(&fctx->root.ctx, NULL, NULL, NULL, 0);
	fctx->sp = fctx->stack;
	fctx->sp->fiber = &fctx->root;
	fill_trace_info(&fctx->sp->tinfo);
	fctx->loop = loop;
}

static void ev_wakeup_io(EV_P_ ev_io *w, int event)
{
	struct fbr_context *fctx;
	struct fbr_fiber *fiber;
	fiber = container_of(w, struct fbr_fiber, w_io);
	fctx = (struct fbr_context *)w->data;

	ENSURE_ROOT_FIBER;

	fbr_call(FBR_A_ fiber, 0);
}

static void ev_wakeup_timer(EV_P_ ev_timer *w, int event)
{
	struct fbr_context *fctx;
	struct fbr_fiber *fiber;
	fiber = container_of(w, struct fbr_fiber, w_timer);
	fctx = (struct fbr_context *)w->data;

	ENSURE_ROOT_FIBER;

	fbr_call(FBR_A_ fiber, 0);
}

static inline int ptrcmp(void *p1, void *p2)
{
	return !(p1 == p2);
}

static void unsubscribe_all(FBR_P_ struct fbr_fiber *fiber)
{
	struct fbr_multicall *call;
	struct fbr_fiber *tmp;
	for(call=fctx->multicalls; call != NULL; call=call->hh.next) {
		DL_SEARCH(call->fibers, tmp, fiber, ptrcmp);
		if(tmp == fiber)
			DL_DELETE(call->fibers, fiber);
	}
}

static void fiber_cleanup(FBR_P_ struct fbr_fiber *fiber)
{
	struct fbr_call_info *elt, *tmp;
	//coro_destroy(&fiber->ctx);
	unsubscribe_all(FBR_A_ fiber);
	ev_io_stop(fctx->loop, &fiber->w_io);
	ev_timer_stop(fctx->loop, &fiber->w_timer);
	obstack_free(&fiber->obstack, NULL);
	DL_FOREACH_SAFE(fiber->call_list, elt, tmp) {
		DL_DELETE(fiber->call_list, elt);
		free(elt);
	}
}

void fbr_reclaim(FBR_P_ struct fbr_fiber *fiber)
{
	if(fiber->reclaimed)
		return;
	fiber_cleanup(FBR_A_ fiber);
	fiber->reclaimed = 1;
	LL_PREPEND(fctx->reclaimed, fiber);
}

int fbr_is_reclaimed(FBR_P_ struct fbr_fiber *fiber)
{
	return fiber->reclaimed;
}

static void fiber_prepare(FBR_P_ struct fbr_fiber *fiber)
{
	obstack_init(&fiber->obstack);
	ev_init(&fiber->w_io, ev_wakeup_io);
	ev_init(&fiber->w_timer, ev_wakeup_timer);
	fiber->w_io.data = FBR_A;
	fiber->w_timer.data = FBR_A;
	fiber->reclaimed = 0;
}

static void call_wrapper(FBR_P_ void (*func) (FBR_P))
{
	struct fbr_fiber *fiber = CURRENT_FIBER;
	fiber_prepare(FBR_A_ fiber);

	fiber->func(FBR_A);
	
	fbr_reclaim(FBR_A_ fiber);
	fbr_yield(FBR_A);
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

static struct fbr_multicall * get_multicall(FBR_P_ int mid)
{
	struct fbr_multicall *call;
	HASH_FIND_INT(fctx->multicalls, &mid, call);
	if(NULL == call) {
		call = malloc(sizeof(struct fbr_multicall));
		call->fibers = NULL;
		call->mid = mid;
		HASH_ADD_INT(fctx->multicalls, mid, call);
	}
	return call;
}

void fbr_subscribe(FBR_P_ int mid)
{
	struct fbr_multicall *call = get_multicall(FBR_A_ mid);
	DL_APPEND(call->fibers, CURRENT_FIBER);
}

void fbr_unsubscribe(FBR_P_ int mid)
{
	struct fbr_multicall *call = get_multicall(FBR_A_ mid);
	struct fbr_fiber *fiber = CURRENT_FIBER;
	struct fbr_fiber *tmp;
	DL_SEARCH(call->fibers, tmp, fiber, ptrcmp);
	if(tmp == fiber)
		DL_DELETE(call->fibers, fiber);
}

void fbr_unsubscribe_all(FBR_P)
{
	unsubscribe_all(FBR_A_ CURRENT_FIBER);
}

void fbr_vcall(FBR_P_ struct fbr_fiber *callee, int argnum, va_list ap)
{
	struct fbr_fiber *caller = fctx->sp->fiber;
	int i;
	struct fbr_call_info *info;
	
	assert(argnum < FBR_MAX_ARG_NUM);

	fctx->sp++;

	fctx->sp->fiber = callee;
	fill_trace_info(&fctx->sp->tinfo);

	info = malloc(sizeof(struct fbr_call_info));
	info->caller = caller;
	info->argc = argnum;
	for(i = 0; i < argnum; i++)
		info->argv[i] = va_arg(ap, struct fbr_fiber_arg);

	DL_APPEND(callee->call_list, info);

	coro_transfer(&caller->ctx, &callee->ctx);
}

void fbr_call(FBR_P_ struct fbr_fiber *callee, int argnum, ...)
{
	va_list ap;
	va_start(ap, argnum);
	fbr_vcall(FBR_A_ callee, argnum, ap);
	va_end(ap);
}

void fbr_multicall(FBR_P_ int mid, int argnum, ...)
{
	struct fbr_multicall *call = get_multicall(FBR_A_ mid);
	struct fbr_fiber *fiber;
	va_list ap;
	DL_FOREACH(call->fibers,fiber) {
		va_start(ap, argnum);
		fbr_vcall(FBR_A_ fiber, argnum, ap);
		va_end(ap);
	}
}

int fbr_next_call_info(FBR_P_ struct fbr_call_info **info_ptr)
{
	struct fbr_fiber *fiber = CURRENT_FIBER;
	struct fbr_call_info *tmp;

	if(NULL == fiber->call_list)
		return 0;
	tmp = fiber->call_list;
	DL_DELETE(fiber->call_list, fiber->call_list);
	if(NULL == info_ptr)
		free(tmp);
	else {
		if(NULL != *info_ptr)
			free(*info_ptr);
		*info_ptr = tmp;
	}
	return 1;
}

void fbr_yield(FBR_P)
{
	struct fbr_fiber *callee = fctx->sp->fiber;
	struct fbr_fiber *caller = (--fctx->sp)->fiber;
	coro_transfer(&callee->ctx, &caller->ctx);
}

ssize_t fbr_read(FBR_P_ int fd, void *buf, size_t count)
{
	struct fbr_fiber *fiber = CURRENT_FIBER;
	ssize_t r;

	ev_io_set(&fiber->w_io, fd, EV_READ);
	ev_io_start(fctx->loop, &fiber->w_io);
		fbr_yield(FBR_A);
		if(!CALLED_BY_ROOT) {
			errno = EINTR;
			r = -1;
			goto finish;
		}
		do {
			r = read(fd, buf, count);
		} while(-1 == r && EINTR == errno);

finish:
		ev_io_stop(fctx->loop, &fiber->w_io);
		return r;
}

ssize_t fbr_read_all(FBR_P_ int fd, void *buf, size_t count, ssize_t *done)
{
	struct fbr_fiber *fiber = CURRENT_FIBER;
	ssize_t r;
	ssize_t local_done = 0;
	int uninterruptable = (NULL == done);
	if(uninterruptable) done = &local_done;

	ev_io_set(&fiber->w_io, fd, EV_READ);
	ev_io_start(fctx->loop, &fiber->w_io);
	while (count != *done) {
next:
		fbr_yield(FBR_A);
		if(!CALLED_BY_ROOT) {
			if(uninterruptable)
				continue;
			ev_io_stop(fctx->loop, &fiber->w_io);
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
	ev_io_stop(fctx->loop, &fiber->w_io);

	return *done;

error:
	ev_io_stop(fctx->loop, &fiber->w_io);
	return -1;
}

ssize_t fbr_readline(FBR_P_ int fd, void *buffer, size_t n)
{
    ssize_t num_read;                    /* # of bytes fetched by last read() */
    size_t total_read;                     /* Total bytes read so far */
    char *buf;
    char ch;

    if (n <= 0 || buffer == NULL) {
        errno = EINVAL;
        return -1;
    }

    buf = buffer;                       /* No pointer arithmetic on "void *" */

    total_read = 0;
    for (;;) {
        num_read = fbr_read(FBR_A_ fd, &ch, 1);

        if (num_read == -1) {
            if (errno == EINTR)         /* Interrupted --> restart read() */
                continue;
            else
                return -1;              /* Some other error */

        } else if (num_read == 0) {      /* EOF */
            if (total_read == 0)           /* No bytes read; return 0 */
                return 0;
            else                        /* Some bytes read; add '\0' */
                break;

        } else {                        /* 'numRead' must be 1 if we get here */
            if (total_read < n - 1) {      /* Discard > (n - 1) bytes */
                total_read++;
                *buf++ = ch;
            }

            if (ch == '\n')
                break;
        }
    }

    *buf = '\0';
    return total_read;
}

ssize_t fbr_write(FBR_P_ int fd, const void *buf, size_t count, ssize_t *done)
{
	struct fbr_fiber *fiber = CURRENT_FIBER;
	ssize_t r;
	ssize_t local_done = 0;
	int uninterruptable = (NULL == done);
	if(uninterruptable) done = &local_done;

	ev_io_set(&fiber->w_io, fd, EV_WRITE);
	ev_io_start(fctx->loop, &fiber->w_io);
	while (count != *done) {
next:
		fbr_yield(FBR_A);
		if(!CALLED_BY_ROOT) {
			if(uninterruptable)
				continue;
			ev_io_stop(fctx->loop, &fiber->w_io);
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
	ev_io_stop(fctx->loop, &fiber->w_io);

	return *done;

error:
	ev_io_stop(fctx->loop, &fiber->w_io);
	return -1;
}

ssize_t fbr_recvfrom(FBR_P_ int sockfd, void *buf, size_t len, int flags, struct
		sockaddr *src_addr, socklen_t *addrlen)
{
	struct fbr_fiber *fiber = CURRENT_FIBER;
	int nbytes;

	ev_io_set(&fiber->w_io, sockfd, EV_READ);
	ev_io_start(fctx->loop, &fiber->w_io);
	fbr_yield(FBR_A);
	ev_io_stop(fctx->loop, &fiber->w_io);
	if(CALLED_BY_ROOT) {
		nbytes = recvfrom(sockfd, buf, len, flags, src_addr, addrlen);
		return nbytes;
	}
	errno = EINTR;
	return -1;
}

ssize_t fbr_sendto(FBR_P_ int sockfd, const void *buf, size_t len, int flags, const
		struct sockaddr *dest_addr, socklen_t addrlen)
{
	struct fbr_fiber *fiber = CURRENT_FIBER;
	int nbytes;

	ev_io_set(&fiber->w_io, sockfd, EV_WRITE);
	ev_io_start(fctx->loop, &fiber->w_io);
	fbr_yield(FBR_A);
	ev_io_stop(fctx->loop, &fiber->w_io);
	if(CALLED_BY_ROOT) {
		nbytes = sendto(sockfd, buf, len, flags, dest_addr, addrlen);
		return nbytes;
	}
	errno = EINTR;
	return -1;
}

int fbr_accept(FBR_P_ int sockfd, struct sockaddr *addr, socklen_t *addrlen)
{
	struct fbr_fiber *fiber = CURRENT_FIBER;
	int r;

	ev_io_set(&fiber->w_io, sockfd, EV_READ);
	ev_io_start(fctx->loop, &fiber->w_io);
	fbr_yield(FBR_A);
	if(!CALLED_BY_ROOT) {
		ev_io_stop(fctx->loop, &fiber->w_io);
		errno = EINTR;
		return -1;
	}
	do {
		r = accept(sockfd, addr, addrlen);
	} while(-1 == r && EINTR == errno);
	ev_io_stop(fctx->loop, &fiber->w_io);

	return r;
}

ev_tstamp fbr_sleep(FBR_P_ ev_tstamp seconds)
{
	struct fbr_fiber *fiber = CURRENT_FIBER;
	ev_tstamp expected = ev_now(fctx->loop) + seconds;
	ev_timer_set(&fiber->w_timer, seconds, 0.);
	ev_timer_start(fctx->loop, &fiber->w_timer);
	fbr_yield(FBR_A);
	ev_timer_stop(fctx->loop, &fiber->w_timer);
	if(CALLED_BY_ROOT)
		return 0.;
	return expected - ev_now(fctx->loop);
}

struct fbr_fiber * fbr_create(FBR_P_ const char *name, void (*func) (FBR_P))
{
	struct fbr_fiber *fiber;
	if(fctx->reclaimed) {
		fiber = fctx->reclaimed;
		LL_DELETE(fctx->reclaimed, fctx->reclaimed);
	} else {
		fiber = malloc(sizeof(struct fbr_fiber));
		fiber->stack = malloc(FBR_STACK_SIZE);
		(void)VALGRIND_STACK_REGISTER(fiber->stack, fiber->stack + FBR_STACK_SIZE);
	}
	coro_create(&fiber->ctx, (coro_func)call_wrapper, FBR_A, fiber->stack,
			FBR_STACK_SIZE);
	fiber->call_list = NULL;
	fiber->name = name;
	fiber->func = func;
	return fiber;
}

void * fbr_alloc(FBR_P_ size_t size)
{
	return obstack_alloc(&CURRENT_FIBER->obstack, size);
}

void fbr_dump_stack(FBR_P)
{
	struct fbr_stack_item *ptr = fctx->sp;
		fprintf(stderr, "%s\n%s\n", "Fiber call stack:",
				"-------------------------------");
	while(ptr >= fctx->stack) {
		fprintf(stderr, "fiber_call: 0x%lx\t%s\n",
				(long unsigned int)ptr->fiber,
				ptr->fiber->name);
		print_trace_info(&ptr->tinfo);
		fprintf(stderr, "%s\n", "-------------------------------");
		ptr--;
	}
}
