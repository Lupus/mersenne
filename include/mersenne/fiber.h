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

#ifndef _FIBER_H_
#define _FIBER_H_

#include <unistd.h>
#include <stdarg.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <obstack.h>
#include <ev.h>
#include <coro.h>
#include <mersenne/context_fwd.h>
#include <mersenne/util.h>

#define obstack_chunk_alloc malloc
#define obstack_chunk_free free

#define FBR_CALL_STACK_SIZE 16
#define FBR_STACK_SIZE 64 * 1024 // 64 KB
#define FBR_MAX_ARG_NUM 10 // 64 KB

#ifdef  NDEBUG
#define fbr_assert(expr)           ((void)(0))
#else
#define fbr_assert(expr)                                                                          \
	do {                                                                                      \
		__typeof__(expr) ex = (expr);                                                     \
		if(ex) (void)(0);                                                                 \
		else {                                                                            \
			fbr_dump_stack(ME_A);                                                     \
			__assert_fail (__STRING(expr), __FILE__, __LINE__, __ASSERT_FUNCTION);    \
		}                                                                                 \
	} while(0);
#endif

typedef void (*fbr_fiber_func_t)(ME_P);

struct fbr_fiber_arg {
	union {
		int i;
		void *v;
	};
};

struct fbr_call_info {
	int argc;
	struct fbr_fiber_arg argv[FBR_MAX_ARG_NUM];
	struct fbr_fiber *caller;
	struct fbr_call_info *next, *prev;
};

struct fbr_fiber {
	const char *name;
	fbr_fiber_func_t func;
	coro_context ctx;
	char *stack;
	struct fbr_call_info *call_list;
	struct obstack obstack;
	ev_io w_io;
	ev_timer w_timer;
	int reclaimed;

	struct fbr_fiber *next;
};

struct fbr_stack_item {
	struct fbr_fiber *fiber;
	struct trace_info tinfo;
};

struct fbr_context {
	struct fbr_stack_item stack[FBR_CALL_STACK_SIZE];
	struct fbr_stack_item *sp;
	struct fbr_fiber root;
	struct fbr_fiber *reclaimed;
};

#define FBR_CONTEXT_INITIALIZER { \
	.stack = {{0}}, \
	.sp = NULL, \
	.root = { \
		.stack = NULL, \
		.name = "root", \
	}, \
	.reclaimed = NULL, \
}

void fbr_init(ME_P);
struct fbr_fiber * fbr_create(ME_P_ const char *name, void (*func) (ME_P));
void fbr_reclaim(ME_P_ struct fbr_fiber *fiber);
int fbr_is_reclaimed(ME_P_ struct fbr_fiber *fiber);
struct fbr_fiber_arg fbr_arg_i(int i);
struct fbr_fiber_arg fbr_arg_v(void *v);
void fbr_call(ME_P_ struct fbr_fiber *fiber, int argnum, ...);
void fbr_yield(ME_P);
void * fbr_alloc(ME_P_ size_t size);
void fbr_destroy(ME_P_ struct fbr_fiber *fiber);
int fbr_next_call_info(ME_P_ struct fbr_call_info **info_ptr);
void fbr_free_call_info(ME_P_ struct fbr_call_info *info);
ssize_t fbr_read(ME_P_ int fd, void *buf, size_t count);
ssize_t fbr_read_all(ME_P_ int fd, void *buf, size_t count, ssize_t *done);
ssize_t fbr_write(ME_P_ int fd, const void *buf, size_t count, ssize_t *done);
ssize_t fbr_recvfrom(ME_P_ int sockfd, void *buf, size_t len, int flags, struct
		sockaddr *src_addr, socklen_t *addrlen);
ssize_t fbr_sendto(ME_P_ int sockfd, const void *buf, size_t len, int flags, const
		struct sockaddr *dest_addr, socklen_t addrlen);
int fbr_accept(ME_P_ int sockfd, struct sockaddr *addr, socklen_t *addrlen);
ev_tstamp fbr_sleep(ME_P_ ev_tstamp seconds);
void fbr_dump_stack(ME_P);

#endif
