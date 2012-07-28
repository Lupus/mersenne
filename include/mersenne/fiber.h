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
#include <ev.h>
#include <coro.h>
#include <mersenne/context_fwd.h>

#define FBR_CALL_STACK_SIZE 16
#define FBR_STACK_SIZE 64 * 1024 // 64 KB
#define FBR_MAX_ARG_NUM 10 // 64 KB

#define FBR_ARG(i) ((*mctx->fbr.sp)->argv[i])

typedef void (*fbr_fiber_func_t)(ME_P);

struct fbr_fiber {
	fbr_fiber_func_t func;
	coro_context ctx;
	char *stack;
	void *argv[FBR_MAX_ARG_NUM];
	ev_io w_io;
};

struct fbr_context {
	struct fbr_fiber *stack[FBR_CALL_STACK_SIZE];
	struct fbr_fiber **sp;
	struct fbr_fiber root;
};

#define FBR_CONTEXT_INITIALIZER { \
	.stack = {0}, \
	.sp = NULL, \
	.root = { \
		.stack = NULL, \
	}, \
}

void fbr_init(ME_P);
struct fbr_fiber * fbr_create(ME_P_ void (*func) (ME_P));
void fbr_call(ME_P_ struct fbr_fiber *fiber, int argnum, ...);
void fbr_yield(ME_P);
void fbr_destroy(ME_P_ struct fbr_fiber *fiber);
ssize_t fbr_read(ME_P_ int fd, void *buf, size_t count);
ssize_t fbr_write(ME_P_ int fd, const void *buf, size_t count);

#endif
