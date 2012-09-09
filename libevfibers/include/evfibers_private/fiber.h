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

#ifndef _FIBER_PRIVATE_H_
#define _FIBER_PRIVATE_H_

#define _FBR_NO_CTX_STUB
#include <evfibers/fiber.h>
#include <evfibers_private/trace.h>
#include <coro.h>
#include <uthash.h>
#include <obstack.h>

#define obstack_chunk_alloc malloc
#define obstack_chunk_free free

#define FBR_CALL_STACK_SIZE 16
#define FBR_STACK_SIZE 64 * 1024 // 64 KB
#define FBR_MAX_ARG_NUM 10

#define container_of(ptr, type, member) ({            \
 const typeof( ((type *)0)->member ) *__mptr = (ptr);    \
 (type *)( (char *)__mptr - offsetof(type,member) );})

struct fbr_multicall {
	struct fbr_fiber *fibers;
	int mid;
	UT_hash_handle hh;
};

struct fbr_fiber {
	const char *name;
	fbr_fiber_func_t func;
	coro_context ctx;
	char *stack;
	struct fbr_call_info *call_list;
	struct obstack obstack;
	ev_io w_io;
	int w_io_expected;
	struct trace_info w_io_tinfo;
	ev_timer w_timer;
	struct trace_info w_timer_tinfo;
	int w_timer_expected;
	int reclaimed;
	struct trace_info reclaim_tinfo;
	struct fbr_fiber *children;
	struct fbr_fiber *parent;

	struct fbr_fiber *next, *prev;
};

struct fbr_stack_item {
	struct fbr_fiber *fiber;
	struct trace_info tinfo;
};

struct fbr_context_private {
	struct fbr_stack_item stack[FBR_CALL_STACK_SIZE];
	struct fbr_stack_item *sp;
	struct fbr_fiber root;
	struct fbr_fiber *reclaimed;
	struct fbr_multicall *multicalls;
	struct ev_loop *loop;
};

#endif

