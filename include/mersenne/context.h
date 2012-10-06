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

#ifndef _CONTEXT_H_
#define _CONTEXT_H_

#include <ev.h>

#include <evfibers/fiber.h>
#include <mersenne/leader.h>
#include <mersenne/paxos.h>
#include <mersenne/context_fwd.h>
#include <mersenne/cmdline.h>

struct me_peer;

struct me_context {
	struct gengetopt_args_info args_info;
	struct ev_loop *loop;
	int counter;
	ev_io socket_watcher;
	int fd;
	int client_fd;
	struct me_peer *peers;
	struct me_peer *me;
	int bulk_hdr_size;
	struct ldr_context ldr;
	struct pxs_context pxs;
	struct fbr_context fbr;
	struct fbr_fiber *fiber_main;
	struct fbr_fiber *fiber_leader;
	struct fbr_fiber *fiber_acceptor;
	struct fbr_fiber *fiber_proposer;
	struct fbr_fiber *fiber_client;
};

#define ME_CONTEXT_INITIALIZER { \
	.loop = NULL, \
	.counter = 0, \
	.peers = NULL, \
	.me = NULL, \
	.ldr = LDR_CONTEXT_INITIALIZER, \
	.pxs = PXS_CONTEXT_INITIALIZER, \
	.fiber_main = NULL, \
	.fiber_leader = NULL, \
	.fiber_proposer = NULL, \
}

#define ME_P struct me_context *mctx
#define ME_P_ ME_P,
#define ME_A mctx
#define ME_A_ ME_A,

#endif
