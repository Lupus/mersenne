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

#include <sys/queue.h>
#include <ev.h>

#include <evfibers/fiber.h>
#include <mersenne/leader.h>
#include <mersenne/paxos.h>
#include <mersenne/context_fwd.h>
#include <mersenne/cmdline.h>

struct me_peer;

struct fiber_tailq_i {
	fbr_id_t id;
	TAILQ_ENTRY(fiber_tailq_i) entries;
};

TAILQ_HEAD(fiber_tailq, fiber_tailq_i);

struct me_context {
	struct gengetopt_args_info args_info;
	struct ev_loop *loop;
	int counter;
	int fd;
	int client_fd;
	int client_tcp_fd;
	struct me_peer *peers;
	struct me_peer *me;
	struct ldr_context ldr;
	struct pxs_context pxs;
	struct fbr_context fbr;
	fbr_id_t fiber_main;
	fbr_id_t fiber_listener;
	fbr_id_t fiber_leader;
	fbr_id_t fiber_acceptor;
	fbr_id_t fiber_proposer;
	fbr_id_t fiber_client;
	fbr_id_t fiber_tcp_client;
	struct fiber_tailq learners;
};

#define ME_CONTEXT_INITIALIZER { \
	.loop = NULL, \
	.counter = 0, \
	.peers = NULL, \
	.me = NULL, \
	.ldr = LDR_CONTEXT_INITIALIZER, \
	.pxs = PXS_CONTEXT_INITIALIZER, \
	.fiber_main = FBR_ID_NULL, \
	.fiber_leader = FBR_ID_NULL, \
	.fiber_listener = FBR_ID_NULL, \
	.fiber_proposer = FBR_ID_NULL, \
	.fiber_client = FBR_ID_NULL, \
	.fiber_tcp_client = FBR_ID_NULL, \
}

#define ME_P struct me_context *mctx
#define ME_P_ ME_P,
#define ME_A mctx
#define ME_A_ ME_A,

#endif
