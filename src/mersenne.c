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

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <time.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/epoll.h>
#include <err.h>
#include <errno.h>
#include <ev.h>
#include <execinfo.h>
#include <valgrind/valgrind.h>
#include <evfibers/fiber.h>
#include <evfibers/eio.h>

#include <mersenne/me_protocol.h>
#include <mersenne/context.h>
#include <mersenne/peers.h>
#include <mersenne/paxos.h>
#include <mersenne/util.h>
#include <mersenne/fiber_args.h>
#include <mersenne/client.h>
#include <mersenne/sharedmem.h>
#include <mersenne/statd.h>
#include <mersenne/cmdline.h>

#define LISTEN_BACKLOG 50

static int wait_for_debugger;

static void set_up_udp_socket(ME_P)
{
	int yes = 1;

	if ((mctx->fd = socket(AF_INET, SOCK_DGRAM, 0)) < 0)
		err(EXIT_FAILURE, "failed to create an udp socket");

	make_socket_non_blocking(mctx->fd);

	/* allow multiple sockets to use the same PORT number */
	if (setsockopt(mctx->fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0)
		err(EXIT_FAILURE, "reusing of address failed");

	/* bind to receive address */
	if (bind(mctx->fd, (struct sockaddr *) &mctx->me->addr, sizeof(mctx->me->addr)) < 0)
		err(EXIT_FAILURE, "bind failed");
}

static void set_up_client_tcp_socket(ME_P)
{
	static const int yes = 1;
	struct sockaddr_in addr;
	int retval;
	int port;

	mctx->client_tcp_fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	if (0 > mctx->client_tcp_fd)
		err(EXIT_FAILURE, "failed to create a client socket");

	make_socket_non_blocking(mctx->client_tcp_fd);

	retval = setsockopt(mctx->client_tcp_fd, SOL_SOCKET, SO_REUSEADDR,
			&yes, sizeof(yes));
	if (-1 == retval)
		err(EXIT_FAILURE, "failed to set SO_REUSEADDR");

	memset(&addr, 0, sizeof(addr));
	memcpy(&addr, &mctx->me->addr, sizeof(mctx->me->addr));
	port = mctx->args_info.client_port_arg;
	if (port <= 0 || port >= 0xffff)
		errx(EXIT_FAILURE, "invalid client port number");
	addr.sin_port = htons(port);

	retval = bind(mctx->client_tcp_fd, (struct sockaddr *)&addr,
			sizeof(addr));
	if (retval)
		err(EXIT_FAILURE, "client tcp socket bind failed");

	retval = listen(mctx->client_tcp_fd, LISTEN_BACKLOG);
	if (retval)
               err(EXIT_FAILURE, "client tcp socket listen failed");
}

static void setup_logging(ME_P)
{
	enum fbr_log_level log_level = FBR_LOG_INFO;
	switch(mctx->args_info.log_level_arg) {
		case log_level_arg_error:
			log_level = FBR_LOG_ERROR;
			break;
		case log_level_arg_warning:
			log_level = FBR_LOG_WARNING;
			break;
		case log_level_arg_notice:
			log_level = FBR_LOG_NOTICE;
			break;
		case log_level_arg_info:
			log_level = FBR_LOG_INFO;
			break;
		case log_level_arg_debug:
			log_level = FBR_LOG_DEBUG;
			break;
		case log_level__NULL:
			errx(EXIT_FAILURE, "invalid log level");
	}
	fbr_set_log_level(&mctx->fbr, log_level);
}

static void sigsegv_handler(int signum)
{
	const int bt_size = 32;
	void *array[bt_size];
	size_t size;

	size = backtrace(array, bt_size);
	fprintf(stderr, "--------------------------------------------------------------\n");
	fprintf(stderr, "Program received signal SIGSEGV, Segmentation fault.\n");
	backtrace_symbols_fd(array, size, STDERR_FILENO);
	fprintf(stderr, "--------------------------------------------------------------\n");
	if(wait_for_debugger) {
		fprintf(stderr, "Pid is %d, waiting for a debugger...\n", getpid());
		for(;;) sleep(100);
	} else
		exit(EXIT_FAILURE);
}

static void ev_signal_dtor(struct fbr_context *fiber_context, void *arg)
{
	struct me_context *mctx;
	ev_signal *s = arg;

	mctx = container_of(fiber_context, struct me_context, fbr);
	ev_signal_stop(mctx->loop, s);
}

static void stats_report(ME_P)
{

	statd_send_counter(ME_A_ "messages.sent",
			mctx->delayed_stats.msg_sent);
	mctx->delayed_stats.msg_sent = 0;
	statd_send_counter(ME_A_ "messages.received",
			mctx->delayed_stats.msg_recv);
	mctx->delayed_stats.msg_recv = 0;

	statd_send_counter(ME_A_ "proposer.p1.executions",
			mctx->delayed_stats.proposer_p1_execs);
	mctx->delayed_stats.proposer_p1_execs = 0;
	statd_send_counter(ME_A_ "proposer.p1.timeouts",
			mctx->delayed_stats.proposer_p1_timeouts);
	mctx->delayed_stats.proposer_p1_timeouts = 0;
	statd_send_counter(ME_A_ "proposer.p2.executions",
			mctx->delayed_stats.proposer_p2_execs);
	mctx->delayed_stats.proposer_p2_execs = 0;
	statd_send_counter(ME_A_ "proposer.p2.timeouts",
			mctx->delayed_stats.proposer_p2_timeouts);
	mctx->delayed_stats.proposer_p2_timeouts = 0;
	statd_send_counter(ME_A_ "proposer.queue.accepted",
			mctx->delayed_stats.proposer_queue_accepted);
	mctx->delayed_stats.proposer_queue_accepted = 0;
	statd_send_counter(ME_A_ "proposer.queue.dropped",
			mctx->delayed_stats.proposer_queue_dropped);
	mctx->delayed_stats.proposer_queue_dropped = 0;
	statd_send_counter(ME_A_ "proposer.values_delivered",
			mctx->delayed_stats.proposer_values_delivered);
	mctx->delayed_stats.proposer_values_delivered = 0;

	statd_send_counter(ME_A_ "learner.retransmit.instances",
			mctx->delayed_stats.learner_retransmits);
	mctx->delayed_stats.learner_retransmits = 0;
	statd_send_counter(ME_A_ "learner.retransmit.req.unicast",
			mctx->delayed_stats.learner_unicast_requests);
	mctx->delayed_stats.learner_unicast_requests = 0;
	statd_send_counter(ME_A_ "learner.retransmit.req.broadcast",
			mctx->delayed_stats.learner_broadcast_requests);
	mctx->delayed_stats.learner_broadcast_requests = 0;

	statd_send_gauge(ME_A_ "acc_storage.highest_accepted",
			acs_get_highest_accepted(ME_A));
	statd_send_gauge(ME_A_ "acc_storage.highest_finalized",
			acs_get_highest_finalized(ME_A));

	statd_send_counter(ME_A_ "acceptor.learner_fast_path_fails",
			mctx->delayed_stats.acceptor_lea_fast_path_failures);
	mctx->delayed_stats.acceptor_lea_fast_path_failures = 0;
}

static void fiber_stats(struct fbr_context *fiber_context, void *_arg)
{
	struct me_context *mctx;

	mctx = container_of(fiber_context, struct me_context, fbr);
	for (;;) {
		fbr_sleep(&mctx->fbr, 1);
		stats_report(ME_A);
	}
}

static void mersenne_start(ME_P)
{
	fbr_id_t id;
	TAILQ_INIT(&mctx->learners);

	load_peer_list(ME_A_ mctx->args_info.peer_number_arg);

	set_up_udp_socket(ME_A);
	set_up_client_tcp_socket(ME_A);

	if (mctx->args_info.statd_ip_given) {
		statd_init(ME_A_ mctx->args_info.statd_ip_arg,
				mctx->args_info.statd_port_arg);
		id = fbr_create(&mctx->fbr, "main", fiber_stats, NULL, 0);
		fbr_transfer(&mctx->fbr, id);
	}
	pxs_fiber_init(ME_A);

	mctx->fiber_listener = fbr_create(&mctx->fbr, "listener",
			fiber_listener, NULL, 0);
	fbr_transfer(&mctx->fbr, mctx->fiber_listener);

	mctx->fiber_leader = fbr_create(&mctx->fbr, "leader", ldr_fiber, NULL,
			0);
	fbr_transfer(&mctx->fbr, mctx->fiber_leader);

	mctx->fiber_tcp_client = fbr_create(&mctx->fbr, "client",
			clt_tcp_fiber, NULL, 0);
	fbr_transfer(&mctx->fbr, mctx->fiber_tcp_client);
}

static void mersenne_stop(ME_P)
{
	fbr_reclaim(&mctx->fbr, mctx->fiber_tcp_client);
	fbr_reclaim(&mctx->fbr, mctx->fiber_leader);
	fbr_reclaim(&mctx->fbr, mctx->fiber_listener);
	pxs_fiber_shutdown(ME_A);
	destroy_peer_list(ME_A);
}

static void fiber_main(struct fbr_context *fiber_context, void *_arg)
{
	struct me_context *mctx;
	ev_signal sigint_watcher;
	ev_signal sigterm_watcher;
	ev_signal sighup_watcher;
	struct fbr_ev_watcher evw_sigint;
	struct fbr_ev_watcher evw_sigterm;
	struct fbr_ev_watcher evw_sighup;
	struct fbr_destructor sigint_dtor = FBR_DESTRUCTOR_INITIALIZER;
	struct fbr_destructor sigterm_dtor = FBR_DESTRUCTOR_INITIALIZER;
	struct fbr_destructor sighup_dtor = FBR_DESTRUCTOR_INITIALIZER;
	struct fbr_ev_base *fb_events[4];
	int n_events;

	mctx = container_of(fiber_context, struct me_context, fbr);

	ev_signal_init(&sigint_watcher, NULL, SIGINT);
	ev_signal_init(&sigterm_watcher, NULL, SIGTERM);
	ev_signal_init(&sighup_watcher, NULL, SIGHUP);
	fbr_ev_watcher_init(&mctx->fbr, &evw_sigint,
			(struct ev_watcher *)&sigint_watcher);
	fbr_ev_watcher_init(&mctx->fbr, &evw_sigterm,
			(struct ev_watcher *)&sigterm_watcher);
	fbr_ev_watcher_init(&mctx->fbr, &evw_sighup,
			(struct ev_watcher *)&sighup_watcher);
	sigint_dtor.func = ev_signal_dtor;
	sigint_dtor.arg = &sigint_watcher;
	sigterm_dtor.func = ev_signal_dtor;
	sigterm_dtor.arg = &sigterm_watcher;
	sighup_dtor.func = ev_signal_dtor;
	sighup_dtor.arg = &sighup_watcher;

	if (!RUNNING_ON_VALGRIND) {
		fb_events[0] = &evw_sigint.ev_base;
		fb_events[1] = &evw_sigterm.ev_base;
		fb_events[2] = &evw_sighup.ev_base;
		fb_events[3] = NULL;
	} else {
		fb_events[0] = &evw_sigterm.ev_base;
		fb_events[1] = &evw_sighup.ev_base;
		fb_events[2] = NULL;
	}

	fbr_log_i(&mctx->fbr, "Initializing acceptor storage");
	acs_initialize(ME_A);
	mersenne_start(ME_A);

	for(;;) {
		if (!RUNNING_ON_VALGRIND)
			ev_signal_start(mctx->loop, &sigint_watcher);
		ev_signal_start(mctx->loop, &sigterm_watcher);
		ev_signal_start(mctx->loop, &sighup_watcher);
		fbr_destructor_add(&mctx->fbr, &sigint_dtor);
		fbr_destructor_add(&mctx->fbr, &sigterm_dtor);
		fbr_destructor_add(&mctx->fbr, &sighup_dtor);
		n_events = fbr_ev_wait(&mctx->fbr, fb_events);
		assert(n_events >= 0);
		if (!n_events)
			continue;
		fbr_destructor_remove(&mctx->fbr, &sigint_dtor, 1 /* Call? */);
		fbr_destructor_remove(&mctx->fbr, &sigterm_dtor, 1 /* Call? */);
		fbr_destructor_remove(&mctx->fbr, &sighup_dtor, 1 /* Call? */);
		if (evw_sigint.ev_base.arrived) {
			fbr_log_d(&mctx->fbr, "got SIGINT");
			break;
		}
		if (evw_sigterm.ev_base.arrived) {
			fbr_log_d(&mctx->fbr, "got SIGTERM");
			break;
		}
		if (evw_sighup.ev_base.arrived) {
			/* Nothing here for now */
		}
	}

	mersenne_stop(ME_A);
	acs_destroy(ME_A);

	ev_break(mctx->loop, EVBREAK_ALL);
}

static int does_file_exists(const char *filename)
{
	struct stat st;
	int retval = stat(filename, &st);
	if(-1 == retval) {
		if(ENOENT == errno)
			return 0;
		else
			err(EXIT_FAILURE, "stat on %s failed", filename);
	}
	return 1;
}

int main(int argc, char *argv[])
{
	struct me_context context = ME_CONTEXT_INITIALIZER;
	struct me_context *mctx = &context;
	struct cmdline_parser_params *params;

	if (!RUNNING_ON_VALGRIND)
		signal(SIGSEGV, sigsegv_handler);
	signal(SIGPIPE, SIG_IGN);

	srand(time(NULL) ^ getpid());

	params = cmdline_parser_params_create();

	params->initialize = 1;
	params->print_errors = 1;
	params->check_required = 0;

	if (does_file_exists("mersenne.conf")) {
		if(0 != cmdline_parser_config_file("mersenne.conf",
					&mctx->args_info, params))
			exit(EXIT_FAILURE + 1);
		params->initialize = 0;
	}
	params->check_required = 1;
	if(0 != cmdline_parser_ext(argc, argv, &mctx->args_info, params))
		exit(EXIT_FAILURE + 1);

	cmdline_parser_dump(stdout, &mctx->args_info);
	setenv("TZ", "UTC", 1); // We're operating in UTC

	wait_for_debugger = mctx->args_info.wait_for_debugger_flag;

	// use the default event loop unless you have special needs
	mctx->loop = EV_DEFAULT;
	fbr_init(&mctx->fbr, mctx->loop);
	fbr_eio_init();
	setup_logging(ME_A);
	fbr_mutex_init(&mctx->fbr, &mctx->pxs.pro.pending_mutex);
	fbr_cond_init(&mctx->fbr, &mctx->pxs.pro.pending_cond);

	fbr_log_i(&mctx->fbr, "PID: %d", getpid());

	mctx->fiber_main = fbr_create(&mctx->fbr, "main", fiber_main, NULL, 0);
	fbr_transfer(&mctx->fbr, mctx->fiber_main);

	fbr_log_i(&mctx->fbr, "Starting main loop");
	ev_loop(context.loop, 0);
	fbr_log_i(&mctx->fbr, "Exiting");

	fbr_mutex_destroy(&mctx->fbr, &mctx->pxs.pro.pending_mutex);
	fbr_cond_destroy(&mctx->fbr, &mctx->pxs.pro.pending_cond);
	fbr_destroy(&mctx->fbr);

	cmdline_parser_free(&mctx->args_info);
	free(params);

	sm_report_leaked();

	return 0;
}
