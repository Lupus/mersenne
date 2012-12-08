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
#if _USE_PROFILER
#include <google/profiler.h>
#endif

#include <mersenne/me_protocol.h>
#include <mersenne/context.h>
#include <mersenne/peers.h>
#include <mersenne/paxos.h>
#include <mersenne/util.h>
#include <mersenne/fiber_args.h>
#include <mersenne/client.h>
#include <mersenne/sharedmem.h>
#include <mersenne/cmdline.h>

#define LISTEN_BACKLOG 50

static int wait_for_debugger;

static void message_destructor(void *context, void *ptr)
{
	xdr_free((xdrproc_t)xdr_me_message, ptr);
}

static void process_message(ME_P_ XDR *xdrs, struct me_peer *from)
{
	int pos;
	struct me_message *msg;
	struct fbr_buffer *fb;
	struct msg_info *info;

	msg = sm_calloc_ext(1, sizeof(struct me_message), message_destructor,
			NULL);
	pos = xdr_getpos(xdrs);
	if(!xdr_me_message(xdrs, msg))
		errx(EXIT_FAILURE, "xdr_me_message: unable to decode a "
				"message at %d", pos);

	switch(msg->super_type) {
		case ME_LEADER:
			fb = fbr_get_user_data(&mctx->fbr, mctx->fiber_leader);
			info = fbr_buffer_alloc_prepare(&mctx->fbr, fb,
					sizeof(struct msg_info));
			info->msg = sm_in_use(msg);
			info->from = from;
			fbr_buffer_alloc_commit(&mctx->fbr, fb);
			break;
		case ME_PAXOS:
			pxs_do_message(ME_A_ msg, from);
			break;
	}
	sm_free(msg);
}

static void process_message_buf(ME_P_ char* buf, int buf_size, const struct sockaddr *addr,
		socklen_t addrlen)
{
	struct me_peer *p;
	XDR xdrs;

	if(addr->sa_family != AF_INET) {
		fbr_log_w(&mctx->fbr, "unsupported address family: %d", (addr->sa_family));
		return;
	}

	p = find_peer(ME_A_ (struct sockaddr_in *)addr);
	if(!p) {
		fbr_log_w(&mctx->fbr, "got message from unknown peer --- ignoring");
		return;
	}

	xdrmem_create(&xdrs, buf, buf_size, XDR_DECODE);
	process_message(ME_A_ &xdrs, p);
	xdr_destroy(&xdrs);
}

static void fiber_main(struct fbr_context *fiber_context, void *_arg)
{
	struct me_context *mctx;
	int nbytes;
	struct sockaddr client_addr;
	socklen_t client_addrlen = sizeof(client_addr);
	char msgbuf[ME_MAX_XDR_MESSAGE_LEN];

	mctx = container_of(fiber_context, struct me_context, fbr);
	for(;;) {
		nbytes = fbr_recvfrom(&mctx->fbr, mctx->fd, msgbuf,
				ME_MAX_XDR_MESSAGE_LEN, 0, &client_addr,
				&client_addrlen);
		if (nbytes < 0 && errno != EINTR)
				err(1, "recvfrom");
		process_message_buf(ME_A_ msgbuf, nbytes, &client_addr, client_addrlen);
	}
}

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

static void set_up_client_socket(ME_P)
{
	struct sockaddr_un addr;
	char *rendezvous = mctx->args_info.client_socket_arg;

	if ((mctx->client_fd = socket(AF_UNIX, SOCK_STREAM, 0)) < 0)
		err(EXIT_FAILURE, "failed to create a client socket");

	make_socket_non_blocking(mctx->client_fd);

	addr.sun_family = AF_UNIX;
	strcpy(addr.sun_path, rendezvous);
	unlink(rendezvous);
	/* bind to receive address */
	if (bind(mctx->client_fd, (struct sockaddr *) &addr, sizeof(addr)) < 0)
		err(EXIT_FAILURE, "client socket bind failed");
	if (-1 == listen(mctx->client_fd, LISTEN_BACKLOG))
               err(EXIT_FAILURE, "client socket listen failed");
}

static void setup_logging(ME_P)
{
	enum fbr_log_level log_level;
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

static void sigint_cb (EV_P_ ev_signal *w, int revents)
{
	ev_break (EV_A_ EVBREAK_ALL);
}

static void sigterm_cb (EV_P_ ev_signal *w, int revents)
{
	ev_break (EV_A_ EVBREAK_ALL);
}

static void sighup_cb (EV_P_ ev_signal *w, int revents)
{
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
#if _USE_PROFILER
	char profile_filename[256];
#endif
	ev_signal sigint_watcher;
	ev_signal sigterm_watcher;
	ev_signal sighup_watcher;

	params = cmdline_parser_params_create();

	params->initialize = 1;
	params->print_errors = 1;
	params->check_required = 0;

	if(does_file_exists("mersenne.conf")) {
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
	TAILQ_INIT(&mctx->learners);

	setup_logging(ME_A);

	load_peer_list(ME_A_ mctx->args_info.peer_number_arg);

	set_up_udp_socket(ME_A);
	set_up_client_socket(ME_A);

	if(!RUNNING_ON_VALGRIND) {
		ev_signal_init(&sigint_watcher, sigint_cb, SIGINT);
		ev_signal_start(mctx->loop, &sigint_watcher);
		signal(SIGSEGV, sigsegv_handler);
	}
	ev_signal_init(&sigterm_watcher, sigterm_cb, SIGTERM);
	ev_signal_start(mctx->loop, &sigterm_watcher);
	ev_signal_init(&sighup_watcher, sighup_cb, SIGHUP);
	ev_signal_start(mctx->loop, &sighup_watcher);
	signal(SIGPIPE, SIG_IGN);

	pxs_fiber_init(ME_A);

	mctx->fiber_main = fbr_create(&mctx->fbr, "main", fiber_main, NULL, 0);
	mctx->fiber_leader = fbr_create(&mctx->fbr, "leader", ldr_fiber, NULL, 0);
	mctx->fiber_client = fbr_create(&mctx->fbr, "client", clt_fiber, NULL, 0);

	fbr_transfer(&mctx->fbr, mctx->fiber_main);
	fbr_transfer(&mctx->fbr, mctx->fiber_leader);
	fbr_transfer(&mctx->fbr, mctx->fiber_client);

#if _USE_PROFILER
	snprintf(profile_filename, sizeof(profile_filename), "cpu_profile.%d",
			mctx->me->index);
	ProfilerStart(profile_filename);
	fbr_log_i(&mctx->fbr, "Starting main loop (cpu profiling is ON)");
#else
	fbr_log_i(&mctx->fbr, "Starting main loop");
#endif
	ev_loop(context.loop, 0);
	fbr_log_i(&mctx->fbr, "Exiting");
#if _USE_PROFILER
	ProfilerStop();
#endif

	destroy_peer_list(ME_A);
	pxs_fiber_shutdown(ME_A);

	fbr_destroy(&mctx->fbr);

	cmdline_parser_free(&mctx->args_info);
	free(params);

	return 0;
}
