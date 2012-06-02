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
#include <fcntl.h>
#include <uthash.h>
#include <err.h>
#include <errno.h>
#include <ev.h>
#include <pty.h>
#include <utarray.h>
#include <sys/time.h>
#include <assert.h>
#include <regex.h>

#define BUF_SIZE 256
#define LINE_BUF_SIZE 10 * 1024
#define PROC_NUM 4


struct child_proc_item {
	int cpi_fd;
	pid_t cpi_pid;
	int cpi_index;
	ev_io cpi_io;
	char cpi_buf[LINE_BUF_SIZE];
	char *cpi_buf_ptr;
	struct timeval cpi_tv;
	UT_hash_handle hh;
};

static struct child_proc_item *children;
//static ev_timer kill_timer;
static struct ev_loop *loop;
static struct timeval start_tv;

static regex_t rx_new_round;
static regex_t rx_leader;

static UT_icd double_icd = {sizeof(double), NULL, NULL, NULL };

//static float ts;
//static float first_ts;
//static float election_time;
static UT_array *measurements;
static int *leaders = NULL;
static float start_time = -1;
int current_round = 0;

static void make_fd_blocking(int fd)
{
	int flags, s;

	flags = fcntl(fd, F_GETFL, 0);
	if (flags == -1)
		err(EXIT_FAILURE, "%s", "fcntl failed");

	flags |= O_NONBLOCK;
	s = fcntl(fd, F_SETFL, flags);
	if (s == -1)
		err(EXIT_FAILURE, "%s", "fcntl failed");
}

static float timeval_msec_delta(struct timeval *from, struct timeval *to)
{
	long long u_to = to->tv_sec * 1e6 + to->tv_usec;
	long long u_from = from->tv_sec * 1e6 + from->tv_usec;
	return (u_to - u_from) / 1000.0;
}

static int check_leader_election()
{
	int i;
	int leader;

	assert(PROC_NUM > 1);
	leader = leaders[0];
	for(i = 1; i < PROC_NUM; i++) {
		if(leaders[i] != leader)
			return 0;
	}
	if(leader < 0)
		return 0;
	return 1;
}

static void process_line(char *line, int idx, float ts)
{
	regmatch_t match[2];
	int round, leader;
	int retval;
	double d;
	retval = regexec(&rx_new_round, line, 0, NULL, 0);
	if(retval) {
		if(REG_NOMATCH != retval)
			errx(EXIT_FAILURE, "%s", "regexec failed");
	} else {
		fprintf(stderr, "*** new round line\n");
		if(start_time < 0) {
			start_time = ts;
			memset(leaders, -1, sizeof(int) * PROC_NUM);
			fprintf(stderr, "*** Registered new round\n");
		}
		return;
	}

	retval = regexec(&rx_leader, line, 2, match, 0);
	if(retval) {
		if(REG_NOMATCH != retval)
			errx(EXIT_FAILURE, "%s", "regexec failed");
	} else {
		line[match[0].rm_eo + 1] = '\0';
		line[match[1].rm_eo + 1] = '\0';
		round = atoi(line + match[0].rm_so);
		leader = atoi(line + match[1].rm_so);
		fprintf(stderr, "*** Round %d, Leader %d\n", round, leader);
		if(round > current_round) {
			memset(leaders, -1, sizeof(int) * PROC_NUM);
			current_round = round;
		}
		leaders[idx] = leader;
		if(start_time >= 0 && check_leader_election()) {
			d = ts - start_time;
			utarray_push_back(measurements, &d);
			start_time = -1;
			fprintf(stderr, "*** Leader elected, time %f ms\n", d);
		}
	}

}

static void pipe_read_cb(EV_P_ ev_io *w, int revents)
{
	struct child_proc_item *child;
	char buf;
	int retval;
	int bytes_left;
	HASH_FIND_INT(children, &w->fd, child);
	assert(child != NULL);
	for(;;) {
		if(child->cpi_buf_ptr == child->cpi_buf) {
			gettimeofday(&child->cpi_tv, NULL);
		}
		retval = read(w->fd, &buf, 1);
		if(-1 == retval) {
			if(errno == EINTR)
				continue;
			else if(errno == EAGAIN)
				return;
			else
				err(EXIT_FAILURE, "%s", "reading from pipe failed");
		}
		if(0 == retval) {
			printf("Peer #%d closed the pipe\n", child->cpi_index);
			ev_io_stop (EV_A_ w);
			return;
		}
		if(buf == '\n')
			*child->cpi_buf_ptr++ = '\0';
		else
			*child->cpi_buf_ptr++ = buf;

		bytes_left = LINE_BUF_SIZE - (child->cpi_buf_ptr - child->cpi_buf);
		if(bytes_left == 1) {
			*child->cpi_buf_ptr++ = '\0';
		}
		if(bytes_left == 1 || buf == '\n') {
			fprintf(stderr, "%f P%.2d %s\n",
				timeval_msec_delta(&start_tv, &child->cpi_tv),
				child->cpi_index,
				child->cpi_buf
			);
			process_line(child->cpi_buf, child->cpi_index, timeval_msec_delta(&start_tv, &child->cpi_tv));
			child->cpi_buf_ptr = child->cpi_buf; 
		}
	}
}

// another callback, this time for a time-out
// static void timeout_cb (EV_P_ ev_timer *w, int revents)
// {
// }

void init_child_proc_item(struct child_proc_item *item)
{
	item->cpi_buf_ptr = item->cpi_buf;
}

int main()
{
	int proc_num = PROC_NUM;
	char *mersenne_exe = "./mersenne";
	char buf[BUF_SIZE];
	char *argv[3] = {mersenne_exe, buf, NULL};
	struct child_proc_item *child;
	int i;

	// use the default event loop unless you have special needs
	loop = EV_DEFAULT;
	leaders = (int *)calloc(proc_num, sizeof(int));
	if(regcomp (&rx_new_round, "^R [[:digit:]]: new round started", 0))
		err(EXIT_FAILURE, "%s", "regcomp failed");
	if(regcomp (&rx_leader, "^R \\([[:digit:]]\\+\\): Leader=\\([[:digit:]]\\+\\)", 0))
		err(EXIT_FAILURE, "%s", "regcomp failed");
	gettimeofday(&start_tv, NULL);
	utarray_new(measurements, &double_icd);

	for(i = 0; i < proc_num; i++) {
		snprintf(buf, BUF_SIZE, "%d", i);
		child = (struct child_proc_item *)
			malloc(sizeof(struct child_proc_item));
		init_child_proc_item(child);
		child->cpi_index = i;
		child->cpi_pid = forkpty(&child->cpi_fd, NULL, NULL, NULL);
		make_fd_blocking(child->cpi_fd);
		if(-1 == child->cpi_pid)
			err(EXIT_FAILURE, "%s", "forkpty");
		if(child->cpi_pid == 0) {
			if(-1 == execv(mersenne_exe, argv))
				err(EXIT_FAILURE, "%s", "exec failed");
		}
		ev_io_init(&child->cpi_io, pipe_read_cb, child->cpi_fd, EV_READ);
		ev_io_start(loop, &child->cpi_io);
		HASH_ADD_INT(children, cpi_fd, child);
	}

	// initialise a timer watcher, then start it
	// simple non-repeating 0.5 second timeout
	// ev_timer_init(&kill_timer, timeout_cb, 5., 0.);
	// ev_timer_start(loop, &kill_timer);

	// now wait for events to arrive
	ev_loop(loop, 0);

	printf("Simulation has finished\n");

	// break was called, so exit
	return 0;

}
