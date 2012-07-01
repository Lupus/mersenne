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
#include <gsl/gsl_statistics_double.h>

#define BUF_SIZE 256
#define LINE_BUF_SIZE 10 * 1024
//#define DEBUG
//#define PROXY_OUTPUT

#define PROC_LEADER_NOT_SURE	-1
#define PROC_LEADER_UNDEFINED	-2

static int proc_num;

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
static ev_timer sim_timer;
static ev_timer kill_timer;
static struct ev_loop *loop;
static struct timeval start_tv;
static int killed_children;
static int current_leader;

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

static void clear_leaders()
{
	int i;
	for(i = 0; i < proc_num; i++) {
		leaders[i] = PROC_LEADER_UNDEFINED;

	}
}

#ifdef DEBUG

static void dump_leaders()
{
	int i;
	fprintf(stderr, "[");
	for(i = 0; i < proc_num; i++) {
		fprintf(stderr, "%d", leaders[i]);
		if(i < proc_num - 1)
			fprintf(stderr, ", ");

	}
	fprintf(stderr, "]\n");
}

#endif

static int check_leader_election()
{
	int i;
	int leader = PROC_LEADER_UNDEFINED;
	int votes = 0;

	for(i = 0; i < proc_num; i++) {
		if(leaders[i] == PROC_LEADER_UNDEFINED)
			continue;
		if(leader == PROC_LEADER_UNDEFINED)
			leader = leaders[i];
		else if(leaders[i] != leader)
			return 0;
		votes++;
	}
	if(leader < 0)
		return 0;
	if(votes < proc_num - killed_children)
		return 0;
	current_leader = leader;
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
		if(start_time < 0) {
			start_time = ts;
			clear_leaders();
#ifdef DEBUG
			fprintf(stderr, "*** Registered new round\n");
#endif
		}
		return;
	}

	retval = regexec(&rx_leader, line, 3, match, 0);
	if(retval) {
		if(REG_NOMATCH != retval)
			errx(EXIT_FAILURE, "%s", "regexec failed");
	} else {
		line[match[1].rm_eo + 1] = '\0';
		line[match[2].rm_eo + 1] = '\0';
		round = atoi(line + match[1].rm_so);
		leader = atoi(line + match[2].rm_so);
		if(round > current_round) {
			clear_leaders();
			current_round = round;
		}
		leaders[idx] = leader;
		if(start_time >= 0 && check_leader_election()) {
			d = ts - start_time;
			utarray_push_back(measurements, &d);
			start_time = -1;
#ifdef DEBUG
			fprintf(stderr, "*** Leader elected, time %f ms, round %d, leader %d\n", d, round, leader);
#endif
		}
	}
#ifdef DEBUG
	dump_leaders();
#endif
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
#ifdef PROXY_OUTPUT
			fprintf(stderr, "%f P%.2d %s\n",
				timeval_msec_delta(&start_tv, &child->cpi_tv),
				child->cpi_index,
				child->cpi_buf
			);
#endif
			process_line(child->cpi_buf, child->cpi_index, timeval_msec_delta(&start_tv, &child->cpi_tv));
			child->cpi_buf_ptr = child->cpi_buf; 
		}
	}
}

static void spawn_child(struct child_proc_item *child)
{
	char *mersenne_exe = "./mersenne";
	char buf[BUF_SIZE];
	char *argv[3] = {mersenne_exe, buf, NULL};
	struct child_proc_item *tmp;
	
	HASH_FIND_INT(children, &child->cpi_fd, tmp);
	if(NULL != tmp)
		HASH_DEL(children, child);

	snprintf(buf, BUF_SIZE, "%d", child->cpi_index);
	child->cpi_pid = forkpty(&child->cpi_fd, NULL, NULL, NULL);
	if(-1 == child->cpi_pid)
		err(EXIT_FAILURE, "%s", "forkpty");
	if(child->cpi_pid == 0) {
		if(-1 == execv(mersenne_exe, argv))
			err(EXIT_FAILURE, "%s", "exec failed");
	}
	make_fd_blocking(child->cpi_fd);
	ev_io_init(&child->cpi_io, pipe_read_cb, child->cpi_fd, EV_READ);
	ev_io_start(loop, &child->cpi_io);

	HASH_ADD_INT(children, cpi_fd, child);
}

static void kill_child(struct child_proc_item *child)
{
	if(0 == child->cpi_pid)
		return;
	ev_io_stop(loop, &child->cpi_io);
	if(-1 == kill(child->cpi_pid, SIGTERM))
		err(EXIT_FAILURE, "failed to kill mersenne");
	child->cpi_pid = 0;
	close(child->cpi_fd);
}

static void sim_timeout_cb (EV_P_ ev_timer *w, int revents)
{
	struct child_proc_item *i, *tmp;
	HASH_ITER(hh, children, i, tmp) {
		kill_child(i);
		HASH_DEL(children,i);
		free(i);
	}
	ev_break(EV_A_ EVBREAK_ALL);
}

static void kill_timeout_cb (EV_P_ ev_timer *w, int revents)
{
	struct child_proc_item *c;
	int spawned = 0;

	for(c=children; c != NULL; c=c->hh.next) {
		if(0 == c->cpi_pid) {
			spawn_child(c);
			killed_children--;
			spawned++;
		} 
	}

	if(current_leader < 0 || spawned > 0)
		return;

	for(c=children; c != NULL; c=c->hh.next) {
		if(current_leader == c->cpi_index) {
			kill_child(c);
			killed_children++;
		}
	}
}

static void init_child_proc_item(struct child_proc_item *item)
{
	item->cpi_buf_ptr = item->cpi_buf;
}

static void process_statistic()
{
	double *arr;
	double tmp, tmp2;
	int size, i;
	
	arr = (double*)utarray_front(measurements);
	size = utarray_len(measurements);

	for(i = 0; i < size; i++) {
		printf("%f\n", arr[i]);
	}
	printf("\n");
	tmp = gsl_stats_mean(arr, 1, size);
	printf("Average: %f\n", tmp);
	tmp = gsl_stats_sd_m(arr, 1, size, tmp);
	printf("Stdev: %f\n", tmp);
	gsl_stats_minmax (&tmp, &tmp2, arr, 1, size);
	printf("Min: %f, Max: %f\n", tmp, tmp2);

}

static void init_peers_file()
{
	FILE *peers;
	int i;

	peers = fopen("peers", "w+");
	for(i = 0; i < proc_num; i++)
		fprintf(peers, "127.0.0.%d\n", i + 1);
	fclose(peers);
}

int main(int argc, char *argv[])
{
	struct child_proc_item *child;
	int i;

	if(2 != argc)
		errx(EXIT_FAILURE, "expecting a number of processes as the first argument");
	
	proc_num = atoi(argv[1]);
	if(proc_num < 1 || proc_num > 254)
		errx(EXIT_FAILURE, "invalid number of processes");

	init_peers_file();

	utarray_new(measurements,&double_icd);
	// use the default event loop unless you have special needs
	loop = EV_DEFAULT;
	leaders = (int *)calloc(proc_num, sizeof(int));
	clear_leaders();
	killed_children = 0;
	current_leader = PROC_LEADER_UNDEFINED;
	if(regcomp (&rx_new_round, "^R [[:digit:]]: new round started", 0))
		err(EXIT_FAILURE, "%s", "regcomp failed");
	if(regcomp (&rx_leader, "^R \\([[:digit:]]\\+\\): Leader=\\([[:digit:]]\\+\\)", 0))
		err(EXIT_FAILURE, "%s", "regcomp failed");
	gettimeofday(&start_tv, NULL);
	utarray_new(measurements, &double_icd);

	for(i = 0; i < proc_num; i++) {
		child = (struct child_proc_item *)
			malloc(sizeof(struct child_proc_item));
		init_child_proc_item(child);
		child->cpi_index = i;
		spawn_child(child);
	}

	ev_timer_init(&sim_timer, sim_timeout_cb, 60., 0.);
	ev_timer_start(loop, &sim_timer);
	
	ev_timer_init(&kill_timer, kill_timeout_cb, 1., 1.);
	ev_timer_start(loop, &kill_timer);

	ev_loop(loop, 0);

	printf("Simulation has finished\n");

	process_statistic();

	utarray_free(measurements);

	// break was called, so exit
	return 0;

}
