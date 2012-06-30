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
#include <rpc/xdr.h>

#define MERSENNE_PORT		6377
#define MSGBUFSIZE		256
#define SEND_BUFFER_SIZE	MSGBUFSIZE * 50
#define TIME_DELTA		50
#define TIME_EPSILON		0

#define MSG_OK			0x01
#define MSG_START		0x02
#define MSG_ALERT		0x03

struct peer {
	int index;
	struct sockaddr_in addr;
	time_t last_heartbeat;

	UT_hash_handle hh;
};

struct message {
	char buf[MSGBUFSIZE];
	XDR xdrs;
};

struct message_header {
	int type;
	int count;
	struct timeval sent;
};

static struct omega {
	int r;
	int leader;
	int ok_count;
	int alert_count;
	int delta_count;
} omega;

static struct ev_loop *loop;
static int counter;
static ev_io socket_watcher;
static ev_timer delta_timer;
static int fd;
static struct peer *peers = NULL;
static struct peer *me = NULL;

bool_t xdr_timeval(XDR *xdrs, const struct timeval *tv)
{
	return (
			xdr_uint64_t(xdrs, (uint64_t *)&tv->tv_sec) &&
			xdr_uint64_t(xdrs, (uint64_t *)&tv->tv_usec)
	       );
}

bool_t xdr_message_header(XDR *xdrs, const struct message_header *pp)
{
	return (
			xdr_uint32_t(xdrs, (uint32_t *)&pp->type) &&
			xdr_uint32_t(xdrs, (uint32_t *)&pp->count) &&
			xdr_timeval(xdrs, &pp->sent)
	       );
}

void add_peer(struct peer *p)
{
	HASH_ADD(hh, peers, addr.sin_addr, sizeof(struct in_addr), p);
}

struct peer *find_peer(struct sockaddr_in *addr)
{
	struct peer *p;
	HASH_FIND(hh, peers, &addr->sin_addr, sizeof(struct in_addr), p);
	return p;
}

struct peer *find_peer_by_index(int index)
{
	struct peer *p;

	for(p=peers; p != NULL; p=p->hh.next) {
		if(p->index == index)
			return p;
	}
	return NULL;
}

void delete_peer(struct peer *peer)
{
	HASH_DEL(peers, peer);
}

void message_header_init(struct message_header *header)
{
	header->count = counter++;
	gettimeofday(&header->sent, NULL);
}

void message_start(struct message *msg, const int type)
{
	struct message_header header;

	xdrmem_create(&msg->xdrs, msg->buf, MSGBUFSIZE, XDR_ENCODE);

	message_header_init(&header);
	header.type = type;

	if(!xdr_message_header(&msg->xdrs, &header))
		err(EXIT_FAILURE, "failed to encode message_header");
}

void message_send_to(const struct message *msg, const int peer_num)
{
	struct peer *p;
	int size = xdr_getpos(&msg->xdrs);

	p = find_peer_by_index(peer_num);
	if(!p) {
		warn("unable to find peer #%d", peer_num);
		return;
	}

	if (sendto(fd, msg->buf, size, 0, (struct sockaddr *) &p->addr, sizeof(p->addr)) < 0)
		err(EXIT_FAILURE, "failed to send message");
}

void message_send_all(const struct message *msg)
{
	struct peer *p;
	int size = xdr_getpos(&msg->xdrs);

	for(p=peers; p != NULL; p=p->hh.next) {
		if (sendto(fd, msg->buf, size, 0, (struct sockaddr *) &p->addr, sizeof(p->addr)) < 0)
			err(EXIT_FAILURE, "failed to send message");
	}
}

void message_finish(struct message *msg)
{
	xdr_destroy(&msg->xdrs);
}

static int make_socket_non_blocking(int fd)
{
	int flags, s;

	flags = fcntl(fd, F_GETFL, 0);
	if (flags == -1) {
		perror("fcntl");
		return -1;
	}

	flags |= O_NONBLOCK;
	s = fcntl(fd, F_SETFL, flags);
	if (s == -1) {
		perror("fcntl");
		return -1;
	}

	return 0;
}

void check_leader() {
	if(omega.alert_count == 0 && omega.ok_count >= 2) {
		omega.leader = omega.r % HASH_COUNT(peers);
		printf("R %d: Leader check has passed, alerts = %d, ok = %d\n",
			omega.r,
			omega.alert_count,
			omega.ok_count
		);
	} else
		printf("R %d: Leader check has FAILED, alerts = %d, ok = %d\n",
			omega.r,
			omega.alert_count,
			omega.ok_count
		);
}

int is_expired(struct message_header *header)
{
	struct timeval now;
	int delta;

	gettimeofday(&now, NULL);
	delta = (now.tv_sec - header->sent.tv_sec) * 1000;
	delta += (now.tv_usec - header->sent.tv_usec) / 1000;
	if(delta > TIME_DELTA + 2 * TIME_EPSILON)
		return 1;
	else
		return 0;
}

void restart_timer()
{
	ev_timer_again(loop, &delta_timer);
	omega.delta_count = 0;
}

void send_start(int s, int to)
{
	struct message msg;
	message_start(&msg, MSG_START);

	if(!xdr_int32_t(&msg.xdrs, &s))
		err(EXIT_FAILURE, "failed to encode int32");
	
	if(to >= 0)
		message_send_to(&msg, to);
	else
		message_send_all(&msg);
	message_finish(&msg);
}

void send_ok(int s)
{
	struct message msg;
	message_start(&msg, MSG_OK);

	if(!xdr_int32_t(&msg.xdrs, &s))
		err(EXIT_FAILURE, "failed to encode int32");

	message_send_all(&msg);
	message_finish(&msg);

}

void send_alert(int s)
{
	struct message msg;
	message_start(&msg, MSG_OK);

	if(!xdr_int32_t(&msg.xdrs, &s))
		err(EXIT_FAILURE, "failed to encode int32");

	message_send_all(&msg);
	message_finish(&msg);
}

void start_round(const int s)
{
	int n = HASH_COUNT(peers);

	send_alert(s);
	if(me->index != s % n)
		send_start(s, -1);
	omega.r = s;
	omega.leader = -1;
	omega.ok_count = 0;

	printf("R %d: new round started\n", omega.r);

	restart_timer();
}

void do_msg_start(XDR *xdrs, const struct peer *from)
{
	int k;

	if(!xdr_int32_t(xdrs, &k))
		err(EXIT_FAILURE, "failed to decode int32");
	
	printf("R %d: Got START(%d) from peer #%d\n",
		omega.r,
		k,
		from->index
	);

	if(k > omega.r)
		start_round(k);
	else if(k < omega.r)
		start_round(omega.r);

}

void do_msg_ok(XDR *xdrs, const struct peer *from)
{
	int k;

	if(!xdr_int32_t(xdrs, &k))
		err(EXIT_FAILURE, "failed to decode int32");

	printf("R %d: Got OK(%d) from peer #%d\n",
		omega.r,
		k,
		from->index
	);

	if(k > omega.r)
		start_round(k);
	else if(k == omega.r) {
		if(omega.ok_count < 2) {
			omega.ok_count++;
			check_leader();
		}
		restart_timer();
	} else if(k < omega.r)
		send_start(omega.r, from->index);
}

void do_msg_alert(XDR *xdrs, const struct peer *from)
{
	int k;

	if(!xdr_int32_t(xdrs, &k))
		err(EXIT_FAILURE, "failed to decode int32");

	printf("R %d: Got ALERT(%d) from peer #%d\n",
		omega.r,
		k,
		from->index
	);

	if(k > omega.r) {
		omega.leader = -1;
		omega.alert_count = 6;
	}
}

void do_message(char* buf, int buf_size, const struct sockaddr *addr,
		socklen_t addrlen)
{
	struct peer *p;
	struct message_header header;
	XDR xdrs;

	xdrmem_create(&xdrs, buf, buf_size, XDR_DECODE);
	if(!xdr_message_header(&xdrs, &header))
		err(2, "unable to decode xdr_message_header");

	if(is_expired(&header)) {
		warnx("got expired message");
		return;
	}
	if (addr->sa_family != AF_INET) {
		warnx("unsupported address family: %d", (addr->sa_family));
		return;
	}
	p = find_peer((struct sockaddr_in *)addr);
	if(!p) {
		warnx("unknown peer heartbeat --- ignoring");
		return;
	}

	switch (header.type) {
		case MSG_START:
			do_msg_start(&xdrs, p);
			break;
		case MSG_OK:
			do_msg_ok(&xdrs,p);
			break;
		case MSG_ALERT:
			do_msg_alert(&xdrs,p);
			break;
		default:
			warnx("got unknown message from peer #%d\n", p->index);
			break;
	}
}

static void socket_read_cb (EV_P_ ev_io *w, int revents)
{
	int nbytes;
	struct sockaddr client_addr;
	socklen_t client_addrlen = sizeof(client_addr);
	char msgbuf[MSGBUFSIZE];

	while(1) {
		nbytes = recvfrom(w->fd, msgbuf, MSGBUFSIZE, 0,
				&client_addr, &client_addrlen);
		if (nbytes < 0) {
			if (errno == EAGAIN)
				break;
			else if (errno != EINTR)
				err(1, "recvfrom");
		} else {
			do_message(msgbuf, nbytes, &client_addr, client_addrlen);
		}
	}

}

// another callback, this time for a time-out
static void timeout_cb (EV_P_ ev_timer *w, int revents)
{
	int n = HASH_COUNT(peers);

	if(me->index == omega.r % n)
		send_ok(omega.r);

	printf("R %d: Leader=%d\n", omega.r, omega.leader);

	omega.delta_count++;
	if(omega.delta_count > 2)
		start_round(omega.r + 1);
	
	if(omega.alert_count > 0) {
		omega.alert_count--;
		check_leader();
	}
}

void load_peer_list(int my_index) {
	int i;
	char line_buf[255];
	FILE* peers_file;
	struct peer *p;

	peers_file = fopen("peers", "r");
	if(!peers_file)
		err(1, "fopen");
	i = 0;
	while(1) {
		fgets(line_buf, 255, peers_file);
		if(feof(peers_file))
			break;
		p = malloc(sizeof(struct peer));
		memset(p, 0, sizeof(struct peer));
		p->index = i;
		p->addr.sin_family = AF_INET;
		if(0 == inet_aton(line_buf, &p->addr.sin_addr))
			errx(EXIT_FAILURE, "invalid address: %s", line_buf);
		p->addr.sin_port = htons(MERSENNE_PORT);
		if(i == my_index) {
			me = p;
			printf("My ip is %s\n", line_buf);
		}
		add_peer(p);
		i++;
	}
	fclose(peers_file);
}

int main(int argc, char *argv[])
{
	int yes = 1;

	if(argc != 2) {
		puts("Please enter peer number!");
		abort();
	}


	setenv("TZ", "UTC", 1); // We're operating in UTC

	load_peer_list(atoi(argv[1]));


	/* create what looks like an ordinary UDP socket */
	if ((fd = socket(AF_INET, SOCK_DGRAM, 0)) < 0)
		err(EXIT_FAILURE, "failed to create a socket");

	if (-1 == make_socket_non_blocking(fd))
		abort();

	/* allow multiple sockets to use the same PORT number */
	if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0)
		err(EXIT_FAILURE, "reusing of address failed");

	/* bind to receive address */
	if (bind(fd, (struct sockaddr *) &me->addr, sizeof(me->addr)) < 0)
		err(EXIT_FAILURE, "bind failed");

	// use the default event loop unless you have special needs
	loop = EV_DEFAULT;

	// initialise an io watcher, then start it
	// this one will watch for stdin to become readable
	ev_io_init(&socket_watcher, socket_read_cb, fd, EV_READ);
	ev_io_start(loop, &socket_watcher);

	// initialise a timer watcher, then start it
	// simple non-repeating 0.5 second timeout
	ev_timer_init(&delta_timer, timeout_cb, TIME_DELTA / 1000., TIME_DELTA / 1000.);
	ev_timer_start(loop, &delta_timer);

	start_round(0);

	// now wait for events to arrive
	printf("Starting main loop\n");
	ev_loop(loop, 0);
	printf("Exiting\n");

	// break was called, so exit
	return 0;
}

