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
#include <uuid/uuid.h>
#include <uthash.h>
#include <err.h>
#include <errno.h>
#include <ev.h>
#include <rpc/xdr.h>

#define MERSENNE_PORT		12345
#define MERSENNE_GEOUP		"225.0.0.37"
#define MSGBUFSIZE		256
#define SEND_BUFFER_SIZE	MSGBUFSIZE * 50
#define TIME_DELTA		5000
#define TIME_EPSILON		100

#define MSG_OK			0x01
#define MSG_START		0x02
#define MSG_STOP		0x03

struct peer {
	uuid_t id;
	int index;
	struct sockaddr_in addr;
	time_t last_heartbeat;

	UT_hash_handle hh;
};

struct message_header {
	int type;
	int count;
	uuid_t sender;
	uuid_t recipient;
	struct timeval sent;
};

static struct omega {
	int r;
	int leader;
	int delta_count;
} omega;

static struct ev_loop *loop;
static int counter;
static ev_io socket_watcher;
static ev_timer delta_timer;
static struct sockaddr_in mcast_addr;
static int fd;
static struct peer *peers = NULL;
static struct peer *me = NULL;

void message_header_init(struct message_header *header)
{
	header->count = counter++;
	uuid_copy(header->sender, me->id);
	uuid_clear(header->recipient);
	gettimeofday(&header->sent, NULL);
}

void add_peer(struct peer *p)
{
	HASH_ADD(hh, peers, id, sizeof(uuid_t), p);
}

struct peer *find_peer(uuid_t id)
{
	struct peer *p;
	HASH_FIND(hh, peers, id, sizeof(uuid_t), p);
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

bool_t xdr_uuid(XDR *xdrs, const unsigned char uuid[16])
{
	return xdr_vector(xdrs, (void*)uuid, 16, sizeof(unsigned char), (xdrproc_t)xdr_u_char);
}

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
			xdr_uuid(xdrs, pp->sender) &&
			xdr_uuid(xdrs, pp->recipient) &&
			xdr_timeval(xdrs, &pp->sent)
	       );
}

void delete_peer(struct peer *peer)
{
	HASH_DEL(peers, peer);
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

int is_expired(struct message_header *header)
{
	struct timeval now;
	int delta;

	gettimeofday(&now, NULL);
	delta = (now.tv_sec - header->sent.tv_sec) * 1000;
	delta += now.tv_usec - header->sent.tv_usec;
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

void send_start(int s, int peer_index)
{
	char buf[MSGBUFSIZE];
	int size = 0;
	struct message_header header;
	struct peer *p;
	XDR xdrs;

	p = find_peer_by_index(peer_index);

	if(!p) {
		warnx("could not find peer with index %d", peer_index);
		return;
	}

	xdrmem_create(&xdrs, buf, MSGBUFSIZE, XDR_ENCODE);

	message_header_init(&header);
	header.type = MSG_START;
	uuid_copy(header.recipient, p->id);

	if(!xdr_message_header(&xdrs, &header))
		err(EXIT_FAILURE, "failed to encode message_header");

	if(!xdr_int32_t(&xdrs, &s))
		err(EXIT_FAILURE, "failed to encode int32");

	size = xdr_getpos(&xdrs);

	if (sendto(fd, buf, size, 0, (struct sockaddr *) &mcast_addr, sizeof(mcast_addr)) < 0)
		err(EXIT_FAILURE, "failed to send message");

}

void send_ok(int s)
{
	char buf[MSGBUFSIZE];
	int size = 0;
	struct message_header header;
	XDR xdrs;

	xdrmem_create(&xdrs, buf, MSGBUFSIZE, XDR_ENCODE);

	message_header_init(&header);
	header.type = MSG_OK;

	if(!xdr_message_header(&xdrs, &header))
		err(EXIT_FAILURE, "failed to encode message_header");

	if(!xdr_int32_t(&xdrs, &s))
		err(EXIT_FAILURE, "failed to encode int32");

	size = xdr_getpos(&xdrs);

	if (sendto(fd, buf, size, 0, (struct sockaddr *) &mcast_addr, sizeof(mcast_addr)) < 0)
		err(EXIT_FAILURE, "failed to send message");

}

void send_stop(int s, int peer_index)
{
	char buf[MSGBUFSIZE];
	int size = 0;
	struct message_header header;
	struct peer *p;
	XDR xdrs;

	p = find_peer_by_index(peer_index);

	if(!p) {
		warnx("could not find peer with index %d", peer_index);
		return;
	}

	xdrmem_create(&xdrs, buf, MSGBUFSIZE, XDR_ENCODE);

	message_header_init(&header);
	header.type = MSG_STOP;
	uuid_copy(header.recipient, p->id);

	if(!xdr_message_header(&xdrs, &header))
		err(EXIT_FAILURE, "failed to encode message_header");

	if(!xdr_int32_t(&xdrs, &s))
		err(EXIT_FAILURE, "failed to encode int32");

	size = xdr_getpos(&xdrs);

	if (sendto(fd, buf, size, 0, (struct sockaddr *) &mcast_addr, sizeof(mcast_addr)) < 0)
		err(EXIT_FAILURE, "failed to send message");

}

void start_round(int s)
{
	int n = HASH_COUNT(peers);

	if(me->index != s % n)
		send_start(s, s % n);
	omega.r = s;
	omega.leader = -1;
	restart_timer();
}

void do_msg_start(XDR *xdrs, struct peer *from)
{
	int k;

	if(!xdr_int32_t(xdrs, &k))
		err(EXIT_FAILURE, "failed to decode int32");

	printf("R%2d: Got START(%d) from peer #%d\n", omega.r, k, from->index);

	if(k > omega.r)
		start_round(k);
}

void do_msg_ok(XDR *xdrs, struct peer *from)
{
	int k;

	if(!xdr_int32_t(xdrs, &k))
		err(EXIT_FAILURE, "failed to decode int32");

	printf("R%2d: Got OK(%d) from peer #%d\n", omega.r, k, from->index);

	if(k > omega.r)
		start_round(k);
	else if(k == omega.r) {
		if(omega.leader < 0) {
			omega.leader--;
			if(omega.leader <= -3)
				omega.leader = omega.r % HASH_COUNT(peers);
		}
		restart_timer();
	}
}

void do_msg_stop(XDR *xdrs, struct peer *from)
{
	int k;

	if(!xdr_int32_t(xdrs, &k))
		err(EXIT_FAILURE, "failed to decode int32");

	printf("R%2d: Got STOP(%d) from peer #%d\n", omega.r, k, from->index);

	if(k >= omega.r)
		start_round(k + 1);
}

void do_message(char* buf, int buf_size, const struct sockaddr *addr,
		socklen_t addrlen)
{
	struct peer *p;
	struct message_header header;
	XDR xdrs;

	xdrmem_create(&xdrs, buf, buf_size, XDR_DECODE);
	if(!xdr_message_header(&xdrs, &header))
		err(2, "xdr_message");

	if(is_expired(&header))
		return;

	if(!uuid_is_null(header.recipient) &&
			uuid_compare(header.recipient, me->id) != 0)
		return;

	p = find_peer(header.sender);
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
		case MSG_STOP:
			do_msg_stop(&xdrs,p);
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

	printf("R%2d: Leader=%d\n", omega.r, omega.leader);

	omega.delta_count++;
	if(omega.delta_count > 2) {
		send_stop(omega.r, omega.r % n);
		start_round(omega.r + 1);
	}
}

void load_peer_list(int my_index) {
	int i;
	char uuid_buf[255];
	FILE* peers_file;
	struct peer *p;

	peers_file = fopen("peers", "r");
	if(!peers_file)
		err(1, "fopen");
	i = 0;
	while(1) {
		fgets(uuid_buf, 255, peers_file);
		if(feof(peers_file))
			break;
		uuid_buf[36] = '\0';
		p = malloc(sizeof(struct peer));
		memset(p, 0, sizeof(struct peer));
		p->index = i;
		if(-1 == uuid_parse(uuid_buf, p->id))
			err(1,"uuid_parse: cannot parse %s", uuid_buf);
		if(i == my_index) {
			me = p;
			printf("My id is %s\n", uuid_buf);
		}
		add_peer(p);
		i++;
	}
	fclose(peers_file);
}

int main(int argc, char *argv[])
{
	struct ip_mreqn mreq;
	int yes = 1;

	if(argc != 2) {
		puts("Please enter peer number!");
		abort();
	}


	setenv("TZ", "UTC", 1); // We're operating in UTC

	load_peer_list(atoi(argv[1]));


	/* create what looks like an ordinary UDP socket */
	if ((fd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
		perror("socket");
		abort();
	}

	if (-1 == make_socket_non_blocking(fd))
		abort();

	/* allow multiple sockets to use the same PORT number */
	if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0) {
		perror("Reusing ADDR failed");
		abort();
	}

	/* set up destination address */
	memset(&mcast_addr, 0, sizeof(mcast_addr));
	mcast_addr.sin_family = AF_INET;
	mcast_addr.sin_addr.s_addr = inet_addr(MERSENNE_GEOUP);
	mcast_addr.sin_port = htons(MERSENNE_PORT);

	/* bind to receive address */
	if (bind(fd, (struct sockaddr *) &mcast_addr, sizeof(mcast_addr)) < 0) {
		perror("bind");
		exit(1);
	}

	/* use setsockopt() to request that the kernel join a multicast group */
	mreq.imr_multiaddr.s_addr = inet_addr(MERSENNE_GEOUP);
	mreq.imr_address.s_addr = htonl(INADDR_ANY);
	mreq.imr_ifindex = 0;
	if (setsockopt(fd, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq)) < 0) {
		perror("setsockopt");
		exit(1);
	}

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

