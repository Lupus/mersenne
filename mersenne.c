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
#define TIMER_FREQUENCY		2

#define MSG_HEARTBEAT		0x01

struct mers_peer {
	uuid_t id;
	int index;
	struct sockaddr_in addr;
	time_t last_heartbeat;

	UT_hash_handle hh;
};

struct mers_message {
	int type;
	int count;
	uuid_t id;
};

struct omega {
	int r;
} omega;

static int counter;
static ev_io socket_watcher;
static ev_timer timeout_watcher;
static struct sockaddr_in mcast_addr;
static int fd;
static struct mers_peer *peers = NULL;
static struct mers_peer *me = NULL;

void add_peer(struct mers_peer *p)
{
	HASH_ADD(hh, peers, id, sizeof(uuid_t), p);
}

struct mers_peer *find_peer(uuid_t id)
{
	struct mers_peer *p;
	HASH_FIND(hh, peers, id, sizeof(uuid_t), p);
	return p;
}

int is_alive(struct mers_peer *p)
{
	return time(NULL) - p->last_heartbeat < 2 * TIMER_FREQUENCY;
}


bool_t xdr_uuid(XDR *xdrs, unsigned char uuid[16])
{
	return xdr_vector(xdrs, (void*)uuid, 16, sizeof(unsigned char), (xdrproc_t)xdr_u_char);
}

bool_t xdr_mers_message(XDR *xdrs, struct mers_message *pp)
{
	return (
			xdr_int32_t(xdrs, &pp->type) &&
			xdr_int32_t(xdrs, &pp->count) &&
			xdr_uuid(xdrs, pp->id)
		   );
}


void delete_peer(struct mers_peer *peer)
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

void do_message(char* buf, int buf_size, const struct sockaddr *addr,
		socklen_t addrlen)
{
	char uuid_buf[37];
	struct mers_peer *p;
	struct mers_message msg;
	XDR xdrs;

	xdrmem_create(&xdrs, buf, buf_size, XDR_DECODE);
	if(!xdr_mers_message(&xdrs, &msg))
		err(2, "xdr_mers_message");

	if (uuid_compare(msg.id, me->id) == 0)
		return;

	uuid_unparse(msg.id, uuid_buf);

	switch (msg.type) {
		case MSG_HEARTBEAT:
			printf("Got a heartbeat message #%d from %s\n", msg.count,
					uuid_buf);
			p = find_peer(msg.id);
			if(!p) {
				puts("Unknown peer heartbeat --- ignoring");
				break;
			}
			p->last_heartbeat = time(NULL);
			break;
		default:
			printf("Got unknown message from %s\n", uuid_buf);
			break;
	}
}

void do_heartbeat()
{
	char buf[MSGBUFSIZE];
	int size = 0;
	struct mers_message msg;
	XDR xdrs;

	xdrmem_create(&xdrs, buf, MSGBUFSIZE, XDR_ENCODE);
	msg.type = MSG_HEARTBEAT;
	msg.count = counter++;
	uuid_copy(msg.id, me->id);

	if(!xdr_mers_message(&xdrs, &msg))
		err(2, "xdr_mers_message");

	size = xdr_getpos(&xdrs);

	if (sendto(fd, buf, size, 0, (struct sockaddr *) &mcast_addr, sizeof(mcast_addr)) < 0) {
		perror("sendto");
		abort();
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
	do_heartbeat();
}

void load_peer_list(int my_index) {
	int i;
	char uuid_buf[255];
	FILE* peers_file;
	struct mers_peer *p;

	peers_file = fopen("peers", "r");
	if(!peers_file)
		err(1, "fopen");
	i = 0;
	while(1) {
		fgets(uuid_buf, 255, peers_file);
		if(feof(peers_file))
			break;
		uuid_buf[36] = '\0';
		p = malloc(sizeof(struct mers_peer));
		memset(p, 0, sizeof(struct mers_peer));
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
	struct ev_loop *loop = EV_DEFAULT;

	// initialise an io watcher, then start it
	// this one will watch for stdin to become readable
	ev_io_init(&socket_watcher, socket_read_cb, fd, EV_READ);
	ev_io_start(loop, &socket_watcher);

	// initialise a timer watcher, then start it
	// simple non-repeating 0.5 second timeout
	ev_timer_init(&timeout_watcher, timeout_cb, TIMER_FREQUENCY, TIMER_FREQUENCY);
	ev_timer_start(loop, &timeout_watcher);

	// now wait for events to arrive
	printf("Starting main loop\n");
	ev_loop(loop, 0);
	printf("Exiting\n");

	// break was called, so exit
	return 0;
}

