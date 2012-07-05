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
#include <openssl/evp.h>
#include <bitmask.h>

#include <me_protocol.h>

#define MERSENNE_PORT		6377
#define MSGBUFSIZE		256
#define SEND_BUFFER_SIZE	MSGBUFSIZE * 50
#define TIME_DELTA		100
#define TIME_EPSILON		0

#define MSG_OK			0x01
#define MSG_START		0x02
#define MSG_ALERT		0x03

struct peer {
	int index;
	struct sockaddr_in addr;
	int ack_ttl;

	UT_hash_handle hh;
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
static int fd;
static struct peer *peers = NULL;
static struct peer *me = NULL;

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

void trust_init(struct me_omega_trust *trust)
{
	EVP_MD_CTX mdctx;
	const EVP_MD *md = EVP_md4();
	int count = HASH_COUNT(peers);
	struct peer *p;
	
	EVP_MD_CTX_init(&mdctx);
        EVP_DigestInit_ex(&mdctx, md, NULL);

	trust->mask = bitmask_alloc(count);
	bitmask_clearall(trust->mask);

	for(p=peers; p != NULL; p=p->hh.next) {
        	EVP_DigestUpdate(&mdctx, &p->index, sizeof(int));
		EVP_DigestUpdate(&mdctx, &p->addr.sin_addr.s_addr,
				sizeof(in_addr_t));
		if(p->ack_ttl > 0)
			bitmask_setbit(trust->mask, p->index);
	}

	trust->config_checksum.config_checksum_val = 
		malloc(EVP_MAX_MD_SIZE);
	EVP_DigestFinal_ex(&mdctx, trust->config_checksum.config_checksum_val,
			&trust->config_checksum.config_checksum_len);
        EVP_MD_CTX_cleanup(&mdctx);
}

void trust_free(struct me_omega_trust *trust)
{
	free(trust->config_checksum.config_checksum_val);
	bitmask_free(trust->mask);
}

void get_config_checksum(char **buf, int *size)
{
	EVP_MD_CTX mdctx;
	const EVP_MD *md = EVP_md4();
	struct peer *p;
	*buf = malloc(EVP_MAX_MD_SIZE);
	
	EVP_MD_CTX_init(&mdctx);
        EVP_DigestInit_ex(&mdctx, md, NULL);

	for(p=peers; p != NULL; p=p->hh.next) {
        	EVP_DigestUpdate(&mdctx, &p->index, sizeof(int));
		EVP_DigestUpdate(&mdctx, &p->addr.sin_addr.s_addr,
				sizeof(in_addr_t));
	}

	EVP_DigestFinal_ex(&mdctx, (unsigned char *)*buf, (unsigned int *)size);
        EVP_MD_CTX_cleanup(&mdctx);
}

void message_init(struct me_message *msg)
{
	struct me_omega_msg_header *hdr;

	msg->mm_stype = ME_OMEGA;
	hdr = &msg->me_message_u.omega_message.header;
	hdr->count = counter++;
	gettimeofday(&hdr->sent, NULL);
}

void message_send_to(struct me_message *msg, const int peer_num)
{
	struct peer *p;
	int size;
	XDR xdrs;
	char buf[ME_MAX_XDR_MESSAGE_LEN];

	xdrmem_create(&xdrs, buf, ME_MAX_XDR_MESSAGE_LEN, XDR_ENCODE);
	if(!xdr_me_message(&xdrs, msg))
		err(EXIT_FAILURE, "failed to encode a message");
	size = xdr_getpos(&xdrs);

	p = find_peer_by_index(peer_num);
	if(!p) {
		warn("unable to find peer #%d", peer_num);
		return;
	}

	if (sendto(fd, buf, size, 0, (struct sockaddr *) &p->addr, sizeof(p->addr)) < 0)
		err(EXIT_FAILURE, "failed to send message");

	xdr_destroy(&xdrs);
}

void message_send_all(struct me_message *msg)
{
	struct peer *p;
	int size;
	XDR xdrs;
	char buf[ME_MAX_XDR_MESSAGE_LEN];

	xdrmem_create(&xdrs, buf, ME_MAX_XDR_MESSAGE_LEN, XDR_ENCODE);
	if(!xdr_me_message(&xdrs, msg))
		err(EXIT_FAILURE, "failed to encode a message");
	size = xdr_getpos(&xdrs);

	for(p=peers; p != NULL; p=p->hh.next) {
		if (sendto(fd, buf, size, 0, (struct sockaddr *) &p->addr, sizeof(p->addr)) < 0)
			err(EXIT_FAILURE, "failed to send message");
	}
	xdr_destroy(&xdrs);
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

int is_expired(struct me_message *msg)
{
	struct timeval now;
	struct me_omega_msg_header *hdr;
	int delta;

	hdr = &msg->me_message_u.omega_message.header;
	gettimeofday(&now, NULL);
	delta = (now.tv_sec - hdr->sent.tv_sec) * 1000;
	delta += (now.tv_usec - hdr->sent.tv_usec) / 1000;
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
	struct me_message msg;
	struct me_omega_msg_data *data;

	message_init(&msg);

	data = &msg.me_message_u.omega_message.data;
	data->type = ME_OMEGA_START;
	data->me_omega_msg_data_u.round.k = s;

	if(to >= 0)
		message_send_to(&msg, to);
	else
		message_send_all(&msg);
}

void send_ok(int s)
{
	struct me_message msg;
	struct me_omega_msg_data *data;

	message_init(&msg);
	data = &msg.me_message_u.omega_message.data;
	data->type = ME_OMEGA_OK;
	trust_init(&data->me_omega_msg_data_u.ok.trust);
	data->me_omega_msg_data_u.ok.k = s;
	message_send_all(&msg);
	trust_free(&data->me_omega_msg_data_u.ok.trust);
}

void send_ack(int s, int to)
{
	struct me_message msg;
	struct me_omega_msg_data *data;

	message_init(&msg);

	data = &msg.me_message_u.omega_message.data;
	data->type = ME_OMEGA_ACK;
	data->me_omega_msg_data_u.round.k = s;

	message_send_to(&msg, to);
}

void start_round(const int s)
{
	struct peer *p;
	int n = HASH_COUNT(peers);

	if(me->index != s % n)
		send_start(s, -1);
	omega.r = s;
	omega.leader = omega.r % n;

	for(p=peers; p != NULL; p=p->hh.next) {
		p->ack_ttl = 3;
	}

	printf("R %d: new round started\n", omega.r);

	restart_timer();
}

void do_msg_start(struct me_message *msg, struct peer *from)
{
	int k;
	struct me_omega_msg_data *data;

	data = &msg->me_message_u.omega_message.data;
	k = data->me_omega_msg_data_u.round.k;

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

void do_msg_ok(struct me_message *msg, struct peer *from)
{
	int k;
	struct peer *p;
	struct me_omega_msg_data *data;
	char buf[512];
	char *checksum;
	int cs_size;

	get_config_checksum(&checksum, &cs_size);
	data = &msg->me_message_u.omega_message.data;
	k = data->me_omega_msg_data_u.ok.k;

	bitmask_displayhex(buf, 512, data->me_omega_msg_data_u.ok.trust.mask);
	printf("R %d: Got OK(%d) from peer #%d, trust: %s\n",
		omega.r,
		k,
		from->index,
		buf
	);

	if(strncmp(checksum, (char*)data->me_omega_msg_data_u.ok.trust.config_checksum.config_checksum_val, cs_size)) {
		warnx("config checksum mismatch, ignoring OK message");
		return;
	}

	if(k > omega.r) {
		send_ack(k, k % HASH_COUNT(peers));
		start_round(k);
		return;
	} else if(k < omega.r) {
		send_start(omega.r, from->index);
		return;
	}

	if(!bitmask_isbitset(data->me_omega_msg_data_u.ok.trust.mask, me->index)) {
		start_round(omega.r + 1);
		return;
	}

	for(p=peers; p != NULL; p=p->hh.next) {
		if(bitmask_isbitset(data->me_omega_msg_data_u.ok.trust.mask, p->index))
			p->ack_ttl = 1;
	}

	send_ack(omega.r, omega.r % HASH_COUNT(peers));
	restart_timer();
}

void do_msg_ack(struct me_message *msg, struct peer *from)
{
	int k;
	struct me_omega_msg_data *data;

	data = &msg->me_message_u.omega_message.data;
	k = data->me_omega_msg_data_u.round.k;
	
	printf("R %d: Got ACK(%d) from peer #%d\n",
		omega.r,
		k,
		from->index
	);

	if(k == omega.r) {
		from->ack_ttl = 3;
	} else
		warnx("got ACK from round other than mine");
}

void do_message(char* buf, int buf_size, const struct sockaddr *addr,
		socklen_t addrlen)
{
	struct peer *p;
	struct me_message msg;
	struct me_omega_msg_data *data;
	XDR xdrs;

	memset(&msg, 0, sizeof(msg));
	xdrmem_create(&xdrs, buf, buf_size, XDR_DECODE);
	if(!xdr_me_message(&xdrs, &msg))
		err(EXIT_FAILURE, "unable to decode a message");

	if(is_expired(&msg)) {
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

	data = &msg.me_message_u.omega_message.data;

	switch (data->type) {
		case ME_OMEGA_START:
			do_msg_start(&msg, p);
			break;
		case ME_OMEGA_OK:
			do_msg_ok(&msg, p);
			break;
		case ME_OMEGA_ACK:
			do_msg_ack(&msg, p);
			break;
		default:
			warnx("got unknown message from peer #%d\n", p->index);
			break;
	}
	xdr_free((xdrproc_t)xdr_me_message, (caddr_t)&msg);
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
	struct peer *p;
	int n = HASH_COUNT(peers);


	if(me->index == omega.r % n) {
		for(p=peers; p != NULL; p=p->hh.next) {
			p->ack_ttl--;
		}
		send_ok(omega.r);
	}

	printf("R %d: Leader=%d\n", omega.r, omega.leader);

	

	omega.delta_count++;
	if(omega.delta_count > 2)
		start_round(omega.r + 1);
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

