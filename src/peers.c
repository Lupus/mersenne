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
#include <uthash.h>
#include <regex.h>
#include <err.h>

#include <mersenne/peers.h>
#include <mersenne/context.h>
#include <mersenne/sharedmem.h>
#include <mersenne/util.h>
#include <mersenne/log.h>

struct bulk_stats {
	size_t num_sent;
	size_t bytes_sent;
	size_t num_overfills;
	size_t num_timeouts;
};

static struct bulk_stats stats;
static struct bulk_stats last_stats;
static struct ev_timer stats_timer;
#define BULK_TIMER_MIN 0.001
#define BULK_TIMER_MAX 0.5
static ev_tstamp bulk_timer = (BULK_TIMER_MAX - BULK_TIMER_MIN) / 2 + BULK_TIMER_MIN;

void add_peer(ME_P_ struct me_peer *p)
{
	HASH_ADD(hh, mctx->peers, addr.sin_addr, sizeof(struct in_addr), p);
}

struct me_peer *find_peer(ME_P_ struct sockaddr_in *addr)
{
	struct me_peer *p;
	HASH_FIND(hh, mctx->peers, &addr->sin_addr, sizeof(struct in_addr), p);
	return p;
}

struct me_peer *find_peer_by_index(ME_P_ int index)
{
	struct me_peer *p;

	for(p=mctx->peers; p != NULL; p=p->hh.next) {
		if(p->index == index)
			return p;
	}
	return NULL;
}

void delete_peer(ME_P_ struct me_peer *peer)
{
	HASH_DEL(mctx->peers, peer);
}

static int bulk_header_size()
{
	int size;
	XDR xdrs;
	char buf[ME_MAX_XDR_MESSAGE_LEN];
	struct me_message msg;

	msg.super_type = ME_BULK;

	xdrmem_create(&xdrs, buf, ME_MAX_XDR_MESSAGE_LEN, XDR_ENCODE);
	if(!xdr_me_message(&xdrs, &msg))
		errx(EXIT_FAILURE, "xdr_me_message: failed to encode a message");
	size = xdr_getpos(&xdrs);
	xdr_destroy(&xdrs);

	return size;
}

static void flush_queue(ME_P_ struct me_peer *p)
{
	int retval;
	int i;
	XDR xdrs;
	char buf[ME_MAX_XDR_MESSAGE_LEN];
	struct msghdr hdr;
	struct me_message msg;
	
	if(1 == p->pending_last)
		return;

	msg.super_type = ME_BULK;
	msg.me_message_u.size = p->pending_last - 1;
	
	xdrmem_create(&xdrs, buf, ME_MAX_XDR_MESSAGE_LEN, XDR_ENCODE);
	if(!xdr_me_message(&xdrs, &msg))
		errx(EXIT_FAILURE, "xdr_me_message: failed to encode a message");
	p->pending_messages[0].iov_base = buf;
	p->pending_messages[0].iov_len = xdr_getpos(&xdrs);
	xdr_destroy(&xdrs);

	memset(&hdr, 0x00, sizeof(hdr));
	hdr.msg_name = &p->addr;
	hdr.msg_namelen = sizeof(p->addr);
	hdr.msg_iov = p->pending_messages;
	hdr.msg_iovlen = p->pending_last;
	retval = sendmsg(mctx->fd, &hdr, 0);
	if(-1 == retval)
		err(EXIT_FAILURE, "sendmsg failed");
	for(i = 1; i < hdr.msg_iovlen; i++)
		sm_free(hdr.msg_iov[i].iov_base);
	//fprintf(stderr, "Flushed a bulk of size %d with %lu iovectors for #%d\n", msg.me_message_u.size, hdr.msg_iovlen, p->index);
	p->pending_last = 1;
	p->pending_data_size = 0;
}

static void queue_timeout_cb (EV_P_ ev_timer *w, int revents)
{
	struct me_context *mctx = (struct me_context *)w->data;
	struct me_peer *peer;
	peer = container_of(w, struct me_peer, pending_timer);
	stats.num_sent++;
	stats.bytes_sent += peer->pending_data_size;
	stats.num_timeouts++;
	flush_queue(ME_A_ peer);
}

void peer_enque_message(ME_P_ struct me_peer *p, void *buf, size_t len)
{
	const int base_size = 32;
	int size;
	if(p->pending_data_size + len > ME_MAX_XDR_MESSAGE_LEN -
			mctx->bulk_hdr_size) {
		stats.num_sent++;
		stats.bytes_sent += p->pending_data_size;
		stats.num_overfills++;
		flush_queue(ME_A_ p);
	}
	if(0 == p->pending_data_size) {
		p->pending_timer.repeat = bulk_timer;
		ev_timer_again(mctx->loop, &p->pending_timer);
	}
	if(NULL == p->pending_messages) {
		p->pending_messages = calloc(base_size, sizeof(struct iovec));
		p->pending_last = 1;
		p->pending_size = base_size;
	} else if(p->pending_last == p->pending_size) {
		size = p->pending_size * 2;
		p->pending_messages = realloc(p->pending_messages, size *
				sizeof(struct iovec));
		p->pending_size = size;
	}
	p->pending_messages[p->pending_last].iov_base = sm_in_use(buf);
	p->pending_messages[p->pending_last].iov_len = len;
	//fprintf(stderr, "Adding %lu to %lu\n", len, p->pending_data_size);
	p->pending_data_size += len;
	p->pending_last++;
	//fprintf(stderr, "Enqueued a message #%lu for #%d\n", p->pending_last - 1, p->index);
}

static void stats_timeout_cb (EV_P_ ev_timer *w, int revents)
{
	double modifier = 1.0;
	double last_fullness;
	double fullness;
	if(last_stats.num_sent && stats.num_sent) {
		last_fullness = (double)last_stats.bytes_sent / (last_stats.num_sent
				* ME_MAX_XDR_MESSAGE_LEN);
		fullness = (double)stats.bytes_sent / (stats.num_sent *
				ME_MAX_XDR_MESSAGE_LEN);
		modifier = fullness / last_fullness;
		bulk_timer *= modifier;
		if(bulk_timer < BULK_TIMER_MIN)
			bulk_timer = BULK_TIMER_MIN;
		else if(bulk_timer > BULK_TIMER_MAX)
			bulk_timer = BULK_TIMER_MAX;
	}
	log(LL_INFO, "[BULK] Sent %lu bulks with %f fill, %lu timed out, %lu"
			" overfilled, new timer is %f modified by %f\n",
			stats.num_sent,
			(double)stats.bytes_sent / (stats.num_sent *
				ME_MAX_XDR_MESSAGE_LEN) * 100,
			stats.num_timeouts,
			stats.num_overfills,
			bulk_timer, modifier);
	last_stats = stats;
	memset(&stats, 0x00, sizeof(stats));
}

void load_peer_list(ME_P_ int my_index)
{
	int i;
	char *addr, *flag;
	char line_buf[255];
	FILE* peers_file;
	struct me_peer *p;
	static regex_t rx_config_line;
	regmatch_t match[4];
	int retval;

	mctx->bulk_hdr_size = bulk_header_size();
	
	memset(&stats, 0x00, sizeof(stats));
	ev_timer_init(&stats_timer, stats_timeout_cb, 1.0, 1.0);
	ev_timer_start(mctx->loop, &stats_timer);
	stats_timer.data = mctx;

	peers_file = fopen("peers", "r");
	if(!peers_file)
		err(1, "fopen");
	if(regcomp(&rx_config_line, "^\\([[:digit:]]\\+\\.[[:digit:]]\\+\\.[[:digit:]]\\+\\.[[:digit:]]\\+\\)\\(\\:\\(a\\)\\)\\?", 0))
		err(EXIT_FAILURE, "%s", "regcomp failed");
	i = 0;
	while(1) {
		fgets(line_buf, 255, peers_file);
		if(feof(peers_file))
			break;
		p = malloc(sizeof(struct me_peer));
		memset(p, 0, sizeof(struct me_peer));
		ev_init(&p->pending_timer, queue_timeout_cb);
		p->pending_timer.repeat = bulk_timer;
		p->pending_timer.data = mctx;
		p->index = i;
		
		retval = regexec(&rx_config_line, line_buf, 4, match, 0);
		if(retval) {
			if(REG_NOMATCH != retval)
				errx(EXIT_FAILURE, "%s", "regexec failed");
			errx(EXIT_FAILURE, "Invalid config line: %s", line_buf);
		} else {
			line_buf[match[1].rm_eo] = '\0';
			line_buf[match[3].rm_eo] = '\0';
			addr = line_buf + match[1].rm_so;
			flag = line_buf + match[3].rm_so;
		}

		p->addr.sin_family = AF_INET;
		if(0 == inet_aton(addr, &p->addr.sin_addr))
			errx(EXIT_FAILURE, "invalid address: %s", line_buf);
		p->addr.sin_port = htons(mctx->args_info.port_num_arg);
		if('a' == flag[0])
			p->pxs.is_acceptor = 1;
		if(i == my_index) {
			mctx->me = p;
			printf("My ip is %s\n", line_buf);
			if(p->pxs.is_acceptor)
				printf("I am an acceptor\n");
		}
		add_peer(ME_A_ p);
		i++;
	}
	regfree(&rx_config_line);
	fclose(peers_file);
}

void destroy_peer_list(ME_P)
{
	struct me_peer *elt, *tmp;
	HASH_ITER(hh, mctx->peers, elt, tmp) {
		HASH_DEL(mctx->peers, elt);
		free(elt);
	}
}

int peer_count(ME_P)
{
	return HASH_COUNT(mctx->peers);
}

int peer_count_matching(ME_P_ int (*predicate)(struct me_peer *, void *context), void *context)
{
	int count = 0;
	struct me_peer *p;

	for(p=mctx->peers; p != NULL; p=p->hh.next) {
		if(predicate(p, context))
			count++;
	}
	return count;
}
