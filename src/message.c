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

#include <err.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <errno.h>
#include <mersenne/message.h>
#include <mersenne/context.h>
#include <mersenne/sharedmem.h>
#include <mersenne/me_protocol.h>
#include <mersenne/strenum.h>
#include <mersenne/util.h>

static void message_destructor(void *context, void *ptr)
{
	xdr_free((xdrproc_t)xdr_me_message, ptr);
}

static void process_message(ME_P_ XDR *xdrs, struct me_peer *from, size_t size)
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

	msg_perf_dump(ME_A_ msg, MSG_DIR_RECEIVED, &from->addr, size);

	switch(msg->super_type) {
		case ME_LEADER:
			fb = fbr_get_user_data(&mctx->fbr, mctx->fiber_leader);
			buffer_ensure_writable(ME_A_ fb,
					sizeof(struct msg_info));
			info = fbr_buffer_alloc_prepare(&mctx->fbr, fb,
					sizeof(struct msg_info));
			info->msg = sm_in_use(msg);
			info->from = from;
			info->received_ts = ev_now(mctx->loop);
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
	process_message(ME_A_ &xdrs, p, buf_size);
	xdr_destroy(&xdrs);
}

void fiber_listener(struct fbr_context *fiber_context, void *_arg)
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
		process_message_buf(ME_A_ msgbuf, nbytes, &client_addr,
				client_addrlen);
	}
}

static inline int p_peer(struct me_peer *peer, void *context)
{
	return peer->index == *(int *)context;
}

void msg_send_to(ME_P_ struct me_message *msg, const int peer_num)
{
	msg_send_matching(ME_A_ msg, &p_peer, (int *)&peer_num);
}

static inline int p_all(struct me_peer *peer, void *context)
{
	return 1;
}

void msg_send_all(ME_P_ struct me_message *msg)
{
	msg_send_matching(ME_A_ msg, &p_all, NULL);
}

/*
static void format_buffer(struct buffer *buffer, char *out, size_t out_size)
{
	char tmp[8];
	size_t size;
	if (NULL == buffer) {
		snprintf(out, out_size, "(null)");
		return;
	}
	size = min(sizeof(tmp) - 1, buffer->size1);
	memcpy(tmp, buffer->ptr, size);
	tmp[size] = '\0';
	snprintf(out, out_size, "{size=%u, ptr='%s'}", buffer->size1, tmp);
}
*/

void msg_perf_dump(ME_P_ struct me_message *msg, enum msg_direction dir,
		struct sockaddr_in *addr, size_t size)
{
	char *type = NULL, *subtype = NULL;
	char str[INET_ADDRSTRLEN];
	char *direction = dir ? "OUT" : "IN";
	char *data = NULL;
	char buf[512];
	int retval;
	ev_tstamp now;
	struct me_leader_message *lmsg;
	struct me_paxos_message *pmsg;
	ev_now_update(mctx->loop);
	now = ev_now(mctx->loop);
	switch (msg->super_type) {
	case ME_LEADER:
		type = "LEADER";
		lmsg = &msg->me_message_u.leader_message;
		switch (lmsg->data.type) {
		case ME_LEADER_OK:
			subtype = "OK";
			bm_displayhex(buf, sizeof(buf),
					lmsg->data.me_leader_msg_data_u.ok.trust);
			retval = asprintf(&data, "%d\t%s",
					lmsg->data.me_leader_msg_data_u.ok.k,
					buf);
			if (-1 == retval)
				err(EXIT_FAILURE, "asprintf");
			break;
		case ME_LEADER_START:
			subtype = "START";
			retval = asprintf(&data, "%d",
					lmsg->data.me_leader_msg_data_u.round.k);
			if (-1 == retval)
				err(EXIT_FAILURE, "asprintf");
			break;
		case ME_LEADER_ACK:
			subtype = "ACK";
			retval = asprintf(&data, "%d",
					lmsg->data.me_leader_msg_data_u.round.k);
			if (-1 == retval)
				err(EXIT_FAILURE, "asprintf");
			break;
		}
		break;
	case ME_PAXOS:
		type = "PAXOS";
		pmsg = &msg->me_message_u.paxos_message;
		switch (pmsg->data.type) {
		case ME_PAXOS_PREPARE:
			subtype = "PREPARE";
			retval = asprintf(&data, "%lu\t%lu",
					pmsg->data.me_paxos_msg_data_u.prepare.i,
					pmsg->data.me_paxos_msg_data_u.prepare.b);
			if (-1 == retval)
				err(EXIT_FAILURE, "asprintf");
			break;
		case ME_PAXOS_PROMISE:
			subtype = "PROMISE";
			retval = asprintf(&data, "%lu\t%lu\t%lu",
					pmsg->data.me_paxos_msg_data_u.promise.i,
					pmsg->data.me_paxos_msg_data_u.promise.b,
					pmsg->data.me_paxos_msg_data_u.promise.vb);
			if (-1 == retval)
				err(EXIT_FAILURE, "asprintf");
			break;
		case ME_PAXOS_ACCEPT:
			subtype = "ACCEPT";
			retval = asprintf(&data, "%lu\t%lu",
					pmsg->data.me_paxos_msg_data_u.accept.i,
					pmsg->data.me_paxos_msg_data_u.accept.b);
			if (-1 == retval)
				err(EXIT_FAILURE, "asprintf");
			break;
		case ME_PAXOS_REJECT:
			subtype = "REJECT";
			retval = asprintf(&data, "%lu\t%lu",
					pmsg->data.me_paxos_msg_data_u.reject.i,
					pmsg->data.me_paxos_msg_data_u.reject.b);
			if (-1 == retval)
				err(EXIT_FAILURE, "asprintf");
			break;
		case ME_PAXOS_LAST_ACCEPTED:
			subtype = "LAST_ACCEPTED";
			retval = asprintf(&data, "%lu",
					pmsg->data.me_paxos_msg_data_u.last_accepted.i);
			if (-1 == retval)
				err(EXIT_FAILURE, "asprintf");
			break;
		case ME_PAXOS_LEARN:
			subtype = "LEARN";
			retval = asprintf(&data, "%lu\t%lu",
					pmsg->data.me_paxos_msg_data_u.learn.i,
					pmsg->data.me_paxos_msg_data_u.learn.b);
			if (-1 == retval)
				err(EXIT_FAILURE, "asprintf");
			break;
		case ME_PAXOS_RELEARN:
			subtype = "RELEARN";
			retval = asprintf(&data, "%lu\t%lu",
					pmsg->data.me_paxos_msg_data_u.learn.i,
					pmsg->data.me_paxos_msg_data_u.learn.b);
			if (-1 == retval)
				err(EXIT_FAILURE, "asprintf");
			break;
		case ME_PAXOS_RETRANSMIT:
			subtype = "RETRANSMIT";
			retval = asprintf(&data, "%lu\t%lu",
					pmsg->data.me_paxos_msg_data_u.retransmit.from,
					pmsg->data.me_paxos_msg_data_u.retransmit.to);
			if (-1 == retval)
				err(EXIT_FAILURE, "asprintf");
			break;
		case ME_PAXOS_CLIENT_VALUE:
			subtype = "CLIENT_VALUE";
			retval = asprintf(&data, "%s", "");
			if (-1 == retval)
				err(EXIT_FAILURE, "asprintf");
			break;
		}
	}
	if (NULL == inet_ntop(AF_INET, &addr->sin_addr.s_addr, str, INET_ADDRSTRLEN))
		err(EXIT_FAILURE, "inet_ntop failed");
	if (NULL == data)
		data = strdup("");
	printf("MSG_PERFLOG\t%s\t%s\t%s\t%f\t%zd\t%s\t%s\r\n",
			direction, type, subtype, now, size, str, data);
	free(data);
}

void msg_send_matching(ME_P_ struct me_message *msg,
		int (*predicate)(struct me_peer *, void *context),
		void *context)
{
	struct me_peer *p;
	int size;
	int retval;
	XDR xdrs;
	char buf[ME_MAX_XDR_MESSAGE_LEN];

	xdrmem_create(&xdrs, buf, ME_MAX_XDR_MESSAGE_LEN, XDR_ENCODE);
	if(!xdr_me_message(&xdrs, msg))
		errx(EXIT_FAILURE, "xdr_me_message: failed to encode a message");
	size = xdr_getpos(&xdrs);

	for(p=mctx->peers; p != NULL; p=p->hh.next) {
		if(!predicate(p, context))
			continue;
		if (mctx->me == p) {
			msg_perf_dump(ME_A_ msg, 1, &p->addr, size);
			process_message_buf(ME_A_ buf, size,
					(struct sockaddr *)&mctx->me->addr,
					sizeof(mctx->me->addr));
			continue;
		}
		retval = fbr_sendto(&mctx->fbr, mctx->fd, buf, size, 0,
				(struct sockaddr *)&p->addr, sizeof(p->addr));
		if (-1 == retval)
			err(EXIT_FAILURE, "failed to send message");
		if (retval < size)
			fbr_log_n(&mctx->fbr, "message got truncated from %d to"
					" %d while sending", size, retval);
		msg_perf_dump(ME_A_ msg, 1, &p->addr, size);
	}
	xdr_destroy(&xdrs);
}
