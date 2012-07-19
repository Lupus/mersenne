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

#include <ev.h>
#include <openssl/evp.h>
#include <err.h>

#include <leader.h>
#include <context.h>
#include <vars.h>
#include <util.h>

struct bitmask * get_trust(ME_P)
{
	struct bitmask *bmp;
	struct me_peer *p;
	bmp = bitmask_alloc(HASH_COUNT(mctx->peers));
	bitmask_clearall(bmp);

	for(p=mctx->peers; p != NULL; p=p->hh.next) {
		if(p->ack_ttl > 0)
			bitmask_setbit(bmp, p->index);
	}
	return bmp;
}

void get_config_checksum(ME_P_ char **buf, int *size)
{
	EVP_MD_CTX mdctx;
	const EVP_MD *md = EVP_md4();
	struct me_peer *p;

	if(NULL == *buf)
		*buf = malloc(EVP_MAX_MD_SIZE);

	EVP_MD_CTX_init(&mdctx);
	EVP_DigestInit_ex(&mdctx, md, NULL);

	for(p=mctx->peers; p != NULL; p=p->hh.next) {
		EVP_DigestUpdate(&mdctx, &p->index, sizeof(int));
		EVP_DigestUpdate(&mdctx, &p->addr.sin_addr.s_addr,
				sizeof(in_addr_t));
	}

	EVP_DigestFinal_ex(&mdctx, (unsigned char *)*buf, (unsigned int *)size);
	EVP_MD_CTX_cleanup(&mdctx);
}

void message_init(ME_P_ struct me_message *msg)
{
	struct me_leader_msg_header *hdr;
	char *ptr;

	msg->super_type = ME_LEADER;
	hdr = &msg->me_message_u.leader_message.header;
	hdr->count = mctx->counter++;
	ptr = hdr->config_checksum;
	get_config_checksum(ME_A_ (char **)&ptr, NULL);
	gettimeofday(&hdr->sent, NULL);
}

int is_expired(struct me_message *msg)
{
	struct timeval now;
	struct me_leader_msg_header *hdr;
	int delta;

	hdr = &msg->me_message_u.leader_message.header;
	gettimeofday(&now, NULL);
	delta = (now.tv_sec - hdr->sent.tv_sec) * 1000;
	delta += (now.tv_usec - hdr->sent.tv_usec) / 1000;
	if(delta > TIME_DELTA + 2 * TIME_EPSILON)
		return 1;
	else
		return 0;
}

int config_match(ME_P_ struct me_message *msg)
{
	struct me_leader_msg_header *hdr;
	char *checksum = NULL;
	int cs_size;

	get_config_checksum(ME_A_ &checksum, &cs_size);
	hdr = &msg->me_message_u.leader_message.header;

	if(strncmp(checksum, hdr->config_checksum, cs_size))
		return 0;
	return 1;
}

void restart_timer(ME_P)
{
	ev_timer_again(mctx->loop, &mctx->ldr.delta_timer);
	mctx->ldr.delta_count = 0;
}

void send_start(ME_P_ int s, int to)
{
	struct me_message msg;
	struct me_leader_msg_data *data;

	message_init(ME_A_ &msg);

	data = &msg.me_message_u.leader_message.data;
	data->type = ME_LEADER_START;
	data->me_leader_msg_data_u.round.k = s;

	if(to >= 0)
		msg_send_to(ME_A_ &msg, to);
	else
		msg_send_all(ME_A_ &msg);
}

void send_ok(ME_P_ int s)
{
	struct me_message msg;
	struct me_leader_msg_data *data;

	message_init(ME_A_ &msg);
	data = &msg.me_message_u.leader_message.data;
	data->type = ME_LEADER_OK;
	data->me_leader_msg_data_u.ok.trust = get_trust(ME_A);
	data->me_leader_msg_data_u.ok.k = s;
	msg_send_all(ME_A_ &msg);
	bitmask_free(data->me_leader_msg_data_u.ok.trust);
}

void send_ack(ME_P_ int s, int to)
{
	struct me_message msg;
	struct me_leader_msg_data *data;

	message_init(ME_A_ &msg);

	data = &msg.me_message_u.leader_message.data;
	data->type = ME_LEADER_ACK;
	data->me_leader_msg_data_u.round.k = s;

	msg_send_to(ME_A_ &msg, to);
}

void start_round(ME_P_ const int s)
{
	struct me_peer *p;
	int n = HASH_COUNT(mctx->peers);

	if(mctx->me->index != s % n)
		send_start(ME_A_ s, -1);
	mctx->ldr.r = s;
	mctx->ldr.leader = mctx->ldr.r % n;

	for(p=mctx->peers; p != NULL; p=p->hh.next) {
		p->ack_ttl = 3;
	}

	printf("R %d: new round started\n", mctx->ldr.r);

	restart_timer(ME_A);
}

void do_msg_start(ME_P_ struct me_message *msg, struct me_peer *from)
{
	int k;
	struct me_leader_msg_data *data;

	data = &msg->me_message_u.leader_message.data;
	k = data->me_leader_msg_data_u.round.k;

	printf("R %d: Got START(%d) from peer #%d\n",
			mctx->ldr.r,
			k,
			from->index
	      );

	if(k > mctx->ldr.r)
		start_round(ME_A_ k);
	else if(k < mctx->ldr.r)
		start_round(ME_A_ mctx->ldr.r);

}

void do_msg_ok(ME_P_ struct me_message *msg, struct me_peer *from)
{
	int k;
	struct me_peer *p;
	struct me_leader_msg_data *data;
	char buf[512];

	data = &msg->me_message_u.leader_message.data;
	k = data->me_leader_msg_data_u.ok.k;

	bitmask_displayhex(buf, 512, data->me_leader_msg_data_u.ok.trust);
	printf("R %d: Got OK(%d) from peer #%d, trust: %s\n",
			mctx->ldr.r,
			k,
			from->index,
			buf
	      );

	if(k > mctx->ldr.r) {
		send_ack(ME_A_ k, k % HASH_COUNT(mctx->peers));
		start_round(ME_A_ k);
		return;
	} else if(k < mctx->ldr.r) {
		send_start(ME_A_ mctx->ldr.r, from->index);
		return;
	}

	if(!bitmask_isbitset(data->me_leader_msg_data_u.ok.trust, mctx->me->index)) {
		start_round(ME_A_ mctx->ldr.r + 1);
		return;
	}

	for(p=mctx->peers; p != NULL; p=p->hh.next) {
		if(bitmask_isbitset(data->me_leader_msg_data_u.ok.trust, p->index))
			p->ack_ttl = 1;
	}

	send_ack(ME_A_ mctx->ldr.r, mctx->ldr.r % HASH_COUNT(mctx->peers));
	restart_timer(ME_A);
}

void do_msg_ack(ME_P_ struct me_message *msg, struct me_peer *from)
{
	int k;
	struct me_leader_msg_data *data;

	data = &msg->me_message_u.leader_message.data;
	k = data->me_leader_msg_data_u.round.k;

	printf("R %d: Got ACK(%d) from peer #%d\n",
			mctx->ldr.r,
			k,
			from->index
	      );

	if(k == mctx->ldr.r) {
		from->ack_ttl = 3;
	} else
		warnx("got ACK from round other than mine");
}


static void timeout_cb (EV_P_ ev_timer *w, int revents)
{
	struct me_peer *p;
	struct ldr_context *ldr;
	struct me_context *mctx;
	ldr = container_of(w, struct ldr_context, delta_timer);
	mctx = container_of(ldr, struct me_context, ldr);

	int n = HASH_COUNT(mctx->peers);


	if(mctx->me->index == mctx->ldr.r % n) {
		for(p=mctx->peers; p != NULL; p=p->hh.next) {
			p->ack_ttl--;
		}
		send_ok(ME_A_ mctx->ldr.r);
	}

	printf("R %d: Leader=%d\n", mctx->ldr.r, mctx->ldr.leader);

	mctx->ldr.delta_count++;
	if(mctx->ldr.delta_count > 2)
		start_round(ME_A_ mctx->ldr.r + 1);
}

void ldr_do_message(ME_P_ struct me_message *msg, struct me_peer *from)
{
	struct me_leader_msg_data *data;

	if(is_expired(msg)) {
		warnx("got expired message");
		return;
	}
	if(!config_match(ME_A_ msg)) {
		warnx("sender configuration does not match mine, ignoring message");
		return;
	}
	data = &msg->me_message_u.leader_message.data;

	switch (data->type) {
		case ME_LEADER_START:
			do_msg_start(ME_A_ msg, from);
			break;
		case ME_LEADER_OK:
			do_msg_ok(ME_A_ msg, from);
			break;
		case ME_LEADER_ACK:
			do_msg_ack(ME_A_ msg, from);
			break;
		default:
			warnx("got unknown message from peer #%d\n", from->index);
			break;
	}
}

void ldr_fiber_init(ME_P)
{
	ev_timer_init(&mctx->ldr.delta_timer, timeout_cb, TIME_DELTA / 1000., TIME_DELTA / 1000.);
	ev_timer_start(mctx->loop, &mctx->ldr.delta_timer);

	start_round(ME_A_ 0);
}
