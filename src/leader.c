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
#include <assert.h>

#include <mersenne/leader.h>
#include <mersenne/context.h>
#include <mersenne/proposer.h>
#include <mersenne/vars.h>
#include <mersenne/util.h>
#include <mersenne/fiber.h>
#include <mersenne/fiber_args.h>

struct bm_mask * get_trust(ME_P)
{
	struct bm_mask *bmp;
	struct me_peer *p;
	int nbits = HASH_COUNT(mctx->peers);
	bmp = malloc(bm_size(nbits));
	bm_init(bmp, nbits);

	for(p=mctx->peers; p != NULL; p=p->hh.next) {
		if(p->ack_ttl > 0)
			bm_set_bit(bmp, p->index, 1);
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

void lost_leadership(ME_P)
{
	puts("Lost Leadership :(");
	pro_stop(ME_A);
}

void gained_leadership(ME_P)
{
	puts("Gained Leadership :)");
	pro_start(ME_A);
}

void update_leader(ME_P_ int new_leader)
{
	int old_leader;
	old_leader = mctx->ldr.leader;
	if(old_leader != new_leader) {
		if(old_leader == mctx->me->index)
			lost_leadership(ME_A);
		else if(new_leader == mctx->me->index)
			gained_leadership(ME_A);
	}
	mctx->ldr.leader = new_leader;
	printf("R %d: Leader=%d\n", mctx->ldr.r, mctx->ldr.leader);
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
	free(data->me_leader_msg_data_u.ok.trust);
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

	printf("R %d: new round started\n", mctx->ldr.r);

	if(mctx->me->index != s % n)
		send_start(ME_A_ s, -1);
	mctx->ldr.r = s;
	update_leader(ME_A_ mctx->ldr.r % n);

	for(p=mctx->peers; p != NULL; p=p->hh.next) {
		p->ack_ttl = 3;
	}

	restart_timer(ME_A);
}

void do_msg_start(ME_P_ struct me_message *msg, struct me_peer *from)
{
	int k;
	struct me_leader_msg_data *data;

	data = &msg->me_message_u.leader_message.data;
	k = data->me_leader_msg_data_u.round.k;

#ifdef _LDR_DEBUG_MSG
	printf("R %d: Got START(%d) from peer #%d\n",
			mctx->ldr.r,
			k,
			from->index
	      );
#endif

	if(k > mctx->ldr.r)
		start_round(ME_A_ k);
	else if(k < mctx->ldr.r)
		send_start(ME_A_ mctx->ldr.r, from->index);

}

void do_msg_ok(ME_P_ struct me_message *msg, struct me_peer *from)
{
	int k;
	struct me_peer *p;
	struct me_leader_msg_data *data;
	char buf[512];

	data = &msg->me_message_u.leader_message.data;
	k = data->me_leader_msg_data_u.ok.k;

#ifdef _LDR_DEBUG_MSG
	bm_displayhex(buf, 512, data->me_leader_msg_data_u.ok.trust);
	printf("R %d: Got OK(%d) from peer #%d, trust: %s\n",
			mctx->ldr.r,
			k,
			from->index,
			buf
	      );
#endif

	if(k > mctx->ldr.r) {
		send_ack(ME_A_ k, k % HASH_COUNT(mctx->peers));
		start_round(ME_A_ k);
		return;
	} else if(k < mctx->ldr.r) {
		send_start(ME_A_ mctx->ldr.r, from->index);
		return;
	}

	if(!bm_get_bit(data->me_leader_msg_data_u.ok.trust, mctx->me->index)) {
		bm_displayhex(buf, 512, data->me_leader_msg_data_u.ok.trust);
		printf("Current leader does not trust me, trustmask: %s\n", buf);
		start_round(ME_A_ mctx->ldr.r + 1);
		return;
	}

	for(p=mctx->peers; p != NULL; p=p->hh.next) {
		if(bm_get_bit(data->me_leader_msg_data_u.ok.trust, p->index))
			p->ack_ttl = 3;
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

#ifdef _LDR_DEBUG_MSG
	printf("R %d: Got ACK(%d) from peer #%d\n",
			mctx->ldr.r,
			k,
			from->index
	      );
#endif

	if(k == mctx->ldr.r) {
		from->ack_ttl = 3;
	} else if(k < mctx->ldr.r) {
		send_start(ME_A_ mctx->ldr.r, from->index);
		return;
	} else
		warnx("got ACK from round higher than mine O.o");
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

	//printf("R %d: Leader=%d\n", mctx->ldr.r, mctx->ldr.leader);
	//FIXME: Without this line simulation works wierd

	mctx->ldr.delta_count++;
	if(mctx->ldr.delta_count > 2) {
		printf("Current leader timed out\n");
		start_round(ME_A_ mctx->ldr.r + 1);
	}
}

static void do_message(ME_P_ struct me_message *msg, struct me_peer *from)
{
	struct me_leader_msg_data *data;
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
			warnx("got unknown message from peer #%d\n",
					from->index);
			break;
	}
}

void ldr_fiber(ME_P)
{
	struct me_message *msg;
	struct me_peer *from;
	struct fbr_call_info *info;
	
	fbr_next_call_info(ME_A_ NULL);

	ev_timer_init(&mctx->ldr.delta_timer, timeout_cb, TIME_DELTA / 1000.,
			TIME_DELTA / 1000.);
	ev_timer_start(mctx->loop, &mctx->ldr.delta_timer);

	mctx->ldr.leader = -1;
	start_round(ME_A_ 0);

start:
	fbr_yield(ME_A);
	while(fbr_next_call_info(ME_A_ &info)) {
		assert(FAT_ME_MESSAGE == info->argv[0].i);
		msg = info->argv[1].v;
		from = info->argv[1].v;

		if(is_expired(msg)) {
			warnx("got expired message");
			continue;
		}
		if(!config_match(ME_A_ msg)) {
			warnx("sender configuration does not match mine, "
					"ignoring message");
			continue;
		}

		do_message(ME_A_ msg, from);
	}
	goto start;
}
