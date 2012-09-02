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

#define __MTU     1500
#define __UDP_HEADER_SIZE 28
%#define ME_UDP_HEADER_SIZE __UDP_HEADER_SIZE

#define __MAX_XDR_MESSAGE_LEN (__MTU - __UDP_HEADER_SIZE - 8)
%#define ME_MAX_XDR_MESSAGE_LEN __MAX_XDR_MESSAGE_LEN

%#include <openssl/evp.h>
%#include <mersenne/xdr.h>

enum me_message_supertype {
	ME_LEADER,
	ME_PAXOS
};

enum me_leader_message_type {
	ME_LEADER_OK,
	ME_LEADER_START,
	ME_LEADER_ACK
};

struct me_leader_msg_header {
	int count;
	struct timeval sent;
	opaque config_checksum[EVP_MAX_MD_SIZE];
};

struct me_leader_msg_ok_data {
	int k;
	bm_mask_ptr trust;
};

struct me_leader_msg_round_data {
	int k;
};

union me_leader_msg_data switch(me_leader_message_type type) {
	case ME_LEADER_START:
	case ME_LEADER_ACK:
	struct me_leader_msg_round_data round;

	case ME_LEADER_OK:
	struct me_leader_msg_ok_data ok;

	default:
	void;
};

struct me_leader_message {
	struct me_leader_msg_header header;
	struct me_leader_msg_data data;
};

enum me_paxos_message_type {
	ME_PAXOS_PREPARE,
	ME_PAXOS_PROMISE,
	ME_PAXOS_ACCEPT,
	ME_PAXOS_REJECT,
	ME_PAXOS_LAST_ACCEPTED,
	ME_PAXOS_LEARN,
	ME_PAXOS_RETRANSMIT,
	ME_PAXOS_CLIENT_VALUE
};

struct me_paxos_prepare_data {
	uint64_t i;
	uint64_t b;
};

struct me_paxos_promise_data {
	uint64_t i;
	uint64_t b;
	struct buffer *v;
	uint64_t vb;
};

struct me_paxos_accept_data {
	uint64_t i;
	uint64_t b;
	struct buffer *v;
};

struct me_paxos_reject_data {
	uint64_t i;
	uint64_t b;
};

struct me_paxos_last_accepted_data {
	uint64_t i;
};

struct me_paxos_learn_data {
	uint64_t i;
	uint64_t b;
	struct buffer *v;
};

struct me_paxos_retransmit_data {
	uint64_t from;
	uint64_t to;
};

struct me_paxos_client_value_data {
	struct buffer *v;
};

union me_paxos_msg_data switch(me_paxos_message_type type) {
	case ME_PAXOS_PREPARE:
	struct me_paxos_prepare_data prepare;
	case ME_PAXOS_PROMISE:
	struct me_paxos_promise_data promise;
	case ME_PAXOS_ACCEPT:
	struct me_paxos_accept_data accept;
	case ME_PAXOS_REJECT:
	struct me_paxos_reject_data reject;
	case ME_PAXOS_LAST_ACCEPTED:
	struct me_paxos_last_accepted_data last_accepted;
	case ME_PAXOS_LEARN:
	struct me_paxos_learn_data learn;
	case ME_PAXOS_RETRANSMIT:
	struct me_paxos_retransmit_data retransmit;
	case ME_PAXOS_CLIENT_VALUE:
	struct me_paxos_client_value_data client_value;

	default:
	void;
};

struct me_paxos_message {
	struct me_paxos_msg_data data;
};

union me_message switch(me_message_supertype super_type) {
	case ME_LEADER:
	struct me_leader_message leader_message;
	case ME_PAXOS:
	struct me_paxos_message paxos_message;

	default:
	void;
};
