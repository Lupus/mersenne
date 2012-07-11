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

%#include <xdr.h>

enum me_message_supertype {
	ME_LEADER
};

enum me_message_type {
	ME_OMEGA_OK,
	ME_OMEGA_START,
	ME_OMEGA_ACK
};

struct me_leader_msg_header {
	int count;
	struct timeval sent;
	opaque config_checksum[36];
};

struct me_leader_msg_ok_data {
	int k;
	bitmask_ptr trust;
};

struct me_leader_msg_round_data {
	int k;
};

union me_leader_msg_data switch(me_message_type type) {
	case ME_OMEGA_START:
	case ME_OMEGA_ACK:
	struct me_leader_msg_round_data round;

	case ME_OMEGA_OK:
	struct me_leader_msg_ok_data ok;

	default:
	void;
};

struct me_leader_message {
	struct me_leader_msg_header header;
	struct me_leader_msg_data data;
};

union me_message switch(me_message_supertype super_type) {
	case ME_LEADER:
	struct me_leader_message leader_message;

	default:
	void;
};
