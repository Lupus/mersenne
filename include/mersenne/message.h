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

#ifndef _MESSAGE_H_
#define _MESSAGE_H_

#include <ev.h>

#include <mersenne/me_protocol.h>
#include <mersenne/context_fwd.h>

struct me_peer;

struct msg_info {
	struct me_message *msg;
	struct me_peer *from;
	struct buffer *buf;
	ev_tstamp received_ts;
};

enum msg_direction {
	MSG_DIR_RECEIVED = 0,
	MSG_DIR_SENT = 1,
};

void msg_send_to(ME_P_ struct me_message *msg, const int peer_num);
void msg_send_all(ME_P_ struct me_message *msg);
void msg_send_matching(ME_P_ struct me_message *msg, int (*predicate)(struct me_peer *, void *context), void *context);
void msg_dump(ME_P_ struct me_message *msg, enum msg_direction dir, struct sockaddr_in *addr);

#endif
