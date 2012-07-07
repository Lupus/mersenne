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

#ifndef _PEERS_H_
#define _PEERS_H_

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <uthash.h>
#include <context_fwd.h>

struct sockaddr_in;

struct me_peer {
	int index;
	struct sockaddr_in addr;
	int ack_ttl;

	UT_hash_handle hh;
};

void add_peer(ME_P_ struct me_peer *p);
struct me_peer *find_peer(ME_P_ struct sockaddr_in *addr);
struct me_peer *find_peer_by_index(ME_P_ int index);
void delete_peer(ME_P_ struct me_peer *peer);
void load_peer_list(ME_P_ int my_index);

#endif
