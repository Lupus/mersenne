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

#include <uthash.h>
#include <regex.h>
#include <err.h>

#include <mersenne/peers.h>
#include <mersenne/context.h>

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

struct me_peer *find_peer_by_acc_index(ME_P_ int acc_index)
{
	struct me_peer *p;

	for(p=mctx->peers; p != NULL; p=p->hh.next) {
		if(p->acc_index == acc_index)
			return p;
	}
	return NULL;
}

void delete_peer(ME_P_ struct me_peer *peer)
{
	HASH_DEL(mctx->peers, peer);
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
	int last_acc_index = 0;
	char *fgets_ret;

	peers_file = fopen("peers", "r");
	if (!peers_file)
		err(1, "fopen");
	if (regcomp(&rx_config_line, "^\\([[:digit:]]\\+\\.[[:digit:]]\\+\\.[[:digit:]]\\+\\.[[:digit:]]\\+\\)\\(\\:\\(a\\)\\)\\?", 0))
		err(EXIT_FAILURE, "%s", "regcomp failed");
	i = 0;
	while (1) {
		fgets_ret = fgets(line_buf, 255, peers_file);
		if (feof(peers_file))
			break;
		if (NULL == fgets_ret)
			err(EXIT_FAILURE, "fgets failed");
		p = malloc(sizeof(struct me_peer));
		memset(p, 0, sizeof(struct me_peer));
		p->index = i;

		retval = regexec(&rx_config_line, line_buf, 4, match, 0);
		if (retval) {
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
		if (0 == inet_aton(addr, &p->addr.sin_addr))
			errx(EXIT_FAILURE, "invalid address: %s", line_buf);
		p->addr.sin_port = htons(mctx->args_info.port_num_arg);
		if ('a' == flag[0])
			p->pxs.is_acceptor = 1;
		if (i == my_index) {
			mctx->me = p;
			fbr_log_i(&mctx->fbr, "My ip is %s", line_buf);
			if (p->pxs.is_acceptor)
				fbr_log_i(&mctx->fbr, "I am an acceptor");
		}
		if (p->pxs.is_acceptor)
			p->acc_index = last_acc_index++;
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

