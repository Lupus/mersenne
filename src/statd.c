/********************************************************************

  Copyright 2014 Konstantin Olkhovskiy <lupus@oxnull.net>

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

#include <mersenne/statd.h>

void statd_init(ME_P_ const char *ip, int port)
{
	mctx->statd_addr.sin_port = htons(port);
	if (0 == inet_aton(ip, &mctx->statd_addr.sin_addr))
		errx(EXIT_FAILURE, "invalid statd address: %s", ip);
	mctx->use_statd = 1;
}

void statd_send_keyval(ME_P_ const char *key, double value)
{
	char buf[32];
	int rv;
	if (!mctx->use_statd)
		return;
	rv = snprintf(buf, sizeof(buf), "%f", value);
	assert(rv > 0 && rv < sizeof(buf));
	statd_send(ME_A_ key, buf, "kv", NULL);
}

void statd_send_gauge(ME_P_ const char *key, double value)
{
	char buf[32];
	int rv;
	if (!mctx->use_statd)
		return;
	assert(value >= 0);
	rv = snprintf(buf, sizeof(buf), "%f", value);
	assert(rv > 0 && rv < sizeof(buf));
	statd_send(ME_A_ key, buf, "g", NULL);
}

void statd_send_gauge_delta(ME_P_ const char *key, double value)
{
	char buf[32];
	int rv;
	if (!mctx->use_statd)
		return;
	rv = snprintf(buf, sizeof(buf), "%+f", value);
	assert(rv > 0 && rv < sizeof(buf));
	statd_send(ME_A_ key, buf, "g", NULL);
}

void statd_send_timer(ME_P_ const char *key, double value)
{
	char buf[32];
	int rv;
	if (!mctx->use_statd)
		return;
	rv = snprintf(buf, sizeof(buf), "%f", value);
	assert(rv > 0 && rv < sizeof(buf));
	statd_send(ME_A_ key, buf, "ms", NULL);
}

void statd_send_counter(ME_P_ const char *key, double value)
{
	char buf[32];
	int rv;
	if (!mctx->use_statd)
		return;
	rv = snprintf(buf, sizeof(buf), "%f", value);
	assert(rv > 0 && rv < sizeof(buf));
	statd_send(ME_A_ key, buf, "c", NULL);
}

void statd_send_unique(ME_P_ const char *key, const char *value)
{
	if (!mctx->use_statd)
		return;
	statd_send(ME_A_ key, value, "s", NULL);
}

void statd_send(ME_P_ const char *key, const char *value, const char *type,
		const char *flag)
{
	char buf[1024];
	int rv;
	ssize_t retval;
	if (!mctx->use_statd)
		return;
	if (flag)
		rv = snprintf(buf, sizeof(buf), "mersenne.%s:%s|%s|@%s\n", key,
				value, type, flag);
	else
		rv = snprintf(buf, sizeof(buf), "mersenne.%s:%s|%s\n", key,
				value, type);
	assert(rv > 0 && rv < sizeof(buf));
	retval = fbr_sendto(&mctx->fbr, mctx->fd, buf, rv, 0,
			(struct sockaddr *)&mctx->statd_addr,
			sizeof(mctx->statd_addr));
	if (-1 == retval) {
		fbr_log_n(&mctx->fbr, "statd message got truncated from %d to"
					" %zd while sending", rv, retval);
		return;
	}
	if (retval < rv)
		fbr_log_n(&mctx->fbr, "statd message got truncated from %d to"
					" %zd while sending", rv, retval);
}
