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

#ifndef _MERSENNE_STATD_H_
#define _MERSENNE_STATD_H_

#include <mersenne/context.h>

void statd_init(ME_P_ const char *ip, int port);
void statd_send_keyval(ME_P_ const char *key, double value);
void statd_send_gauge(ME_P_ const char *key, double value);
void statd_send_gauge_delta(ME_P_ const char *key, double value);
void statd_send_timer(ME_P_ const char *key, double value);
void statd_send_counter(ME_P_ const char *key, double value);
void statd_send_unique(ME_P_ const char *key, const char *value);
void statd_send(ME_P_ const char *key, const char *value, const char *type,
		const char *flag);

#endif
