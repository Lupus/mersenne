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
#ifndef _LOG_H_
#define _LOG_H_

enum log_level {
	LL_EMERG = 0,
	LL_ALERT,
	LL_CRIT,
	LL_ERR,
	LL_WARNING,
	LL_NOTICE,
	LL_INFO,
	LL_DEBUG
};

void log_set_level(enum log_level level);
void log(enum log_level level, const char *format, ...) __attribute__ ((format (printf, 2, 3)));
void log_abort();
int log_level_match(enum log_level level);

#endif
