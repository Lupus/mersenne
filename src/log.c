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
#include <stdio.h>
#include <stdlib.h>
#include <mersenne/log.h>

enum log_level current_level;

static const char * level_to_string(enum log_level level)
{
	switch(level) {
		case LL_EMERG: return "EMERG";
		case LL_ALERT: return "ALERT";
		case LL_CRIT: return "CRIT";
		case LL_ERR: return "ERR";
		case LL_WARNING: return "WARNING";
		case LL_NOTICE: return "NOTICE";
		case LL_INFO: return "INFO";
		case LL_DEBUG: return "DEBUG";
	}
}

void log_set_level(enum log_level level)
{
	current_level = level;
}

void log(enum log_level level, const char *format, ...)
{
	va_list ap;
	if(level > current_level)
		return;
	printf("[%s] ", level_to_string(level));
	va_start(ap, format);
	vprintf(format, ap);
	va_end(ap);
}

void log_abort()
{
	printf("Aborting...\n");
	fflush(stdout);
	abort();
}

int log_level_match(enum log_level level)
{
	if(level > current_level)
		return 0;
	return 1;
}
