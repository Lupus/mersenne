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

#ifndef _TRACE_PRIVATE_H_
#define _TRACE_PRIVATE_H_

#define TRACE_SIZE 16

struct trace_info {
       void *array[TRACE_SIZE];
       size_t size;
};

void fill_trace_info(struct trace_info *info);
void print_trace_info(struct trace_info *info);

#endif


