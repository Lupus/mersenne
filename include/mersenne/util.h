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
#ifndef _UTIL_H_
#define _UTIL_H_

#include <evfibers/fiber.h>

#include <mersenne/context_fwd.h>

#define container_of(ptr, type, member) ({            \
 const typeof( ((type *)0)->member ) *__mptr = (ptr);    \
 (type *)( (char *)__mptr - offsetof(type,member) );})


#define max(a,b) \
   ({ __typeof__ (a) _a = (a); \
       __typeof__ (b) _b = (b); \
     _a > _b ? _a : _b; })

#define min(a,b) \
   ({ __typeof__ (a) _a = (a); \
       __typeof__ (b) _b = (b); \
     _a < _b ? _a : _b; })

#ifndef SLIST_FOREACH_SAFE
#define SLIST_FOREACH_SAFE(var, head, field, tvar)                       \
	for ((var) = SLIST_FIRST((head));                                \
			(var) && ((tvar) = SLIST_NEXT((var), field), 1); \
			(var) = (tvar))
#endif

static inline uint64_t round_down(uint64_t n, uint64_t m) {
	return n / m * m;
}

static inline uint64_t round_up(uint64_t n, uint64_t m) {
	return (n + m - 1) / m;
}

struct perf_snap {
	ev_tstamp start;
	ev_tstamp total;
	size_t encounters;
	const char *name;
};

void make_socket_non_blocking(int fd);
void buffer_ensure_writable(ME_P_ struct fbr_buffer *fb, size_t size);
void *find_majority_element(void *arr, size_t size, size_t el_size,
		int (*eq)(void *a, void *b));
void perf_snap_init(ME_P_ struct perf_snap *snap);
void perf_snap_start(ME_P_ struct perf_snap *snap);
void perf_snap_finish(ME_P_ struct perf_snap *snap);

void encode_varint(uint64_t value, uint8_t* output, uint8_t* output_size_ptr);
uint64_t decode_varint(uint8_t* input, uint8_t input_size);

#endif
