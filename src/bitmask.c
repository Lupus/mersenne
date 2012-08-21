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

#include <assert.h>
#include <string.h>
#include <stdint.h>
#include <stdio.h>

#include <mersenne/bitmask.h>
#include <mersenne/util.h>

#define BITS_PER_LONG (8 * sizeof(unsigned long))
#define HOW_MANY(x,y) (((x)+((y)-1))/(y))
#define LONGS_PER_BITS(n) HOW_MANY(n, BITS_PER_LONG)
#define HEX_CHUNK_SIZE 32
#define HEX_CHAR_SIZE 8

unsigned int bm_size(unsigned int n)
{
	return sizeof(struct bm_mask) +
		(LONGS_PER_BITS(n) - 1) * sizeof(unsigned long);
}

void bm_set_all(struct bm_mask *mask)
{
	memset(&mask->mask, 0xff, LONGS_PER_BITS(mask->size));
}

void bm_clear_all(struct bm_mask *mask)
{
	memset(&mask->mask, 0x00, LONGS_PER_BITS(mask->size));
}

void bm_init(struct bm_mask *mask, unsigned int nbits)
{
	mask->size = LONGS_PER_BITS(nbits);
	mask->nbits = nbits;
	bm_clear_all(mask);
}

unsigned int bm_get_bit(const struct bm_mask *mask, unsigned int n)
{
	if(n >= mask->nbits)
		return 0;
	return (mask->mask[n/BITS_PER_LONG] >> (n % BITS_PER_LONG)) & 1;
}

void bm_set_bit(struct bm_mask *mask, unsigned int n, unsigned int v)
{
	assert(n < mask->nbits);
	if(v)
		mask->mask[n/BITS_PER_LONG] |= 1UL << (n % BITS_PER_LONG);
	else
		mask->mask[n/BITS_PER_LONG] &= ~(1UL << (n % BITS_PER_LONG));
}

unsigned int bm_hweight(struct bm_mask *mask)
{
	int i;
	unsigned int w = 0;
	for (i = 0; i < mask->nbits; i++)
		if(bm_get_bit(mask, i))
			w++;
	return w;
}

int bm_displayhex(char *buf, int buflen, const struct bm_mask *bmp)
{
	int chunk;
	int cnt = 0;
	const char *sep = "";

	if (buflen < 1)
		return 0;
	buf[0] = 0;

	for (chunk = HOW_MANY(bmp->size, HEX_CHUNK_SIZE) - 1; chunk >= 0; chunk--) {
		uint32_t val = 0;
		int bit;

		for (bit = HEX_CHUNK_SIZE - 1; bit >= 0; bit--)
			val = val << 1 | bm_get_bit(bmp, chunk * HEX_CHUNK_SIZE + bit);
		cnt += snprintf(buf + cnt, max(buflen - cnt, 0), "%s%0*x",
			sep, HEX_CHAR_SIZE, val);
		sep = ",";
	}
	return cnt;
}

