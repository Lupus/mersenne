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

#ifndef _BITMASK_H_
#define _BITMASK_H_

struct bm_mask {
	unsigned int size;
	unsigned int nbits;
	unsigned long mask[1];
};

unsigned int bm_size(unsigned int nbits);
void bm_set_all(struct bm_mask *mask);
void bm_clear_all(struct bm_mask *mask);
void bm_init(struct bm_mask *mask, unsigned int nbits);
unsigned int bm_get_bit(const struct bm_mask *mask, unsigned int n);
void bm_set_bit(struct bm_mask *mask, unsigned int n, unsigned int v);
unsigned int bm_hweight(struct bm_mask *mask);
int bm_displayhex(char *buf, int buflen, const struct bm_mask *bmp);
int bm_ffs(struct bm_mask *mask);

#endif
