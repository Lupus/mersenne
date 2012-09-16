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
#ifndef _MEM_CACHE_H_
#define _MEM_CACHE_H_

struct mc_cache;

struct mc_cache * mc_init_cache(size_t memb_size, size_t count_hint);
void mc_destroy_cache(struct mc_cache *cache);
void * mc_alloc(struct mc_cache *cache, size_t size);
void mc_free(void *ptr);

#endif
