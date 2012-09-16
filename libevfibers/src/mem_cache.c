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
#include <sys/mman.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <utlist.h>
#include <err.h>
#include <valgrind/valgrind.h>
#include <stdio.h>
#include <evfibers_private/mem_cache.h>

struct mc_data_header {
	struct mc_slab *slab;
} __attribute__ ((__packed__));

struct mc_free {
	struct mc_free *next;
} __attribute__ ((__packed__));


struct mc_slab {
	struct mc_free *free_list;
	struct mc_slab *next;
	struct mc_cache *cache;
	size_t data_size;
};

struct mc_cache {
	size_t memb_size;
	struct mc_slab *full;
	struct mc_slab *partial;
	struct mc_slab *free;
	size_t count_hint;
	long sz;
	size_t allocated;
};

struct mc_cache * mc_init_cache(size_t memb_size, size_t count_hint)
{
	struct mc_cache *cache;
	cache = malloc(sizeof(struct mc_cache));
	memset(cache, 0x00, sizeof(struct mc_cache));
	cache->memb_size = memb_size;
	cache->count_hint = count_hint;
	cache->sz = sysconf(_SC_PAGESIZE);
	return cache;
}

void mc_destroy_cache(struct mc_cache *cache)
{
}

static size_t round_up_to_page_size(size_t size, struct mc_cache *cache)
{
	size_t remainder = size % cache->sz;
	if(remainder == 0)
		return size;
	return size + cache->sz - remainder;
}

static void init_slab_data(struct mc_slab *slab)
{
	size_t hdr_size = sizeof(struct mc_data_header);
	size_t memb_size = slab->cache->memb_size;
	size_t block_size = hdr_size + memb_size;
	struct mc_free *free_block;
	char *data = (char *)(slab + 1);
	char *ptr;
	for(ptr = data; ptr < data + slab->data_size; ptr += block_size) {
		if(ptr + block_size > data + slab->data_size)
			break;
		((struct mc_data_header *)ptr)->slab = slab;
		free_block = (struct mc_free *)
			(ptr + sizeof(struct mc_data_header *));
		LL_PREPEND(slab->free_list, free_block);
	}
}

static void allocate_new_slab(struct mc_cache *cache)
{
	struct mc_slab *slab;
	size_t size;
	size = cache->memb_size + sizeof(struct mc_data_header);
	size *= cache->count_hint;
	size += sizeof(struct mc_slab);
	size = round_up_to_page_size(size, cache);
	slab = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
	if(MAP_FAILED == slab)
		err(EXIT_FAILURE, "mmap failed");
	slab->cache = cache;
	slab->data_size = size - sizeof(struct mc_slab);
	init_slab_data(slab);
	LL_PREPEND(cache->free, slab);
	fprintf(stderr, "new slab allocated\n");
}

void * mc_alloc(struct mc_cache *cache, size_t size)
{
	struct mc_slab *slab;
	void *ptr;
	struct mc_free *free_block;
	if(cache->partial) {
		slab = cache->partial;
		free_block = slab->free_list;
		LL_DELETE(slab->free_list, free_block);
		ptr = free_block;
		if(NULL == slab->free_list) {
			LL_DELETE(cache->partial, slab);
			LL_PREPEND(cache->full, slab);
		}
	} else if(cache->free) {
use_free_slab:
		slab = cache->free;
		free_block = slab->free_list;
		LL_DELETE(slab->free_list, free_block);
		ptr = free_block;
		LL_DELETE(cache->free, slab);
		LL_PREPEND(cache->partial, slab);
	} else {
		allocate_new_slab(cache);
		goto use_free_slab;
	}
	VALGRIND_MALLOCLIKE_BLOCK(ptr, size, 0, 0);
	//fprintf(stderr, "allocated %lu objects\n", ++cache->allocated);

	return ptr;
}

void mc_free(void *ptr)
{
	struct mc_slab *slab;
	slab = ((struct mc_data_header *)ptr - 1)->slab;
	if(NULL == slab->free_list) {
		LL_DELETE(slab->cache->full, slab);
		LL_PREPEND(slab->cache->partial, slab);
	}
	struct mc_free *free_block = ptr;
	LL_PREPEND(slab->free_list, free_block);
	slab->cache->allocated--;
	VALGRIND_FREELIKE_BLOCK(ptr, 0);
}
