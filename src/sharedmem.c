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
#include <stdint.h>
#include <string.h>
#include <valgrind/valgrind.h>
#include <mersenne/sharedmem.h>

#define SM_MAGIC ((uint32_t)0xdeadbaba)

struct sm_block_info {
	uint32_t magic;
	uint64_t references;
	size_t size;
	sm_destructor_t destructor;
	void *destructor_context;
};

void * sm_alloc(size_t size)
{
	return sm_alloc_ext(size, NULL, NULL);
}

void * sm_alloc_ext(size_t size, sm_destructor_t destructor, void *context)
{
	struct sm_block_info *block;
	block = malloc(sizeof(struct sm_block_info) + size);
	block->magic = SM_MAGIC;
	block->references = 1;
	block->size = size;
	block->destructor = destructor;
	block->destructor_context = context;
	return block + 1;
}

void * sm_calloc(size_t nmemb, size_t size)
{
	return sm_calloc_ext(nmemb, size, NULL, NULL);
}

void * sm_calloc_ext(size_t nmemb, size_t size, sm_destructor_t destructor, void *context)
{
	void *ptr = sm_alloc_ext(nmemb * size, destructor, context);
	memset(ptr, 0x00, nmemb * size);
	return ptr;
}

static struct sm_block_info *get_block(void *ptr)
{
	struct sm_block_info *block;
	block = (struct sm_block_info *)ptr - 1;
	if(SM_MAGIC != block->magic) {
		fprintf(stderr, "sm: 0x%lu is not a shared memory block\n",
				(unsigned long)ptr);
		if(!RUNNING_ON_VALGRIND)
			abort();
	}
	return block;
}

void sm_set_destructor(void *ptr, sm_destructor_t destructor, void *context)
{
	struct sm_block_info *block;
	block = get_block(ptr);
	block->destructor = destructor;
	block->destructor_context = context;
}

size_t sm_size(void *ptr)
{
	return get_block(ptr)->size;
}

void * sm_in_use(void *ptr)
{
	struct sm_block_info *block;
	block = get_block(ptr);
	block->references++;
	return ptr;
}

void sm_free(void *ptr)
{
	struct sm_block_info *block;
	if (NULL == ptr)
		return;
	block = get_block(ptr);
	block->references--;
	if(0 == block->references) {
		if(NULL != block->destructor)
			block->destructor(block->destructor_context, ptr);
		memset(block, 0x00, sizeof(struct sm_block_info));
		free(block);
	}
}
