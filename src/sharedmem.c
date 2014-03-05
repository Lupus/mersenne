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
#include <sys/queue.h>
#include <sys/types.h>
#include <unistd.h>
#include <execinfo.h>
#include <string.h>
#include <err.h>
#include <valgrind/valgrind.h>
#include <zlib.h>
#include <mersenne/sharedmem.h>
#include <mersenne/kvec.h>
#include <mersenne/khash.h>

//#define SM_DEBUG

#define SM_MAGIC ((uint32_t)0xdeadbaba)

#ifdef SM_DEBUG
#define TRACE_SIZE 32

struct trace_info {
       void *array[TRACE_SIZE];
       size_t size;
};

enum action_type {
	SM_ACT_ALLOC,
	SM_ACT_IN_USE,
	SM_ACT_FREE,
};

struct alloc_history_item {
	struct trace_info tinfo;
	enum action_type action;
	int ref_count;
};
#endif

struct sm_block_info {
	uint32_t magic;
	uint64_t references;
	size_t size;
	sm_destructor_t destructor;
	void *destructor_context;
#ifdef SM_DEBUG
	SLIST_ENTRY(sm_block_info) entries;
	kvec_t(struct alloc_history_item) history;
#endif
};

#ifdef SM_DEBUG
SLIST_HEAD(sm_blocks, sm_block_info) sm_blocks =
		SLIST_HEAD_INITIALIZER(sm_blocks);

static void fill_trace_info(struct trace_info *info)
{
	info->size = backtrace(info->array, TRACE_SIZE);
}

static void print_trace_info(FILE *report, struct trace_info *info)
{
	size_t i;
	char **strings;
	strings = backtrace_symbols(info->array, info->size);
	for (i = 0; i < info->size; i++)
		fprintf(report, "  %s\n", strings[i]);
	free(strings);
}

static void record_action(struct sm_block_info *block, enum action_type action)
{
	struct alloc_history_item* item;
	item = kv_pushp(struct alloc_history_item, block->history);
	fill_trace_info(&item->tinfo);
	item->action = action;
	item->ref_count = block->references;
}

struct leak_case {
	struct sm_block_info *block;
	size_t occurances;
	uint32_t checksum;
};

int compare(const void* a, const void* b)
{
     uint32_t int_a = *((uint32_t *)a);
     uint32_t int_b = *((uint32_t *)b);

     if (int_a == int_b)
	    return 0;
     else if ( int_a < int_b )
	     return -1;
     else
	     return 1;
}

static uint32_t leak_case_hash(struct sm_block_info *block)
{
	uint32_t crc;
	struct alloc_history_item *item;
	crc = crc32(0L, Z_NULL, 0);
	int i;
	uint32_t *crcs = calloc(kv_size(block->history), sizeof(uint32_t));
	void **crc_ptrs = calloc(kv_size(block->history), sizeof(void *));
	for (i = 0; i < kv_size(block->history); i++) {
		item = &kv_A(block->history, i);
		crcs[i] = crc32(0L, Z_NULL, 0);
		crcs[i] = crc32(crcs[i], (void *)&item->action,
				sizeof(item->action));
		crcs[i] = crc32(crcs[i], (void *)item->tinfo.array,
				item->tinfo.size * sizeof(void *));
		crc_ptrs[i] = crcs + i;
	}
	qsort(crc_ptrs, kv_size(block->history), sizeof(uint32_t), compare);
	crc = crc32(crc, (void *)crcs,
			kv_size(block->history) * sizeof(uint32_t));
	return crc;
}

KHASH_MAP_INIT_INT(leaks, struct leak_case)

void register_case(khash_t(leaks) *h, struct sm_block_info *block)
{
	khiter_t k;
	int ret;
	struct leak_case *leak_case;
	uint32_t checksum = leak_case_hash(block);
	k = kh_put(leaks, h, checksum, &ret);
	switch (ret) {
	case -1: /* if the operation failed */
		errx(EXIT_FAILURE, "kh_put(leaks, ...) failed");
	case 0: /* if the key is present in the hash table */
		kh_value(h, k).occurances++;
		break;
	case 1: /* if the bucket is empty (never used) */
	case 2: /* if the element in the bucket has been deleted */
		leak_case = &kh_value(h, k);
		leak_case->block = block;
		leak_case->occurances = 1;
		leak_case->checksum = checksum;
		break;
	default:
		errx(EXIT_FAILURE, "unexpected ret from kh_put: %d", ret);
	};
}

void report_cases(FILE *report, khash_t(leaks) *h)
{
	khiter_t k;
	int i;
	int count = 0;
	struct leak_case *leak_case;
	struct sm_block_info *block;
	struct alloc_history_item *item;
	for (k = kh_begin(h); k != kh_end(h); k++) {
		if (!kh_exist(h, k))
		       continue;
		leak_case = &kh_value(h, k);
		block = leak_case->block;
		fprintf(report, "\n****** Leaked shared memory block #%d:\n",
				count++);
		fprintf(report, "\n       (Registered %zd occurances)\n",
				leak_case->occurances);
		for (i = 0; i < kv_size(block->history); i++) {
			item = &kv_A(block->history, i);
			switch (item->action) {
			case SM_ACT_ALLOC:
				fprintf(report, "\nAllocated (ref: 1):\n");
				break;
			case SM_ACT_IN_USE:
				fprintf(report, "\nReferenced (ref: %d):\n",
						item->ref_count);
				break;
			case SM_ACT_FREE:
				fprintf(report, "\nUnreferenced (ref: %d):\n",
						item->ref_count);
				break;
			}
			print_trace_info(report, &item->tinfo);
		}
	}
}

void sm_report_leaked()
{
	struct sm_block_info *block;
	FILE *report;
	char report_name[256];

	sprintf(report_name, "sm_leaks.%d", getpid());
	report = fopen(report_name, "w+");
	if (NULL == report)
		err(EXIT_FAILURE, "fopen");
	printf("Generating leak report into %s\n", report_name);
	khash_t(leaks) *h = kh_init(leaks);
	printf("Collecting leak cases...\n");
	SLIST_FOREACH(block, &sm_blocks, entries) {
		register_case(h, block);
	}
	printf("Done!\n");
	printf("Reporting leak cases...\n");
	report_cases(report, h);
	printf("Done!\n");
	kh_destroy(leaks, h);
	fclose(report);
	printf("Report has finished\n");
}
#else
void sm_report_leaked()
{
	/* SM_DEBUG is not defined, so reporting is disabled */
}
#endif

void *sm_alloc(size_t size)
{
	return sm_alloc_ext(size, NULL, NULL);
}

void *sm_alloc_ext(size_t size, sm_destructor_t destructor, void *context)
{
	struct sm_block_info *block;
	block = malloc(sizeof(struct sm_block_info) + size);
	block->magic = SM_MAGIC;
	block->references = 1;
	block->size = size;
	block->destructor = destructor;
	block->destructor_context = context;
#ifdef SM_DEBUG
	SLIST_INSERT_HEAD(&sm_blocks, block, entries);
	kv_init(block->history);
	record_action(block, SM_ACT_ALLOC);
#endif
	return block + 1;
}

void *sm_calloc(size_t nmemb, size_t size)
{
	return sm_calloc_ext(nmemb, size, NULL, NULL);
}

void *sm_calloc_ext(size_t nmemb, size_t size, sm_destructor_t destructor,
		void *context)
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

void *sm_in_use(void *ptr)
{
	struct sm_block_info *block;
	block = get_block(ptr);
	block->references++;
#ifdef SM_DEBUG
	record_action(block, SM_ACT_IN_USE);
#endif
	return ptr;
}

void sm_free(void *ptr)
{
	struct sm_block_info *block;
	if (NULL == ptr)
		return;
	block = get_block(ptr);
	block->references--;
#ifdef SM_DEBUG
	record_action(block, SM_ACT_FREE);
#endif
	if(0 == block->references) {
		if(NULL != block->destructor)
			block->destructor(block->destructor_context, ptr);
#ifdef SM_DEBUG
		kv_destroy(block->history);
		SLIST_REMOVE(&sm_blocks, block, sm_block_info, entries);
#endif
		memset(block, 0x00, sizeof(struct sm_block_info));
		free(block);
	}
}
