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
#include <err.h>
#include <stdlib.h>
#include <fcntl.h>
#include <stdio.h>
#include <mersenne/util.h>
#include <mersenne/context.h>

void make_socket_non_blocking(int fd)
{
	int flags, s;

	flags = fcntl(fd, F_GETFL, 0);
	if (-1 == flags)
		err(EXIT_FAILURE, "fcntl failed");

	flags |= O_NONBLOCK;
	s = fcntl(fd, F_SETFL, flags);
	if (-1 == s)
		errx(EXIT_FAILURE, "fcntl failed");
}

void buffer_ensure_writable(ME_P_ struct fbr_buffer *fb, size_t size)
{
	size_t current_size;
	int retval;
	if (fbr_buffer_can_write(&mctx->fbr, fb, size))
		return;
	current_size = fbr_buffer_size(&mctx->fbr, fb);
	retval = fbr_buffer_resize(&mctx->fbr, fb, current_size * 2);
	if (0 != retval)
		errx(EXIT_FAILURE, "failed to resize fbr buffer to %zd bytes",
				current_size * 2);
	fbr_log_i(&mctx->fbr, "buffer %p resized to %zd bytes", fb,
			current_size * 2);
}

void *find_majority_element(void *arr, size_t size, size_t el_size,
		int (*eq)(void *a, void *b)) {
	size_t count = 0;
	void *i, *majority_element;
	void *end = arr + ((size + 1) * el_size);
	for (i = arr; i < end; i+= el_size) {
		if (count == 0)
			majority_element = i;
		if (eq(i, majority_element))
			count++;
		else
			count--;
	}
	count = 0;
	for (i = arr; i < end; i+= el_size)
		if (eq(i, majority_element))
			count++;
	if (count > size / 2)
		return majority_element;
	else
		return NULL;
}
