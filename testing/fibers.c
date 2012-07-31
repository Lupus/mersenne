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
#include <mersenne/fiber.h>
#include <mersenne/context.h>

void coro_io_test(ME_P)
{
	char buf[11] = {0};
	int fd = STDIN_FILENO;
	for(;;) {
		fbr_read(ME_A_ fd, buf, 10);
		fbr_write(ME_A_ STDOUT_FILENO, "read: ", 6);
		fbr_write(ME_A_ STDOUT_FILENO, buf, 10);
		fbr_write(ME_A_ STDOUT_FILENO, "\n", 1);
	}
}

void coro_test(ME_P)
{
	int i;
	struct fbr_fiber *io_fiber;

	io_fiber = fbr_create(ME_A_ coro_io_test);
	fbr_call(ME_A_ io_fiber, 0);

	for(i = 0;;i++) {
		printf("%d: %s\n", i, (char *)FBR_ARG_V(0));
		fbr_yield(ME_A);
	}
}


int main(int argc, char *argv[]) {
	struct me_context context = ME_CONTEXT_INITIALIZER;
	struct me_context *mctx = &context;
	struct fbr_fiber *fiber;

	context.loop = EV_DEFAULT;

	fbr_init(ME_A);
	fiber = fbr_create(ME_A_ coro_test);

	fbr_call(ME_A_ fiber, 1, fbr_arg_v("Hello, muthafucka!"));
	fbr_call(ME_A_ fiber, 1, fbr_arg_v("Hello, you fucker!"));
	fbr_call(ME_A_ fiber, 1, fbr_arg_v("Blah, blah, blah!"));


	ev_loop(context.loop, 0);

	return 0;
}
