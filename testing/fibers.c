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
#include <assert.h>
#include <evfibers/fiber.h>

void coro_io_test(FBR_P)
{
	char buf[11] = {0};
	int fd = STDIN_FILENO;
	for(;;) {
		fbr_read_all(FBR_A_ fd, buf, 10);
		fbr_write_all(FBR_A_ STDOUT_FILENO, "read: ", 6);
		fbr_write_all(FBR_A_ STDOUT_FILENO, buf, 10);
		fbr_write_all(FBR_A_ STDOUT_FILENO, "\n", 1);
	}
}

void coro_timer_test(FBR_P)
{
	int i = 0;
	struct fbr_call_info *info = NULL;
	struct fbr_fiber *printer;
	ev_tstamp timer_interval;
	const int buf_size = 256;
	char buf[buf_size];

	assert(1 == fbr_next_call_info(FBR_A_ &info));
	assert(1 == info->argc);
	timer_interval = *(ev_tstamp *)info->argv[0].v;
	printer = info->caller;
	for(;;) {
		snprintf(buf, buf_size, "Timer #%d\n", i++);
		fbr_call(FBR_A_ printer, 1, fbr_arg_v(buf));
		fbr_sleep(FBR_A_ timer_interval);
	}
}

void coro_test(FBR_P)
{
	int i;
	struct fbr_fiber *io_fiber;
	struct fbr_fiber *timer_fiber;
	struct fbr_call_info *info = NULL;
	ev_tstamp timer_interval = 5.5;

	io_fiber = fbr_create(FBR_A_ "io_test", coro_io_test);
	fbr_call(FBR_A_ io_fiber, 0);
	
	timer_fiber = fbr_create(FBR_A_ "timer_test", coro_timer_test);
	fbr_call(FBR_A_ timer_fiber, 1, fbr_arg_v(&timer_interval));

	for(i = 0;;i++) {
		while(fbr_next_call_info(FBR_A_ &info)) {
			printf("%d: %s\n", i++, (char *)info->argv[0].v);
			fbr_yield(FBR_A);
		}
	}
}

int main(int argc, char *argv[]) {
	struct fbr_context context;
	struct fbr_context *fctx = &context;
	struct fbr_fiber *fiber;
	context._opaque_context_data_[0] = 'x';

	fbr_init(FBR_A_ EV_DEFAULT);
	fiber = fbr_create(FBR_A_ "coro_test", coro_test);

	fbr_call(FBR_A_ fiber, 1, fbr_arg_v("Hello, muthafucka!"));
	fbr_call(FBR_A_ fiber, 1, fbr_arg_v("Hello, you fucker!"));
	fbr_call(FBR_A_ fiber, 1, fbr_arg_v("Blah, blah, blah!"));


	ev_loop(EV_DEFAULT, 0);

	return 0;
}
