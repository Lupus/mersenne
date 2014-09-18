#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <assert.h>
#include <err.h>
#include <sys/time.h>

static double now()
{
	struct timeval tv;
	int retval;
	retval = gettimeofday(&tv, NULL);
	if (retval)
		err(EXIT_FAILURE, "gettimeofday");
	return tv.tv_sec + tv.tv_usec / 1e6;
}

static void randomize_buffer(char *buf, size_t size)
{
	unsigned int i;


	for (i = 0; i < size; i++) {
		/* ASCII characters 33 to 126 */
		buf[i] = rand() % (126 - 33 + 1) + 33;
	}
}

int main(int argc, char **argv)
{
	assert(2 == argc);
	size_t blk_size;
	char *buf;
	FILE *f;
	int i;
	int iters;
	double t0, t1;
	ssize_t retval;
	f = fopen(argv[1], "w+");
	if (NULL == f)
		err(EXIT_FAILURE, "fopen");
	blk_size = 64 * 1400 + 64 * 20;
	buf = malloc(blk_size);
	if (NULL == buf)
		err(EXIT_FAILURE, "malloc");
	iters = 1000;
	for (i = 0; i < iters; i++) {
		randomize_buffer(buf, blk_size);
		t0 = now();
		retval = fwrite(buf, blk_size, 1, f);
		if (1 != retval)
			err(EXIT_FAILURE, "fwrite");
		retval = fdatasync(fileno(f));
		if (retval)
			err(EXIT_FAILURE, "fdatasync");
		t1 = now();
		printf("%f s% 20f b/s\n", t1 - t0, blk_size / (t1 - t0));
	}
	return 0;
}
