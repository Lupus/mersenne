CC = gcc
CFLAGS = -g -O0 -Wall -Werror -MMD -D_GNU_SOURCE
LDLIBS = -lev -lutil

all: simulate

simulate: simulate.o

clean:
	rm -f *.o
	rm -f simulate.o

.PHONY: all clean
