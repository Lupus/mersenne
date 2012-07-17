CC = gcc
CFLAGS = -g -O0 -Wall -Werror -MMD -D_GNU_SOURCE
LDLIBS = -lclang

all: genstrenum

genstrenum: genstrenum.o

clean:
	rm -f *.o
	rm -f genstrenum.o

.PHONY: all clean
