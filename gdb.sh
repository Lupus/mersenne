#!/bin/bash
NUM=$1
if [ ! -d socks ] ; then
	mkdir socks
fi
echo -e "set args -s ./socks/$NUM -p $NUM --acceptor-wal-dir "acceptor$NUM"" > gdb.init
gdb -x gdb.init ./build/mersenne
rm gdb.init
