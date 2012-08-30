#!/bin/bash
NUM=$1
if [ ! -d socks ] ; then
	mkdir socks
fi
echo -e "set args -s ./socks/$NUM -p $NUM --acceptor-storage-options '-i $NUM'\nfs next" > gdb.init
gdb --tui -x gdb.init ./build/mersenne
rm gdb.init
