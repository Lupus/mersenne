#!/bin/bash
NUM=$1
if [ ! -d socks ] ; then
	mkdir socks
fi
export UNIX_SOCKET=./socks/$NUM
echo "set args $NUM" > gdb.init
gdb -x gdb.init ./build/mersenne
rm gdb.init
