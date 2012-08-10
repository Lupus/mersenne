#!/bin/bash
NUM=$1
if [ ! -d socks ] ; then
	mkdir socks
fi
export UNIX_SOCKET=./socks/$NUM
exec ./build/mersenne $NUM
