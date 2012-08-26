#!/bin/bash
NUM=$1
if [ ! -d socks ] ; then
	mkdir socks
fi
exec ./build/mersenne -s ./socks/$NUM -p $NUM --acceptor-storage-options "-i $NUM"
