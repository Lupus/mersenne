#!/bin/bash
NUM=$1
if [ ! -d socks ] ; then
	mkdir socks
fi
cgdb -- --args ./build/mersenne -s ./socks/$NUM -p $NUM --acceptor-wal-dir "acceptor$NUM"
