#!/bin/bash
NUM=$1
mkdir -p acceptor$NUM/{snap,wal}
cgdb -- --args ./build/mersenne -p $NUM --acceptor-wal-dir "acceptor$NUM/wal" --acceptor-snap-dir "acceptor$NUM/snap"
