#!/bin/bash
NUM=$1
exec ./build/mersenne -g -p $NUM --acceptor-wal-dir "acceptor$NUM"
