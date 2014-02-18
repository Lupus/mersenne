#!/bin/bash
NUM=$1
cgdb -- --args ./build/mersenne -p $NUM --acceptor-wal-dir "acceptor$NUM"
