#!/bin/bash
NUM=$1
mkdir -p acceptor$NUM/db
cgdb -- --args ./build/mersenne -p $NUM --db-dir "state$NUM"
