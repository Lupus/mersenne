#!/bin/bash
NUM=$1
mkdir -p acceptor$NUM/db
cgdb -- --args ./build/mersenne -p $NUM --acceptor-db-dir "acceptor$NUM/db"
