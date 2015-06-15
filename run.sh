#!/bin/bash
export ASAN_SYMBOLIZER_PATH="/usr/local/bin/llvm-symbolizer"
export ASAN_OPTIONS="symbolize=1:detect_leaks=1:report_objects=1:fast_unwind_on_malloc=0:detect_stack_use_after_return=1:sleep_before_dying=600"
NUM=$1
mkdir -p acceptor$NUM/db
exec ./build/mersenne -g -p $NUM --acceptor-db-dir "acceptor$NUM/db"
