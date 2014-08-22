#!/bin/bash -x
FILE=$1
SIZE=$(du -b $FILE | cut -f1)
OFFSET=$(shuf -i 1000-$((SIZE-1000)) -n 1)
BYTES=1
dd if=/dev/urandom count=$BYTES bs=1 seek=$OFFSET of=$FILE conv=notrunc
