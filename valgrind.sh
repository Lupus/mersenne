#!/bin/bash
exec valgrind --leak-check=full --track-origins=yes --show-possibly-lost=yes --track-fds=yes --log-file=valgrind.$1.log ./run.sh $1
