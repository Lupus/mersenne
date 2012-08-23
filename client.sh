#!/bin/bash
export UNIX_SOCKET=./socks/$1
exec ./build/testing/strings_client $2 $3
