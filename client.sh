#!/bin/bash
export UNIX_SOCKET=./socks/$1
cat /dev/urandom | base64 | head -n $2 | ./build/testing/strings_client
