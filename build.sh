#!/bin/bash

if [ -d build ] ; then
	rm -rf build
fi
mkdir build

pushd build

case $1 in
	clang)
		export LD=/usr/bin/clang
		export CC=/usr/bin/clang
		cmake -DUSE_PROFILER=NO -DCMAKE_BUILD_TYPE=Debug ..
		;;
	clang-prof)
		export LD=/usr/bin/clang
		export CC=/usr/bin/clang
		cmake -DUSE_PROFILER=YES -DCMAKE_BUILD_TYPE=Debug ..
		;;
	gcc)
		export LD=/usr/bin/gcc
		export CC=/usr/bin/gcc
		cmake -DUSE_PROFILER=NO -DCMAKE_BUILD_TYPE=Debug ..
		;;
	gcc-prof)
		export LD=/usr/bin/gcc
		export CC=/usr/bin/gcc
		cmake -DUSE_PROFILER=YES -DCMAKE_BUILD_TYPE=Debug ..
		;;
	libevfibers)
		export LD=/usr/bin/clang
		export CC=/usr/bin/clang
		cmake -DUSE_PROFILER=YES \
			-DCMAKE_BUILD_TYPE=Debug \
			-DLIBEVFIBERS_INCLUDE_PATH=~/git/libevfibers/include \
			-DLIBEVFIBERS_LIBDIR=~/git/libevfibers/build \
			..
		;;
	*)
		echo "Please enter valid build type"
		;;
esac
make

popd
