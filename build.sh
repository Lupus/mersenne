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
		cmake -DCMAKE_BUILD_TYPE=Debug ..
		;;
	clang-release)
		export LD=/usr/bin/clang
		export CC=/usr/bin/clang
		cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo ..
		;;
	gcc)
		export LD=/usr/bin/gcc
		export CC=/usr/bin/gcc
		cmake -DCMAKE_BUILD_TYPE=Debug ..
		;;
	gcc-release)
		export LD=/usr/bin/gcc
		export CC=/usr/bin/gcc
		cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo ..
		;;
	libevfibers)
		export LD=/usr/bin/clang
		export CC=/usr/bin/clang
		cmake -DCMAKE_BUILD_TYPE=Debug \
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
