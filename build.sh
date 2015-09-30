#!/bin/bash

if [ -d build ] ; then
	rm -rf build
fi
mkdir build

pushd build

case $1 in
	clang)
		export LD=/usr/local/bin/clang
		export CC=/usr/local/bin/clang
		cmake -DCMAKE_BUILD_TYPE=Debug -DUSE_ASAN=TRUE ..
		;;
	clang-release)
		export LD=/usr/local/bin/clang
		export CC=/usr/local/bin/clang
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
	clang-analyze)
		scan-build -o ./scan-build-report-cmake cmake -DCMAKE_BUILD_TYPE=Debug ..
		scan-build -o ./scan-build-report make
		popd
		exit 0
		;;
	*)
		echo "Please enter valid build type"
		;;
esac
make

popd
