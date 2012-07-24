release:
	if [ ! -d build ] ; then mkdir build ; fi
	cd build && cmake -DCMAKE_BUILD_TYPE=Release .. && make

debug:
	if [ ! -d build ] ; then mkdir build ; fi
	cd build && cmake -DCMAKE_BUILD_TYPE=Debug .. && make

clean:
	rm -rf build
