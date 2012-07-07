set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

cmake_minimum_required(VERSION 2.6)
project(mersenne C)

include(CheckIncludeFiles)
include(CheckLibraryExists)
include(FindPkgConfig)

find_package(LibEV REQUIRED)
find_package(RpcGen REQUIRED)

include_directories(${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR})

set(CMAKE_C_FLAGS "-Wall -Werror")

RPCGEN_CREATE_XDR(me_protocol.x)
set_source_files_properties( me_protocol.c PROPERTIES COMPILE_FLAGS "-Wno-unused" )

add_executable(mersenne
	mersenne.c
	context.h
	context.c
	leader.h
	leader.c
	peers.h
	peers.c
	util.h
	util.c
	xdr.h
	xdr.c
	me_protocol.h
	me_protocol.c
	vars.h
)
target_link_libraries(mersenne ev crypto bitmask)
