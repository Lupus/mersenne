set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "./cmake")

cmake_minimum_required(VERSION 2.6)
project(mersenne C)

include(CheckIncludeFiles)
include(CheckLibraryExists)
include(FindPkgConfig)

find_package(LibEV REQUIRED)
find_package(RpcGen REQUIRED)

set(CMAKE_C_FLAGS "-g -O0 -Wall -Werror")

RPCGEN_CREATE_XDR(me_protocol.x)
set_source_files_properties( me_protocol.c PROPERTIES COMPILE_FLAGS "-Wno-unused" )

add_executable(mersenne mersenne.c me_protocol.h me_protocol.c)
target_link_libraries(mersenne ev)
