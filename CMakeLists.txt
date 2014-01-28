set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

cmake_minimum_required(VERSION 2.6)
project(mersenne C)

set(CMAKE_EXPORT_COMPILE_COMMANDS 1)

include(CheckIncludeFiles)
include(CheckLibraryExists)
include(FindPkgConfig)

find_package(LibEV REQUIRED)
find_package(LibEVfibers REQUIRED)
find_package(RpcGen REQUIRED)
find_package(GenGetOpt REQUIRED)
find_package(ProtoBufC REQUIRED)
find_package(Threads REQUIRED)

add_definitions(-D_GNU_SOURCE)

include_directories(
	"${CMAKE_CURRENT_SOURCE_DIR}/include"
	"${CMAKE_CURRENT_BINARY_DIR}/include"
	"${CMAKE_CURRENT_SOURCE_DIR}/libevfibers/include"
	"${CMAKE_CURRENT_BINARY_DIR}/libevfibers/include"
	)

set(CMAKE_C_FLAGS "-Wall -Werror")
set(MERSENNE_PROTO "src/acc.proto")
add_custom_command(
	OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/src/acc.pb-c.c"
	       "${CMAKE_CURRENT_BINARY_DIR}/include/mersenne/acc.pb-c.h"
	COMMAND mkdir -p "${CMAKE_CURRENT_BINARY_DIR}/include/mersenne"
	COMMAND mkdir -p "${CMAKE_CURRENT_BINARY_DIR}/src"
	COMMAND protoc-c -I "${CMAKE_CURRENT_SOURCE_DIR}/src" "${CMAKE_CURRENT_SOURCE_DIR}/${MERSENNE_PROTO}" --c_out "${CMAKE_CURRENT_BINARY_DIR}/src"
	COMMAND cp "${CMAKE_CURRENT_BINARY_DIR}/src/acc.pb-c.h" "${CMAKE_CURRENT_BINARY_DIR}/include/mersenne/acc.pb-c.h"
	DEPENDS ${MERSENNE_PROTO}
	COMMENT "Generating Protobuf-C from ${MERSENNE_PROTO}"
	)

RPCGEN_CREATE_XDR("include/mersenne/me_protocol.x")
RPCGEN_CREATE_XDR("include/mersenne/cl_protocol.x")
set_source_files_properties("include/mersenne/me_protocol.c"
	PROPERTIES COMPILE_FLAGS "-Wno-unused"
	)
set_source_files_properties("include/mersenne/cl_protocol.c"
	PROPERTIES COMPILE_FLAGS "-Wno-unused"
	)

set(GENGETOPT_SOURCES "include/mersenne/cmdline.ggo")
ADD_GENGETOPT_FILES(${GENGETOPT_SOURCES} GGO_GEN_SOURCES)

aux_source_directory("${CMAKE_CURRENT_SOURCE_DIR}/src" MERSENNE_SOURCES)
aux_source_directory("${CMAKE_CURRENT_SOURCE_DIR}/include/mersenne" MERSENNE_HEADERS)
set(MERSENNE_GEN_SOURCES
	"include/mersenne/me_protocol.h"
	"include/mersenne/me_protocol.c"
	"include/mersenne/cl_protocol.h"
	"include/mersenne/cl_protocol.c"
	"${CMAKE_CURRENT_BINARY_DIR}/src/acc.pb-c.c"
	)

add_executable(mersenne
	${MERSENNE_SOURCES}
	${MERSENNE_HEADERS}
	${MERSENNE_GEN_SOURCES}
	${GENGETOPT_SOURCES}
	${GGO_GEN_SOURCES}
	${LIBCORO_SOURCES}
	)
target_link_libraries(mersenne
	${LIBEVFIBERS_LDFLAGS}
	${LIBEV_LDFLAGS}
	${PROTOBUFC_LIBRARIES}
	${CMAKE_THREAD_LIBS_INIT}
	crypto
	)

subdirs(testing)
