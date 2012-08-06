set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

cmake_minimum_required(VERSION 2.6)
project(mersenne C)

include(CheckIncludeFiles)
include(CheckLibraryExists)
include(FindPkgConfig)

add_subdirectory(tools) 

MACRO(GENSTRENUM INFILE DEPS)
	GET_FILENAME_COMPONENT(SRCPATH "${INFILE}" PATH)
	GET_FILENAME_COMPONENT(SRCBASE "${INFILE}" NAME_WE)
	SET(OUTFILE1 "${CMAKE_CURRENT_BINARY_DIR}/${SRCPATH}/${SRCBASE}.strenum.h")
	SET(OUTFILE2 "${CMAKE_CURRENT_BINARY_DIR}/${SRCPATH}/${SRCBASE}.strenum.c")
	FILE(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/${SRCPATH}")
	ADD_CUSTOM_COMMAND(OUTPUT ${OUTFILE1} ${OUTFILE2}
		COMMAND "${CMAKE_CURRENT_BINARY_DIR}/tools/genstrenum"
		ARGS
			-I "${CMAKE_CURRENT_SOURCE_DIR}/include"
			-I "${CMAKE_CURRENT_BINARY_DIR}/include"
			"${CMAKE_CURRENT_SOURCE_DIR}/${INFILE}"
		DEPENDS ${INFILE}
		WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/${SRCPATH}"
		COMMENT "Generating strval_* functions from ${INFILE}"
		DEPENDS genstrenum ${DEPS}
		)
ENDMACRO(GENSTRENUM)

find_package(LibEV REQUIRED)
find_package(RpcGen REQUIRED)

include_directories(
	"${CMAKE_CURRENT_SOURCE_DIR}/include"
	"${CMAKE_CURRENT_BINARY_DIR}/include"
	)

set(CMAKE_C_FLAGS "-Wall -Werror")

GENSTRENUM(include/mersenne/proposer_prv.h include/mersenne/me_protocol.h)
RPCGEN_CREATE_XDR(include/mersenne/me_protocol.x)
set_source_files_properties(include/mersenne/me_protocol.c
	PROPERTIES COMPILE_FLAGS "-Wno-unused"
	)

aux_source_directory("${CMAKE_CURRENT_SOURCE_DIR}/src" MERSENNE_SOURCES)
aux_source_directory("${CMAKE_CURRENT_SOURCE_DIR}/include/mersenne" MERSENNE_HEADERS)
set(LIBCORO_SOURCES
	"${CMAKE_CURRENT_SOURCE_DIR}/3rdparty/coro.c"
	"${CMAKE_CURRENT_SOURCE_DIR}/include/coro.h"
	)
set(MERSENNE_GEN_SOURCES
	"include/mersenne/me_protocol.h"
	"include/mersenne/me_protocol.c"
	"include/mersenne/proposer_prv.strenum.h"
	"include/mersenne/proposer_prv.strenum.c"
	)

add_executable(mersenne
	${MERSENNE_SOURCES}
	${MERSENNE_HEADERS}
	${MERSENNE_GEN_SOURCES}
	${LIBCORO_SOURCES}
	)
target_link_libraries(mersenne ${LIBEV_LDFLAGS} crypto)

subdirs(testing)
