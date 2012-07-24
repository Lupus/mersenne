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
		ARGS -I ${CMAKE_CURRENT_SOURCE_DIR} -I ${CMAKE_CURRENT_BINARY_DIR}
			"${CMAKE_CURRENT_SOURCE_DIR}/${INFILE}"
		DEPENDS ${INFILE}
		WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
		COMMENT "Generating strval_* functions from ${INFILE}"
		DEPENDS genstrenum ${DEPS}
		)
ENDMACRO(GENSTRENUM)

find_package(LibEV REQUIRED)
find_package(RpcGen REQUIRED)

include_directories(${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR})

set(CMAKE_C_FLAGS "-Wall -Werror")

GENSTRENUM(proposer_prv.h me_protocol.h)
RPCGEN_CREATE_XDR(me_protocol.x)
set_source_files_properties( me_protocol.c PROPERTIES COMPILE_FLAGS "-Wno-unused" )

AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} MERSENNE_SOURCES)
SET(MERSENNE_GEN_SOURCES me_protocol.h me_protocol.c proposer_prv.strenum.h proposer_prv.strenum.c)

add_executable(mersenne ${MERSENNE_SOURCES} ${MERSENNE_GEN_SOURCES})
target_link_libraries(mersenne ev crypto bitmask)
