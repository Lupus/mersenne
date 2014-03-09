set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

cmake_minimum_required(VERSION 2.6)
project(mersenne C)

set(CMAKE_EXPORT_COMPILE_COMMANDS 1)

option(USE_LTO "Use Link-Time Optimization" FALSE)
option(USE_ASAN "Use AddressSanitizer" FALSE)
option(LINK_ASAN_LIB "Link AddressSanitizer library (for gcc)" FALSE)
option(PERFLOG "Write performance log to stdout" FALSE)
if(USE_LTO)
	set(WANT_LTO TRUE)
endif()

add_subdirectory(evfibers)
add_subdirectory(msgpack-proto-ragel)

find_package(RpcGen REQUIRED)
find_package(GenGetOpt REQUIRED)
find_package(ProtoBufC REQUIRED)
find_package(Ragel REQUIRED)

add_definitions(-D_GNU_SOURCE)
if(PERFLOG)
	add_definitions(-DME_WANT_PERFLOG)
endif()

include_directories(
	"${CMAKE_CURRENT_SOURCE_DIR}/include"
	"${CMAKE_CURRENT_BINARY_DIR}/include"
	"${CMAKE_CURRENT_SOURCE_DIR}/evfibers/include"
	"${CMAKE_CURRENT_BINARY_DIR}/evfibers/include"
	"${CMAKE_CURRENT_SOURCE_DIR}/msgpack-proto-ragel/include"
	"${CMAKE_CURRENT_BINARY_DIR}/msgpack-proto-ragel/include"
	)

set(CMAKE_C_FLAGS "-Wall -fno-strict-aliasing")
if(USE_LTO)
	set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -flto -fwhole-program")
	set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -flto -fuse-linker-plugin -fwhole-program")
	set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -flto -fuse-linker-plugin -fwhole-program")
endif()
if(USE_ASAN)
	set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=address")
endif()

if(LINK_ASAN_LIB)
	set(ASAN_LIBS "asan")
else()
	set(ASAN_LIBS "")
endif()

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
set_source_files_properties("include/mersenne/me_protocol.c"
	PROPERTIES COMPILE_FLAGS "-Wno-unused"
	)

set(GENGETOPT_SOURCES "include/mersenne/cmdline.ggo")
ADD_GENGETOPT_FILES(${GENGETOPT_SOURCES} GGO_GEN_SOURCES)

aux_source_directory("${CMAKE_CURRENT_SOURCE_DIR}/include/mersenne" MERSENNE_HEADERS)
set(MERSENNE_SOURCES
	"src/context.c"
	"src/fiber_args.c"
	"src/xdr.c"
	"src/sharedmem.c"
	"src/bitmask.c"
	"src/acc_storage.c"
	"src/peers.c"
	"src/learner.c"
	"src/message.c"
	"src/paxos.c"
	"src/proposer.c"
	"src/util.c"
	"src/buffer.c"
	"src/client.c"
	"src/leader.c"
	"src/acceptor.c"
	"src/mersenne.c"
	)
ragel_add_include("${CMAKE_CURRENT_SOURCE_DIR}/msgpack-proto-ragel/include")
ragel_gen("src/proto.rl" RAGEL_CLI_GEN_SOURCES)
set(MERSENNE_CLI_SOURCES
	"src/proto.rl"
	${RAGEL_CLI_GEN_SOURCES}
	)
set(MERSENNE_GEN_SOURCES
	"include/mersenne/me_protocol.h"
	"include/mersenne/me_protocol.c"
	"${CMAKE_CURRENT_BINARY_DIR}/src/acc.pb-c.c"
	)

add_library(mersenne_cli SHARED
	${MERSENNE_CLI_SOURCES}
	)
target_link_libraries(mersenne_cli
	${MPPR_EMBED_LIBS})
add_executable(mersenne
	${MERSENNE_SOURCES}
	${MERSENNE_HEADERS}
	${MERSENNE_GEN_SOURCES}
	${GGO_GEN_SOURCES}
	)
target_link_libraries(mersenne
	mersenne_cli
	${EVFIBERS_EMBED_LIBS}
	${PROTOBUFC_LIBRARIES}
	${CMAKE_THREAD_LIBS_INIT}
	${ASAN_LIBS}
	crypto
	z
	uuid
	msgpack
	)

subdirs(testing)
