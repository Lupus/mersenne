set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

cmake_minimum_required(VERSION 2.6)
project(mersenne C CXX)

set(CMAKE_EXPORT_COMPILE_COMMANDS 1)
set(CMAKE_C_LINK_EXECUTABLE ${CMAKE_CXX_LINK_EXECUTABLE})

option(USE_LTO "Use Link-Time Optimization" FALSE)
option(USE_ASAN "Use AddressSanitizer" FALSE)
option(PERFLOG "Write performance log to stdout" FALSE)
if(USE_LTO)
	set(WANT_LTO TRUE)
endif()

if(USE_ASAN)
	set(WANT_ASAN TRUE)
endif()
add_subdirectory(evfibers)
add_subdirectory(msgpack-proto-ragel)

find_package(RpcGen REQUIRED)
find_package(GenGetOpt REQUIRED)
find_package(ProtoBufC REQUIRED)
find_package(Ragel REQUIRED)
#find_package(LDB REQUIRED)

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
	${ROCKSDB_INCLUDE_DIR}
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

RPCGEN_CREATE_XDR("include/mersenne/me_protocol.x")
set_source_files_properties("include/mersenne/me_protocol.c"
	PROPERTIES COMPILE_FLAGS "-Wno-unused"
	)

set(GENGETOPT_SOURCES "include/mersenne/cmdline.ggo")
ADD_GENGETOPT_FILES(${GENGETOPT_SOURCES} GGO_GEN_SOURCES)

add_executable(embedfile "util/embedfile.c")

set(MIME_FOR_EXT_CSS "text/css")
set(MIME_FOR_EXT_HTML "text/html")
set(MIME_FOR_EXT_JS "application/javascript")
set(MIME_FOR_EXT_PNG "image/png")
set(MIME_FOR_EXT_GIF "image/gif")
set(MIME_FOR_EXT_SWP "application/octet-stream")

set(ASSET_REGISTRY "${CMAKE_CURRENT_BINARY_DIR}/include/asset_registry.h")

file(WRITE ${ASSET_REGISTRY} "struct asset {\n")
file(APPEND ${ASSET_REGISTRY} "\tconst char *content_ptr;\n")
file(APPEND ${ASSET_REGISTRY} "\tconst size_t *length_ptr;\n")
file(APPEND ${ASSET_REGISTRY} "\tconst char *orig_filename;\n")
file(APPEND ${ASSET_REGISTRY} "\tconst char *mime_type;\n")
file(APPEND ${ASSET_REGISTRY} "};\n\n")

set(ASSETS_BASE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/assets")
file(GLOB_RECURSE ASSETS RELATIVE ${ASSETS_BASE_DIR} "${ASSETS_BASE_DIR}/*.*")

set(IDX 1)
foreach(ASSET ${ASSETS})
	get_filename_component(ASSET_NAME ${ASSET} NAME)
	string(REGEX REPLACE "[.-]" "_" ASSET_VAR_NAME ${ASSET_NAME})
	set(ASSET_VAR_NAME "asset_${IDX}_${ASSET_VAR_NAME}")
	file(APPEND ${ASSET_REGISTRY} "extern const char ${ASSET_VAR_NAME}[];\n")
	file(APPEND ${ASSET_REGISTRY} "extern const size_t ${ASSET_VAR_NAME}_len;\n")
	math(EXPR IDX "${IDX} + 1")
endforeach()

file(APPEND ${ASSET_REGISTRY} "static const struct asset assets[] = {\n")

set(ASSET_SOURCES)
set(IDX 1)
foreach(ASSET ${ASSETS})
	get_filename_component(ASSET_NAME ${ASSET} NAME)
	string(REGEX REPLACE "[.-]" "_" ASSET_VAR_NAME ${ASSET_NAME})
	set(ASSET_VAR_NAME "asset_${IDX}_${ASSET_VAR_NAME}")
	set(ASSET_FILENAME "${ASSET_VAR_NAME}.c")
	string(REGEX MATCH "([^.]*)$" ASSET_EXT ${ASSET})
	string(TOUPPER ${ASSET_EXT} ASSET_EXT)
	set(ASSET_MIME ${MIME_FOR_EXT_${ASSET_EXT}})
	if("${ASSET_MIME}" STREQUAL "")
		message(FATAL_ERROR "no mime registered for ${ASSET_EXT}")
	endif()

	file(APPEND ${ASSET_REGISTRY} "\t{\n")
	file(APPEND ${ASSET_REGISTRY} "\t\t${ASSET_VAR_NAME},\n")
	file(APPEND ${ASSET_REGISTRY} "\t\t&${ASSET_VAR_NAME}_len,\n")
	file(APPEND ${ASSET_REGISTRY} "\t\t\"/${ASSET}\",\n")
	file(APPEND ${ASSET_REGISTRY} "\t\t\"${ASSET_MIME}\"\n")
	file(APPEND ${ASSET_REGISTRY} "\t},\n")

	add_custom_command(
		OUTPUT ${ASSET_FILENAME}
		COMMAND embedfile ${ASSET_VAR_NAME} "${ASSETS_BASE_DIR}/${ASSET}"
		DEPENDS "${ASSETS_BASE_DIR}/${ASSET}")
	list(APPEND ASSET_SOURCES ${ASSET_FILENAME})
	math(EXPR IDX "${IDX} + 1")
endforeach()
file(APPEND ${ASSET_REGISTRY} "};\n\n")
file(APPEND ${ASSET_REGISTRY} "static const size_t assets_len = sizeof(assets) / sizeof(assets[0]);\n")


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
	"src/statd.c"
	"src/mersenne.c"
	"src/dummy.cpp" # Convince CMAKE to link using g++
	"src/http_parser.c"
	"src/htstatus.c"
	"src/kstring.c"
	"src/ccan-json.c"
	${ASSET_SOURCES}
	)
ragel_add_include("${CMAKE_CURRENT_SOURCE_DIR}/msgpack-proto-ragel/include")
ragel_gen("src/proto.rl" RAGEL_CLI_GEN_SOURCES)
ragel_gen("src/wal_obj.rl" RAGEL_WAL_GEN_SOURCES)
set(MERSENNE_CLI_SOURCES
	"src/proto.rl"
	${RAGEL_CLI_GEN_SOURCES}
	)
set(MERSENNE_GEN_SOURCES
	"include/mersenne/me_protocol.h"
	"include/mersenne/me_protocol.c"
	${RAGEL_WAL_GEN_SOURCES}
	)

add_library(mersenne_md5 STATIC
	"src/md5.c"
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
add_dependencies(mersenne sophia)
target_link_libraries(mersenne
	mersenne_cli
	mersenne_md5
	${EVFIBERS_EMBED_LIBS}
	${PROTOBUFC_LIBRARIES}
	${CMAKE_THREAD_LIBS_INIT}
	crypto
	z
	uuid
	msgpack
	rocksdb
	)

subdirs(testing)

INSTALL(TARGETS mersenne mersenne_cli mersenne_md5
  RUNTIME DESTINATION bin
  LIBRARY DESTINATION lib
  ARCHIVE DESTINATION lib
)
