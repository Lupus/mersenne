add_definitions(-DHASH_FUNCTION=HASH_FNV)
file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}")
include_directories("${CMAKE_CURRENT_BINARY_DIR}")
set(GENGETOPT_SOURCES "cmdline.ggo")
ADD_GENGETOPT_FILES(${GENGETOPT_SOURCES} GGO_CLI_GEN_SOURCES)
add_executable(strings_client
	strings_client.c
	paxos.c
	${GGO_CLI_GEN_SOURCES}
	)
target_link_libraries(strings_client
	${EVFIBERS_EMBED_LIBS}
	mersenne_cli
	mersenne_md5
	msgpack
	uuid
	rt)

add_executable(disk_wal_test
	disk_wal_test.c
	)

INSTALL(TARGETS strings_client
  RUNTIME DESTINATION bin
  LIBRARY DESTINATION lib
)
