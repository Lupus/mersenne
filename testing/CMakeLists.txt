add_definitions(-DHASH_FUNCTION=HASH_FNV)
add_executable(strings_client
	strings_client.c
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
