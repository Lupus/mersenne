RPCGEN_CREATE_XDR("../include/mersenne/cl_protocol.x")
set_source_files_properties("${CMAKE_CURRENT_BINARY_DIR}/../include/mersenne/cl_protocol.c"
	PROPERTIES COMPILE_FLAGS "-Wno-unused"
	)
add_definitions(-DHASH_FUNCTION=HASH_FNV)
add_executable(strings_client
	strings_client.c
	"${CMAKE_CURRENT_BINARY_DIR}/../include/mersenne/cl_protocol.c"
	../src/util.c
	../src/xdr.c
	../src/buffer.c
	../src/bitmask.c
	../src/sharedmem.c
	)
target_link_libraries(strings_client
	${EVFIBERS_EMBED_LIBS}
	rt)
