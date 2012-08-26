add_executable(fibers fibers.c)
target_link_libraries(fibers evfibers ev)

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
	)
target_link_libraries(strings_client evfibers ev)
add_executable(acceptor_dump acceptor_dump.c)
target_link_libraries(acceptor_dump dl hiredis)
