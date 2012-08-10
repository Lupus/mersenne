add_executable(fibers fibers.c ../src/fiber.c ../src/util.c ../3rdparty/coro.c)
target_link_libraries(fibers ev)

set_source_files_properties("${CMAKE_CURRENT_BINARY_DIR}/../include/mersenne/cl_protocol.c"
	PROPERTIES COMPILE_FLAGS "-Wno-unused"
	)
add_executable(strings_client
	strings_client.c
	"${CMAKE_CURRENT_BINARY_DIR}/../include/mersenne/cl_protocol.c"
	../src/util.c
	)
