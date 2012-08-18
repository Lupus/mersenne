aux_source_directory("${CMAKE_CURRENT_SOURCE_DIR}/src" EVFIBERS_SOURCES)
aux_source_directory("${CMAKE_CURRENT_SOURCE_DIR}/include/evfibers" EVFIBERS_HEADERS)
aux_source_directory("${CMAKE_CURRENT_SOURCE_DIR}/include/evfibers_private" EVFIBERS_PRIVATE_HEADERS)
aux_source_directory("${CMAKE_CURRENT_SOURCE_DIR}/coro" CORO_SOURCES)

include_directories(
	"${CMAKE_CURRENT_SOURCE_DIR}/include"
	"${CMAKE_CURRENT_SOURCE_DIR}/coro"
	)

macro(SIZEOF SIZE H_INCLUDE TYPENAME)
	string(REGEX REPLACE " " "_" SIZEOF_NAME "${TYPENAME}")
	set(SIZEOF_FILENAME "${CMAKE_CURRENT_BINARY_DIR}/sizeof_${SIZEOF_NAME}.c")
	file(WRITE ${SIZEOF_FILENAME}
		"#include <stdio.h>\n"
		"#include <${H_INCLUDE}>\n"
		"int main() {\n"
		"	printf(\"%lu\", sizeof(${TYPENAME}));\n"
		"	return 0;\n"
		"}\n"
		)
	get_property(INC_DIRS DIRECTORY PROPERTY INCLUDE_DIRECTORIES)
	try_run(RUN_RESULT COMPILE_RESULT
		${CMAKE_CURRENT_BINARY_DIR} ${SIZEOF_FILENAME}
		CMAKE_FLAGS "-DINCLUDE_DIRECTORIES:STRING=${INC_DIRS}"
		COMPILE_OUTPUT_VARIABLE COMPILE_OUTPUT
		RUN_OUTPUT_VARIABLE RUN_OUTPUT)
	if(NOT COMPILE_RESULT)
		message( FATAL_ERROR "Cannot determine the size of ${TYPENAME}!\n${COMPILE_OUTPUT}")
	endif()
	message(STATUS "Figured out sizeof(${TYPENAME}) to be ${RUN_OUTPUT} bytes")
	set(${SIZE} ${RUN_OUTPUT})
endmacro(SIZEOF)

SIZEOF(CONTEXT_SIZEOF "evfibers_private/fiber.h" "struct fbr_context")
configure_file(
	"${CMAKE_CURRENT_SOURCE_DIR}/include/evfibers/context_size.h.in"
	"${CMAKE_CURRENT_BINARY_DIR}/include/evfibers/context_size.h"
	)

add_library(evfibers
	${EVFIBERS_SOURCES}
	${EVFIBERS_HEADERS}
	${EVFIBERS_PRIVATE_HEADERS}
	${CORO_SOURCES}
	"${CMAKE_CURRENT_BINARY_DIR}/include/evfibers/context_size.h"
	)
