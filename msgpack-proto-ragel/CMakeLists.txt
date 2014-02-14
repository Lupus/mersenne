cmake_minimum_required(VERSION 2.8.7)
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
	# We are building as stand-alone project
	project(libmsgpack-proto-ragel C)
else()
	# We are building as part of other project
	set(MPPR_EMBED TRUE)
endif()

if(NOT CMAKE_BUILD_TYPE)
	message(STATUS "No build type selected, defaulting to RelWithDebInfo")
	set(CMAKE_BUILD_TYPE "RelWithDebInfo")
endif()

aux_source_directory("${CMAKE_CURRENT_SOURCE_DIR}/src" MPPR_SOURCES)

include_directories(
	"${CMAKE_CURRENT_SOURCE_DIR}/include"
	"${CMAKE_CURRENT_BINARY_DIR}/include"
	)

add_library(mppr_static STATIC ${MPPR_SOURCES})
set_target_properties(mppr_static  PROPERTIES
	OUTPUT_NAME mppr
	COMPILE_FLAGS "-fPIC")

set(MPPR_EMBED_LIBS mppr_static
	PARENT_SCOPE)

if(NOT MPPR_EMBED)
	install(TARGETS mppr_static
		RUNTIME DESTINATION bin
		LIBRARY DESTINATION lib
		ARCHIVE DESTINATION lib
		)
	file(GLOB DEV_HEADERS "${CMAKE_CURRENT_SOURCE_DIR}/include/msgpack-proto-ragel/*.*")
	install(FILES ${DEV_HEADERS} DESTINATION "include/msgpack-proto-ragel")
endif()
