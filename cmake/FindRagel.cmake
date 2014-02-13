find_program(RAGEL "ragel")

function(ragel_gen in_rl)
endfunction(ragel_gen)
macro(ragel_gen SRC_FILES OUT_FILES)
	set(NEW_SOURCE_FILES)
	foreach (CURRENT_FILE ${SRC_FILES})
		get_filename_component(SRCPATH "${CURRENT_FILE}" PATH)
		get_filename_component(SRCBASE "${CURRENT_FILE}" NAME_WE)
		file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/${SRCPATH}")
		set(OUT "${CMAKE_CURRENT_BINARY_DIR}/${SRCPATH}/${SRCBASE}.c")
		set(INFILE "${CMAKE_CURRENT_SOURCE_DIR}/${CURRENT_FILE}")
		add_custom_command(
			OUTPUT ${OUT}
			COMMAND ${RAGEL} -o ${OUT} ${INFILE} -I ${CMAKE_CURRENT_SOURCE_DIR}
			DEPENDS ${INFILE}
			)
		list(APPEND NEW_SOURCE_FILES ${OUT})
	endforeach(CURRENT_FILE)
	set(${OUT_FILES} ${NEW_SOURCE_FILES})
endmacro(ragel_gen)



if(RAGEL)
	message(STATUS "ragel found at: ${RAGEL}")
else(RAGEL)
	message(FATAL_ERROR "ragel not found")
endif(RAGEL)
