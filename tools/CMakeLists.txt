find_package(LibClang REQUIRED)

add_executable(genstrenum genstrenum.c)
target_link_libraries(genstrenum ${LIBCLANG_LDFLAGS})
