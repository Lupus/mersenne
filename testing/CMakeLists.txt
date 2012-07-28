add_executable(fibers fibers.c ../src/fiber.c ../3rdparty/coro.c)
target_link_libraries(fibers ev)
