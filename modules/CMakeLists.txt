find_package (Threads)

add_library(acc_storage_uthash SHARED acc_storage_uthash.c)
add_library(acc_storage_redis SHARED acc_storage_redis.c)
target_link_libraries(acc_storage_redis hiredis)
add_library(acc_storage_tokyo SHARED acc_storage_tokyo.c)
target_link_libraries(acc_storage_tokyo tokyocabinet ${CMAKE_THREAD_LIBS_INIT})
