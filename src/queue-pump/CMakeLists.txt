add_executable(queue-pump queue-pump.cpp)
target_link_libraries(queue-pump grape_data_array boost_program_options ${GRAPE_COMMON_LIBRARIES})

add_executable(queue-push queue-push.cpp)
target_link_libraries(queue-push boost_program_options ${GRAPE_COMMON_LIBRARIES})

add_executable(queue-pull queue-pull.cpp)
target_link_libraries(queue-pull grape_data_array boost_program_options ${GRAPE_COMMON_LIBRARIES})

