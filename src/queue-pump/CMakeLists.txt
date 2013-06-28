add_executable(queue-pump queue-pump.cpp)
target_link_libraries(queue-pump grape_data_array boost_program_options ${GRAPE_COMMON_LIBRARIES})
