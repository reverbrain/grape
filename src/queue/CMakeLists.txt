add_library(queue STATIC queue.cpp chunk.cpp)
target_link_libraries(queue ${GRAPE_COMMON_LIBRARIES} ${elliptics_LIBRARIES} grape_data_array)

add_executable(queue-app app.cpp)
set_target_properties(queue-app PROPERTIES
	OUTPUT_NAME "queue"
)
target_link_libraries(queue-app queue ${GRAPE_COMMON_LIBRARIES} rt)

install(TARGETS queue-app
    RUNTIME DESTINATION lib/grape
    COMPONENT runtime
)
