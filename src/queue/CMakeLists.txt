add_library(queue STATIC queue.cpp chunk.cpp)
target_link_libraries(queue ${GRAPE_COMMON_LIBRARIES} ${elliptics_LIBRARIES})

add_executable(queue-test test.cpp)
target_link_libraries(queue-test queue ${GRAPE_COMMON_LIBRARIES})

add_executable(queue-starter starter.cpp)
target_link_libraries(queue-starter queue ${GRAPE_COMMON_LIBRARIES} ${elliptics_LIBRARIES})


add_executable(queue-app app.cpp)
set_target_properties(queue-app PROPERTIES
	OUTPUT_NAME "queue"
)
target_link_libraries(queue-app queue ${GRAPE_COMMON_LIBRARIES} rt)

install(TARGETS queue-app queue-starter
    RUNTIME DESTINATION lib/grape
    COMPONENT runtime
)
