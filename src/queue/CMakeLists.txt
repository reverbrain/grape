add_library(queue STATIC queue.cpp)
#set_target_properties(queue PROPERTIES
#    COMPILE_FLAGS "-std=c++0x"
#)
target_link_libraries(queue
	cocaine-framework
	jsoncpp
	${elliptics_LIBRARIES}
	${Boost_SYSTEM_LIBRARY}
	${Boost_THREAD_LIBRARY}
)


add_executable(block block_cli.cpp)
set_target_properties(block PROPERTIES COMPILE_FLAGS "-std=c++0x")
target_link_libraries(block
	queue
	${Boost_PROGRAM_OPTIONS_LIBRARY}
)


add_executable(queue-tool queue_cli.cpp)
set_target_properties(queue-tool PROPERTIES COMPILE_FLAGS "-std=c++0x")
target_link_libraries(queue-tool
	queue
	cocaine-framework
	jsoncpp
	${Boost_PROGRAM_OPTIONS_LIBRARY}
)


add_executable(queue-app queue_app.cpp)
set_target_properties(queue-app PROPERTIES
	COMPILE_FLAGS "-std=c++0x"
	OUTPUT_NAME "queue"
)
target_link_libraries(queue-app
	queue
	cocaine-framework
	ev
	jsoncpp
	${Boost_PROGRAM_OPTIONS_LIBRARY}
	boost_system-mt
	boost_filesystem-mt
)


install(TARGETS block queue-tool
    RUNTIME DESTINATION bin
    COMPONENT runtime
)

install(TARGETS queue-app
    RUNTIME DESTINATION lib/grape
    COMPONENT runtime
)
