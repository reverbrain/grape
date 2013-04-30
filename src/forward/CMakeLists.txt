add_executable(forward forward.cpp)
set_target_properties(forward PROPERTIES
	COMPILE_FLAGS "-std=c++0x -Wall -Wextra"
)
target_link_libraries(forward
	${elliptics_LIBRARIES}
	jsoncpp
	cocaine-framework
	ev
	boost_system-mt
	boost_filesystem-mt
)

install(TARGETS forward
    RUNTIME DESTINATION lib/grape
    COMPONENT runtime
)
