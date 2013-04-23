add_executable(testerhead-cpp testerhead_app.cpp)
set_target_properties(testerhead-cpp PROPERTIES
	COMPILE_FLAGS "-std=c++0x"
)
target_link_libraries(testerhead-cpp
	${elliptics_LIBRARIES}
	cocaine-framework
	ev
	jsoncpp
	${Boost_PROGRAM_OPTIONS_LIBRARY}
	boost_system-mt
	boost_filesystem-mt
)

install(TARGETS testerhead-cpp
    RUNTIME DESTINATION lib/grape
    COMPONENT runtime
)

