add_executable(testerhead-cpp app.cpp)
set_target_properties(testerhead-cpp PROPERTIES
	COMPILE_FLAGS "-std=c++0x"
)
target_link_libraries(testerhead-cpp ${GRAPE_COMMON_LIBRARIES}
	${Boost_PROGRAM_OPTIONS_LIBRARY}
)

install(TARGETS testerhead-cpp
    RUNTIME DESTINATION lib/grape
    COMPONENT runtime
)

