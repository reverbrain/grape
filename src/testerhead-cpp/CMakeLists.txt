add_executable(testerhead-cpp app.cpp)
set_target_properties(testerhead-cpp PROPERTIES
	COMPILE_FLAGS "-std=c++0x"
)
target_link_libraries(testerhead-cpp
	${Boost_PROGRAM_OPTIONS_LIBRARY}
    ${GRAPE_COMMON_LIBRARIES}
    grape_data_array
)

install(TARGETS testerhead-cpp
    RUNTIME DESTINATION lib/grape
    COMPONENT runtime
)

