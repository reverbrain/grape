add_library(queue-driver MODULE driver.cpp module.cpp)
set_target_properties(queue-driver PROPERTIES
	PREFIX ""
	SUFFIX ".cocaine-plugin"
	COMPILE_FLAGS "-std=c++0x -Wall -Wextra"
)

target_link_libraries(queue-driver
	rt
	${elliptics_cpp_LIBRARIES}
	${Cocaine_LIBRARIES}
	grape_data_array
)

install(TARGETS queue-driver
	LIBRARY DESTINATION lib/cocaine
	COMPONENT runtime
)
