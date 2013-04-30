find_package(Cocaine REQUIRED)

add_library(queue-getter MODULE driver.cpp module.cpp)
set_target_properties(queue-getter PROPERTIES
	PREFIX ""
	SUFFIX ".cocaine-plugin"
	COMPILE_FLAGS "-std=c++0x -Wall -Wextra"
)
target_link_libraries(queue-getter
	elliptics_cpp
	jsoncpp
	cocaine-core
)

install(TARGETS queue-getter
	LIBRARY DESTINATION lib/cocaine
	COMPONENT runtime
)
