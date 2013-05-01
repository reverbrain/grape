add_executable(forward forward.cpp)
set_target_properties(forward PROPERTIES
	COMPILE_FLAGS "-std=c++0x -Wall -Wextra"
)
target_link_libraries(forward ${GRAPE_COMMON_LIBRARIES})

install(TARGETS forward
    RUNTIME DESTINATION lib/grape
    COMPONENT runtime
)
