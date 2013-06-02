add_library(grape_data_array SHARED data_array.cpp)
target_link_libraries(grape_data_array ${elliptics_LIBRARIES} ${MSGPACK_LIBRARIES})

set_target_properties(grape_data_array PROPERTIES
	VERSION ${grape_VERSION}
    	SOVERSION ${grape_VERSION_MAJOR}
)

install(TARGETS grape_data_array
	LIBRARY DESTINATION lib
)
