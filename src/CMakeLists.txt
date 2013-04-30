add_library(grape SHARED logger.cpp grape.cpp elliptics/elliptics.cpp)
set_target_properties(grape PROPERTIES COMPILE_FLAGS "-std=c++0x")
target_link_libraries(grape ${Boost_SYSTEM_LIBRARY} ${Boost_THREAD_LIBRARY} grapejson ${elliptics_LIBRARIES})

set_target_properties(grape PROPERTIES VERSION ${grape_VERSION_MAJOR} SOVERSION ${grape_VERSION_MAJOR})

#SET(CMAKE_INSTALL_LIBDIR lib${LIB_SUFFIX} CACHE PATH "Output directory for libraries")

install(TARGETS grape LIBRARY DESTINATION lib)
