add_library(grape SHARED logger.cpp grape.cpp elliptics/elliptics.cpp)
target_link_libraries(grape ${Boost_SYSTEM_LIBRARY} ${Boost_THREAD_LIBRARY} grapejson ${elliptics_LIBRARIES})

set_target_properties(grape PROPERTIES VERSION ${grape_VERSION} SOVERSION ${grape_VERSION})

SET(LIB_SUFFIX "")
IF(CMAKE_SIZEOF_VOID_P EQUAL 8)
    SET(LIB_SUFFIX 64)
ENDIF(CMAKE_SIZEOF_VOID_P EQUAL 8)

SET(CMAKE_INSTALL_LIBDIR lib${LIB_SUFFIX} CACHE PATH "Output directory for libraries")

install(TARGETS grape LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
