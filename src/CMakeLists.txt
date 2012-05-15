add_library(xroute SHARED logger.cpp xroute.cpp elliptics/handler.cpp)
target_link_libraries(xroute ${Boost_SYSTEM_LIBRARY} ${Boost_THREAD_LIBRARY} ${elliptics_LIBRARIES})

set_target_properties(xroute PROPERTIES VERSION ${xroute_VERSION} SOVERSION ${xroute_VERSION})

SET(LIB_SUFFIX "")
IF(CMAKE_SIZEOF_VOID_P EQUAL 8)
    SET(LIB_SUFFIX 64)
ENDIF(CMAKE_SIZEOF_VOID_P EQUAL 8)

SET(CMAKE_INSTALL_LIBDIR lib${LIB_SUFFIX} CACHE PATH "Output directory for libraries")

install(TARGETS xroute LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
