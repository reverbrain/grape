cmake_minimum_required (VERSION 2.6)
project (grape)

add_definitions("-std=c++0x")

# The version number.
FILE (READ "${CMAKE_CURRENT_SOURCE_DIR}/debian/changelog" DEBCHANGELOG)
string(REGEX MATCH "([0-9]+\\.[0-9]+\\.[0-9]+)" DEBFULLVERSION "${DEBCHANGELOG}")
STRING (REGEX MATCH "([0-9]+\\.[0-9]+)" grape_VERSION_MAJOR "${DEBFULLVERSION}")
STRING (REGEX MATCH "([0-9]+$)" grape_VERSION_MINOR "${DEBFULLVERSION}")
set(${grape_VERSION} "${grape_VERSION_MAJOR}.${grape_VERSION_MINOR}")

find_package(Boost REQUIRED system thread program_options)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")

find_package(elliptics REQUIRED)

FILE(GLOB headers "${CMAKE_CURRENT_SOURCE_DIR}/include/grape/*.hpp" "${CMAKE_CURRENT_SOURCE_DIR}/include/grape/*.h")
install(FILES ${headers} DESTINATION include/grape)

include_directories(BEFORE ${PROJECT_SOURCE_DIR}/foreign/jsoncpp-src-0.6.0-rc2/include)
include_directories(${PROJECT_SOURCE_DIR} ${elliptics_INCLUDE_DIRS} ${PROJECT_BINARY_DIR} "${PROJECT_SOURCE_DIR}/include")

add_library(grapejson
	foreign/jsoncpp-src-0.6.0-rc2/src/lib_json/json_value
	foreign/jsoncpp-src-0.6.0-rc2/src/lib_json/json_reader
	foreign/jsoncpp-src-0.6.0-rc2/src/lib_json/json_writer)
set_target_properties(grapejson PROPERTIES COMPILE_FLAGS -fPIC)

install(TARGETS grapejson
	RUNTIME DESTINATION bin COMPONENT runtime
	LIBRARY DESTINATION lib COMPONENT runtime
	ARCHIVE DESTINATION lib COMPONENT developement)

install(DIRECTORY foreign/jsoncpp-src-0.6.0-rc2/include/
	DESTINATION include/grape
	COMPONENT development)

add_subdirectory(src)
add_subdirectory(example)

set(ARCHIVE_NAME ${CMAKE_PROJECT_NAME})
add_custom_target(dist
    COMMAND git archive --prefix=${ARCHIVE_NAME}-${grape_VERSION}/ HEAD
    | bzip2 > ${CMAKE_BINARY_DIR}/${ARCHIVE_NAME}-${grape_VERSION}.tar.bz2
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
