cmake_minimum_required (VERSION 2.6)
project (xroute)
# The version number.
set (xroute_VERSION "0.0.1")

find_package(Boost REQUIRED system thread)
set(CMAKE_CXX_FLAGS "-g -W -Wall")

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")

find_package(elliptics REQUIRED)
include_directories(${PROJECT_SOURCE_DIR} ${elliptics_INCLUDE_DIRS})

include_directories("${PROJECT_BINARY_DIR}", "${PROJECT_SOURCE_DIR}/include")
FILE(GLOB headers "${CMAKE_CURRENT_SOURCE_DIR}/include/xroute/*.hpp" "${CMAKE_CURRENT_SOURCE_DIR}/include/xroute/*.h")
install(FILES ${headers} DESTINATION include/xroute)

add_subdirectory(src)
add_subdirectory(example)

set(ARCHIVE_NAME ${CMAKE_PROJECT_NAME})
add_custom_target(dist
    COMMAND git archive --prefix=${ARCHIVE_NAME}-${smack_VERSION}/ HEAD
    | bzip2 > ${CMAKE_BINARY_DIR}/${ARCHIVE_NAME}-${smack_VERSION}.tar.bz2
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
