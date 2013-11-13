cmake_minimum_required (VERSION 2.6)
project (grape)

add_definitions("-std=c++0x -Wreorder -Wreturn-type -Wunused-variable")

# The version number.
FILE (READ "${CMAKE_CURRENT_SOURCE_DIR}/debian/changelog" DEBCHANGELOG)
string(REGEX MATCH "([0-9]+\\.[0-9]+\\.[0-9]+)" DEBFULLVERSION "${DEBCHANGELOG}")
STRING (REGEX MATCH "([0-9]+\\.[0-9]+)" grape_VERSION_MAJOR "${DEBFULLVERSION}")
STRING (REGEX MATCH "([0-9]+$)" grape_VERSION_MINOR "${DEBFULLVERSION}")
set(grape_VERSION "${grape_VERSION_MAJOR}.${grape_VERSION_MINOR}")

find_package(Boost REQUIRED system program_options)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")

find_package(elliptics REQUIRED)
find_package(CocaineNative REQUIRED)
find_package(Cocaine REQUIRED)

include(cmake/Modules/locate_library.cmake)
locate_library(LIBEV "ev++.h" "ev" "libev")
locate_library(MSGPACK "msgpack.hpp" "msgpack")

set(GRAPE_COMMON_LIBRARIES
  ${elliptics_cpp_LIBRARY}
  ${CocaineNative_LIBRARIES}
  ${Cocaine_LIBRARY}
  ${LIBEV_LIBRARIES}
  ${Boost_SYSTEM_LIBRARY}
  ${Boost_THREAD_LIBRARY}
)

install(DIRECTORY include/ DESTINATION include)

include_directories(${PROJECT_SOURCE_DIR}/include ${elliptics_INCLUDE_DIRS} ${CocaineNative_INCLUDE_DIRS} ${LIBEV_INCLUDE_DIRS})

########################
# TESTING
########################

enable_testing()
add_subdirectory(test)

add_subdirectory(src/data_array)
add_subdirectory(src/driver)
add_subdirectory(src/queue)
add_subdirectory(src/queue-pump)
add_subdirectory(src/testerhead-cpp)

#add_subdirectory(example)

#set(ARCHIVE_NAME ${CMAKE_PROJECT_NAME})
#add_custom_target(dist
#    COMMAND git archive --prefix=${ARCHIVE_NAME}-${grape_VERSION}/ HEAD
#    | bzip2 > ${CMAKE_BINARY_DIR}/${ARCHIVE_NAME}-${grape_VERSION}.tar.bz2
#    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
