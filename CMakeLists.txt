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
find_package(JsonCpp REQUIRED)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")

find_package(elliptics REQUIRED)
find_package(CocaineNative REQUIRED)

FILE(GLOB headers "${CMAKE_CURRENT_SOURCE_DIR}/include/grape/*.hpp" "${CMAKE_CURRENT_SOURCE_DIR}/include/grape/*.h")
install(FILES ${headers} DESTINATION include/grape)

include_directories(${PROJECT_SOURCE_DIR}/include ${elliptics_INCLUDE_DIRS} ${CocaineNative_INCLUDE_DIRS})

add_subdirectory(src/queue)
add_subdirectory(src/queue-getter)
add_subdirectory(src/forward)
add_subdirectory(src/testerhead-cpp)

#add_subdirectory(example)

#set(ARCHIVE_NAME ${CMAKE_PROJECT_NAME})
#add_custom_target(dist
#    COMMAND git archive --prefix=${ARCHIVE_NAME}-${grape_VERSION}/ HEAD
#    | bzip2 > ${CMAKE_BINARY_DIR}/${ARCHIVE_NAME}-${grape_VERSION}.tar.bz2
#    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
