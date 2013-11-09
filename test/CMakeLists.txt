find_package(Threads REQUIRED)
include_directories(${Threads_INCLUDE_DIR})

# GTest
find_package(GTest)

if (NOT GTEST_FOUND)
	find_path(GTEST_SRC_DIR src/gtest.cc HINTS /usr/src/gtest)
	find_path(GTEST_INCLUDE_DIRS gtest.h HINTS /usr/include/gtest)
	if (GTEST_SRC_DIR and GTEST_INCLUDE_DIRS)
		message(STATUS "GTest source package found. Compiling...")
		add_subdirectory(${GTEST_SRC_DIR} ${CMAKE_BINARY_DIR}/gtest)

		set(GTEST_FOUND 1)
		set(GTEST_MAIN_LIBRARIES ${CMAKE_BINARY_DIR}/gtest/libgtest_main.a)
		set(GTEST_LIBRARIES ${CMAKE_BINARY_DIR}/gtest/libgtest.a)
		set(GTEST_BOTH_LIBRARIES ${GTEST_LIBRARIES} ${GTEST_MAIN_LIBRARIES})
	else (GTEST_SRC_DIR and GTEST_INCLUDE_DIRS)
		message(SEND_ERROR "Cannot find required package: GTest")
	endif (GTEST_SRC_DIR and GTEST_INCLUDE_DIRS)
else (NOT GTEST_FOUND)
endif (NOT GTEST_FOUND)

include_directories(${GTEST_INCLUDE_DIRS})
set(GTEST_TARGETS gtest gtest_main)

# Touch test's CMakeLists to update tests sources
add_custom_target(check_tests ALL touch ${CMAKE_CURRENT_SOURCE_DIR}/CMakeLists.txt)

# Show tests printouts
set(CMAKE_CTEST_COMMAND ctest -V)
add_custom_target(run_tests ${CMAKE_CTEST_COMMAND})

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "bin/")

include_directories(${gtest_SOURCE_DIR}/include ${gtest_SOURCE_DIR})

# Get all test source files
file(GLOB_RECURSE test_src_files RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}/src "*.test.cpp")

foreach(test_src_file IN LISTS test_src_files)
    string(REGEX REPLACE "[.]test[.]cpp$" "" test_exec_file ${test_src_file})
    add_executable(${test_exec_file} src/${test_src_file})
    target_link_libraries(${test_exec_file} ${CMAKE_THREAD_LIBS_INIT} ${GTEST_BOTH_LIBRARIES})
    add_test(${test_exec_file} ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/${test_exec_file})
    add_dependencies(${test_exec_file} ${GTEST_TARGETS})
endforeach(test_src_file)
