# GTest
add_subdirectory( contrib/gtest )

# Touch test's CMakeLists to update tests sources
add_custom_target(check_tests ALL touch ${CMAKE_CURRENT_SOURCE_DIR}/CMakeLists.txt)

# Show tests printouts
set(CMAKE_CTEST_COMMAND ctest -V)
add_custom_target(run_tests ${CMAKE_CTEST_COMMAND})

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "bin/")

find_package(Threads REQUIRED)

include_directories(${Threads_INCLUDE_DIR})
include_directories(${gtest_SOURCE_DIR}/include ${gtest_SOURCE_DIR})

# Get all test source files
file(GLOB_RECURSE test_src_files RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}/src "*.test.cpp")

foreach(test_src_file IN LISTS test_src_files)
    string(REGEX REPLACE "[.]test[.]cpp$" "" test_exec_file ${test_src_file})
    add_executable(${test_exec_file} src/${test_src_file})
    target_link_libraries(${test_exec_file} ${CMAKE_THREAD_LIBS_INIT} gtest gtest_main)
    add_test(${test_exec_file} ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/${test_exec_file})
endforeach(test_src_file)
