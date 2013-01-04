project(grape_etest)
add_library(grape_etest SHARED test.cpp)
target_link_libraries(grape_etest grape ${elliptics_LIBRARIES})

project(grape_etest_single)
add_library(grape_etest_single SHARED test_single.cpp)
target_link_libraries(grape_etest_single grape ${elliptics_LIBRARIES})

project(grape_test_start)
add_executable(grape_test_start test_start.cpp)
target_link_libraries(grape_test_start grape ${elliptics_LIBRARIES} ${Boost_PROGRAM_OPTIONS_LIBRARY})
