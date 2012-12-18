project(grape_etest)
add_library(grape_etest SHARED test.cpp)
target_link_libraries(grape_etest grape ${elliptics_LIBRARIES})

project(grape_etest_single)
add_library(grape_etest_single SHARED test_single.cpp)
target_link_libraries(grape_etest_single grape ${elliptics_LIBRARIES})
