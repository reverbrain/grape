project(grape_etest)
add_library(grape_etest SHARED test.cpp)
target_link_libraries(grape_etest grape)
