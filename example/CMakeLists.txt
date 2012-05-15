project(xroute_etest)
add_library(xroute_etest SHARED test.cpp)
target_link_libraries(xroute_etest xroute)
