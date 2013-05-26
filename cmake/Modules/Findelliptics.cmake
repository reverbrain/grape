find_path(elliptics_INCLUDE_DIR elliptics/cppdef.h PATHS ${ELLIPTICS_PREFIX}/include /usr/include)
find_path(cocaine_INCLUDE_DIR cocaine/common.hpp PATHS ${COCAINE_PREFIX}/include /usr/include)

#find_library(elliptics_LIBRARY NAMES elliptics PATHS ${ELLIPTICS_PREFIX}/lib ${ELLIPTICS_PREFIX}/lib64 /usr/lib /usr/lib64)
find_library(elliptics_cpp_LIBRARY NAMES elliptics_cpp PATHS ${ELLIPTICS_PREFIX}/lib ${ELLIPTICS_PREFIX}/lib64 /usr/lib /usr/lib64)
find_library(elliptics_client_LIBRARY NAMES elliptics_client PATHS ${ELLIPTICS_PREFIX}/lib ${ELLIPTICS_PREFIX}/lib64 /usr/lib /usr/lib64)

set(elliptics_LIBRARIES ${elliptics_LIBRARY} ${elliptics_cpp_LIBRARY} ${elliptics_client_LIBRARY})
set(elliptics_INCLUDE_DIRS ${elliptics_INCLUDE_DIR} ${cocaine_INCLUDE_DIR})
list(REMOVE_DUPLICATES elliptics_INCLUDE_DIRS)

message(STATUS "elliptics_INCLUDE_DIR: " ${elliptics_INCLUDE_DIR})
message(STATUS "elliptics_INCLUDE_DIRS: " ${elliptics_INCLUDE_DIRS})

include(FindPackageHandleStandardArgs)
# handle the QUIETLY and REQUIRED arguments and set LIBXML2_FOUND to TRUE
# if all listed variables are TRUE
find_package_handle_standard_args(elliptics DEFAULT_MSG	elliptics_LIBRARIES elliptics_INCLUDE_DIRS)

mark_as_advanced(elliptics_INCLUDE_DIRS elliptics_LIBRARIES)
