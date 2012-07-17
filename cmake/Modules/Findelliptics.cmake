find_path(elliptics_INCLUDE_DIR elliptics/cppdef.h PATH ${ELLIPTICS_PREFIX}/include)
find_path(eblob_INCLUDE_DIR eblob/blob.h PATH ${EBLOB_PREFIX}/include)
find_path(cocaine_INCLUDE_DIR cocaine/binary.hpp PATH ${COCAINE_PREFIX}/include)

find_library(elliptics_LIBRARY NAMES elliptics PATHS ${ELLIPTICS_PREFIX}/lib)
find_library(elliptics_cpp_LIBRARY NAMES elliptics_cpp PATHS ${ELLIPTICS_PREFIX}/lib)
find_library(eblob_LIBRARY NAMES eblob PATHS ${EBLOB_PREFIX}/lib)

set(elliptics_LIBRARIES ${elliptics_LIBRARY} ${elliptics_cpp_LIBRARY} ${eblob_LIBRARY})
set(elliptics_INCLUDE_DIRS ${elliptics_INCLUDE_DIR} ${eblob_INCLUDE_DIR} ${cocaine_INCLUDE_DIR})

include(FindPackageHandleStandardArgs)
# handle the QUIETLY and REQUIRED arguments and set LIBXML2_FOUND to TRUE
# if all listed variables are TRUE
find_package_handle_standard_args(elliptics DEFAULT_MSG	elliptics_LIBRARIES elliptics_INCLUDE_DIRS)

mark_as_advanced(elliptics_INCLUDE_DIRS elliptics_LIBRARIES)
