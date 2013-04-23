# Find Cocaine Native Framework - base for building c++ applications running under the Cocaine.
# https://github.com/cocaine/cocaine-framework-native
#
# This module defines
#  Cocaine_FOUND - whether the cocaine-framework-native was found
#  Cocaine_LIBRARIES - it's libraries
#  Cocaine_INCLUDE_DIRS - it's include paths
#  Cocaine_CFLAGS - flags to compile with

find_path(Cocaine_INCLUDE_DIR cocaine/context.hpp)

find_library(Cocaine_LIBRARY cocaine-core)

set(Cocaine_INCLUDE_DIRS "${Cocaine_INCLUDE_DIR}")
set(Cocaine_LIBRARIES "${Cocaine_LIBRARY}")

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Cocaine DEFAULT_MSG Cocaine_LIBRARY Cocaine_INCLUDE_DIR)

mark_as_advanced(Cocaine_LIBRARY Cocaine_INCLUDE_DIR)
