# This module defines
#  JsonCpp_FOUND - whether the cocaine-framework-native was found
#  JsonCpp_LIBRARIES - it's libraries
#  JsonCpp_INCLUDE_DIRS - it's include paths
#  JsonCpp_CFLAGS - flags to compile with

find_path(JsonCpp_INCLUDE_DIR json/json.h)

find_library(JsonCpp_LIBRARY jsoncpp)

set(JsonCpp_INCLUDE_DIRS "${JsonCpp_INCLUDE_DIR}")
set(JsonCpp_LIBRARIES "${JsonCpp_LIBRARY}")

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(JsonCpp DEFAULT_MSG JsonCpp_LIBRARY JsonCpp_INCLUDE_DIR)

mark_as_advanced(JsonCpp_LIBRARY JsonCpp_INCLUDE_DIR)
