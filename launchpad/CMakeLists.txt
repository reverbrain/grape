install(DIRECTORY . DESTINATION lib${LIB_SUFFIX}/grape/launchpad
    COMPONENT runtime
    USE_SOURCE_PERMISSIONS
    PATTERN "*.pyc" EXCLUDE
    PATTERN "CMakeLists.txt" EXCLUDE
    PATTERN "*.py"
)
