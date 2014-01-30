install(DIRECTORY . DESTINATION lib/grape/launchpad
    COMPONENT runtime
    USE_SOURCE_PERMISSIONS
    PATTERN "*.pyc" EXCLUDE
    PATTERN "CMakeLists.txt" EXCLUDE
    PATTERN "*.py"
)
