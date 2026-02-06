cmake_minimum_required(VERSION 3.5)

set(TARGET_NAME test_utils)

set(EXTENSION_NAME ${TARGET_NAME}_extension)

project(${TARGET_NAME})


if(NOT DEFINED DUCKDB_MAJOR_VERSION)
    message(FATAL_ERROR "DUCKDB_MAJOR_VERSION is not defined")
endif()

if(NOT DEFINED DUCKDB_MINOR_VERSION)
    message(FATAL_ERROR "DUCKDB_MINOR_VERSION is not defined")
endif()

if(NOT DEFINED DUCKDB_PATCH_VERSION)
    message(FATAL_ERROR "DUCKDB_PATCH_VERSION is not defined")
endif()

include_directories(src/include)

set(EXTENSION_SOURCES
  src/utils/file_writer.cpp
  src/utils/helpers.cpp
  src/utils/logger.cpp
  src/utils/misc.cpp
  src/utils/serialization_helpers.cpp
  src/execute_all_from_file.cpp
  src/serialize_queries_plans.cpp
  src/serialize_results.cpp
  src/state.cpp
  src/test_utils_extension.cpp
  src/tu_compare_results.cpp
)

add_definitions(-DDUCKDB_MAJOR_VERSION=${DUCKDB_MAJOR_VERSION})
add_definitions(-DDUCKDB_MINOR_VERSION=${DUCKDB_MINOR_VERSION})
add_definitions(-DDUCKDB_PATCH_VERSION=${DUCKDB_PATCH_VERSION})

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

set_target_properties(${TARGET_NAME}_loadable_extension PROPERTIES CXX_VISIBILITY_PRESET default)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}"
)
