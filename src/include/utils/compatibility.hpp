#pragma once

// Version comparison macros
#define DUCKDB_VERSION_AT_MOST(major, minor, patch) \
    (DUCKDB_MAJOR_VERSION < (major) || \
     (DUCKDB_MAJOR_VERSION == (major) && DUCKDB_MINOR_VERSION <= (minor)) || \
     (DUCKDB_MAJOR_VERSION == (major) && DUCKDB_MINOR_VERSION == (minor) && DUCKDB_PATCH_VERSION <= (patch)))
