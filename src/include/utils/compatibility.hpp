#pragma once

// TODO we cannot run these checks becuse they are not defined for DuckDB < 1.4.x
// #ifndef DUCKDB_MAJOR_VERSION
// #error "DUCKDB_MAJOR_VERSION is not defined"
// ...

#define DUCKDB_VERSION_AT_MOST(major, minor, patch)                                                                    \
	(DUCKDB_MAJOR_VERSION < (major) || (DUCKDB_MAJOR_VERSION == (major) && DUCKDB_MINOR_VERSION < (minor)) ||          \
	 (DUCKDB_MAJOR_VERSION == (major) && DUCKDB_MINOR_VERSION == (minor) && DUCKDB_PATCH_VERSION <= (patch)))
