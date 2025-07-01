#define DUCKDB_EXTENSION_MAIN

#include "state.hpp"
#include "test_utils_extension.hpp"
#include "utils/helpers.hpp"
#include <duckdb.hpp>

namespace duckdb {

bool SerializeQueriesPlansFromFile(ClientContext &, const vector<Value> &);
bool ExecuteAllPlansFromMemory(ClientContext &);
bool ExecuteAllPlansFromFile(ClientContext &, const vector<Value> &);
bool CompareResults(ClientContext &, const vector<Value> &);

static void LoadInternal(DatabaseInstance &instance) {
	// Register the storage extension
	auto ext = duckdb::make_uniq<duckdb::StorageExtension>();
	ext->storage_info = duckdb::make_uniq<TUStorageExtensionInfo>();
	instance.config.storage_extensions[STORAGE_EXTENSION_KEY] = std::move(ext);

	// Register the functions
	REGISTER_TF("serialize_queries_plans", SerializeQueriesPlansFromFile, 2);
	REGISTER_TF("execute_all_plans_from_memory", ExecuteAllPlansFromMemory, 0);
	REGISTER_TF("execute_all_plans_from_file", ExecuteAllPlansFromFile, 2);
	REGISTER_TF("compare_results", CompareResults, 1);
}

void TestUtilsExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string TestUtilsExtension::Name() {
	return "test_utils";
}

std::string TestUtilsExtension::Version() const {
#ifdef EXT_VERSION_TEST_UTILS
	return EXT_VERSION_TEST_UTILS;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void test_utils_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::TestUtilsExtension>();
}

DUCKDB_EXTENSION_API const char *test_utils_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
