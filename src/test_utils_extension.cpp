#define DUCKDB_EXTENSION_MAIN

#include "state.hpp"
#include "test_utils_extension.hpp"
#include "utils/helpers.hpp"
#include "utils/logger.hpp"

#include <duckdb.hpp>

namespace duckdb {

bool SerializeQueriesPlansFromFile(ClientContext &, const vector<Value> &);
bool ExecuteAllPlansFromFile(ClientContext &, const vector<Value> &);
bool CompareResults(ClientContext &, const vector<Value> &);
bool SerializeResults(ClientContext &, const vector<Value> &);

#if DUCKDB_VERSION_AT_MOST(1, 3, 2)
static void LoadInternal(DatabaseInstance &instance) {
#else
static void LoadInternal(ExtensionLoader &loader) {
	auto &instance = loader.GetDatabaseInstance();
#endif
	// Register the storage extension
	auto ext = duckdb::make_uniq<duckdb::StorageExtension>();
	ext->storage_info = duckdb::make_uniq<TUStorageExtensionInfo>();
	if (instance.config.storage_extensions.find(STORAGE_EXTENSION_KEY) != instance.config.storage_extensions.end()) {
		return;
	}

	set_log_level_from_env();

	instance.config.storage_extensions[STORAGE_EXTENSION_KEY] = std::move(ext);

	// Register the functions
	REGISTER_TF("serialize_queries_plans", SerializeQueriesPlansFromFile, 2);
	REGISTER_TF("execute_all_plans_from_file", ExecuteAllPlansFromFile, 2);
	REGISTER_TF("compare_results", CompareResults, 1);
	REGISTER_TF("compare_results", CompareResults, 2);
	REGISTER_TF("serialize_results", SerializeResults, 1);
}

#if DUCKDB_VERSION_AT_MOST(1, 3, 2)
void TestUtilsExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
#else
void TestUtilsExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
#endif

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

#if DUCKDB_VERSION_AT_MOST(1, 3, 2)
DUCKDB_EXTENSION_API void test_utils_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::TestUtilsExtension>();
}

DUCKDB_EXTENSION_API const char *test_utils_version() {
	return duckdb::DuckDB::LibraryVersion();
}

#else
DUCKDB_CPP_EXTENSION_ENTRY(test_utils, loader) {
	duckdb::LoadInternal(loader);
}
#endif
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
