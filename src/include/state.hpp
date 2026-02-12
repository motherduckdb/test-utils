#pragma once

#include <string>

#include <duckdb/common/types/uuid.hpp>
#include <duckdb/storage/storage_extension.hpp>

#include "utils/serialization_helpers.hpp"

namespace duckdb {
const static std::string STORAGE_EXTENSION_KEY = "test_utils_storage";

class TUStorageExtensionInfo : public StorageExtensionInfo {
public:
	static TUStorageExtensionInfo &GetState(const DatabaseInstance &instance);

	void AddQuery(const SQLLogicQuery &query);

	void AddResult(unique_ptr<SerializedResult> &&result);

	const SerializedResult &GetResult(hugeint_t uuid);

	const SQLLogicQuery &GetQuery(hugeint_t uuid);

	const std::vector<hugeint_t> &GetOrderedResultsUuids() const;

private:
	std::mutex mutex;
	std::map<hugeint_t, SQLLogicQuery> queries;
	std::map<hugeint_t, unique_ptr<SerializedResult>> results;
	std::vector<hugeint_t> ordered_results_uuids;
};

} // namespace duckdb
