#pragma once

#include <string>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/common/types/uuid.hpp>
#include <duckdb/storage/storage_extension.hpp>
#include <duckdb/main/connection.hpp>

namespace duckdb {
const static std::string STORAGE_EXTENSION_KEY = "test_utils_storage";

struct SerializedPlan {
	MemoryStream serialized_plan;
	hugeint_t uuid;
};

struct SerializedResult {
	hugeint_t uuid;
	bool success;
	ErrorData error;
	vector<LogicalType> types;
	vector<string> names;
	vector<unique_ptr<DataChunk>> chunks;
};

class TUStorageExtensionInfo : public StorageExtensionInfo {
public:
	static TUStorageExtensionInfo &GetState(const DatabaseInstance &instance);

	void PushPlan(SerializedPlan &&plan);

	SerializedPlan PopPlan();

	bool HasPlan();

	void AddResult(hugeint_t uuid, SerializedResult &&result);

	const SerializedResult &GetResult(hugeint_t uuid);

private:
	std::mutex mutex;
	std::queue<SerializedPlan> plans;
	std::map<hugeint_t, SerializedResult> results;
};

} // namespace duckdb
