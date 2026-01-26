#include "state.hpp"

#include <duckdb/main/database.hpp>

#include "utils/compatibility.hpp"
#include "utils/misc.hpp"

namespace duckdb {

TUStorageExtensionInfo &TUStorageExtensionInfo::GetState(const DatabaseInstance &instance) {
	auto &config = instance.config;
#if DUCKDB_VERSION_AT_MOST(1, 4, 4)
	auto it = config.storage_extensions.find(STORAGE_EXTENSION_KEY);
	if (it == config.storage_extensions.end()) {
		throw std::runtime_error("Fatal error: couldn't find the extension state.");
	}
	return *static_cast<TUStorageExtensionInfo *>(it->second->storage_info.get());
#else
	auto ext = StorageExtension::Find(config, STORAGE_EXTENSION_KEY);
	if (!ext) {
		throw std::runtime_error("Fatal error: couldn't find the extension state.");
	}
	return *static_cast<TUStorageExtensionInfo *>(ext->storage_info.get());
#endif
}

void TUStorageExtensionInfo::PushPlan(SerializedPlan &&plan) {
	std::lock_guard<std::mutex> lock(mutex);
	plans.push(std::move(plan));
}

SerializedPlan TUStorageExtensionInfo::PopPlan() {
	std::lock_guard<std::mutex> lock(mutex);
	if (plans.empty()) {
		throw InternalException("No plans available to pop.");
	}

	auto plan = std::move(plans.front());
	plans.pop();
	return plan;
}

bool TUStorageExtensionInfo::HasPlan() {
	std::lock_guard<std::mutex> lock(mutex);
	return !plans.empty();
}

void TUStorageExtensionInfo::AddResult(unique_ptr<SerializedResult> &&result) {
	std::lock_guard<std::mutex> lock(mutex);
	ordered_results_uuids.push_back(result->uuid);
	results.emplace(result->uuid, std::move(result));
}

void TUStorageExtensionInfo::AddQuery(const SQLLogicQuery &query) {
	std::lock_guard<std::mutex> lock(mutex);
	queries[query.uuid] = query;
}

const SerializedResult &TUStorageExtensionInfo::GetResult(hugeint_t uuid) {
	std::lock_guard<std::mutex> lock(mutex);
	auto it = results.find(uuid);
	if (it == results.end()) {
		throw InternalException("No result found for '%s'", UUIDToString(uuid));
	}
	return *it->second;
}

const std::vector<hugeint_t> &TUStorageExtensionInfo::GetOrderedResultsUuids() const {
	return ordered_results_uuids;
}

const SQLLogicQuery &TUStorageExtensionInfo::GetQuery(hugeint_t uuid) {
	std::lock_guard<std::mutex> lock(mutex);
	auto it = queries.find(uuid);
	if (it == queries.end()) {
		throw InternalException("No query found for '%s'", UUIDToString(uuid));
	}
	return it->second;
}

} // namespace duckdb
