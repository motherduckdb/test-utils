#include "state.hpp"

#include <duckdb/main/database.hpp>

namespace duckdb {

TUStorageExtensionInfo &TUStorageExtensionInfo::GetState(const DatabaseInstance &instance) {
	auto &config = instance.config;
	auto it = config.storage_extensions.find(STORAGE_EXTENSION_KEY);
	if (it == config.storage_extensions.end()) {
		throw std::runtime_error("Fatal error: couldn't find the UI extension state.");
	}
	return *static_cast<TUStorageExtensionInfo *>(it->second->storage_info.get());
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
	return std::move(plan);
}

bool TUStorageExtensionInfo::HasPlan() {
	std::lock_guard<std::mutex> lock(mutex);
	return !plans.empty();
}

void TUStorageExtensionInfo::AddResult(hugeint_t uuid, SerializedResult &&result) {
	std::lock_guard<std::mutex> lock(mutex);
	results[uuid] = std::move(result);
}

const SerializedResult &TUStorageExtensionInfo::GetResult(hugeint_t uuid) {
	std::lock_guard<std::mutex> lock(mutex);
	auto it = results.find(uuid);
	if (it == results.end()) {
		throw InternalException("No result found for '" + uuid.ToString() + "'");
	}
	return it->second;
}

} // namespace duckdb
