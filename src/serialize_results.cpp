#include <duckdb.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/buffered_file_reader.hpp>
#include <duckdb/common/serializer/binary_deserializer.hpp>

#include "state.hpp"
#include "utils/logger.hpp"
#include "utils/misc.hpp"

namespace duckdb {

bool SerializeResults(ClientContext &context, const vector<Value> &params) {
	const auto output_file = params[0].GetValue<string>();

	BufferedFileWriter file_writer(context.db->GetFileSystem(), output_file);

	auto &state = TUStorageExtensionInfo::GetState(*context.db);
	const auto &results_uuids = state.GetOrderedResultsUuids();

	const int32_t count = static_cast<int32_t>(results_uuids.size());

	BinarySerializer serializer(file_writer);
	serializer.Begin();
	serializer.WriteProperty(100, "count", count);
	serializer.End();

	for (const auto &result_uuid : results_uuids) {
		LOG_DEBUG("Serializing result for query # " << UUIDToString(result_uuid) << ")");
		auto &in_mem_query = state.GetQuery(result_uuid);
		serializer.Begin();
		in_mem_query.Serialize(serializer);
		serializer.End();

		auto &in_mem_result = state.GetResult(result_uuid);
		serializer.Begin();
		in_mem_result.Serialize(serializer);
		serializer.End();
	}

	file_writer.Sync();
	file_writer.Close();
	return true;
}
} // namespace duckdb
