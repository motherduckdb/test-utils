#include <duckdb.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/buffered_file_reader.hpp>
#include <duckdb/common/serializer/binary_deserializer.hpp>

#include "state.hpp"

namespace duckdb {
bool SerializeResults(ClientContext &context, const vector<Value> &params) {
	const auto input_file = params[0].GetValue<string>();
	const auto output_file = params[1].GetValue<string>();

	BufferedFileReader file_reader(context.db->GetFileSystem(), input_file.c_str());
	BufferedFileWriter file_writer(context.db->GetFileSystem(), output_file);

	BinaryDeserializer deserializer(file_reader);
	deserializer.Begin();
	auto count = deserializer.ReadProperty<int32_t>(100, "count");
	deserializer.End();

	BinarySerializer serializer(file_writer);
	serializer.Begin();
	serializer.WriteProperty(100, "count", count);
	serializer.End();

	auto &state = TUStorageExtensionInfo::GetState(*context.db);
	for (int32_t i = 0; i < count; ++i) {
		deserializer.Begin();
		auto file_result = SerializedResult::Deserialize(deserializer);
		deserializer.End();

		auto &in_mem_query = state.GetQuery(file_result->uuid);
		serializer.Begin();
		in_mem_query.Serialize(serializer);
		serializer.End();

		auto &in_mem_result = state.GetResult(file_result->uuid);
		serializer.Begin();
		in_mem_result.Serialize(serializer);
		serializer.End();
	}

	file_writer.Sync();
	return true;
}
} // namespace duckdb
