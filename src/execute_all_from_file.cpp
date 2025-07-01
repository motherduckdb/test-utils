#include <duckdb.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/buffered_file_reader.hpp>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/parser/statement/logical_plan_statement.hpp>

#include <fstream>

#include "state.hpp"

namespace duckdb {

static void SerializeResult(unique_ptr<QueryResult>, const hugeint_t &, BufferedFileWriter &);

bool ExecuteAllPlansFromFile(ClientContext &context, const vector<Value> &params) {
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

	Connection con(*context.db);

	for (int32_t i = 0; i < count; ++i) {
		con.BeginTransaction();
		BinaryDeserializer deserializer(file_reader);
		deserializer.Set<ClientContext &>(*con.context);
		deserializer.Begin();
		auto uuid = deserializer.ReadProperty<hugeint_t>(100, "uuid");
		auto plan = LogicalOperator::Deserialize(deserializer);
		deserializer.End();

		plan->ResolveOperatorTypes();
		auto results = con.context->Query(make_uniq<LogicalPlanStatement>(std::move(plan)), false);
		if (results->HasError()) {
			std::cerr << "Query failed with message: " << results->GetError() << std::endl;
		}

		SerializeResult(std::move(results), uuid, file_writer);
		con.Rollback();
	}

	file_writer.Sync();

	return true;
}

static void SerializeResult(unique_ptr<QueryResult> results, const hugeint_t &uuid, BufferedFileWriter &file_writer) {
	BinarySerializer serializer(file_writer);
	serializer.Begin();
	serializer.WriteProperty(200, "uuid", uuid);
	serializer.WriteProperty(201, "success", !results->HasError());
	if (results->HasError()) {
		// TODO - serialize the error object
		auto error = results->GetErrorObject();
		error.ConvertErrorToJSON();
		serializer.WriteProperty(202, "error", error.RawMessage());
	} else {
		serializer.WriteProperty(203, "types", results->types);
		serializer.WriteProperty(204, "names", results->names);
		vector<unique_ptr<DataChunk>> chunks;
		while (true) {
			auto chunk = results->Fetch();
			if (!chunk) {
				break;
			}
			chunks.push_back(std::move(chunk));
		}
		serializer.WriteProperty(205, "chunks_count", static_cast<uint32_t>(chunks.size()));
		for (const auto &chunk : chunks) {
			chunk->Serialize(serializer);
		}
	}

	serializer.End();
}

} // namespace duckdb
