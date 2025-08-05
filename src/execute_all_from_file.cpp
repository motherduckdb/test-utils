#include <duckdb.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/buffered_file_reader.hpp>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/parser/statement/logical_plan_statement.hpp>

#include <fstream>

#include "state.hpp"
#include "utils/serialization_helpers.hpp"

namespace duckdb {

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
		BinaryDeserializer deserializer(file_reader);
		deserializer.Set<ClientContext &>(*con.context);
		deserializer.Begin();
		auto sqll_query = SQLLogicQuery::Deserialize(deserializer);
		deserializer.End();

		if (sqll_query->can_deserialize_plan) {
			for (idx_t statement_idx = 0; statement_idx < sqll_query->nb_statements; ++statement_idx) {
				con.BeginTransaction();
				deserializer.Begin();
				auto plan = LogicalOperator::Deserialize(deserializer);
				deserializer.End();

				plan->ResolveOperatorTypes();
				auto results = con.context->Query(make_uniq<LogicalPlanStatement>(std::move(plan)), false);
				serializer.Begin();
				SerializedResult(sqll_query->uuid, *results).Serialize(serializer);
				serializer.End();
				con.Commit();
			}
		} else {
			con.BeginTransaction();
			auto results = con.context->Query(sqll_query->query, false);
			serializer.Begin();
			SerializedResult(sqll_query->uuid, *results).Serialize(serializer);
			serializer.End();
			con.Commit();
		}
	}

	file_writer.Sync();

	return true;
}

} // namespace duckdb
