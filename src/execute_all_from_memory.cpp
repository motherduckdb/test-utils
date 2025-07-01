#include <duckdb.hpp>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/parser/statement/logical_plan_statement.hpp>

#include "state.hpp"

namespace duckdb {

static void SerializeResult(unique_ptr<QueryResult>, SerializedResult &);

bool ExecuteAllPlansFromMemory(ClientContext &context) {
	Connection con(*context.db);

	auto &state = TUStorageExtensionInfo::GetState(*context.db);
	while (state.HasPlan()) {
		auto plan = state.PopPlan();
		plan.serialized_plan.Rewind();
		con.BeginTransaction();
		BinaryDeserializer deserializer(plan.serialized_plan);
		deserializer.Set<ClientContext &>(*con.context);
		deserializer.Begin();
		auto deserialized_plan = LogicalOperator::Deserialize(deserializer);
		deserializer.End();

		deserialized_plan->ResolveOperatorTypes();
		auto deserialized_results =
		    con.context->Query(make_uniq<LogicalPlanStatement>(std::move(deserialized_plan)), false);
		if (deserialized_results->HasError()) {
			std::cerr << "Query failed with message: " << deserialized_results->GetError() << std::endl;
		}

		SerializedResult result;
		SerializeResult(std::move(deserialized_results), result);
		state.AddResult(plan.uuid, std::move(result));
		con.Rollback();
	}

	return true;
}

static void SerializeResult(unique_ptr<QueryResult> query_result, SerializedResult &serialized_result) {
	serialized_result.success = !query_result->HasError();
	if (!serialized_result.success) {
		serialized_result.error = query_result->GetErrorObject();
		return;
	}

	serialized_result.types = query_result->types;
	serialized_result.names = query_result->names;

	while (true) {
		auto chunk = query_result->Fetch();
		if (!chunk) {
			break;
		}

		serialized_result.chunks.push_back(std::move(chunk));
	}
}

} // namespace duckdb
