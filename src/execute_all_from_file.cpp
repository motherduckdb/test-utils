#include <duckdb.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/buffered_file_reader.hpp>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/parser/statement/logical_plan_statement.hpp>

#include <fstream>

#include "state.hpp"
#include "utils/serialization_helpers.hpp"
#include "utils/file_writer.hpp"
#include "utils/logger.hpp"

namespace duckdb {

void SerializeResult(const SQLLogicQuery &sqll_query, Connection &con, BinarySerializer &serializer,
                     QueryResult &result);

bool ExecuteAllPlansFromFile(ClientContext &context, const vector<Value> &params) {
	const auto input_file = params[0].GetValue<string>();
	const auto output_file = params[1].GetValue<string>();

	BufferedFileReader file_reader(context.db->GetFileSystem(), input_file.c_str());
	TUFileWriter file_writer(context, output_file);

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

		if (sqll_query->should_skip_query) {
			LOG_DEBUG("Skipping query #" << i << ": '" << sqll_query->query << "'");
			serializer.Begin();
			// Serialize an empty result for skipped queries
			SerializedResult::SerializeSkipped(serializer);
			serializer.End();
			continue;
		}

		if (sqll_query->can_parse_query && sqll_query->can_deserialize_plan) {
			LOG_INFO("Executing query from plan #" << i << ": '" << sqll_query->query << "'");
			for (idx_t statement_idx = 0; statement_idx < sqll_query->nb_statements; ++statement_idx) {
				try {
					con.BeginTransaction();
					deserializer.Begin();
					auto plan = LogicalOperator::Deserialize(deserializer);
					deserializer.End();

					plan->ResolveOperatorTypes();
					auto result = con.context->Query(make_uniq<LogicalPlanStatement>(std::move(plan)), false);
					LOG_DEBUG("Executed statement #" << statement_idx << " of query #" << i << ": '"
					                                 << sqll_query->query << "' - "
					                                 << (result->HasError() ? "failed" : "succeeded"));
					SerializeResult(*sqll_query, con, serializer, *result);
				} catch (const Exception &e) {
					LOG_ERROR("Failed to execute statement #" << statement_idx << " of query #" << i << ": '"
					                                          << sqll_query->query << "': " << e.what())
					throw;
				}
			}
		} else {
			LOG_INFO("Executing query from SQL #" << i << ": '" << sqll_query->query << "'");
			try {
				con.BeginTransaction();
				auto result = con.context->Query(sqll_query->query, false);
				SerializeResult(*sqll_query, con, serializer, *result);
				LOG_DEBUG("Executed query #" << i << ": '" << sqll_query->query << "' - "
				                             << (result->HasError() ? "failed" : "succeeded"));
			} catch (const Exception &e) {
				LOG_ERROR("Failed to execute query #" << i << ": '" << sqll_query->query << "': " << e.what())
				throw;
			}
		}
	}

	file_writer.Sync();

	// Detach all databases attached during the query
	DatabaseManager &db_manager = context.db->GetDatabaseManager();
	auto databases = db_manager.GetDatabases(context);
	for (auto &db : databases) {
		auto &db_instance = db.get();
		auto name = db_instance.GetName();
		if (!db_instance.IsSystem() && !db_instance.IsTemporary() && name != "memory") {
			db_manager.DetachDatabase(context, name, OnEntryNotFound::THROW_EXCEPTION);
		}
	}

	return true;
}

void SerializeResult(const SQLLogicQuery &sqll_query, Connection &con, BinarySerializer &serializer,
                     QueryResult &result) {
	serializer.Begin();
	SerializedResult(sqll_query.uuid, result).Serialize(serializer);
	serializer.End();
	if (!result.HasError()) {
		con.Commit();
		return;
	}

	// Make sure the execution didn't cause DB to get invalidated.
	auto &db_inst = DatabaseInstance::GetDatabase(*con.context);
	if (ValidChecker::IsInvalidated(db_inst)) {
		result.ThrowError(); // We don't want to continue at this point.
	}

	con.Commit();
}

} // namespace duckdb
