
#include <duckdb.hpp>
#include <algorithm>
#include <fstream>
#include <iostream>
#include <string>

#include "state.hpp"
#include "utils/serialization_helpers.hpp"
#include "utils/file_writer.hpp"
#include "utils/logger.hpp"
#include "utils/misc.hpp"

#include "utils/compatibility.hpp"

#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/common/types/uuid.hpp>
#include <duckdb/parser/parser.hpp>
#include <duckdb/parser/parsed_data/transaction_info.hpp>
#include <duckdb/parser/statement/logical_plan_statement.hpp>
#include <duckdb/planner/operator/logical_simple.hpp>
#include <duckdb/planner/planner.hpp>

namespace duckdb {

static void SerializeQueryStatements(Connection &con, BinarySerializer &serializer, SQLLogicQuery &current_query,
                                     TUStorageExtensionInfo &state, Parser &parser,
                                     TestDrivenTransactionState &test_driven_transaction_state);

constexpr const char *tag_prefix = "-- bwc_tag:";

bool SerializeQueriesPlansFromFile(ClientContext &context, const vector<Value> &params) {
	const auto input_file = params[0].GetValue<string>();
	const auto output_file = params[1].GetValue<string>();

	std::ifstream queries_if(input_file);
	if (!queries_if) {
		throw InvalidInputException("Could not open input file: '%s'", input_file.c_str());
	}

	string line;
	vector<SQLLogicQuery> queries;
	std::ostringstream current_query;
	std::map<string, string> query_flags;
	static const uint32_t tag_prefix_length = strlen(tag_prefix);
	bool is_first_query_line = true;
	while (std::getline(queries_if, line)) {
		if (line.rfind(tag_prefix, 0) != 0) {
			if (is_first_query_line) {
				is_first_query_line = false;
			} else {
				current_query << "\n";
			}
			current_query << line;
			continue;
		}

		auto tag_key_value = line.substr(tag_prefix_length);
		if (tag_key_value == "end_query") {
			queries.push_back(SQLLogicQuery {current_query.str(), static_cast<uint32_t>(queries.size()), query_flags});
			current_query.str("");
			current_query.clear();
			query_flags.clear();
			is_first_query_line = true;
		} else {
			auto pos = tag_key_value.find('=');
			if (pos == string::npos) {
				query_flags[tag_key_value] = "";
			} else {
				auto key = tag_key_value.substr(0, pos);
				auto value = tag_key_value.substr(pos + 1);
				query_flags[key] = value;
			}
		}
	}

	TUFileWriter file_writer(context, output_file);

	BinarySerializer serializer(file_writer);
	{
		// Write the number of queries to the output file
		serializer.Begin();
		serializer.WriteProperty(100, "count", static_cast<int32_t>(queries.size()));
		serializer.End();
	}

	Connection con(*context.db);

	auto &state = TUStorageExtensionInfo::GetState(*context.db);
	TestDrivenTransactionState test_driven_transaction_state = TestDrivenTransactionState::NONE;
	// Now run each query and serialize the plan to the output file.
	for (auto &slq : queries) {
		Parser parser;
		try {
			parser.ParseQuery(slq.query);
			slq.nb_statements = parser.statements.size();
		} catch (const std::exception &e) {
			if (slq.ExpectSuccess()) {
				LOG_ERROR("Failed to parse query '" << slq.query << "': " << e.what());
			}

			slq.can_deserialize_plan = false;
			slq.can_parse_query = false;
			serializer.Begin();
			slq.Serialize(serializer);
			serializer.End();

			auto result = con.context->Query(slq.query, false);
			state.AddQuery(slq);
			state.AddResult(make_uniq<SerializedResult>(slq.uuid, *result));
			continue;
		}

		try {
			SerializeQueryStatements(con, serializer, slq, state, parser, test_driven_transaction_state);
		} catch (const std::exception &e) {
			LOG_ERROR("Failed to serialize query #" << slq.query_idx << " '" << slq.query << "': " << e.what());
			throw;
		}

		if (!slq.load_db_name.empty()) {
			bool need_tx = test_driven_transaction_state == TestDrivenTransactionState::NONE;
			UseDBAndDetachOthers(con, slq.load_db_name, need_tx);
		}
	}

	file_writer.Sync();
	file_writer.Close();

	// Detach all databases attached during the query
	DetachAllDatabases(context);

	return true;
}

static void SerializeQueryStatements(Connection &con, BinarySerializer &serializer, SQLLogicQuery &slq,
                                     TUStorageExtensionInfo &state, Parser &parser,
                                     TestDrivenTransactionState &test_driven_transaction_state) {
	const auto &query = slq.query;
	idx_t statement_idx = 0;

	if (parser.statements.size() > 1) {
		LOG_INFO("Serializing query " << UUIDToString(slq.uuid) << " with " << parser.statements.size()
		                              << " statements: '" << query << "'");
	} else {
		LOG_INFO("Serializing query " << UUIDToString(slq.uuid) << ": '" << query << "'");
	}

	for (auto &statement : parser.statements) {
		if (test_driven_transaction_state == TestDrivenTransactionState::NONE) {
			LOG_DEBUG("Beginning new implicit transaction for statement #" << statement_idx << " of query #"
			                                                               << slq.query_idx);
			con.BeginTransaction();
		}

		Planner planner(*con.context);
		bool plan_created = false;
		try {
			planner.CreatePlan(std::move(statement));
			plan_created = !!planner.plan;
		} catch (const std::exception &e) {
			if (slq.ExpectSuccess()) {
				LOG_ERROR("Failed to plan query '" << query << "': " << e.what());
			}
		}

		if (plan_created) {
			const auto &op = *planner.plan;
			const auto type = op.type;
			LOG_DEBUG("Planned statement #" << statement_idx << " of query #" << slq.query_idx
			                                << ": plan type=" << LogicalOperatorToString(type));

			// Manually handle transaction statements
			if (type == LogicalOperatorType::LOGICAL_TRANSACTION) {
				auto tx_type = op.Cast<LogicalSimple>().info->Cast<TransactionInfo>().type;
				UpdateTxStateFromTxType(tx_type, test_driven_transaction_state);
				slq.transaction_type = tx_type;
			}
		} else {
			LOG_DEBUG("Could not plan statement #" << statement_idx << " of query #" << slq.query_idx);
		}

		// If planning failed, we also can't deserialize
		slq.can_deserialize_plan = slq.can_deserialize_plan && !slq.should_skip_query && plan_created;

		if (parser.statements.size() > 1 && !slq.can_deserialize_plan && statement_idx > 0) {
			throw InternalException(
			    "Cannot serialize query with multiple statements when subsequent statement cannot be serialized");
		}

		serializer.Begin();
		slq.Serialize(serializer);
		serializer.End();
		state.AddQuery(slq);

		if (slq.should_skip_query) {
			auto result = make_uniq<SerializedResult>(slq.uuid);
			try {
				UpdateTxStateAfterStatement(con, test_driven_transaction_state, true);
			} catch (const Exception &e) {
				if (slq.ExpectSuccess()) {
					throw;
				}

				result->error = ErrorData(e);
				result->success = false;
			}

			state.AddResult(std::move(result));
			continue;
		}

		if (slq.can_deserialize_plan) {
			serializer.Begin();
			planner.plan->Serialize(serializer);
			serializer.End();
		}

		{
			// Now run the query and serialize the result
			unique_ptr<QueryResult> result;
			// Some statements raise an error when executed, from the serialization plan
			auto type = LogicalOperatorType::LOGICAL_INVALID;
			if (slq.can_deserialize_plan) {
				// If we can deserialize the plan, we execute it
				type = planner.plan->type;
				planner.plan->ResolveOperatorTypes();
				result = con.context->Query(make_uniq<LogicalPlanStatement>(std::move(planner.plan)), false);
			} else {
				result = con.context->Query(query, false);
			}

			if (result->HasError() && slq.ExpectSuccess()) {
				LOG_ERROR("Query '" << query << "' type='" << LogicalOperatorToString(type) << "', executed from "
				                    << (slq.can_deserialize_plan ? "plan" : "SQL")
				                    << " failed with message: " << result->GetError());
			} else if (!result->HasError() && !slq.ExpectSuccess()) {
				LOG_ERROR("Query '" << query << "' type='" << LogicalOperatorToString(type) << "', executed from "
				                    << (slq.can_deserialize_plan ? "plan" : "SQL")
				                    << " succeeded but was expected to fail.");
			} else {
				LOG_DEBUG("Executed statement #"
				          << statement_idx << " of query #" << slq.query_idx << " from "
				          << (slq.can_deserialize_plan ? "plan" : "SQL")
				          << (slq.ExpectSuccess() ? " successfully." : " with expected failure."));
			}

			state.AddResult(make_uniq<SerializedResult>(slq.uuid, *result));
		}

		UpdateTxStateAfterStatement(con, test_driven_transaction_state);

		if (!slq.can_deserialize_plan) {
			// At this point we would have executed the whole query so we must not try to serialized further
			// statements
			break;
		}

		++statement_idx;
	}
}

} // namespace duckdb
