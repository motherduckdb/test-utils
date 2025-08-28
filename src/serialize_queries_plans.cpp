
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

#include <duckdb/parser/parser.hpp>
#include <duckdb/planner/planner.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/types/uuid.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/parser/statement/logical_plan_statement.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_aggregate.hpp>
#include <duckdb/planner/operator/logical_pragma.hpp>
#include <duckdb/planner/operator/logical_set.hpp>
#include <duckdb/planner/operator/logical_limit.hpp>

namespace duckdb {

static void SerializeQueryStatements(Connection &con, BinarySerializer &serializer, SQLLogicQuery &current_query,
                                     TUStorageExtensionInfo &state, Parser &parser);

bool CanExecuteSerializedPlan(const std::string &, const LogicalOperator &);

bool ShouldSkipQuery(const LogicalOperator &op);

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
	// Now run each query and serialize the plan to the output file.
	for (auto &current_query : queries) {
		Parser parser;
		try {
			parser.ParseQuery(current_query.query);
			current_query.nb_statements = parser.statements.size();
		} catch (const std::exception &e) {
			if (current_query.ExpectSuccess()) {
				LOG_ERROR("Failed to parse query '" << current_query.query << "': " << e.what());
			}

			current_query.can_deserialize_plan = false;
			current_query.can_parse_query = false;
			serializer.Begin();
			current_query.Serialize(serializer);
			serializer.End();

			auto result = con.context->Query(current_query.query, false);
			state.AddQuery(current_query);
			state.AddResult(make_uniq<SerializedResult>(current_query.uuid, *result));
			continue;
		}

		try {
			SerializeQueryStatements(con, serializer, current_query, state, parser);
		} catch (const std::exception &e) {
			LOG_ERROR("Failed to serialize query #" << current_query.query_idx << " '" << current_query.query)
			throw;
		}
	}

	file_writer.Sync();
	file_writer.Close();

	// Detach all databases attached during the query
	DetachAllDatabases(context);

	return true;
}

static void SerializeQueryStatements(Connection &con, BinarySerializer &serializer, SQLLogicQuery &current_query,
                                     TUStorageExtensionInfo &state, Parser &parser) {
	const auto &query = current_query.query;
	idx_t statement_idx = 0;
	LOG_INFO("Serializing query with " << parser.statements.size() << " statements: '" << query << "'");
	for (auto &statement : parser.statements) {
		con.BeginTransaction();
		Planner planner(*con.context);
		try {
			planner.CreatePlan(std::move(statement));
			current_query.can_deserialize_plan = !!planner.plan;
		} catch (const std::exception &e) {
			if (current_query.ExpectSuccess()) {
				LOG_ERROR("Failed to plan query '" << query << "': " << e.what());
			}

			current_query.can_deserialize_plan = false;
		}

		// Some queries should not be executed at all (cf. `ShouldSkipQuery`)
		current_query.should_skip_query = current_query.can_deserialize_plan && ShouldSkipQuery(*planner.plan);

		// Some don't serialize well, so execute them directly from SQL
		current_query.can_deserialize_plan = !current_query.should_skip_query && current_query.can_deserialize_plan &&
		                                     CanExecuteSerializedPlan(query, *planner.plan);

		if (parser.statements.size() > 1 && !current_query.can_deserialize_plan && statement_idx > 0) {
			throw InternalException(
			    "Cannot serialize query with multiple statements when subsequent statement cannot be serialized");
		}

		serializer.Begin();
		current_query.Serialize(serializer);
		serializer.End();
		state.AddQuery(current_query);

		if (current_query.should_skip_query) {
			con.Rollback();
			continue;
		}

		if (current_query.can_deserialize_plan) {
			serializer.Begin();
			planner.plan->Serialize(serializer);
			serializer.End();
		}

		{
			// Now run the query and serialize the result
			unique_ptr<QueryResult> result;
			// Some statements raise an error when executed, from the serialization plan
			auto type = LogicalOperatorType::LOGICAL_INVALID;
			if (current_query.can_deserialize_plan) {
				// If we can deserialize the plan, we execute it
				type = planner.plan->type;
				planner.plan->ResolveOperatorTypes();
				result = con.context->Query(make_uniq<LogicalPlanStatement>(std::move(planner.plan)), false);
			} else {
				result = con.context->Query(query, false);
			}

			if (result->HasError() && current_query.ExpectSuccess()) {
				LOG_ERROR("Query '" << query << "' type='" << LogicalOperatorToString(type)
				                    << "' failed with message: " << result->GetError());
			}

			state.AddResult(make_uniq<SerializedResult>(current_query.uuid, *result));
		}

		con.Commit();

		if (!current_query.can_deserialize_plan) {
			// At this point we would have executed the whole query so we must not try to serialized further
			// statements
			break;
		}

		++statement_idx;
	}
}

bool PlanContains(const LogicalOperator &plan, bool (*predicate)(const LogicalOperator &)) {
	if (predicate(plan)) {
		return true;
	}
	for (const auto &child : plan.children) {
		if (PlanContains(*child, predicate)) {
			return true;
		}
	}
	return false;
}

/* ------------------------------------------------------------------------ *
 * ShouldSkipQuery - Check whether a given query should be skipped entirely *
 * ------------------------------------------------------------------------ */

bool ShouldSkipQuery(const LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_TRANSACTION:
		// We're not testing transaction semantics
		return true;
	case LogicalOperatorType::LOGICAL_PRAGMA: {
		auto &pragma_op = op.Cast<LogicalPragma>();
		if (pragma_op.info) {
			auto &name = pragma_op.info->function.name;
			if (name == "enable_verification" || name == "verify_external") {
				// This causes queries to fail with 'Not implemented Error: PLAN_STATEMENT'
				return true;
			}
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_SET: {
		auto &set_op = op.Cast<LogicalSet>();
		if (set_op.name == "threads" || set_op.name == "threads") {
			// Running `SET thread` - or `RESET threads` in the serializing function will sometimes cause
			// `std::__1::system_error: thread::join failed` error. We're not testing
			// thread settings here, so we can skip these queries.
			return true;
		}
		break;
	}
	default:
		break;
	}

	return false;
}

/* ---------------------------------------------------------------------
 * CanExecuteSerializedPlan
 *
 * Determine for a given plan whether we can:
 *    - serialize it
 *    - deserialize it
 *    - execute it
 * --------------------------------------------------------------------- */

bool IsNonExecutableSerializedExpression(const BaseExpression &expr) {
	// TODO - properly traverse expression and find functions by name
	auto expr_name = expr.GetName();
	if (
	    // INTERNAL Error: Attempting to commit a transaction that is read-only but has made changes - this should
	    // not be possible
	    expr_name.find("nextval(") != string::npos
	    // Not implemented Error: FIXME: export state serialize
	    || expr_name.find("finalize(") != string::npos) {
		return true;
	}
	return false;
}

bool HasNonExecutableSerializedExpression(const LogicalOperator &op) {
	for (const auto &expr : op.expressions) {
		if (IsNonExecutableSerializedExpression(*expr)) {
			return true;
		}
	}
	return false;
}

bool IsNonExecutableSerializedBoundLimitNode(const BoundLimitNode &op) {
	if (op.Type() == LimitNodeType::EXPRESSION_VALUE) {
		return IsNonExecutableSerializedExpression(op.GetValueExpression());
	} else if (op.Type() == LimitNodeType::EXPRESSION_PERCENTAGE) {
		return IsNonExecutableSerializedExpression(op.GetPercentageExpression());
	}
	return false;
}

bool IsNonExecutableSerializedLogicalOperator(const LogicalOperator &op) {
	switch (op.type) {
	// DuckSchemaEntry::AddEntryInternal called but this database is not marked as modified
	case LogicalOperatorType::LOGICAL_CREATE_TABLE:
	case LogicalOperatorType::LOGICAL_CREATE_INDEX:
	case LogicalOperatorType::LOGICAL_CREATE_MACRO:
	case LogicalOperatorType::LOGICAL_CREATE_SEQUENCE:
	case LogicalOperatorType::LOGICAL_CREATE_TYPE:

	// Attempting to do catalog changes on a transaction that is read-only - this should not be possible
	case LogicalOperatorType::LOGICAL_ALTER:
	case LogicalOperatorType::LOGICAL_CREATE_SCHEMA:

	// Attempting to commit a transaction that is read-only but has made changes - this should not be possible
	case LogicalOperatorType::LOGICAL_DELETE:
	case LogicalOperatorType::LOGICAL_DROP:
	case LogicalOperatorType::LOGICAL_INSERT:
	case LogicalOperatorType::LOGICAL_UPDATE:

	// Not implemented Error: PLAN_STATEMENT
	case LogicalOperatorType::LOGICAL_CREATE_VIEW:

	// Serialization Error: Unsupported type for deserialization of LogicalOperator!
	case LogicalOperatorType::LOGICAL_PRAGMA:
	case LogicalOperatorType::LOGICAL_PREPARE:
	case LogicalOperatorType::LOGICAL_EXECUTE:
	case LogicalOperatorType::LOGICAL_CREATE_SECRET:
		return true;

	case LogicalOperatorType::LOGICAL_LIMIT: {
		auto &op_limit = op.Cast<LogicalLimit>();
		return IsNonExecutableSerializedBoundLimitNode(op_limit.limit_val) ||
		       IsNonExecutableSerializedBoundLimitNode(op_limit.offset_val);
	}

	case LogicalOperatorType::LOGICAL_GET: {
		auto &op_get = op.Cast<LogicalGet>();
		return op_get.GetName() == "DBGEN" // CALL dbgen(...) DuckSchemaEntry::AddEntryInternal called but this database
		                                   // is not marked as modified
		       || op_get.GetName() == "DSDGEN"   // CALL dbgen(...) DuckSchemaEntry::AddEntryInternal called but this
		                                         // database is not marked as modified
		       || op_get.GetName() == "READ_CSV" // Not implemented Error: CSVReaderSerialize not implemented
		       || op_get.GetName() == "READ_CSV_AUTO"     // Not implemented Error: CSVReaderSerialize not implemented
		       || op_get.GetName() == "READ_JSON"         // Not implemented Error: JSONScan Serialize not implemented
		       || op_get.GetName() == "READ_JSON_AUTO"    // Not implemented Error: JSONScan Serialize not implemented
		       || op_get.GetName() == "READ_JSON_OBJECTS" // Not implemented Error: JSONScan Serialize not implemented
		       ||
		       op_get.GetName() == "READ_JSON_OBJECTS_AUTO" // Not implemented Error: JSONScan Serialize not implemented
		       || op_get.GetName() == "READ_NDJSON"         // Not implemented Error: JSONScan Serialize not implemented
		       || op_get.GetName() == "READ_NDJSON_OBJECTS" // Not implemented Error: JSONScan Serialize not implemented
		       || op_get.GetName() == "READ_NDJSON_AUTO";   // Not implemented Error: JSONScan Serialize not implemented
	}
	default:
		return false;
	}
}

bool CanExecuteSerializedPlan(const std::string &query, const LogicalOperator &plan) {
	if (query.find("EXPORT_STATE") != string::npos) {
		// TODO: traverse expression tree and find the EXPORT_STATE flag
		return false; // Not implemented Error: FIXME: export state serialize
	}

	// Detect known patterns
	return !PlanContains(plan, &IsNonExecutableSerializedLogicalOperator) &&
	       !PlanContains(plan, &HasNonExecutableSerializedExpression);
}

} // namespace duckdb
