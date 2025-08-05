
#include <duckdb.hpp>
#include <algorithm>
#include <fstream>
#include <iostream>
#include <string>

#include "state.hpp"
#include "utils/serialization_helpers.hpp"

#include <duckdb/parser/parser.hpp>
#include <duckdb/planner/planner.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/types/uuid.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/parser/statement/logical_plan_statement.hpp>

namespace duckdb {

static bool SerializeQueriesPlans(ClientContext &, vector<SQLLogicQuery> &, const string &);

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
			queries.push_back(SQLLogicQuery {current_query.str(), query_flags});
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

	return SerializeQueriesPlans(context, queries, output_file);
}

static bool SerializeQueriesPlans(ClientContext &context, vector<SQLLogicQuery> &queries, const string &output_file) {
	BufferedFileWriter file_writer(context.db->GetFileSystem(), output_file,
	                               BufferedFileWriter::DEFAULT_OPEN_FLAGS | FileOpenFlags::FILE_FLAGS_APPEND);

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
		const auto &query = current_query.query;
		Parser p;
		p.ParseQuery(query);

		Planner planner(*con.context);
		idx_t statement_idx = 0;
		for (auto &statement : p.statements) {
			con.BeginTransaction();
			bool failed_to_plan = false;
			try {
				planner.CreatePlan(std::move(statement));
			} catch (const Exception &e) {
				if (current_query.ExpectSuccess()) {
					std::cout << "Failed to plan query '" << query << "': " << e.what() << std::endl;
				}
				current_query.can_deserialize_plan = false;
				failed_to_plan = true;
			}

			current_query.can_deserialize_plan =
			    !failed_to_plan && CanExecuteSerializedOperator(query, planner.plan->type);
			current_query.nb_statements = p.statements.size();
			if (p.statements.size() > 1 && !current_query.can_deserialize_plan && statement_idx > 0) {
				throw InternalException(
				    "Cannot serialize query with multiple statements when subsequent statement cannot be serialized");
			}

			serializer.Begin();
			current_query.Serialize(serializer);
			serializer.End();
			state.AddQuery(current_query);

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
					std::cout << "Query '" << query << "' type='" << LogicalOperatorToString(type)
					          << "' failed with message: " << result->GetError() << std::endl;
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

	file_writer.Sync();
	return true;
}

} // namespace duckdb
