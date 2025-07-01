
#include <duckdb.hpp>
#include <algorithm>
#include <fstream>
#include <iostream>
#include <string>

#include "state.hpp"

#include <duckdb/parser/parser.hpp>
#include <duckdb/planner/planner.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/types/uuid.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>

namespace duckdb {

inline void ltrim(std::string &s) {
	s.erase(s.begin(), std::find_if(s.begin(), s.end(), [](unsigned char ch) { return !std::isspace(ch); }));
}

inline void rtrim(std::string &s) {
	s.erase(std::find_if(s.rbegin(), s.rend(), [](unsigned char ch) { return !std::isspace(ch); }).base(), s.end());
}

inline void trim(std::string &s) {
	rtrim(s);
	ltrim(s);
}

static bool SerializeQueriesPlans(ClientContext &, const vector<string> &, const string &);

bool SerializeQueriesPlansFromFile(ClientContext &context, const vector<Value> &params) {
	const auto input_file = params[0].GetValue<string>();
	const auto output_file = params[1].GetValue<string>();

	std::ifstream queries_if(input_file);
	if (!queries_if) {
		throw InvalidInputException("Could not open input file: '%s'", input_file.c_str());
	}

	// Read everything in a vector of strings
	string current_query;
	vector<string> queries;
	while (std::getline(queries_if, current_query)) {
		trim(current_query);
		if (!current_query.empty()) {
			queries.push_back(std::move(current_query));
		}
	}

	return SerializeQueriesPlans(context, queries, output_file);
}

static bool SerializeQueriesPlans(ClientContext &context, const vector<string> &queries, const string &output_file) {
	BufferedFileWriter file_writer(context.db->GetFileSystem(), output_file);

	{
		// Write the number of queries to the output file
		BinarySerializer serializer(file_writer);
		serializer.Begin();
		serializer.WriteProperty(100, "count", static_cast<int32_t>(queries.size()));
		serializer.End();
	}

	Connection con(*context.db);

	auto &state = TUStorageExtensionInfo::GetState(*context.db);
	// Now run each line as a query and serialize the plan to the output file.
	for (const auto &current_query : queries) {
		auto uuid = UUIDv7::GenerateRandomUUID();
		con.BeginTransaction();
		Parser p;
		p.ParseQuery(current_query);

		Planner planner(*con.context);
		if (p.statements.size() == 0) {
			throw InvalidInputException("No statements found in query: '%s'", current_query.c_str());
		} else if (p.statements.size() > 1) {
			std::cout << "WARNING: " << p.statements.size() << " statements found in query: '" << current_query
			          << "' - will only consider the first one." << std::endl;
		}

		planner.CreatePlan(std::move(p.statements[0]));
		auto plan = std::move(planner.plan);

		auto start_file_size = file_writer.GetFileSize();
		BinarySerializer serializer(file_writer);
		serializer.Begin();
		serializer.WriteProperty(100, "uuid", uuid);
		plan->Serialize(serializer);
		serializer.End();

		auto written_size = file_writer.GetFileSize() - start_file_size;
		// TODO - maybe there's better way to avoid double serialization
		// we could just copy the data out to the file_writer stream
		duckdb::MemoryStream stream(written_size);
		BinarySerializer plan_serializer(stream);
		plan->Serialize(plan_serializer);
		plan_serializer.End();
		state.PushPlan(SerializedPlan {std::move(stream), uuid});

		con.Rollback();
	}

	file_writer.Sync();
	return true;
}

} // namespace duckdb
