#pragma once

#include <map>
#include <duckdb/common/enums/logical_operator_type.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/main/query_result.hpp>

namespace duckdb {
bool CanExecuteSerializedOperator(const std::string &, LogicalOperatorType);

struct SerializedPlan {
	MemoryStream serialized_plan;
	hugeint_t uuid;
};

struct SerializedResult {
	SerializedResult() = default;
	SerializedResult(const hugeint_t &, QueryResult &query_result);

	void Serialize(Serializer &serializer) const;
	static unique_ptr<SerializedResult> Deserialize(Deserializer &deserializer);

	idx_t RowCount() const;
	idx_t ColumnCount() const;
	Value GetValue(idx_t column, idx_t index) const;

	hugeint_t uuid;
	bool success;
	ErrorData error;
	vector<LogicalType> types;
	vector<string> names;
	vector<unique_ptr<DataChunk>> chunks;
};

enum class SortStyle : uint8_t { NO_SORT, ROW_SORT, VALUE_SORT };
enum class ExpectedResultType : uint8_t { UNKNOWN, SUCCESS, ERROR };

class SQLLogicQuery {
public:
	SQLLogicQuery() = default;
	SQLLogicQuery(const string &_query, const std::map<string, string> &_flags);

	void Serialize(Serializer &serializer) const;

	static unique_ptr<SQLLogicQuery> Deserialize(Deserializer &deserializer);

	struct QueryFlags {
		ExpectedResultType expected_result_type = ExpectedResultType::UNKNOWN;
		SortStyle sort_style = SortStyle::NO_SORT;
		bool original_sqlite_test = false; // FIXME
	};

	bool ExpectSuccess() const;

	string query;
	bool can_deserialize_plan = true;
	uint32_t nb_statements;
	hugeint_t uuid;
	QueryFlags flags;
};

} // namespace duckdb
