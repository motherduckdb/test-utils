#include "utils/serialization_helpers.hpp"

#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/deserializer.hpp>
#include <duckdb/common/types/uuid.hpp>

namespace duckdb {

bool CanExecuteSerializedOperator(const std::string &query, LogicalOperatorType type) {
	switch (type) {
	// DuckSchemaEntry::AddEntryInternal called but this database is not marked as modified
	case LogicalOperatorType::LOGICAL_CREATE_TABLE:
	case LogicalOperatorType::LOGICAL_CREATE_MACRO:

	// Attempting to commit a transaction that is read-only but has made changes - this should not be possible
	case LogicalOperatorType::LOGICAL_INSERT:

	// Not implemented Error: PLAN_STATEMENT
	case LogicalOperatorType::LOGICAL_CREATE_VIEW:

	// Serialization Error: Unsupported type for deserialization of LogicalOperator!
	case LogicalOperatorType::LOGICAL_PRAGMA:
		return false;
	default:
		return query.find("CALL dbgen") == string::npos;
	}
}

SerializedResult::SerializedResult(const hugeint_t &_uuid, QueryResult &query_result) {
	uuid = _uuid;
	success = !query_result.HasError();
	if (!success) {
		error = query_result.GetErrorObject();
		return;
	}

	types = query_result.types;
	names = query_result.names;

	while (true) {
		auto chunk = query_result.Fetch();
		if (!chunk) {
			break;
		}

		chunks.push_back(std::move(chunk));
	}
}

void SerializedResult::Serialize(Serializer &serializer) const {
	serializer.WriteProperty(200, "uuid", uuid);
	serializer.WriteProperty(201, "success", success);

	if (success) {
		serializer.WriteProperty(203, "types", types);
		serializer.WriteProperty(204, "names", names);
		serializer.WriteProperty(205, "chunks_count", static_cast<uint32_t>(chunks.size()));

		for (const auto &chunk : chunks) {
			chunk->Serialize(serializer);
		}
	} else {
		// TODO - serialize the error object
		auto json_error = error;
		json_error.ConvertErrorToJSON();
		serializer.WriteProperty(202, "error", json_error.RawMessage());
	}
}

unique_ptr<SerializedResult> SerializedResult::Deserialize(Deserializer &deserializer) {
	auto result = make_uniq<SerializedResult>();
	result->uuid = deserializer.ReadProperty<hugeint_t>(200, "uuid");

	result->success = deserializer.ReadProperty<bool>(201, "success");
	if (!result->success) {
		result->error = ErrorData(deserializer.ReadProperty<string>(202, "error"));
		return result;
	}

	result->types = deserializer.ReadProperty<vector<LogicalType>>(203, "types");
	result->names = deserializer.ReadProperty<vector<string>>(204, "names");

	const auto chunks_count = deserializer.ReadProperty<uint32_t>(205, "chunks_count");
	for (uint32_t i = 0; i < chunks_count; ++i) {
		auto chunk = make_uniq<DataChunk>();
		chunk->Deserialize(deserializer);
		result->chunks.push_back(std::move(chunk));
	}

	return result;
}

idx_t SerializedResult::RowCount() const {
	idx_t count = 0;
	for (const auto &chunk : chunks) {
		count += chunk->size();
	}
	return count;
}

idx_t SerializedResult::ColumnCount() const {
	return chunks.empty() ? 0 : chunks[0]->ColumnCount();
}

Value SerializedResult::GetValue(idx_t column, idx_t index) const {
	idx_t row_index = index;
	for (const auto &chunk : chunks) {
		if (row_index >= chunk->size()) {
			row_index -= chunk->size();
			continue;
		}
		if (column >= chunk->ColumnCount()) {
			throw InternalException("Column index out of bounds in SerializedResult::GetValue");
		}
		return chunk->GetValue(column, row_index);
	}

	throw InternalException("Row index out of bounds in SerializedResult::GetValue");
}

SQLLogicQuery::SQLLogicQuery(const string &_query, const std::map<string, string> &_flags) {
	query = _query;
	uuid = UUID::GenerateRandomUUID();
	flags.sort_style = SortStyle::NO_SORT;

	auto it = _flags.find("sort");
	if (it != _flags.end()) {
		if (it->second == "row_sort") {
			flags.sort_style = SortStyle::ROW_SORT;
		} else if (it->second == "value_sort") {
			flags.sort_style = SortStyle::VALUE_SORT;
		}
	}

	it = _flags.find("expected_result");
	if (it != _flags.end()) {
		if (it->second == "error") {
			flags.expected_result_type = ExpectedResultType::ERROR;
		} else if (it->second == "success") {
			flags.expected_result_type = ExpectedResultType::SUCCESS;
		} else {
			flags.expected_result_type = ExpectedResultType::UNKNOWN;
		}
	}
}

bool SQLLogicQuery::ExpectSuccess() const {
	return flags.expected_result_type == ExpectedResultType::SUCCESS;
}

void SQLLogicQuery::Serialize(Serializer &serializer) const {
	serializer.WriteProperty(101, "uuid", uuid);
	serializer.WriteProperty(102, "query", query);
	serializer.WriteProperty(103, "can_deserialize_plan", can_deserialize_plan);
	serializer.WriteProperty(104, "nb_statements", nb_statements);

	// Write flags
	serializer.WriteProperty(105, "expected_result_type", static_cast<uint8_t>(flags.expected_result_type));
	serializer.WriteProperty(106, "sort_style", static_cast<uint8_t>(flags.sort_style));
}

unique_ptr<SQLLogicQuery> SQLLogicQuery::Deserialize(Deserializer &deserializer) {
	auto query = make_uniq<SQLLogicQuery>();
	query->uuid = deserializer.ReadProperty<hugeint_t>(101, "uuid");
	query->query = deserializer.ReadProperty<string>(102, "query");
	query->can_deserialize_plan = deserializer.ReadProperty<bool>(103, "can_deserialize_plan");
	query->nb_statements = deserializer.ReadProperty<bool>(104, "nb_statements");

	// Read flags
	query->flags.expected_result_type =
	    static_cast<ExpectedResultType>(deserializer.ReadProperty<uint8_t>(105, "expected_result_type"));
	query->flags.sort_style = static_cast<SortStyle>(deserializer.ReadProperty<uint8_t>(106, "sort_style"));
	return query;
}

} // namespace duckdb
