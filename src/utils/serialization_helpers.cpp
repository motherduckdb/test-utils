#include "utils/serialization_helpers.hpp"

#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/deserializer.hpp>
#include <duckdb/common/types/uuid.hpp>

namespace duckdb {

SerializedResult::SerializedResult(const hugeint_t &_uuid, QueryResult &query_result) : SerializedResult(_uuid) {
	is_skipped = false;
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

SerializedResult::SerializedResult(const hugeint_t &_uuid) {
	uuid = _uuid;
	is_skipped = true;
}

void SerializedResult::Serialize(Serializer &serializer) const {
	auto n = 200;
	serializer.WriteProperty(n, "uuid", uuid);
	serializer.WriteProperty(++n, "is_skipped", is_skipped);

	if (is_skipped) {
		return;
	}

	serializer.WriteProperty(++n, "success", success);

	if (success) {
		serializer.WriteProperty(++n, "types", types);
		serializer.WriteProperty(++n, "names", names);
		serializer.WriteProperty(++n, "chunks_count", static_cast<uint32_t>(chunks.size()));

		for (const auto &chunk : chunks) {
			chunk->Serialize(serializer);
		}
	} else {
		// TODO - serialize the error object
		auto json_error = error;
		serializer.WriteProperty(++n, "raw_error_msg", json_error.RawMessage());
		serializer.WriteProperty(++n, "error_type", json_error.Type());
	}
}

unique_ptr<SerializedResult> SerializedResult::Deserialize(Deserializer &deserializer) {
	auto result = make_uniq<SerializedResult>();
	auto n = 200;
	result->uuid = deserializer.ReadProperty<hugeint_t>(n, "uuid");
	result->is_skipped = deserializer.ReadProperty<bool>(++n, "is_skipped");
	if (result->is_skipped) {
		return result;
	}

	result->success = deserializer.ReadProperty<bool>(++n, "success");
	if (!result->success) {
		auto raw_msg = deserializer.ReadProperty<string>(++n, "raw_error_msg");
		auto type = deserializer.ReadProperty<ExceptionType>(++n, "error_type");
		result->error = ErrorData(type, raw_msg);
		return result;
	}

	result->types = deserializer.ReadProperty<vector<LogicalType>>(++n, "types");
	result->names = deserializer.ReadProperty<vector<string>>(++n, "names");

	const auto chunks_count = deserializer.ReadProperty<uint32_t>(++n, "chunks_count");
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

SQLLogicQuery::SQLLogicQuery(const string &_query, const uint32_t _query_idx, const std::map<string, string> &_flags) {
	query = _query;
	query_idx = _query_idx;
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

	it = _flags.find("load_db");
	if (it != _flags.end()) {
		load_db_name = it->second;
	}
}

bool SQLLogicQuery::ExpectSuccess() const {
	return flags.expected_result_type == ExpectedResultType::SUCCESS;
}

void SQLLogicQuery::Serialize(Serializer &serializer) const {
	auto n = 100;
	serializer.WriteProperty(++n, "uuid", uuid);
	serializer.WriteProperty(++n, "idx", query_idx);
	serializer.WriteProperty(++n, "query", query);
	serializer.WriteProperty(++n, "can_parse_query", can_parse_query);
	serializer.WriteProperty(++n, "should_skip_query", should_skip_query);
	serializer.WriteProperty(++n, "can_deserialize_plan", can_deserialize_plan);
	serializer.WriteProperty(++n, "nb_statements", nb_statements);
	serializer.WriteProperty(++n, "transaction_type", transaction_type);

	// Write flags
	serializer.WriteProperty(++n, "expected_result_type", static_cast<uint8_t>(flags.expected_result_type));
	serializer.WriteProperty(++n, "sort_style", static_cast<uint8_t>(flags.sort_style));
}

unique_ptr<SQLLogicQuery> SQLLogicQuery::Deserialize(Deserializer &deserializer) {
	auto n = 100;
	auto query = make_uniq<SQLLogicQuery>();
	query->uuid = deserializer.ReadProperty<hugeint_t>(++n, "uuid");
	query->query_idx = deserializer.ReadProperty<uint32_t>(++n, "idx");
	query->query = deserializer.ReadProperty<string>(++n, "query");
	query->can_parse_query = deserializer.ReadProperty<bool>(++n, "can_parse_query");
	query->should_skip_query = deserializer.ReadProperty<bool>(++n, "should_skip_query");
	query->can_deserialize_plan = deserializer.ReadProperty<bool>(++n, "can_deserialize_plan");
	query->nb_statements = deserializer.ReadProperty<uint32_t>(++n, "nb_statements");
	query->transaction_type = deserializer.ReadProperty<TransactionType>(++n, "transaction_type");

	// Read flags
	query->flags.expected_result_type =
	    static_cast<ExpectedResultType>(deserializer.ReadProperty<uint8_t>(++n, "expected_result_type"));
	query->flags.sort_style = static_cast<SortStyle>(deserializer.ReadProperty<uint8_t>(++n, "sort_style"));
	return query;
}

} // namespace duckdb
