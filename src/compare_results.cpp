#include <duckdb.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/buffered_file_reader.hpp>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/parser/statement/logical_plan_statement.hpp>

#include <fstream>

#include "state.hpp"
#include "utils/compatibility.hpp"

namespace duckdb {

// ExecutorException was introduced in 1.1.2, use SerializationException instead
#if DUCKDB_VERSION_AT_MOST(1, 1, 1)
using ExecutorException = SerializationException;
#endif

SerializedResult DeserializeResult(BufferedFileReader &);
void DoSimpleCompareResults(const SerializedResult &, const SerializedResult &, const SQLLogicQuery &,
                            bool do_compare_values = true);
void DoCompareSortedResults(ClientContext &, const SerializedResult &, const SerializedResult &, const SQLLogicQuery &);

bool AreRowsEquals(const DataChunk &lchunk, const DataChunk &rchunk, idx_t lrow_idx, idx_t rrow_idx);

void ReloadResults(ClientContext &context, const vector<Value> &params);

bool CompareResults(ClientContext &context, const vector<Value> &params) {
	const auto input_file = params[0].GetValue<string>();
	ReloadResults(context, params);

	BufferedFileReader file_reader(context.db->GetFileSystem(), input_file.c_str());

	BinaryDeserializer deserializer(file_reader);
	deserializer.Begin();
	auto count = deserializer.ReadProperty<int32_t>(100, "count");
	deserializer.End();

	auto &state = TUStorageExtensionInfo::GetState(*context.db);
	for (int32_t i = 0; i < count; ++i) {
		deserializer.Begin();
		auto file_result = SerializedResult::Deserialize(deserializer);
		deserializer.End();
		auto &in_mem_result = state.GetResult(file_result->uuid);
		auto &in_mem_query = state.GetQuery(file_result->uuid);

		if (in_mem_query.flags.sort_style == SortStyle::NO_SORT) {
			DoSimpleCompareResults(in_mem_result, *file_result, in_mem_query);
		} else {
			DoCompareSortedResults(context, in_mem_result, *file_result, in_mem_query);
		}
	}
	return true;
}

void ReloadResults(ClientContext &context, const vector<Value> &params) {
	if (params.size() == 1) {
		return;
	}

	auto &state = TUStorageExtensionInfo::GetState(*context.db);
	const auto reload_results_file = params[1].GetValue<string>();
	BufferedFileReader file_reader(context.db->GetFileSystem(), reload_results_file.c_str());
	BinaryDeserializer deserializer(file_reader);
	deserializer.Begin();
	auto count = deserializer.ReadProperty<int32_t>(100, "count");
	deserializer.End();

	for (int32_t i = 0; i < count; ++i) {
		deserializer.Begin();
		auto query = SQLLogicQuery::Deserialize(deserializer);
		deserializer.End();
		state.AddQuery(*query);

		deserializer.Begin();
		auto result = SerializedResult::Deserialize(deserializer);
		deserializer.End();
		state.AddResult(std::move(result));
	}
}

void DoSimpleCompareResults(const SerializedResult &in_mem_result, const SerializedResult &file_result,
                            const SQLLogicQuery &squery, bool do_compare_values) {
	if (in_mem_result.success != file_result.success) {
		const auto msg = in_mem_result.success
		                     ? "Results mismatch: in-memory result is successful but file result is not"
		                     : "Results mismatch: in-memory result failed but file result did not";
		throw ExecutorException(msg);
	}

	if (!in_mem_result.success) {
		if (!(in_mem_result.error == file_result.error)) {
			std::ostringstream oss;
			oss << "Query '" << squery.query << "' results mismatch: error messages differ:\n"
			    << "In-memory error:\n------------\n"
			    << in_mem_result.error.Message() << "\n\n------------\nFile error:\n------------"
			    << file_result.error.Message() << std::endl;
			throw ExecutorException(oss.str());
		}
		return;
	}

	if (in_mem_result.names != file_result.names) {
		std::cerr << "Query '" << squery.query << "' results mismatch: names differ:\n";
		std::cerr << "In-memory names:\n------------\n";
		std::cerr << StringUtil::Join(in_mem_result.names, ", ");
		std::cerr << "\n\n------------\nFile names:\n------------\n";
		std::cerr << StringUtil::Join(file_result.names, ", ") << std::endl;
		// Don't fail just on names
		// cf. test/api/serialized_plans/test_plan_serialization_bwc.cpp:113
	}

	if (in_mem_result.types != file_result.types) {
		std::ostringstream oss;
		oss << "Query '" << squery.query << "' results mismatch: types differ:\n";
		oss << "In-memory types:\n------------\n";
		for (const auto &type : in_mem_result.types) {
			oss << type.ToString() << "; ";
		}
		oss << "\n\n------------\nFile types:\n------------\n";
		for (const auto &type : file_result.types) {
			oss << type.ToString() << "; ";
		}
		throw ExecutorException(oss.str());
	}

	if (!do_compare_values) {
		return;
	}

	if (in_mem_result.chunks.size() == 0) {
		if (file_result.chunks.size() != 0) {
			throw ExecutorException("Results mismatch: in-memory result has no chunks but file result has some");
		} else {
			return; // both are empty
		}
	}

	idx_t expected_col_count = in_mem_result.chunks[0]->ColumnCount();

	// Now iterate over each row
	auto in_mem_it = in_mem_result.chunks.begin();
	auto file_it = file_result.chunks.begin();
	size_t in_mem_index = 0, file_index = 0;
	while (true) {
		// Find the next in-mem chunk
		while (in_mem_index >= (*in_mem_it)->size()) {
			++in_mem_it;
			if (in_mem_it == in_mem_result.chunks.end()) {
				// Make sure we are also at the end of the file result
				bool has_reached_file_end = file_it != file_result.chunks.end() && file_index == (*file_it)->size();
				++file_it;
				if (!has_reached_file_end || file_it != file_result.chunks.end()) {
					throw ExecutorException("Results mismatch: in-memory result has more chunks than file result");
				}

				// All good, results are equal
				return;
			}

			in_mem_index = 0;
		}

		// Find the next file chunk
		while (file_index >= (*file_it)->size()) {
			++file_it;
			if (file_it == file_result.chunks.end()) {
				// We should reach the end only above.
				throw ExecutorException("Results mismatch: file result has more chunks than in-memory result");
			}

			file_index = 0;
		}

		auto &in_mem_chunk = *in_mem_it;
		auto &file_chunk = *file_it;

		if (in_mem_chunk->ColumnCount() != expected_col_count) {
			throw ExecutorException("Results mismatch: expected %d columns but got %d in in-memory chunk",
			                        expected_col_count, in_mem_chunk->ColumnCount());
		}

		if (file_chunk->ColumnCount() != expected_col_count) {
			throw ExecutorException("Results mismatch: expected %d columns but got %d in file chunk",
			                        expected_col_count, file_chunk->ColumnCount());
		}

		if (!AreRowsEquals(*in_mem_chunk, *file_chunk, in_mem_index, file_index)) {
			std::ostringstream oss;
			oss << "Query '" << squery.query << "' data chunks differ at index " << in_mem_index;
			if (in_mem_index != file_index) {
				oss << " and " << file_index;
			}
			oss << "\n";
			oss << "In-memory chunk:\n------------\n";
			oss << in_mem_chunk->ToString();
			oss << "\n\n------------\nFile chunk:\n------------\n";
			oss << file_chunk->ToString();
			throw ExecutorException(oss.str());
		}

		++in_mem_index;
		++file_index;
	}
}

// TODO - factorize with QueryResult::Equals?
bool AreRowsEquals(const DataChunk &lchunk, const DataChunk &rchunk, idx_t lrow_idx, idx_t rrow_idx) {
	const auto cols = lchunk.ColumnCount();
	for (idx_t col = 0; col < cols; ++col) {
		auto lvalue = lchunk.GetValue(col, lrow_idx);
		auto rvalue = rchunk.GetValue(col, rrow_idx);
		if (lvalue.IsNull() && rvalue.IsNull()) {
			continue;
		} else if (lvalue.IsNull() != rvalue.IsNull() || lvalue != rvalue) {
			return false;
		}
	}
	return true;
}

// Extracted from duckdb/test/sqlite/
string SQLLogicTestConvertValue(ClientContext &context, Value value, LogicalType sql_type, bool original_sqlite_test) {
	if (value.IsNull()) {
		return "NULL";
	}

	if (original_sqlite_test) {
		// sqlite test hashes want us to convert floating point numbers to integers
		switch (sql_type.id()) {
		case LogicalTypeId::DECIMAL:
		case LogicalTypeId::FLOAT:
		case LogicalTypeId::DOUBLE:
			return value.CastAs(context, LogicalType::BIGINT).ToString();
		default:
			break;
		}
	}

	switch (sql_type.id()) {
	case LogicalTypeId::BOOLEAN:
		return BooleanValue::Get(value) ? "1" : "0";
	default: {
		string str = value.CastAs(context, LogicalType::VARCHAR).ToString();
		if (str.empty()) {
			return "(empty)";
		} else {
			return StringUtil::Replace(str, string("\0", 1), "\\0");
		}
	}
	}
}

vector<string> StringifyAndSortResults(ClientContext &context, const SerializedResult &result,
                                       const SQLLogicQuery &query) {
	vector<string> result_values_string;

	size_t r, c;
	idx_t row_count = result.RowCount();
	idx_t ncols = result.ColumnCount();
	idx_t total_value_count = row_count * ncols;

	result_values_string.resize(total_value_count);
	for (r = 0; r < row_count; r++) {
		for (c = 0; c < ncols; c++) {
			auto value = result.GetValue(c, r);
			auto converted_value =
			    SQLLogicTestConvertValue(context, value, result.types[c], query.flags.original_sqlite_test);
			result_values_string[r * ncols + c] = converted_value;
		}
	}

	// perform any required query sorts
	if (query.flags.sort_style == SortStyle::ROW_SORT) {
		// row-oriented sorting
		idx_t nrows = total_value_count / ncols;
		vector<vector<string>> rows;
		rows.reserve(nrows);
		for (idx_t row_idx = 0; row_idx < nrows; row_idx++) {
			vector<string> row;
			row.reserve(ncols);
			for (idx_t col_idx = 0; col_idx < ncols; col_idx++) {
				row.push_back(std::move(result_values_string[row_idx * ncols + col_idx]));
			}
			rows.push_back(std::move(row));
		}

		// sort the individual rows
		std::sort(rows.begin(), rows.end(), [](const vector<string> &a, const vector<string> &b) {
			for (idx_t col_idx = 0; col_idx < a.size(); col_idx++) {
				if (a[col_idx] != b[col_idx]) {
					return a[col_idx] < b[col_idx];
				}
			}
			return false;
		});

		// now reconstruct the values from the rows
		for (idx_t row_idx = 0; row_idx < nrows; row_idx++) {
			for (idx_t col_idx = 0; col_idx < ncols; col_idx++) {
				result_values_string[row_idx * ncols + col_idx] = std::move(rows[row_idx][col_idx]);
			}
		}
	} else if (query.flags.sort_style == SortStyle::VALUE_SORT) {
		// sort values independently
		std::sort(result_values_string.begin(), result_values_string.end());
	}
	return result_values_string;
}

void DoCompareSortedResults(ClientContext &context, const SerializedResult &in_mem_result,
                            const SerializedResult &file_result, const SQLLogicQuery &query) {
	// No need to compare values if something else differs before.
	DoSimpleCompareResults(in_mem_result, file_result, query, false);

	auto in_mem_sorted_results = StringifyAndSortResults(context, in_mem_result, query);
	auto file_sorted_results = StringifyAndSortResults(context, file_result, query);

	for (size_t i = 0; i < in_mem_sorted_results.size(); ++i) {
		if (in_mem_sorted_results[i] != file_sorted_results[i]) {
			std::ostringstream oss;
			oss << "Results mismatch: values differ at index " << i << ":\n";
			oss << "In-memory:" << in_mem_sorted_results[i] << "\n";
			oss << "File value: " << file_sorted_results[i] << "\n";
			throw ExecutorException(oss.str());
		}
	}
}

} // namespace duckdb
