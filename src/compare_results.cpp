#include <duckdb.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/buffered_file_reader.hpp>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/parser/statement/logical_plan_statement.hpp>

#include <yyjson.hpp>

#include <fstream>

#include "state.hpp"
#include "utils/compatibility.hpp"
#include "utils/misc.hpp"

namespace duckdb {

namespace utils {

class ComparisonErrorReport {
public:
	ComparisonErrorReport(const SQLLogicQuery &slq) {
		doc = duckdb_yyjson::yyjson_mut_doc_new(NULL);
		root = duckdb_yyjson::yyjson_mut_obj(doc);
		duckdb_yyjson::yyjson_mut_doc_set_root(doc, root);
		SetQueryFields(slq);
	}

	ComparisonErrorReport(const SQLLogicQuery &slq, const std::string &error_message) : ComparisonErrorReport(slq) {
		AddString("error", error_message.c_str());
	}

	~ComparisonErrorReport() {
		duckdb_yyjson::yyjson_mut_doc_free(doc);
		doc = nullptr;
		root = nullptr;
	}

	void AddString(const char *key, const char *value) {
		duckdb_yyjson::yyjson_mut_obj_add_strcpy(doc, root, key, value);
	}

	void AddInt(const char *key, int64_t value) {
		duckdb_yyjson::yyjson_mut_obj_add_int(doc, root, key, value);
	}

	std::string ToString(bool pretty = false) {
		size_t flags = pretty ? duckdb_yyjson::YYJSON_WRITE_PRETTY : 0;
		char *json = duckdb_yyjson::yyjson_mut_write(doc, flags, NULL);
		if (!json) {
			return "{\"error\": \"Failed to serialize JSON report!\"}";
		}

		std::string result(json);
		free((void *)json);
		return result;
	}

private:
	void SetQueryFields(const SQLLogicQuery &slq) {
		AddString("query", slq.query.c_str());
		AddString("uuid", UUIDToString(slq.uuid).c_str());
		AddInt("idx", slq.query_idx);
	}

	duckdb_yyjson::yyjson_mut_doc *doc;
	duckdb_yyjson::yyjson_mut_val *root;
};

class ReportWriter {
public:
	ReportWriter(const std::string &_filename) : filename(_filename) {
	}

	void WriteItem(const std::string &item) {
		if (!writer) {
			writer = make_uniq<std::ofstream>(filename, std::ios::out | std::ios::trunc);
			(*writer) << "[\n";
		} else {
			(*writer) << ",\n";
		}

		(*writer) << item;
	}

	~ReportWriter() {
		if (writer) {
			(*writer) << "\n]\n";
			writer->close();
			writer.reset();
		}
	}

private:
	const std::string &filename;
	std::unique_ptr<std::ofstream> writer;
};

} // namespace utils

SerializedResult DeserializeResult(BufferedFileReader &);
std::unique_ptr<utils::ComparisonErrorReport> DoSimpleCompareResults(const SerializedResult &, const SerializedResult &,
                                                                     const SQLLogicQuery &,
                                                                     bool do_compare_values = true);
std::unique_ptr<utils::ComparisonErrorReport> DoCompareSortedResults(ClientContext &, const SerializedResult &,
                                                                     const SerializedResult &, const SQLLogicQuery &);

bool AreRowsEquals(const DataChunk &lchunk, const DataChunk &rchunk, idx_t lrow_idx, idx_t rrow_idx);

void ReloadResults(ClientContext &context, const vector<Value> &params);

bool CompareResults(ClientContext &context, const vector<Value> &params) {
	const auto input_file = params[0].GetValue<string>();
	ReloadResults(context, params);

	BufferedFileReader file_reader(context.db->GetFileSystem(), input_file.c_str());
	const std::string report_filename = input_file + ".report";
	utils::ReportWriter report_writer(report_filename);

	BinaryDeserializer deserializer(file_reader);
	deserializer.Begin();
	auto count = deserializer.ReadProperty<int32_t>(100, "count");
	deserializer.End();

	auto &state = TUStorageExtensionInfo::GetState(*context.db);
	for (int32_t i = 0; i < count; ++i) {
		deserializer.Begin();
		auto file_result = SerializedResult::Deserialize(deserializer);
		deserializer.End();

		if (file_result->is_skipped) {
			continue;
		}

		auto &in_mem_query = state.GetQuery(file_result->uuid);
		if (in_mem_query.should_skip_query) {
			continue;
		}

		auto &in_mem_result = state.GetResult(file_result->uuid);
		std::unique_ptr<utils::ComparisonErrorReport> comparison_error;
		if (in_mem_query.flags.sort_style == SortStyle::NO_SORT) {
			comparison_error = DoSimpleCompareResults(in_mem_result, *file_result, in_mem_query);
		} else {
			comparison_error = DoCompareSortedResults(context, in_mem_result, *file_result, in_mem_query);
		}

		if (comparison_error) {
			report_writer.WriteItem(comparison_error->ToString());
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

		if (query->should_skip_query) {
			continue;
		}

		deserializer.Begin();
		auto result = SerializedResult::Deserialize(deserializer);
		deserializer.End();
		state.AddResult(std::move(result));
	}
}

std::unique_ptr<utils::ComparisonErrorReport> DoSimpleCompareResults(const SerializedResult &in_mem_result,
                                                                     const SerializedResult &file_result,
                                                                     const SQLLogicQuery &squery,
                                                                     bool do_compare_values) {
	// First make sure that they either both succeeded or both failed
	if (in_mem_result.success != file_result.success) {
		if (in_mem_result.success) {
			auto report =
			    make_uniq<utils::ComparisonErrorReport>(squery, "Expected result to be successful but it failed");
			report->AddString("actual", file_result.error.Message().c_str());
			return std::move(report);
		}

		auto report = make_uniq<utils::ComparisonErrorReport>(squery, "Expected query to fail but it was successful");
		report->AddString("expected", in_mem_result.error.Message().c_str());
		return std::move(report);
	}

	// If they both failed, compare the error messages
	if (!in_mem_result.success) {
		if (in_mem_result.error == file_result.error) {
			return nullptr;
		}

		auto report = make_uniq<utils::ComparisonErrorReport>(squery, "Results mismatch, error messages differ");
		report->AddString("expected", in_mem_result.error.Message().c_str());
		report->AddString("actual", file_result.error.Message().c_str());
		return std::move(report);
	}

	// Otherwise, they both succeeded: compare the actual results
	if (in_mem_result.names != file_result.names) {
		std::cerr << "Query '" << squery.query << "' results mismatch: names differ:\n";
		std::cerr << "Expected names:\n------------\n";
		std::cerr << StringUtil::Join(in_mem_result.names, ", ");
		std::cerr << "\n\n------------\nActual names:\n------------\n";
		std::cerr << StringUtil::Join(file_result.names, ", ") << std::endl;
		// Don't fail just on names
		// cf. test/api/serialized_plans/test_plan_serialization_bwc.cpp:113
	}

	if (in_mem_result.types != file_result.types) {
		auto report = make_uniq<utils::ComparisonErrorReport>(squery, "Results types mismatch");
		{
			std::ostringstream oss;
			for (const auto &type : in_mem_result.types) {
				oss << type.ToString() << "; ";
			}
			report->AddString("expected", oss.str().c_str());
		}

		{
			std::ostringstream oss;
			for (const auto &type : file_result.types) {
				oss << type.ToString() << "; ";
			}
			report->AddString("actual", oss.str().c_str());
		}

		return std::move(report);
	}

	if (!do_compare_values) {
		return nullptr;
	}

	if (in_mem_result.chunks.size() == 0) {
		if (file_result.chunks.size() != 0) {
			return make_uniq<utils::ComparisonErrorReport>(
			    squery, "Results mismatch: expected result has no chunks but actual result has some");
		} else {
			return nullptr; // both are empty
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
					return make_uniq<utils::ComparisonErrorReport>(squery, "Results mismatch: expected more chunks");
				}

				// All good, results are equal
				return nullptr;
			}

			in_mem_index = 0;
		}

		// Find the next file chunk
		while (file_index >= (*file_it)->size()) {
			++file_it;
			if (file_it == file_result.chunks.end()) {
				// We should reach the end only above.
				return make_uniq<utils::ComparisonErrorReport>(squery, "Results mismatch: expected less chunks");
			}

			file_index = 0;
		}

		auto &in_mem_chunk = *in_mem_it;
		auto &file_chunk = *file_it;

		if (in_mem_chunk->ColumnCount() != expected_col_count) {
			auto report =
			    make_uniq<utils::ComparisonErrorReport>(squery, "Results mismatch: invalid column in expected chunk");
			report->AddInt("expected", expected_col_count);
			report->AddInt("actual", in_mem_chunk->ColumnCount());
			return std::move(report);
		}

		if (file_chunk->ColumnCount() != expected_col_count) {
			auto report =
			    make_uniq<utils::ComparisonErrorReport>(squery, "Results mismatch: invalid column in actual chunk");
			report->AddInt("expected", expected_col_count);
			report->AddInt("actual", file_chunk->ColumnCount());
			return std::move(report);
		}

		if (!AreRowsEquals(*in_mem_chunk, *file_chunk, in_mem_index, file_index)) {
			auto report = make_uniq<utils::ComparisonErrorReport>(squery, "Results mismatch: data chunks differ");
			report->AddInt("expected_idx", in_mem_index);
			report->AddInt("actual_idx", file_index);
			report->AddString("expected", in_mem_chunk->ToString().c_str());
			report->AddString("actual", file_chunk->ToString().c_str());
			return std::move(report);
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
		vector<vector<string>> rows;
		rows.reserve(row_count);
		for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
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
		for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
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

std::unique_ptr<utils::ComparisonErrorReport> DoCompareSortedResults(ClientContext &context,
                                                                     const SerializedResult &in_mem_result,
                                                                     const SerializedResult &file_result,
                                                                     const SQLLogicQuery &query) {
	// No need to compare values if something else differs before.
	auto error = DoSimpleCompareResults(in_mem_result, file_result, query, false);
	if (error) {
		return error;
	}

	auto in_mem_sorted_results = StringifyAndSortResults(context, in_mem_result, query);
	auto file_sorted_results = StringifyAndSortResults(context, file_result, query);

	for (size_t i = 0; i < in_mem_sorted_results.size(); ++i) {
		if (in_mem_sorted_results[i] != file_sorted_results[i]) {
			auto report = make_uniq<utils::ComparisonErrorReport>(query, "Results mismatch: values differ");
			report->AddInt("expected_idx", i);
			report->AddInt("actual_idx", i);
			report->AddString("expected", in_mem_sorted_results[i].c_str());
			report->AddString("actual", file_sorted_results[i].c_str());
			return std::move(report);
		}
	}

	return nullptr;
}

} // namespace duckdb
