#include <duckdb.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/buffered_file_reader.hpp>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/parser/statement/logical_plan_statement.hpp>

#include <fstream>

#include "state.hpp"

namespace duckdb {

SerializedResult DeserializeResult(BufferedFileReader &);
void DoCompareResults(const SerializedResult &, const SerializedResult &);
bool AreChunksEqual(const DataChunk &lchunk, const DataChunk &rchunk);

bool CompareResults(ClientContext &context, const vector<Value> &params) {
	const auto input_file = params[0].GetValue<string>();

	BufferedFileReader file_reader(context.db->GetFileSystem(), input_file.c_str());

	BinaryDeserializer deserializer(file_reader);
	deserializer.Begin();
	auto count = deserializer.ReadProperty<int32_t>(100, "count");
	deserializer.End();

	auto &state = TUStorageExtensionInfo::GetState(*context.db);
	for (int32_t i = 0; i < count; ++i) {
		auto file_result = DeserializeResult(file_reader);
		auto &in_mem_result = state.GetResult(file_result.uuid);
		DoCompareResults(in_mem_result, file_result);
	}
	return true;
}

SerializedResult DeserializeResult(BufferedFileReader &reader) {
	BinaryDeserializer deserializer(reader);
	deserializer.Begin();

	SerializedResult result;
	result.uuid = deserializer.ReadProperty<hugeint_t>(200, "uuid");

	result.success = deserializer.ReadProperty<bool>(201, "success");
	if (!result.success) {
		result.error = ErrorData(deserializer.ReadProperty<string>(202, "error"));
		return result;
	}

	result.types = deserializer.ReadProperty<vector<LogicalType>>(203, "types");
	result.names = deserializer.ReadProperty<vector<string>>(204, "names");

	const auto chunks_count = deserializer.ReadProperty<uint32_t>(205, "chunks_count");
	for (uint32_t i = 0; i < chunks_count; ++i) {
		auto chunk = make_uniq<DataChunk>();
		chunk->Deserialize(deserializer);
		result.chunks.push_back(std::move(chunk));
	}

	deserializer.End();
	return std::move(result);
}

void DoCompareResults(const SerializedResult &in_mem_result, const SerializedResult &file_result) {
	if (in_mem_result.success != file_result.success) {
		const auto msg = in_mem_result.success
		                     ? "Results mismatch: in-memory result is successful but file result is not"
		                     : "Results mismatch: in-memory result failed but file result did not";
		throw InternalException(msg);
	}

	if (!in_mem_result.success) {
		if (!(in_mem_result.error == file_result.error)) {
			std::cerr << "In-memory error:\n------------\n"
			          << in_mem_result.error.Message() << "\n\n------------\nFile error:\n------------"
			          << file_result.error.Message() << std::endl;
			throw InternalException("Results mismatch: error messages differ");
		}
		return;
	}

	if (in_mem_result.names != file_result.names) {
		std::cerr << "In-memory names:\n------------\n";
		std::cerr << StringUtil::Join(in_mem_result.names, ", ");
		std::cerr << "\n\n------------\nFile names:\n------------\n";
		std::cerr << StringUtil::Join(file_result.names, ", ") << std::endl;
		// Don't fail just on names
		// cf. test/api/serialized_plans/test_plan_serialization_bwc.cpp:113
	}

	if (in_mem_result.types != file_result.types) {
		std::cerr << "In-memory types:\n------------\n";
		for (const auto &type : in_mem_result.types) {
			std::cerr << type.ToString() << "; ";
		}
		std::cerr << "\n\n------------\nFile types:\n------------\n";
		for (const auto &type : file_result.types) {
			std::cerr << type.ToString() << "; ";
		}
		throw InternalException("Results mismatch: types or names differ");
	}

	if (in_mem_result.chunks.size() != file_result.chunks.size()) {
		throw InternalException("Results mismatch: got " + std::to_string(in_mem_result.chunks.size()) +
		                        " chunk(s) in memory and " + std::to_string(file_result.chunks.size()) + " in file");
	}

	for (size_t i = 0; i < in_mem_result.chunks.size(); ++i) {
		if (!AreChunksEqual(*in_mem_result.chunks[i], *file_result.chunks[i])) {
			// TODO: propagate the query and display it here.
			std::cerr << "In-memory chunk:\n------------\n";
			in_mem_result.chunks[i]->Print();
			std::cerr << "\n\n------------\nFile chunk:\n------------\n";
			file_result.chunks[i]->Print();
			std::cerr << std::endl;
			throw InternalException("Results mismatch: data chunks differ at index " + std::to_string(i));
		}
	}
}

// TODO - factorize with QueryResult::Equals
bool AreChunksEqual(const DataChunk &lchunk, const DataChunk &rchunk) {
	if (lchunk.size() == 0 && rchunk.size() == 0) {
		return true;
	}

	if (lchunk.ColumnCount() != rchunk.ColumnCount() || lchunk.size() != rchunk.size()) {
		return false;
	}

	const auto cols = lchunk.ColumnCount();
	const auto rows = lchunk.size();
	for (idx_t i = 0; i < rows; ++i) {
		for (idx_t col = 0; col < cols; ++col) {
			auto lvalue = lchunk.GetValue(col, i);
			auto rvalue = rchunk.GetValue(col, i);
			if (lvalue.IsNull() && rvalue.IsNull()) {
				continue;
			} else if (lvalue.IsNull() != rvalue.IsNull() || lvalue != rvalue) {
				return false;
			}
		}
	}

	return true;
}

} // namespace duckdb
