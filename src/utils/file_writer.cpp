#include "utils/file_writer.hpp"

#include <duckdb.hpp>

namespace duckdb {

TUFileWriter::TUFileWriter(ClientContext &context, const string &output_file)
    : BufferedFileWriter(context.db->GetFileSystem(), output_file,
                         BufferedFileWriter::DEFAULT_OPEN_FLAGS | FileOpenFlags::FILE_FLAGS_APPEND) {
}

TUFileWriter::~TUFileWriter() {
	if (handle) {
		Sync();
		Close();
	}
}

} // namespace duckdb
