#include "utils/file_writer.hpp"

#include <duckdb.hpp>

namespace duckdb {

TUFileWriter::TUFileWriter(ClientContext &context, const string &output_file)
    : BufferedFileWriter(context.db->GetFileSystem(), output_file,
                         FileFlags::FILE_FLAGS_WRITE | FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW) {
}

TUFileWriter::~TUFileWriter() {
	if (handle) {
		Sync();
		Close();
	}
}

} // namespace duckdb
