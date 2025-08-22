#include <duckdb/common/serializer/buffered_file_writer.hpp>

namespace duckdb {

class TUFileWriter : public BufferedFileWriter {
public:
	TUFileWriter(ClientContext &, const string &output_file);
	~TUFileWriter() override;
};

} // namespace duckdb
