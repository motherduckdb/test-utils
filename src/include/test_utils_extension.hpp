#pragma once

#include "utils/compatibility.hpp"
#include "duckdb.hpp"

namespace duckdb {

class TestUtilsExtension : public Extension {
public:
#if DUCKDB_VERSION_AT_MOST(1, 3, 2)
	void Load(DuckDB &db) override;
#else
	void Load(ExtensionLoader &loader) override;
#endif
	std::string Name() override;
	std::string Version() const override;
};

} // namespace duckdb
