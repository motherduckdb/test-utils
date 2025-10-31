#include "utils/logger.hpp"

#include <duckdb/common/string_util.hpp>

#include <cstdlib>
#include <string>

namespace duckdb {

namespace tu {
TULogLevel log_level_ = TULogLevel::TU_ERROR; // NOLINT
}

void set_log_level_from_env() {
	auto log_level_cstr = getenv("DUCKDB_TEST_UTILS_LOG_LEVEL");
	if (!log_level_cstr) {
		return;
	}

	std::string log_level(log_level_cstr);
	log_level = StringUtil::Upper(log_level);

	tu::log_level_ = tu::TULogLevel::TU_INFO;
	if (log_level == "DEBUG") {
		LOG_INFO("Setting log level to DEBUG");
		tu::log_level_ = tu::TULogLevel::TU_DEBUG;
	} else if (log_level == "INFO") {
		LOG_INFO("Setting log level to INFO");
		tu::log_level_ = tu::TULogLevel::TU_INFO;
	} else if (log_level == "ERROR") {
		LOG_INFO("Setting log level to ERROR");
		tu::log_level_ = tu::TULogLevel::TU_ERROR;
	} else {
		LOG_INFO("Unknown log level: '" << log_level << "'. Valid values are: DEBUG, INFO, ERROR. Setting to NONE");
		tu::log_level_ = tu::TULogLevel::TU_NONE;
	}
}

} // namespace duckdb
