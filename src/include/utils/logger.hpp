#include <cstdint>
#include <iostream>

namespace duckdb {

void set_log_level_from_env();

namespace tu {
enum class TULogLevel : std::uint8_t { TU_NONE, TU_ERROR, TU_INFO, TU_DEBUG };

extern TULogLevel log_level_; // NOLINT

#define LOG_ERROR(msg)                                                                                                 \
	if (tu::log_level_ >= tu::TULogLevel::TU_ERROR) {                                                                  \
		std::cerr << "[ERROR] " << msg << std::endl; /* NOLINT */                                                      \
	}
#define LOG_INFO(msg)                                                                                                  \
	if (tu::log_level_ >= tu::TULogLevel::TU_INFO) {                                                                   \
		std::cout << "[INFO] " << msg << std::endl; /* NOLINT */                                                       \
	}
#define LOG_DEBUG(msg)                                                                                                 \
	if (tu::log_level_ >= tu::TULogLevel::TU_DEBUG) {                                                                  \
		std::cout << "[DEBUG] " << msg << std::endl; /* NOLINT */                                                      \
	}

} // namespace tu

} // namespace duckdb
