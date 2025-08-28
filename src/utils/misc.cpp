#include "utils/misc.hpp"

#include <duckdb/main/attached_database.hpp>
#include "utils/compatibility.hpp"

namespace duckdb {

void DetachAllDatabases(ClientContext &context) {
	// Detach all databases attached during the query
	DatabaseManager &db_manager = context.db->GetDatabaseManager();
	auto databases = db_manager.GetDatabases(context);
	for (auto &db : databases) {
#if DUCKDB_VERSION_AT_MOST(1, 3, 2)
		auto &db_instance = db.get();
#else
		auto &db_instance = *db;
#endif
		auto name = db_instance.GetName();
		if (!db_instance.IsSystem() && !db_instance.IsTemporary() && name != "memory") {
			db_manager.DetachDatabase(context, name, OnEntryNotFound::THROW_EXCEPTION);
		}
	}
}

} // namespace duckdb
