#include "utils/misc.hpp"

#include <duckdb/common/types/uuid.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/parser/parsed_data/transaction_info.hpp>

#include "utils/compatibility.hpp"
#include "utils/logger.hpp"

namespace duckdb {

void UseDBAndDetachOthers(Connection &con, const std::string &db_name, bool need_tx) {
	auto &db_manager = DatabaseManager::Get(*con.context);

	if (need_tx) {
		con.BeginTransaction();
	}

	// First "USE" the desired database
	db_manager.SetDefaultDatabase(*con.context, db_name);

	auto databases = db_manager.GetDatabases(*con.context);
	for (auto &db : databases) {
#if DUCKDB_VERSION_AT_MOST(1, 3, 2)
		auto &db_instance = db.get();
#else
		auto &db_instance = *db;
#endif
		auto name = db_instance.GetName();
		if (name != db_name && !db_instance.IsSystem() && !db_instance.IsTemporary()) {
			db_manager.DetachDatabase(*con.context, name, OnEntryNotFound::THROW_EXCEPTION);
		}
	}

	if (need_tx) {
		con.Commit();
	}
}

bool IsMemoryAttached(Connection &con) {
	auto res = con.Query("select count(*) from duckdb_databases() where database_name='memory';");
	if (res->HasError()) {
		res->ThrowError();
	}

	while (auto chunk = res->Fetch()) {
		for (idx_t i = 0; i < chunk->size(); ++i) {
			return chunk->GetValue(0, i) == 1;
		}
	}

	return false;
}

void DetachAllDatabases(ClientContext &context) {
	DatabaseManager &db_manager = context.db->GetDatabaseManager();

	// First, ensure 'memory' is the current default database
	auto current_default_db = db_manager.GetDefaultDatabase(context);
	if (current_default_db != "memory") {
		Connection con(*context.db);

		bool memory_attached = IsMemoryAttached(con);
		LOG_DEBUG("Current default database is '" << current_default_db << "'. Memory attached: " << memory_attached);

		if (!memory_attached) {
			LOG_DEBUG("Attaching in-memory database 'memory' to switch default database.");
			auto res = con.Query("ATTACH ':memory:';");
			if (res->HasError()) {
				res->ThrowError();
			}

			LOG_DEBUG("Successfully attached in-memory database 'memory'.");
		} else {
			LOG_DEBUG("In-memory database 'memory' is already attached.");
		}
		{
			auto res = con.Query("USE memory;");
			if (res->HasError()) {
				res->ThrowError();
			}
		}

		db_manager.SetDefaultDatabase(context, "memory");
	}

	LOG_DEBUG("Detaching all databases...");
	// Detach all databases attached during the query
	auto databases = db_manager.GetDatabases(context);
	for (auto &db : databases) {
#if DUCKDB_VERSION_AT_MOST(1, 3, 2)
		auto &db_instance = db.get();
#else
		auto &db_instance = *db;
#endif
		auto name = db_instance.GetName();
		if (db_instance.IsSystem() || db_instance.IsTemporary() || name == "memory") {
			continue;
		}

		LOG_DEBUG("Detaching database '" << name << "'");
		db_manager.DetachDatabase(context, name, OnEntryNotFound::THROW_EXCEPTION);
	}
}

std::string TDTxStateToString(TestDrivenTransactionState state) {
	switch (state) {
	case TestDrivenTransactionState::NONE:
		return "NONE";
	case TestDrivenTransactionState::STARTED:
		return "STARTED";
	case TestDrivenTransactionState::TO_COMMIT:
		return "TO_COMMIT";
	case TestDrivenTransactionState::TO_ROLLBACK:
		return "TO_ROLLBACK";
	default:
		return "*** UNKNOWN ***";
	}
}

void UpdateTxStateAfterStatement(Connection &con, TestDrivenTransactionState &state, bool rollback_if_none) {
	const auto original_state = state;
	switch (state) {
	case TestDrivenTransactionState::NONE:
		if (rollback_if_none) {
			LOG_DEBUG("Rolling-back implicit transaction");
			con.Rollback();
		} else {
			LOG_DEBUG("Committing implicit transaction");
			con.Commit();
		}
		break;
	case TestDrivenTransactionState::STARTED:
		break; // Nothing to do yet
	case TestDrivenTransactionState::TO_COMMIT:
		state = TestDrivenTransactionState::NONE;
		LOG_DEBUG("Committing test-driven transaction");
		con.Commit();
		break;
	case TestDrivenTransactionState::TO_ROLLBACK:
		state = TestDrivenTransactionState::NONE;
		LOG_DEBUG("Rolling-back test-driven transaction");
		con.Rollback();
		break;
	default:
		throw InternalException("Unhandled TestDrivenTransactionState");
	}
	LOG_DEBUG("Test-driven tx state updated after statement from " << TDTxStateToString(original_state) << " to "
	                                                               << TDTxStateToString(state));
}

void UpdateTxStateFromTxType(const TransactionType type, TestDrivenTransactionState &state) {
	switch (type) {
	case TransactionType::BEGIN_TRANSACTION:
		if (state != TestDrivenTransactionState::NONE) {
			throw InternalException("Nested transactions not supported in serialization, was in state: " +
			                        TDTxStateToString(state));
		}

		state = TestDrivenTransactionState::STARTED;
		LOG_DEBUG("Test-driven transaction started");
		break;
	case TransactionType::COMMIT:
		if (state != TestDrivenTransactionState::STARTED) {
			throw InternalException("Attempted to commit without active transaction, was in state: " +
			                        TDTxStateToString(state));
		}

		state = TestDrivenTransactionState::TO_COMMIT;
		LOG_DEBUG("Test-driven transaction will be committed");
		break;
	case TransactionType::ROLLBACK:
		if (state != TestDrivenTransactionState::STARTED) {
			throw InternalException("Attempted to rollback without active transaction, was in state: " +
			                        TDTxStateToString(state));
		}
		state = TestDrivenTransactionState::TO_ROLLBACK;
		LOG_DEBUG("Test-driven transaction will be rolled back");
		break;
	default:
		break; // Not a transaction statement
	}
}

std::string UUIDToString(const hugeint_t &uuid) {
#if DUCKDB_VERSION_AT_MOST(1, 2, 2)
	// BaseUUID was introduced in DuckDB 1.3.0
	using BaseUUID = duckdb::UUID;
#endif
	return BaseUUID::ToString(uuid);
}

} // namespace duckdb
