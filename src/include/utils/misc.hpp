#pragma once

#include <duckdb.hpp>

namespace duckdb {

enum class TestDrivenTransactionState { NONE, STARTED, TO_COMMIT, TO_ROLLBACK };

void UpdateTxStateFromTxType(const TransactionType type, TestDrivenTransactionState &state);
void UpdateTxStateAfterStatement(Connection &con, TestDrivenTransactionState &state, bool rollback_if_none = false);

void DetachAllDatabases(ClientContext &);
void UseDBAndDetachOthers(Connection &, const std::string &, bool);

std::string UUIDToString(const hugeint_t &);

} // namespace duckdb
