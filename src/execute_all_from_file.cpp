#include <duckdb.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/buffered_file_reader.hpp>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/parser/statement/logical_plan_statement.hpp>

#include <fstream>

#include "state.hpp"
#include "utils/serialization_helpers.hpp"
#include "utils/file_writer.hpp"
#include "utils/logger.hpp"
#include "utils/misc.hpp"

namespace duckdb {

void SerializeResult(const SQLLogicQuery &slq, Connection &con, BinarySerializer &serializer, QueryResult &result);

std::string ErrorOrSuccess(duckdb::QueryResult &result) {
	if (!result.HasError()) {
		return "success";
	}

	std::string error_message = result.GetError();
	return error_message.length() > 50 ? error_message.substr(0, 47) + "..." : error_message;
}

bool ExecuteAllPlansFromFile(ClientContext &context, const vector<Value> &params) {
	const auto input_file = params[0].GetValue<string>();
	const auto output_file = params[1].GetValue<string>();

	BufferedFileReader file_reader(context.db->GetFileSystem(), input_file.c_str());
	TUFileWriter file_writer(context, output_file);

	BinaryDeserializer deserializer(file_reader);
	deserializer.Begin();
	auto count = deserializer.ReadProperty<int32_t>(100, "count");
	deserializer.End();

	BinarySerializer serializer(file_writer);
	serializer.Begin();
	serializer.WriteProperty(100, "count", count);
	serializer.End();

	LOG_INFO("Executing " << count << " queries from file: '" << input_file << "'");

	Connection con(*context.db);

	TestDrivenTransactionState test_driven_transaction_state = TestDrivenTransactionState::NONE;
	for (int32_t i = 0; i < count; ++i) {
		BinaryDeserializer deserializer(file_reader);
		deserializer.Set<ClientContext &>(*con.context);
		deserializer.Begin();
		auto slq = SQLLogicQuery::Deserialize(deserializer);
		deserializer.End();

		UpdateTxStateFromTxType(slq->transaction_type, test_driven_transaction_state);
		if (slq->should_skip_query) {
			serializer.Begin();
			// Serialize an empty result for skipped queries
			SerializedResult(slq->uuid).Serialize(serializer);
			serializer.End();

			if (slq->transaction_type == TransactionType::BEGIN_TRANSACTION) {
				LOG_DEBUG("Beginning new transaction as requested by query #" << slq->query_idx << ": '" << slq->query
				                                                              << "'");
				con.BeginTransaction();
			} else if (slq->transaction_type != TransactionType::INVALID) {
				LOG_DEBUG("Skipping query #" << i << " (" << UUIDToString(slq->uuid) << "): '" << slq->query << "'");
			}

			// if we're not in a test-driven transaction, we don't need to commit/rollback anything
			if (test_driven_transaction_state != TestDrivenTransactionState::NONE) {
				SerializedResult result(slq->uuid);
				try {
					UpdateTxStateAfterStatement(con, test_driven_transaction_state);
				} catch (const Exception &e) {
					if (slq->ExpectSuccess()) {
						throw;
					}

					result.error = ErrorData(e);
					result.success = false;
				}

				serializer.Begin();
				result.Serialize(serializer);
				serializer.End();
			}
			continue;
		}

		if (slq->can_parse_query && slq->can_deserialize_plan) {
			LOG_INFO("Executing query from plan #" << i << " (" << UUIDToString(slq->uuid) << "): '" << slq->query
			                                       << "'");
			for (idx_t statement_idx = 0; statement_idx < slq->nb_statements; ++statement_idx) {
				try {
					if (test_driven_transaction_state == TestDrivenTransactionState::NONE) {
						LOG_DEBUG("Beginning new transaction for statement #" << statement_idx << " of query #"
						                                                      << slq->query_idx);
						con.BeginTransaction();
					}

					deserializer.Begin();
					auto plan = LogicalOperator::Deserialize(deserializer);
					deserializer.End();

					plan->ResolveOperatorTypes();
					auto result = con.context->Query(make_uniq<LogicalPlanStatement>(std::move(plan)), false);
					LOG_DEBUG("Executed statement #" << statement_idx << " of query #" << i << ": "
					                                 << ErrorOrSuccess(*result));
					SerializeResult(*slq, con, serializer, *result);
				} catch (const Exception &e) {
					LOG_ERROR("Failed to execute statement #" << statement_idx << " of query #" << i << ": "
					                                          << e.what())
					throw;
				}

				UpdateTxStateAfterStatement(con, test_driven_transaction_state);
			}
		} else {
			LOG_INFO("Executing query from SQL #" << i << ": '" << slq->query << "' (" << UUIDToString(slq->uuid)
			                                      << ")");
			try {
				if (test_driven_transaction_state == TestDrivenTransactionState::NONE) {
					LOG_DEBUG("Beginning new transaction for query #" << slq->query_idx);
					con.BeginTransaction();
				}
				auto result = con.context->Query(slq->query, false);
				SerializeResult(*slq, con, serializer, *result);
				LOG_DEBUG("Executed query #" << i << ": " << ErrorOrSuccess(*result));
			} catch (const Exception &e) {
				LOG_ERROR("Failed to execute query #" << i << ": " << e.what())
				throw;
			}

			UpdateTxStateAfterStatement(con, test_driven_transaction_state);
		}

		if (!slq->load_db_name.empty()) {
			bool need_tx = test_driven_transaction_state == TestDrivenTransactionState::NONE;
			UseDBAndDetachOthers(con, slq->load_db_name, need_tx);
		}
	}

	file_writer.Sync();

	// Detach all databases attached during the query
	DetachAllDatabases(context);

	return true;
}

void SerializeResult(const SQLLogicQuery &slq, Connection &con, BinarySerializer &serializer, QueryResult &result) {
	serializer.Begin();
	SerializedResult(slq.uuid, result).Serialize(serializer);
	serializer.End();
	if (!result.HasError()) {
		return;
	}

	// Make sure the execution didn't cause DB to get invalidated.
	auto &db_inst = DatabaseInstance::GetDatabase(*con.context);
	if (ValidChecker::IsInvalidated(db_inst)) {
		result.ThrowError(); // We don't want to continue at this point.
	}
}

} // namespace duckdb
