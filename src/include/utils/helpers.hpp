#pragma once

#include <duckdb.hpp>
#ifndef DUCKDB_CPP_EXTENSION_ENTRY
#include <duckdb/main/extension_util.hpp>
#endif
#include <type_traits>

namespace duckdb {

struct ParamsTableFunctionData : public TableFunctionData {
	explicit ParamsTableFunctionData(const vector<Value> &_params) : params(_params) {
	}

	vector<Value> params;
};

struct RunOnceTableFunctionState : GlobalTableFunctionState {
	RunOnceTableFunctionState() : run(false) {};
	std::atomic<bool> run;

	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &, TableFunctionInitInput &) {
		return make_uniq<RunOnceTableFunctionState>();
	}
};

namespace internal {

unique_ptr<FunctionData> SingleBoolResultBind(ClientContext &, TableFunctionBindInput &, vector<LogicalType> &out_types,
                                              vector<std::string> &out_names);

bool ShouldRun(TableFunctionInput &input);

template <typename Func>
struct CallFunctionHelper;

template <>
struct CallFunctionHelper<bool (*)(ClientContext &, const vector<Value> &)> {
	static bool call(ClientContext &context, const vector<Value> &params,
	                 bool (*f)(ClientContext &, const vector<Value> &)) {
		return f(context, params);
	}
};

template <>
struct CallFunctionHelper<bool (*)(ClientContext &)> {
	static bool call(ClientContext &context, const vector<Value> &, bool (*f)(ClientContext &)) {
		return f(context);
	}
};

template <typename Func, Func func>
void TableFunc(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	if (!ShouldRun(input)) {
		return;
	}

	auto &bind_data = input.bind_data->Cast<ParamsTableFunctionData>();
	const bool result = CallFunctionHelper<Func>::call(context, bind_data.params, func);
	output.SetCardinality(1);
	output.SetValue(0, 0, result);
}

#ifdef DUCKDB_CPP_EXTENSION_ENTRY
template <typename Func, Func func>
void RegisterTF(ExtensionLoader &loader, const char *name, size_t nb_params = 1) {
	vector<LogicalType> param_types(nb_params, LogicalType::VARCHAR);
	TableFunction tf(name, param_types, internal::TableFunc<Func, func>, internal::SingleBoolResultBind,
	                 RunOnceTableFunctionState::Init);
	loader.RegisterFunction(tf);
}
#else
template <typename Func, Func func>
void RegisterTF(DatabaseInstance &instance, const char *name, size_t nb_params = 1) {
	vector<LogicalType> param_types(nb_params, LogicalType::VARCHAR);
	TableFunction tf(name, param_types, internal::TableFunc<Func, func>, internal::SingleBoolResultBind,
	                 RunOnceTableFunctionState::Init);
	ExtensionUtil::RegisterFunction(instance, tf);
}
#endif

} // namespace internal

#ifdef DUCKDB_CPP_EXTENSION_ENTRY
#define REGISTER_TF(name, func, nb_params) internal::RegisterTF<decltype(&func), &func>(loader, name, nb_params)
#else
#define REGISTER_TF(name, func, nb_params) internal::RegisterTF<decltype(&func), &func>(instance, name, nb_params)
#endif

} // namespace duckdb
