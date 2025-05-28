//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/pandas_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb_python/pandas/pandas_bind.hpp"

#include "duckdb_python/pybind11/pybind_wrapper.hpp"

namespace duckdb {

struct PandasScanFunction : public TableFunction {
public:
	static constexpr idx_t PANDAS_PARTITION_COUNT = 50 * STANDARD_VECTOR_SIZE;

public:
	PandasScanFunction();

	static unique_ptr<FunctionData> PandasScanBind(ClientContext &context, TableFunctionBindInput &input,
	                                               vector<LogicalType> &return_types, vector<string> &names);

	static unique_ptr<GlobalTableFunctionState> PandasScanInitGlobal(ClientContext &context,
	                                                                 TableFunctionInitInput &input);
	static unique_ptr<LocalTableFunctionState>
	PandasScanInitLocal(ExecutionContext &context, TableFunctionInitInput &input, GlobalTableFunctionState *gstate);

	static idx_t PandasScanMaxThreads(ClientContext &context, const FunctionData *bind_data_p);

	static bool PandasScanParallelStateNext(ClientContext &context, const FunctionData *bind_data_p,
	                                        LocalTableFunctionState *lstate, GlobalTableFunctionState *gstate);

	static double PandasProgress(ClientContext &context, const FunctionData *bind_data_p,
	                             const GlobalTableFunctionState *gstate);

	//! The main pandas scan function: note that this can be called in parallel without the GIL
	//! hence this needs to be GIL-safe, i.e. no methods that create Python objects are allowed
	static void PandasScanFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output);

	static unique_ptr<NodeStatistics> PandasScanCardinality(ClientContext &context, const FunctionData *bind_data);

	static OperatorPartitionData PandasScanGetPartitionData(ClientContext &context,
	                                                        TableFunctionGetPartitionInput &input);

	// Helper function that transform pandas df names to make them work with our binder
	static py::object PandasReplaceCopiedNames(const py::object &original_df);

	static void PandasBackendScanSwitch(PandasColumnBindData &bind_data, idx_t count, idx_t offset, Vector &out);

	static void PandasSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
	                            const TableFunction &function);
};

struct PandasScanFunctionData : public TableFunctionData {
public:
	// Target type pushdown
	py::handle df;
	idx_t row_count;
	atomic<idx_t> lines_read;
	vector<PandasColumnBindData> pandas_bind_data;
	vector<LogicalType> sql_types;
	unordered_map<idx_t, LogicalType> target_types;
	bool has_target_types = false;
	shared_ptr<DependencyItem> copied_df;
	unordered_map<string, idx_t> column_name_to_index;

public:
	PandasScanFunctionData(py::handle df, idx_t row_count, vector<PandasColumnBindData> pandas_bind_data,
	                       vector<LogicalType> sql_types, shared_ptr<DependencyItem> dependency)
	    : df(df), row_count(row_count), lines_read(0), pandas_bind_data(std::move(pandas_bind_data)),
	      sql_types(std::move(sql_types)), copied_df(std::move(dependency)) {
	}
	~PandasScanFunctionData() override {
		try {
			py::gil_scoped_acquire acquire;
			pandas_bind_data.clear();
		} catch (...) { // NOLINT
		}
	}
};
} // namespace duckdb
