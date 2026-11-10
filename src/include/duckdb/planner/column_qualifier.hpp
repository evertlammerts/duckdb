//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/column_qualifier.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/binder.hpp"

namespace duckdb {
class ColumnAliasBinder;
class HavingBinder;
class LambdaExpression;

class ColumnQualifier {
public:
	explicit ColumnQualifier(Binder &binder, optional_ptr<vector<DummyBinding>> lambda_bindings = nullptr,
	                         optional_ptr<ColumnAliasBinder> alias_binder = nullptr,
	                         optional_ptr<HavingBinder> having_binder = nullptr);
	virtual ~ColumnQualifier() = default;

	//! The function a bare SQL value function name (e.g. current_timestamp) converts to, or empty
	static Identifier SQLValueFunctionName(const Identifier &column_name);

	//! Returns the STRUCT_EXTRACT operator expression
	unique_ptr<ParsedExpression> CreateStructExtract(unique_ptr<ParsedExpression> base, const Identifier &field_name);
	//! Returns a STRUCT_PACK function expression
	unique_ptr<ParsedExpression> CreateStructPack(ColumnRefExpression &col_ref);

	//! Returns a qualified column reference from a column name
	unique_ptr<ParsedExpression> QualifyColumnName(const ParsedExpression &expr, const Identifier &column_name,
	                                               ErrorData &error);
	//! Returns a qualified column reference from a column reference with column_names.size() > 2
	unique_ptr<ParsedExpression> QualifyColumnNameWithManyDots(ColumnRefExpression &col_ref, ErrorData &error);
	//! Returns a qualified column reference from a column reference
	unique_ptr<ParsedExpression> QualifyColumnName(ColumnRefExpression &col_ref, ErrorData &error);
	//! Enables special-handling of lambda parameters by tracking them in lambda_params
	void QualifyColumnNamesInLambda(FunctionExpression &function);
	//! Recursively qualifies the column references in the (children) of the expression. Passes on the
	//! within_function_expression state from outer expressions, or sets it
	void QualifyColumnNames(unique_ptr<ParsedExpression> &expr, const bool within_function_expression = false);

	//! A dot call receiver that is a parameter reference of enclosing_lambdas is left unqualified, for the binder of
	//! its clause
	optional_ptr<CatalogEntry> QualifyFunction(FunctionExpression &function,
	                                           const vector<identifier_set_t> &enclosing_lambdas);
	//! Whether a table of this scope has the qualified reference as [catalog.][schema path.]table.column[.field...]
	bool TableProvidesReference(const ColumnRefExpression &col_ref);
	//! Whether the reference is a parameter of one of the lambdas, or a field access on one. A table of this scope
	//! that provides the reference wins, because that is the order in which the binder resolves it
	bool IsLambdaParameterReference(const vector<identifier_set_t> &lambdas, const ColumnRefExpression &col_ref);
	//! Whether argument index of the call binds as a lambda, filling in the parameter names it declares
	static bool TryGetLambdaParameters(Binder &binder, const FunctionExpression &function, idx_t index,
	                                   identifier_set_t &parameters);
	//! Visits the children of a call, handing each argument that binds as a lambda to lambda_callback instead
	static void EnumerateCallChildren(
	    Binder &binder, FunctionExpression &function,
	    const std::function<void(unique_ptr<ParsedExpression> &child)> &callback,
	    const std::function<void(LambdaExpression &lambda, identifier_set_t &parameters)> &lambda_callback);

private:
	Binder &binder;
	optional_ptr<vector<DummyBinding>> lambda_bindings;
	optional_ptr<ColumnAliasBinder> alias_binder;
	optional_ptr<HavingBinder> having_binder;
	//! The parameters of the lambdas that QualifyColumnNames is inside, innermost last; they are not bound yet
	vector<identifier_set_t> lambda_params;

private:
	unique_ptr<ParsedExpression> QualifyColumnNameWithManyDotsInternal(ColumnRefExpression &col_ref, ErrorData &error,
	                                                                   idx_t &struct_extract_start);
	//! Whether the given (possibly nested) table qualification matches a binding in the current bind context
	bool TableBindingExists(const BindingAlias &alias);
	//! Whether any binding in the current bind context has the given table name (ignoring catalog/schema)
	bool TableNameExists(const Identifier &table_name);
	//! The table binding that has names as [catalog.][schema path.]table.column[.field...], and the column's index
	optional_ptr<Binding> FindTableColumn(const vector<Identifier> &names, idx_t &column_index,
	                                      optional_ptr<ErrorData> error);

	unique_ptr<ParsedExpression> QualifyColumnNameInternal(ColumnRefExpression &col_ref, ErrorData &error);
	//! The argument of the call as written that takes a lambda, looked up once per function name and binder
	static optional_idx GetLambdaArgumentPosition(Binder &binder, const FunctionExpression &function);
	//! The function entry of the call; dot_call says it is written on a column, as in "x.lower()"
	static optional_ptr<CatalogEntry> FindFunction(Binder &binder, const FunctionExpression &function,
	                                               const QualifiedName &name, bool &dot_call);
};

} // namespace duckdb
