#include "duckdb/planner/column_qualifier.hpp"

#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/lambda_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/expression/positional_reference_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/expression_binder/having_binder.hpp"
#include "duckdb/planner/planner_extension.hpp"

namespace duckdb {

ColumnQualifier::ColumnQualifier(Binder &binder_p, optional_ptr<vector<DummyBinding>> lambda_bindings_p,
                                 optional_ptr<ColumnAliasBinder> alias_binder_p,
                                 optional_ptr<HavingBinder> having_binder_p)
    : binder(binder_p), lambda_bindings(lambda_bindings_p), alias_binder(alias_binder_p),
      having_binder(having_binder_p) {
}

Identifier ColumnQualifier::SQLValueFunctionName(const Identifier &column_name) {
	if (column_name == "current_catalog") {
		return "current_catalog";
	}
	if (column_name == "current_date") {
		return "current_date";
	}
	if (column_name == "current_schema") {
		return "current_schema";
	}
	if (column_name == "current_role") {
		return "current_role";
	}
	if (column_name == "current_time") {
		return "get_current_time";
	}
	if (column_name == "current_timestamp") {
		return "get_current_timestamp";
	}
	if (column_name == "current_user") {
		return "current_user";
	}
	if (column_name == "localtime") {
		return "current_localtime";
	}
	if (column_name == "localtimestamp") {
		return "current_localtimestamp";
	}
	if (column_name == "session_user") {
		return "session_user";
	}
	if (column_name == "user") {
		return "user";
	}
	return Identifier();
}

unique_ptr<ParsedExpression> Binder::GetSQLValueFunction(const Identifier &column_name) {
	for (auto &ext : PlannerExtension::Iterate(context)) {
		if (ext.get_sql_value_function) {
			PlannerExtensionInput input {context, *this, ext.planner_info.get()};
			unique_ptr<ParsedExpression> result;
			auto result_type = ext.get_sql_value_function(input, column_name.GetIdentifierName(), result);
			if (result_type == GetSQLValueFunctionReturnType::FINISH_BINDING || result) {
				return result;
			}
		}
	}

	auto value_function = ColumnQualifier::SQLValueFunctionName(column_name);
	if (value_function.empty()) {
		return nullptr;
	}

	vector<unique_ptr<ParsedExpression>> children;
	return make_uniq<FunctionExpression>(value_function, std::move(children));
}

unique_ptr<ParsedExpression> ColumnQualifier::CreateStructExtract(unique_ptr<ParsedExpression> base,
                                                                  const Identifier &field_name) {
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(std::move(base));
	children.push_back(ConstantExpression::String(field_name.GetIdentifierName()));
	auto extract_fun = make_uniq<OperatorExpression>(ExpressionType::STRUCT_EXTRACT, std::move(children));
	return std::move(extract_fun);
}

// Build the candidate table-qualification interpretations for a reference "[qualifiers...].table", in precedence
// order (most top-level first): "catalog + schema path" before "schema path only".
static vector<BindingAlias> GetTableQualifications(const vector<Identifier> &names, idx_t table_index) {
	vector<BindingAlias> result;
	auto &table = names[table_index];
	if (table_index >= 1) {
		// interpret names[0] as the catalog and names[1..table_index-1] as the schema path
		vector<Identifier> schema_path;
		for (idx_t i = 1; i < table_index; i++) {
			schema_path.push_back(names[i]);
		}
		result.emplace_back(names[0], std::move(schema_path), table);
	}
	// interpret all qualifiers as a (nested) schema path with no catalog
	vector<Identifier> schema_path;
	for (idx_t i = 0; i < table_index; i++) {
		schema_path.push_back(names[i]);
	}
	result.emplace_back(Identifier(), std::move(schema_path), table);
	return result;
}

unique_ptr<ParsedExpression> ColumnQualifier::CreateStructPack(ColumnRefExpression &col_ref) {
	D_ASSERT(!col_ref.ColumnNames().empty());

	// the whole reference names a table - try to interpret it as "[catalog.][schema path.]table"
	ErrorData error;
	optional_ptr<Binding> binding;
	auto &names = col_ref.ColumnNames();
	for (auto &alias : GetTableQualifications(names, names.size() - 1)) {
		binding = binder.bind_context.GetBinding(alias, error);
		if (binding) {
			break;
		}
	}
	if (!binding) {
		return nullptr;
	}

	// We found the table, now create the struct_pack expression
	auto &column_names = binding->GetColumnNames();
	vector<FunctionArgument> child_expressions;
	child_expressions.reserve(column_names.size());
	for (const auto &column_name : column_names) {
		auto ref = binder.bind_context.CreateColumnReference(binding->GetBindingAlias(), column_name,
		                                                     ColumnBindType::DO_NOT_EXPAND_GENERATED_COLUMNS);
		child_expressions.emplace_back(column_name, std::move(ref));
	}
	return make_uniq<FunctionExpression>("struct_pack", std::move(child_expressions));
}

unique_ptr<ParsedExpression> ColumnQualifier::QualifyColumnName(const ParsedExpression &expr,
                                                                const Identifier &column_name, ErrorData &error) {
	// try binding as a lambda parameter
	auto lambda_ref = LambdaRefExpression::FindMatchingBinding(lambda_bindings, column_name);
	if (lambda_ref) {
		return lambda_ref;
	}

	auto using_binding = binder.bind_context.GetUsingBinding(column_name);
	if (using_binding) {
		// we are referencing a USING column
		// check if we can refer to one of the base columns directly
		unique_ptr<Expression> expression;
		if (using_binding->primary_binding.IsSet()) {
			// we can! just assign the table name and re-bind
			return binder.bind_context.CreateColumnReference(using_binding->primary_binding, column_name);
		} else {
			// we cannot! we need to bind this as COALESCE between all the relevant columns
			auto coalesce = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_COALESCE);
			coalesce->GetChildrenMutable().reserve(using_binding->bindings.size());
			for (auto &entry : using_binding->bindings) {
				coalesce->GetChildrenMutable().push_back(make_uniq<ColumnRefExpression>(column_name, entry));
			}
			return std::move(coalesce);
		}
	}

	// find a table binding that contains this column name
	auto table_binding = binder.bind_context.GetMatchingBinding(column_name, expr);

	// throw an error if a macro parameter name conflicts with a column name
	auto is_macro_column = false;
	if (binder.macro_binding && binder.macro_binding->HasMatchingBinding(column_name)) {
		is_macro_column = true;
		if (table_binding) {
			throw BinderException(expr, "Conflicting column names for column " + column_name + "!");
		}
	}

	// bind as a macro column
	if (is_macro_column) {
		return binder.bind_context.CreateColumnReference(binder.macro_binding->GetBindingAlias(), column_name);
	}

	// bind as a regular column
	if (table_binding) {
		return binder.bind_context.CreateColumnReference(table_binding->GetBindingAlias(), column_name);
	}

	// it's not, find candidates and error
	auto similar_bindings = binder.bind_context.GetSimilarBindings(column_name);
	error = ErrorData(BinderException::ColumnNotFound(column_name, similar_bindings));
	return nullptr;
}

void ColumnQualifier::QualifyColumnNames(unique_ptr<ParsedExpression> &expr, const bool within_function_expression) {
	bool next_within_function_expression = false;
	switch (expr->GetExpressionType()) {
	case ExpressionType::COLUMN_REF: {
		auto &col_ref = expr->Cast<ColumnRefExpression>();

		// don't qualify a lambda parameter, nor a field access on one: only binding the lambda resolves it
		if (IsLambdaParameterReference(lambda_params, col_ref)) {
			return;
		}

		ErrorData error;
		auto new_expr = QualifyColumnName(col_ref, error);

		if (new_expr) {
			if (!expr->GetAlias().empty()) {
				// Pre-existing aliases are added to the qualified column reference
				new_expr->SetAlias(expr->GetAlias());
			} else if (within_function_expression) {
				// Qualifying the column reference may add an alias, but this needs to be removed within function
				// expressions, because the alias here means a named parameter instead of a positional parameter
				new_expr->ClearAlias();
			}

			// replace the expression with the qualified column reference
			new_expr->SetQueryLocation(col_ref.GetQueryLocation());
			expr = std::move(new_expr);
		}
		return;
	}
	case ExpressionType::POSITIONAL_REFERENCE: {
		auto &ref = expr->Cast<PositionalReferenceExpression>();
		if (ref.GetAlias().empty() && !within_function_expression) {
			Identifier table_name, column_name;
			auto error = binder.bind_context.BindColumn(ref, table_name, column_name);
			if (error.empty()) {
				ref.SetAlias(column_name);
			}
		}
		break;
	}
	case ExpressionType::FUNCTION: {
		// Special-handling for lambdas, which are inside function expressions.
		auto &function = expr->Cast<FunctionExpression>();
		optional_ptr<CatalogEntry> entry;
		if (!ExpressionBinder::IsUnnestFunction(function.FunctionName())) {
			entry = QualifyFunction(function, lambda_params);
		}
		if (function.IsLambdaFunction()) {
			return QualifyColumnNamesInLambda(function, entry);
		}

		next_within_function_expression = true;
		break;
	}
	default: // fall through
		break;
	}

	// recurse on the child expressions
	ParsedExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<ParsedExpression> &child) {
		QualifyColumnNames(child, next_within_function_expression);
	});
}

optional_ptr<CatalogEntry> ColumnQualifier::FindFunction(Binder &binder, const FunctionExpression &function,
                                                         const QualifiedName &name, bool &dot_call) {
	dot_call = false;
	QueryErrorContext error_context(function.GetQueryLocation());
	EntryLookupInfo function_lookup(CatalogType::SCALAR_FUNCTION_ENTRY, QualifiedName(name.Name()), error_context);
	// resolve the qualification: this decides whether a leading component (e.g. "s1" in "s1.s2.my_macro()") is a
	// catalog or the outermost schema of a nested schema path. The name as written is left alone - the dot-call
	// rewrite turns the qualification into a column reference and needs it unresolved.
	auto bound_name = Binder::BindTableName(binder.EntryRetriever(), name);
	auto func = binder.GetCatalogEntry(EntryLookupInfo(function_lookup, bound_name), OnEntryNotFound::RETURN_NULL);
	if (func) {
		return func;
	}
	if (name.Schema().empty()) {
		return nullptr;
	}
	// the schema is set - a function of that name in the system catalog makes this a dot call on a column
	func = binder.GetCatalogEntry(Identifier::InvalidCatalog(), Identifier::InvalidSchema(), function_lookup,
	                              OnEntryNotFound::RETURN_NULL);
	dot_call = func != nullptr;
	return func;
}

optional_ptr<CatalogEntry> ColumnQualifier::QualifyFunction(FunctionExpression &function,
                                                            const vector<identifier_set_t> &enclosing_lambdas) {
	D_ASSERT(!ExpressionBinder::IsUnnestFunction(function.FunctionName()));
	// promote a lone qualifier that names an attached database to a catalog, so that "db.f()" is not mistaken for a
	// dot-call on a column named "db" by the rewrite below
	binder.BindSchemaOrCatalog(function.GetQualifiedNameMutable());

	bool dot_call;
	auto func = FindFunction(binder, function, function.GetQualifiedName(), dot_call);
	if (!dot_call) {
		return func;
	}
	ErrorData error;
	unique_ptr<ColumnRefExpression> colref;
	if (function.GetQualifiedName().Catalog().empty()) {
		colref = make_uniq<ColumnRefExpression>(function.GetQualifiedName().Schema());
	} else {
		colref =
		    make_uniq<ColumnRefExpression>(function.GetQualifiedName().Schema(), function.GetQualifiedName().Catalog());
	}
	unique_ptr<ParsedExpression> new_colref;
	if (!IsLambdaParameterReference(enclosing_lambdas, *colref)) {
		new_colref = QualifyColumnName(*colref, error);
	}
	if (!new_colref) {
		new_colref = std::move(colref);
	}
	new_colref->ClearAlias();
	// transform this into a function call on the column
	// i.e. "x.lower()" becomes "lower(x)"
	function.GetArgumentsMutable().insert(function.GetArgumentsMutable().begin(), std::move(new_colref));
	function.SetQualifiedName(QualifiedName(function.GetQualifiedName().Name()));
	return func;
}

bool ColumnQualifier::IsTableMacro(Binder &binder, const FunctionExpression &function, const QualifiedName &name) {
	QueryErrorContext error_context(function.GetQueryLocation());
	EntryLookupInfo table_lookup(CatalogType::TABLE_FUNCTION_ENTRY, QualifiedName(name.Name()), error_context);
	auto bound_name = Binder::BindTableName(binder.EntryRetriever(), name);
	auto entry = binder.GetCatalogEntry(EntryLookupInfo(table_lookup, bound_name), OnEntryNotFound::RETURN_NULL);
	return entry && entry->type == CatalogType::TABLE_MACRO_ENTRY;
}

bool ColumnQualifier::IsLambdaArgument(Binder &binder, const FunctionExpression &function, idx_t index,
                                       optional_ptr<CatalogEntry> entry) {
	auto name = function.GetQualifiedName();
	bool dot_call = false;
	auto func = entry;
	if (!func) {
		binder.BindSchemaOrCatalog(name);
		func = FindFunction(binder, function, name, dot_call);
	}
	if (!func || func->type == CatalogType::MACRO_ENTRY) {
		// a macro places an argument only when it expands, so an arrow passed to one keeps the lambda reading
		return true;
	}
	if (func->type == CatalogType::SCALAR_FUNCTION_ENTRY) {
		auto lambda_index = ExpressionBinder::GetLambdaArgumentIndex(func->Cast<ScalarFunctionCatalogEntry>());
		// the receiver of a dot call is the function's first argument, so the written arguments start at the second
		const idx_t receiver_count = dot_call ? 1 : 0;
		if (lambda_index.IsValid() && lambda_index.GetIndex() == index + receiver_count) {
			return true;
		}
	}
	// the walks also reach the call of a FROM clause item, which binds to a table function of the name
	return !entry && IsTableMacro(binder, function, name);
}

bool ColumnQualifier::TryGetLambdaParameters(Binder &binder, const FunctionExpression &function, idx_t index,
                                             identifier_set_t &parameters, optional_ptr<CatalogEntry> entry) {
	auto &argument = function.GetArguments()[index].GetExpression();
	if (argument.GetExpressionClass() != ExpressionClass::LAMBDA) {
		return false;
	}
	auto &lambda = argument.Cast<LambdaExpression>();
	if (lambda.GetLambdaSyntaxType() != LambdaSyntaxType::LAMBDA_KEYWORD &&
	    !IsLambdaArgument(binder, function, index, entry)) {
		return false;
	}
	string error_message;
	auto column_refs = lambda.ExtractColumnRefExpressions(error_message);
	if (!error_message.empty()) {
		return false;
	}
	for (auto &column_ref : column_refs) {
		parameters.insert(column_ref.get().Cast<ColumnRefExpression>().GetName());
	}
	return true;
}

void ColumnQualifier::EnumerateCallChildren(
    Binder &binder, FunctionExpression &function,
    const std::function<void(unique_ptr<ParsedExpression> &child)> &callback,
    const std::function<void(LambdaExpression &lambda, identifier_set_t &parameters)> &lambda_callback,
    optional_ptr<CatalogEntry> entry) {
	auto &arguments = function.GetArgumentsMutable();
	for (idx_t i = 0; i < arguments.size(); i++) {
		auto &child = arguments[i].GetExpressionMutable();
		identifier_set_t parameters;
		if (TryGetLambdaParameters(binder, function, i, parameters, entry)) {
			lambda_callback(child->Cast<LambdaExpression>(), parameters);
		} else {
			callback(child);
		}
	}
	if (function.FilterMutable()) {
		callback(function.FilterMutable());
	}
	if (function.OrderByMutable()) {
		for (auto &order : function.OrderByMutable()->orders) {
			callback(order.expression);
		}
	}
}

void ColumnQualifier::QualifyColumnNamesInLambda(FunctionExpression &function, optional_ptr<CatalogEntry> entry) {
	EnumerateCallChildren(
	    binder, function,
	    [&](unique_ptr<ParsedExpression> &child) {
		    if (child->GetExpressionClass() != ExpressionClass::LAMBDA) {
			    QualifyColumnNames(child, true);
			    return;
		    }
		    auto &arrow = child->Cast<LambdaExpression>();
		    QualifyColumnNames(arrow.LeftMutable(), true);
		    QualifyColumnNames(arrow.RightMutable(), true);
	    },
	    [&](LambdaExpression &lambda, identifier_set_t &parameters) {
		    lambda_params.push_back(std::move(parameters));
		    QualifyColumnNames(lambda.RightMutable(), true);
		    lambda_params.pop_back();
	    },
	    entry);
}

unique_ptr<ParsedExpression> ColumnQualifier::QualifyColumnNameWithManyDotsInternal(ColumnRefExpression &col_ref,
                                                                                    ErrorData &error,
                                                                                    idx_t &struct_extract_start) {
	// two or more dots (i.e. "part1.part2.part3.part4...")
	// the reference can be qualified with an arbitrarily deep catalog/schema path, e.g.:
	// -> catalog.schema1.schema2...table.column.field...
	// -> schema1.schema2...table.column.field...
	// -> catalog.table.column.field...
	// -> table.column.field...
	// -> column.field...

	auto &names = col_ref.ColumnNames();
	ErrorData table_error;
	idx_t column_index = 0;
	auto binding = FindTableColumn(names, column_index, table_error);
	if (binding) {
		struct_extract_start = column_index + 1;
		return binder.bind_context.CreateColumnReference(binding->GetBindingAlias(), names[column_index]);
	}
	// part1 could be a column
	ErrorData unused_error;
	auto result_expr = QualifyColumnName(col_ref, names[0], unused_error);
	if (result_expr) {
		// it is! add the struct extract calls
		struct_extract_start = 1;
		return result_expr;
	}
	auto struct_pack = CreateStructPack(col_ref);
	if (struct_pack) {
		return struct_pack;
	}

	// we could not find the column - return the most specific error we encountered
	error = std::move(table_error);
	return nullptr;
}

optional_ptr<Binding> ColumnQualifier::FindTableColumn(const vector<Identifier> &names, idx_t &column_index,
                                                       optional_ptr<ErrorData> error) {
	D_ASSERT(names.size() >= 2);
	// we always prefer the most top-level view - i.e. we resolve the table as far to the right as possible (so the
	// leading components are treated as catalog/schema rather than column/struct fields), and within a given table
	// position we prefer treating the leading component as a catalog over a schema
	// we could not find the column - remember the most specific error to return. In order of preference:
	//  2. the table qualification matched a binding, but it has no such column
	//  1. the table name matches a binding, but the catalog/schema qualification does not
	//  0. the shallowest "table.column" error (nothing matched)
	ErrorData best_error;
	idx_t best_error_priority = 0;
	// try each possible table position, from the most-qualified (table furthest to the right) to the least
	for (idx_t table_index = names.size() - 2;; table_index--) {
		auto &column_name = names[table_index + 1];
		for (auto &alias : GetTableQualifications(names, table_index)) {
			ErrorData attempt_error;
			auto binding = binder.GetMatchingBinding(alias, column_name, attempt_error);
			if (binding) {
				column_index = table_index + 1;
				return binding;
			}
			if (!error) {
				continue;
			}
			idx_t priority = 0;
			if (TableBindingExists(alias)) {
				// the full qualification matched a binding, but it has no such column
				priority = 2;
			} else if (TableNameExists(alias.GetAlias())) {
				// the table name matches a binding, but the catalog/schema qualification does not
				priority = 1;
			}
			// prefer the first (most-qualified) error of the highest priority we have seen
			if (priority > best_error_priority || (priority == best_error_priority && !best_error.HasError())) {
				best_error = std::move(attempt_error);
				best_error_priority = priority;
			}
		}
		if (table_index == 0) {
			break;
		}
	}
	if (error) {
		*error = std::move(best_error);
	}
	return nullptr;
}

bool ColumnQualifier::TableProvidesReference(const ColumnRefExpression &col_ref) {
	if (!col_ref.IsQualified()) {
		return false;
	}
	idx_t column_index = 0;
	return FindTableColumn(col_ref.ColumnNames(), column_index, nullptr) != nullptr;
}

bool ColumnQualifier::TableNameExists(const Identifier &table_name) {
	for (const auto &binding_entry : binder.bind_context.GetBindingsList()) {
		if (binding_entry->GetBindingAlias().GetAlias() == table_name) {
			return true;
		}
	}
	return false;
}

bool ColumnQualifier::IsLambdaParameterReference(const vector<identifier_set_t> &lambdas,
                                                 const ColumnRefExpression &col_ref) {
	if (!LambdaExpression::IsLambdaParameter(lambdas, col_ref.ColumnNames()[0])) {
		return false;
	}
	return !TableProvidesReference(col_ref);
}

bool ColumnQualifier::TableBindingExists(const BindingAlias &alias) {
	try {
		ErrorData error;
		return binder.bind_context.GetBinding(alias, error) != nullptr;
	} catch (const std::exception &) {
		// an ambiguity between multiple matching tables still means the table qualification exists
		return true;
	}
}

unique_ptr<ParsedExpression> ColumnQualifier::QualifyColumnNameWithManyDots(ColumnRefExpression &col_ref,
                                                                            ErrorData &error) {
	idx_t struct_extract_start = col_ref.ColumnNames().size();
	auto result_expr = QualifyColumnNameWithManyDotsInternal(col_ref, error, struct_extract_start);
	if (!result_expr) {
		return nullptr;
	}

	// create a struct extract with all remaining column names
	for (idx_t i = struct_extract_start; i < col_ref.ColumnNames().size(); i++) {
		result_expr = CreateStructExtract(std::move(result_expr), col_ref.ColumnNames()[i]);
	}

	return result_expr;
}

unique_ptr<ParsedExpression> ColumnQualifier::QualifyColumnName(ColumnRefExpression &colref, ErrorData &error) {
	auto qualified_colref = QualifyColumnNameInternal(colref, error);
	if (!qualified_colref) {
		if (alias_binder) {
			return alias_binder->ResolveAlias(colref);
		}
		return nullptr;
	}
	if (!having_binder) {
		return qualified_colref;
	}
	auto group_index = having_binder->TryBindGroup(*qualified_colref);
	if (group_index.IsValid()) {
		return qualified_colref;
	}
	if (having_binder->column_alias_binder.DoesColumnAliasExist(colref)) {
		return nullptr;
	}
	return qualified_colref;
}

unique_ptr<ParsedExpression> ColumnQualifier::QualifyColumnNameInternal(ColumnRefExpression &col_ref,
                                                                        ErrorData &error) {
	if (!col_ref.IsQualified()) {
		// Try binding as a lambda parameter.
		auto lambda_ref = LambdaRefExpression::FindMatchingBinding(lambda_bindings, col_ref.GetColumnName());
		if (lambda_ref) {
			return lambda_ref;
		}
	}

	idx_t column_parts = col_ref.ColumnNames().size();

	// column names can have an arbitrary amount of dots
	// here is how the resolution works:
	if (column_parts == 1) {
		// no dots (i.e. "part1")
		// -> part1 refers to a column
		// check if we can qualify the column name with the table name
		auto qualified_col_ref = QualifyColumnName(col_ref, col_ref.GetColumnName(), error);
		if (qualified_col_ref) {
			// we could: return it
			return qualified_col_ref;
		}
		// we could not! Try creating an implicit struct_pack
		return CreateStructPack(col_ref);
	}

	if (column_parts == 2) {
		// one dot (i.e. "part1.part2")
		// EITHER:
		// -> part1 is a table, part2 is a column
		// -> part1 is a column, part2 is a property of that column (i.e. struct_extract)

		// first check if part1 is a table, and part2 is a standard column name
		auto binding = binder.GetMatchingBinding(col_ref.ColumnNames()[0], col_ref.ColumnNames()[1], error);
		if (binding) {
			// it is! return the column reference directly
			return binder.bind_context.CreateColumnReference(binding->GetBindingAlias(), col_ref.GetColumnName());
		}

		// otherwise check if we can turn this into a struct extract
		ErrorData other_error;
		auto qualified_col_ref = QualifyColumnName(col_ref, col_ref.ColumnNames()[0], other_error);
		if (qualified_col_ref) {
			// we could: create a struct extract
			return CreateStructExtract(std::move(qualified_col_ref), col_ref.ColumnNames()[1]);
		}
		// we could not! Try creating an implicit struct_pack
		return CreateStructPack(col_ref);
	}

	// three or more dots
	return QualifyColumnNameWithManyDots(col_ref, error);
}

} // namespace duckdb
