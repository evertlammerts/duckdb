#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/enums/on_entry_not_found.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/function/scalar_macro_function.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/expression/window_expression.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/expressionlistref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/tableref/pivotref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/parser/expression/lambda_expression.hpp"
#include "duckdb/planner/column_qualifier.hpp"

namespace duckdb {

void ExpressionBinder::ReplaceMacroParametersInLambda(FunctionExpression &function,
                                                      vector<identifier_set_t> &lambda_params) {
	for (auto &child : function.GetArgumentsMutable()) {
		if (child.GetExpression().GetExpressionClass() != ExpressionClass::LAMBDA) {
			ReplaceMacroParameters(child.GetExpressionMutable(), lambda_params);
			continue;
		}

		// Special-handling for LHS lambda parameters.
		// We do not replace them, and we add them to the lambda_params vector.
		auto &lambda_expr = child.GetExpressionMutable()->Cast<LambdaExpression>();
		string error_message;
		auto column_ref_expressions = lambda_expr.ExtractColumnRefExpressions(error_message);

		if (!error_message.empty()) {
			// Possibly a JSON function, replace both LHS and RHS.
			ReplaceMacroParameters(lambda_expr.LeftMutable(), lambda_params);
			ReplaceMacroParameters(lambda_expr.RightMutable(), lambda_params);
			continue;
		}

		// Push the lambda parameter names of this level.
		lambda_params.emplace_back();
		for (const auto &column_ref_expr : column_ref_expressions) {
			const auto &column_ref = column_ref_expr.get().Cast<ColumnRefExpression>();
			lambda_params.back().emplace(column_ref.GetName());
		}

		// Only replace in the RHS of the expression.
		ReplaceMacroParameters(lambda_expr.RightMutable(), lambda_params);

		lambda_params.pop_back();
	}
}

void ExpressionBinder::ReplaceMacroParameters(unique_ptr<ParsedExpression> &expr,
                                              vector<identifier_set_t> &lambda_params) {
	switch (expr->GetExpressionClass()) {
	case ExpressionClass::COLUMN_REF: {
		// If the expression is a column reference, we replace it with its argument.
		auto &col_ref = expr->Cast<ColumnRefExpression>();
		if (LambdaExpression::IsLambdaParameter(lambda_params, col_ref.GetColumnName())) {
			return;
		}

		bool bind_macro_parameter = false;
		if (col_ref.IsQualified()) {
			// the table qualifier is the component directly before the column name
			auto &names = col_ref.ColumnNames();
			if (names[names.size() - 2].StartsWith(DummyBinding::DUMMY_NAME)) {
				bind_macro_parameter = true;
			}
		} else {
			bind_macro_parameter = macro_binding->HasMatchingBinding(col_ref.GetColumnName());
		}

		if (bind_macro_parameter) {
			D_ASSERT(macro_binding->HasMatchingBinding(col_ref.GetColumnName()));
			expr = macro_binding->ParamToArg(col_ref);
		}
		return;
	}
	case ExpressionClass::FUNCTION: {
		// Special-handling for lambdas, which are inside function expressions.
		auto &function = expr->Cast<FunctionExpression>();
		if (function.IsLambdaFunction()) {
			return ReplaceMacroParametersInLambda(function, lambda_params);
		}
		break;
	}
	case ExpressionClass::SUBQUERY: {
		auto &sq = (expr->Cast<SubqueryExpression>()).Subquery();
		ParsedExpressionIterator::EnumerateQueryNodeChildren(
		    *sq->node, [&](unique_ptr<ParsedExpression> &child) { ReplaceMacroParameters(child, lambda_params); });
		break;
	}
	default:
		break;
	}

	ParsedExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<ParsedExpression> &child) { ReplaceMacroParameters(child, lambda_params); });
}

static bool GetLambdaParameters(LambdaExpression &lambda_expr, vector<reference<ColumnRefExpression>> &parameters) {
	auto &lhs = *lambda_expr.LeftMutable();
	if (lhs.GetExpressionClass() == ExpressionClass::COLUMN_REF) {
		auto &column_ref = lhs.Cast<ColumnRefExpression>();
		if (column_ref.IsQualified()) {
			return false;
		}
		parameters.push_back(column_ref);
		return true;
	}
	if (lhs.GetExpressionClass() != ExpressionClass::FUNCTION) {
		return false;
	}
	auto &row = lhs.Cast<FunctionExpression>();
	if (row.FunctionName() != "row" || row.GetArguments().empty()) {
		return false;
	}
	for (auto &argument : row.GetArgumentsMutable()) {
		auto &child = *argument.GetExpressionMutable();
		if (child.GetExpressionClass() != ExpressionClass::COLUMN_REF ||
		    child.Cast<ColumnRefExpression>().IsQualified()) {
			parameters.clear();
			return false;
		}
		parameters.push_back(child.Cast<ColumnRefExpression>());
	}
	return true;
}

static void CollectWrittenNames(ParsedExpression &expr, identifier_set_t &names) {
	if (expr.GetExpressionClass() == ExpressionClass::COLUMN_REF) {
		auto &column_names = expr.Cast<ColumnRefExpression>().ColumnNames();
		names.insert(column_names.begin(), column_names.end());
		return;
	}
	if (expr.GetExpressionClass() == ExpressionClass::FUNCTION) {
		auto &path = expr.Cast<FunctionExpression>().GetQualifiedName().Path();
		names.insert(path.begin(), path.end());
	}
	if (expr.GetExpressionClass() == ExpressionClass::SUBQUERY) {
		auto &sq = expr.Cast<SubqueryExpression>().Subquery();
		ParsedExpressionIterator::EnumerateQueryNodeChildren(
		    *sq->node, [&](unique_ptr<ParsedExpression> &child) { CollectWrittenNames(*child, names); });
	}
	ParsedExpressionIterator::EnumerateChildren(expr,
	                                            [&](ParsedExpression &child) { CollectWrittenNames(child, names); });
}

static void CollectTableName(TableRef &ref, identifier_set_t &names) {
	if (ref.type == TableReferenceType::BASE_TABLE && ref.alias.empty()) {
		auto &path = ref.Cast<BaseTableRef>().GetQualifiedName().Path();
		names.insert(path.begin(), path.end());
	}
	if (!ref.alias.empty()) {
		names.insert(ref.alias);
	} else if (ref.type == TableReferenceType::TABLE_FUNCTION) {
		auto &function = *ref.Cast<TableFunctionRef>().function;
		if (function.GetExpressionClass() == ExpressionClass::FUNCTION) {
			names.insert(function.Cast<FunctionExpression>().FunctionName());
		}
	}
}

static void CollectTableNames(ParsedExpression &expr, identifier_set_t &names) {
	if (expr.GetExpressionClass() == ExpressionClass::SUBQUERY) {
		ParsedExpressionIterator::EnumerateQueryNodeChildren(
		    *expr.Cast<SubqueryExpression>().Subquery()->node,
		    [&](unique_ptr<ParsedExpression> &child) { CollectTableNames(*child, names); },
		    [&](TableRef &ref) { CollectTableName(ref, names); });
	}
	ParsedExpressionIterator::EnumerateChildren(expr,
	                                            [&](ParsedExpression &child) { CollectTableNames(child, names); });
}

// The right side of a semi or anti join is visible to its join condition only, not to the select list.
static void CollectScopeTableNames(TableRef &ref, identifier_set_t &names, bool join_condition = false) {
	if (ref.type != TableReferenceType::JOIN) {
		CollectTableName(ref, names);
		return;
	}
	auto &join = ref.Cast<JoinRef>();
	CollectScopeTableNames(*join.left, names);
	if (join_condition || (join.type != JoinType::SEMI && join.type != JoinType::ANTI)) {
		CollectScopeTableNames(*join.right, names);
	}
}

using scoped_expression_callback_t = std::function<void(unique_ptr<ParsedExpression> &, const identifier_set_t &)>;

static void EnumerateScopedNodeExpressions(QueryNode &node, const identifier_set_t &declared_above,
                                           const scoped_expression_callback_t &callback);

static void EnumerateScopedRefExpressions(TableRef &ref, const identifier_set_t &declared_above,
                                          const identifier_set_t &declared,
                                          const scoped_expression_callback_t &callback) {
	auto visit = [&](unique_ptr<ParsedExpression> &child) {
		callback(child, declared);
	};
	switch (ref.type) {
	case TableReferenceType::JOIN: {
		auto &join = ref.Cast<JoinRef>();
		EnumerateScopedRefExpressions(*join.left, declared_above, declared, callback);
		EnumerateScopedRefExpressions(*join.right, declared_above, declared, callback);
		if (join.condition) {
			auto condition_declared = declared;
			CollectScopeTableNames(join, condition_declared, true);
			callback(join.condition, condition_declared);
		}
		break;
	}
	case TableReferenceType::PIVOT: {
		auto &pivot = ref.Cast<PivotRef>();
		EnumerateScopedRefExpressions(*pivot.source, declared_above, declared, callback);
		for (auto &aggregate : pivot.aggregates) {
			visit(aggregate);
		}
		break;
	}
	case TableReferenceType::SUBQUERY:
		// a derived table sees its FROM siblings, with or without LATERAL
		EnumerateScopedNodeExpressions(*ref.Cast<SubqueryRef>().subquery->node, declared, callback);
		break;
	default:
		ParsedExpressionIterator::EnumerateTableRefChildren(ref, visit);
		break;
	}
}

// A select node sees its FROM clause and the levels around it, not its CTE bodies or other set operation branches.
static void EnumerateScopedNodeExpressions(QueryNode &node, const identifier_set_t &declared_above,
                                           const scoped_expression_callback_t &callback) {
	switch (node.type) {
	case QueryNodeType::SELECT_NODE: {
		auto &select = node.Cast<SelectNode>();
		auto declared = declared_above;
		if (select.from_table) {
			CollectScopeTableNames(*select.from_table, declared);
		}
		auto visit = [&](unique_ptr<ParsedExpression> &child) {
			callback(child, declared);
		};
		for (auto &expr : select.select_list) {
			visit(expr);
		}
		for (auto &expr : select.groups.group_expressions) {
			visit(expr);
		}
		for (auto clause : {&select.where_clause, &select.having, &select.qualify}) {
			if (*clause) {
				visit(*clause);
			}
		}
		if (select.from_table) {
			EnumerateScopedRefExpressions(*select.from_table, declared_above, declared, callback);
		}
		ParsedExpressionIterator::EnumerateQueryNodeModifiers(node, visit);
		break;
	}
	case QueryNodeType::SET_OPERATION_NODE:
		for (auto &child : node.Cast<SetOperationNode>().children) {
			EnumerateScopedNodeExpressions(*child, declared_above, callback);
		}
		ParsedExpressionIterator::EnumerateQueryNodeModifiers(
		    node, [&](unique_ptr<ParsedExpression> &child) { callback(child, declared_above); });
		break;
	case QueryNodeType::RECURSIVE_CTE_NODE: {
		auto &cte = node.Cast<RecursiveCTENode>();
		EnumerateScopedNodeExpressions(*cte.left, declared_above, callback);
		EnumerateScopedNodeExpressions(*cte.right, declared_above, callback);
		ParsedExpressionIterator::EnumerateQueryNodeModifiers(
		    node, [&](unique_ptr<ParsedExpression> &child) { callback(child, declared_above); });
		break;
	}
	default:
		ParsedExpressionIterator::EnumerateQueryNodeChildren(
		    node, [&](unique_ptr<ParsedExpression> &child) { callback(child, declared_above); });
		return;
	}
	for (auto &entry : node.cte_map.map) {
		if (entry.second->query_node) {
			EnumerateScopedNodeExpressions(*entry.second->query_node, declared_above, callback);
		}
	}
}

// The qualifier resolves a dot call's receiver against tables first; reads inside a subquery never move, so skip them.
static bool HasQualifiedReference(ParsedExpression &expr, const Identifier &name) {
	if (expr.GetExpressionClass() == ExpressionClass::COLUMN_REF) {
		auto &column_ref = expr.Cast<ColumnRefExpression>();
		return column_ref.IsQualified() && column_ref.ColumnNames()[0] == name;
	}
	if (expr.GetExpressionClass() == ExpressionClass::FUNCTION) {
		auto &path = expr.Cast<FunctionExpression>().GetQualifiedName().Path();
		if (path.size() >= 2 && path[0] == name) {
			return true;
		}
	}
	bool found = false;
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](unique_ptr<ParsedExpression> &child) { found = found || HasQualifiedReference(*child, name); });
	return found;
}

// A VALUES list, also as the derived table without column aliases it prints as, binds its expressions in the scope
// around it and gives its level only its alias and its column names.
static bool IsValuesList(TableRef &ref, identifier_set_t &level_names) {
	reference<TableRef> inner = ref;
	while (inner.get().type == TableReferenceType::SUBQUERY && inner.get().column_name_alias.empty()) {
		auto &node = *inner.get().Cast<SubqueryRef>().subquery->node;
		if (node.type != QueryNodeType::SELECT_NODE || !node.modifiers.empty() || !node.cte_map.map.empty()) {
			return false;
		}
		auto &select = node.Cast<SelectNode>();
		if (select.select_list.size() != 1 || !select.select_list[0]->Equals(StarExpression()) || !select.from_table ||
		    select.where_clause || !select.groups.group_expressions.empty() || !select.groups.grouping_sets.empty() ||
		    select.having || select.qualify || select.sample) {
			return false;
		}
		inner = *select.from_table;
	}
	if (inner.get().type != TableReferenceType::EXPRESSION_LIST || !inner.get().column_name_alias.empty()) {
		return false;
	}
	auto &values = inner.get().Cast<ExpressionListRef>();
	if (!ref.alias.empty()) {
		level_names.insert(ref.alias);
	}
	for (auto &name : values.expected_names) {
		level_names.insert(name);
	}
	if (values.expected_names.empty() && !values.values.empty()) {
		for (idx_t i = 0; i < values.values[0].size(); i++) {
			level_names.insert(Identifier("col" + to_string(i)));
		}
	}
	return true;
}

static bool HasTable(TableRef &ref, identifier_set_t &level_names) {
	if (ref.type == TableReferenceType::JOIN) {
		auto &join = ref.Cast<JoinRef>();
		auto left = HasTable(*join.left, level_names);
		auto right = HasTable(*join.right, level_names);
		return left || right;
	}
	return ref.type != TableReferenceType::EMPTY_FROM && !IsValuesList(ref, level_names);
}

static identifier_set_t Without(const identifier_set_t &names, const identifier_set_t &removed) {
	identifier_set_t result;
	for (auto &name : names) {
		if (removed.find(name) == removed.end()) {
			result.insert(name);
		}
	}
	return result;
}

using folded_expression_callback_t =
    std::function<void(unique_ptr<ParsedExpression> &expr, const identifier_set_t &names, bool table_in_scope)>;

static void EnumerateFoldedRef(TableRef &ref, const identifier_set_t &names, bool outer_in_scope, bool in_scope,
                               const folded_expression_callback_t &callback);

// A reference binds the parameter where no table of its level or a level around it, and no name of its level, has it.
static void EnumerateFoldedNode(QueryNode &node, const identifier_set_t &names, bool table_in_scope,
                                const folded_expression_callback_t &callback) {
	static const identifier_set_t none;
	switch (node.type) {
	case QueryNodeType::SELECT_NODE: {
		auto &select = node.Cast<SelectNode>();
		identifier_set_t level_names;
		auto in_scope = table_in_scope || (select.from_table && HasTable(*select.from_table, level_names));
		auto unbound_names = Without(names, level_names);
		identifier_set_t clause_names;
		for (auto &name : unbound_names) {
			bool aliased = false;
			for (auto &item : select.select_list) {
				aliased = aliased || item->GetAlias() == name;
			}
			if (!aliased) {
				clause_names.insert(name);
			}
		}
		auto &select_names = in_scope ? none : unbound_names;
		auto &level_clause_names = in_scope ? none : clause_names;
		for (auto &expr : select.select_list) {
			callback(expr, select_names, in_scope);
		}
		for (auto &expr : select.groups.group_expressions) {
			callback(expr, level_clause_names, in_scope);
		}
		for (auto clause : {&select.where_clause, &select.having, &select.qualify}) {
			if (*clause) {
				callback(*clause, level_clause_names, in_scope);
			}
		}
		if (select.from_table) {
			EnumerateFoldedRef(*select.from_table, names, table_in_scope, in_scope, callback);
		}
		ParsedExpressionIterator::EnumerateQueryNodeModifiers(
		    node, [&](unique_ptr<ParsedExpression> &child) { callback(child, level_clause_names, in_scope); });
		break;
	}
	case QueryNodeType::SET_OPERATION_NODE:
		for (auto &child : node.Cast<SetOperationNode>().children) {
			EnumerateFoldedNode(*child, names, table_in_scope, callback);
		}
		ParsedExpressionIterator::EnumerateQueryNodeModifiers(node, [&](unique_ptr<ParsedExpression> &child) {
			callback(child, table_in_scope ? none : names, table_in_scope);
		});
		break;
	case QueryNodeType::RECURSIVE_CTE_NODE: {
		auto &cte = node.Cast<RecursiveCTENode>();
		EnumerateFoldedNode(*cte.left, names, table_in_scope, callback);
		EnumerateFoldedNode(*cte.right, names, table_in_scope, callback);
		break;
	}
	default:
		return;
	}
	for (auto &entry : node.cte_map.map) {
		if (entry.second->query_node) {
			EnumerateFoldedNode(*entry.second->query_node, names, table_in_scope, callback);
		}
	}
}

static void EnumerateFoldedRef(TableRef &ref, const identifier_set_t &names, bool outer_in_scope, bool in_scope,
                               const folded_expression_callback_t &callback) {
	static const identifier_set_t none;
	switch (ref.type) {
	case TableReferenceType::JOIN: {
		auto &join = ref.Cast<JoinRef>();
		EnumerateFoldedRef(*join.left, names, outer_in_scope, in_scope, callback);
		// the right side sees the left side's tables, with or without LATERAL
		identifier_set_t left_names;
		auto left_has_table = HasTable(*join.left, left_names);
		EnumerateFoldedRef(*join.right, Without(names, left_names), outer_in_scope || left_has_table, in_scope,
		                   callback);
		if (join.condition) {
			identifier_set_t join_names;
			HasTable(join, join_names);
			callback(join.condition, in_scope ? none : Without(names, join_names), in_scope);
		}
		break;
	}
	case TableReferenceType::SUBQUERY:
		EnumerateFoldedNode(*ref.Cast<SubqueryRef>().subquery->node, names, outer_in_scope, callback);
		break;
	case TableReferenceType::EXPRESSION_LIST:
	case TableReferenceType::TABLE_FUNCTION:
		ParsedExpressionIterator::EnumerateTableRefChildren(ref, [&](unique_ptr<ParsedExpression> &child) {
			callback(child, outer_in_scope ? none : names, outer_in_scope);
		});
		break;
	default:
		break;
	}
}

namespace {

struct LambdaScope {
	explicit LambdaScope(LambdaExpression &lambda_expr) : lambda_expr(lambda_expr) {
	}
	reference<LambdaExpression> lambda_expr;
	vector<reference<ColumnRefExpression>> parameters;
	identifier_set_t captured;
	//! Whether the lambda lies in a query of the body; a subquery inside the lambda keeps its own tables either way
	bool in_body_query = false;
};

using lambda_callback_t = std::function<void(LambdaExpression &, vector<reference<ColumnRefExpression>> &)>;

class MacroLambdaRenamer {
public:
	MacroLambdaRenamer(const vector<reference<unique_ptr<ParsedExpression>>> &body, Binder &binder,
	                   DummyBinding &macro_binding, optional_ptr<Binder> dummy_binder,
	                   optional_ptr<ParsedExpression> window_aggregate, optional_ptr<ParsedExpression> caller_window,
	                   optional_ptr<QueryNode> body_node)
	    : body(body), binder(binder), macro_binding(macro_binding), dummy_binder(dummy_binder),
	      body_is_qualified(dummy_binder), window_aggregate(window_aggregate), caller_window(caller_window),
	      body_node(body_node) {
	}

	void Rename(unique_ptr<ParsedExpression> &expr) {
		if (window_aggregate && expr.get() == window_aggregate.get()) {
			Capture(CallerWindowNames());
		}
		switch (expr->GetExpressionClass()) {
		case ExpressionClass::COLUMN_REF:
			CaptureColumnReference(expr->Cast<ColumnRefExpression>());
			return;
		case ExpressionClass::FUNCTION: {
			auto &function = expr->Cast<FunctionExpression>();
			auto &path = function.GetQualifiedName().Path();
			// the dummy qualification turns a dot call on a macro parameter, a.upper(), into a parameter reference
			if (body_is_qualified && !scopes.empty() && path.size() >= 2 && !IsLambdaParameter(path[0]) &&
			    macro_binding.HasMatchingBinding(path[0]) && IsDotCall(function, QualifyingBinder())) {
				CaptureMacroParameter(path[0]);
			}
			if (body_is_qualified && path.size() >= 3 && IsLambdaParameter(path[0]) &&
			    IsDotCall(function, QualifyingBinder())) {
				auto &qualified_name = function.GetQualifiedName();
				CaptureByCallerScope(ColumnRefExpression(qualified_name.Schema(), qualified_name.Catalog()));
			}
			vector<bool> folded;
			if (body_is_qualified && FoldedArguments(function, folded)) {
				auto &arguments = function.GetArgumentsMutable();
				for (idx_t i = 0; i < arguments.size(); i++) {
					caller_scope_folding += folded[i];
					Rename(arguments[i].GetExpressionMutable());
					caller_scope_folding -= folded[i];
				}
				if (function.FilterMutable()) {
					Rename(function.FilterMutable());
				}
				if (function.OrderByMutable()) {
					for (auto &order : function.OrderByMutable()->orders) {
						Rename(order.expression);
					}
				}
				return;
			}
			if (!function.IsLambdaFunction()) {
				break;
			}
			EnumerateCallChildren(
			    function, QualifyingBinder(), [&](unique_ptr<ParsedExpression> &child) { Rename(child); },
			    [&](LambdaExpression &lambda_expr, vector<reference<ColumnRefExpression>> &parameters) {
				    scopes.emplace_back(lambda_expr);
				    scopes.back().parameters = std::move(parameters);
				    scopes.back().in_body_query = !body_is_qualified;
				    Rename(lambda_expr.RightMutable());
				    RenameCaptured(scopes.back());
				    scopes.pop_back();
			    });
			return;
		}
		case ExpressionClass::SUBQUERY: {
			// the dummy qualification does not enter a subquery; the caller's scope is asked about a folded one only
			auto &sq = expr->Cast<SubqueryExpression>().Subquery();
			if (body_is_qualified && caller_scope_folding > 0) {
				identifier_set_t names;
				for (auto &scope : scopes) {
					for (auto &parameter : scope.parameters) {
						names.insert(parameter.get().GetColumnName());
					}
				}
				vector<identifier_set_t> inner_parameters;
				ProbeFoldedNode(*sq->node, names, false, inner_parameters);
			}
			auto outer_body_is_qualified = body_is_qualified;
			body_is_qualified = false;
			ParsedExpressionIterator::EnumerateQueryNodeChildren(
			    *sq->node, [&](unique_ptr<ParsedExpression> &child) { Rename(child); });
			body_is_qualified = outer_body_is_qualified;
			break;
		}
		default:
			break;
		}
		ParsedExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<ParsedExpression> &child) { Rename(child); });
	}

private:
	const identifier_set_t &BodyTableNames() {
		if (!body_table_names) {
			body_table_names = make_uniq<identifier_set_t>();
			if (body_node) {
				ParsedExpressionIterator::EnumerateQueryNodeChildren(
				    *body_node,
				    [&](unique_ptr<ParsedExpression> &child) { CollectTableNames(*child, *body_table_names); },
				    [&](TableRef &ref) { CollectTableName(ref, *body_table_names); });
			} else {
				for (auto &expr : body) {
					CollectTableNames(*expr.get(), *body_table_names);
				}
			}
		}
		return *body_table_names;
	}

	bool IsLambdaParameter(const Identifier &name) const {
		for (auto &scope : scopes) {
			for (auto &parameter : scope.parameters) {
				if (parameter.get().GetColumnName() == name) {
					return true;
				}
			}
		}
		return false;
	}

	optional_ptr<LambdaScope> DeclaringScope(const Identifier &name) {
		for (idx_t i = scopes.size(); i > 0; i--) {
			for (auto &parameter : scopes[i - 1].parameters) {
				if (parameter.get().GetColumnName() == name) {
					return scopes[i - 1];
				}
			}
		}
		return nullptr;
	}

	// The caller's scope, where a scalar macro body binds, tries its own tables before lambda parameters.
	void CaptureByCallerScope(const ColumnRefExpression &column_ref) {
		if (!column_ref.IsQualified()) {
			return;
		}
		auto &parameter = column_ref.ColumnNames()[0];
		auto scope = DeclaringScope(parameter);
		if (!scope || scope->captured.find(parameter) != scope->captured.end()) {
			return;
		}
		if (CallerScopeBindsElsewhere(column_ref)) {
			Capture(identifier_set_t {parameter});
		}
	}

	// A folded subquery binds in the lambda's scope wherever no table is in scope, as the lambda's top does.
	void ProbeFoldedNode(QueryNode &node, const identifier_set_t &names, bool table_in_scope,
	                     vector<identifier_set_t> &inner_parameters) {
		EnumerateFoldedNode(node, names, table_in_scope,
		                    [&](unique_ptr<ParsedExpression> &expr, const identifier_set_t &expr_names, bool in_scope) {
			                    ProbeFoldedSubquery(*expr, expr_names, in_scope, inner_parameters);
		                    });
	}

	void ProbeFoldedSubquery(ParsedExpression &expr, const identifier_set_t &names, bool table_in_scope,
	                         vector<identifier_set_t> &inner_parameters) {
		switch (expr.GetExpressionClass()) {
		case ExpressionClass::COLUMN_REF: {
			auto &column_ref = expr.Cast<ColumnRefExpression>();
			auto &leading_name = column_ref.ColumnNames()[0];
			if (names.count(leading_name) && !LambdaExpression::IsLambdaParameter(inner_parameters, leading_name)) {
				CaptureByCallerScope(column_ref);
			}
			return;
		}
		case ExpressionClass::FUNCTION: {
			// a dot call's receiver and an arrow at a lambda position are read as at the top of the lambda
			auto &function = expr.Cast<FunctionExpression>();
			auto &path = function.GetQualifiedName().Path();
			if (path.size() >= 3 && names.count(path[0]) &&
			    !LambdaExpression::IsLambdaParameter(inner_parameters, path[0]) && IsDotCall(function, ProbeBinder())) {
				auto &qualified_name = function.GetQualifiedName();
				CaptureByCallerScope(ColumnRefExpression(qualified_name.Schema(), qualified_name.Catalog()));
			}
			EnumerateCallChildren(
			    function, ProbeBinder(),
			    [&](unique_ptr<ParsedExpression> &child) {
				    ProbeFoldedSubquery(*child, names, table_in_scope, inner_parameters);
			    },
			    [&](LambdaExpression &lambda_expr, vector<reference<ColumnRefExpression>> &parameters) {
				    inner_parameters.emplace_back();
				    for (auto &parameter : parameters) {
					    inner_parameters.back().insert(parameter.get().GetColumnName());
				    }
				    ProbeFoldedSubquery(*lambda_expr.RightMutable(), names, table_in_scope, inner_parameters);
				    inner_parameters.pop_back();
			    });
			return;
		}
		case ExpressionClass::SUBQUERY:
			// the IN, ANY or ALL operand is a child of the subquery expression, read below
			ProbeFoldedNode(*expr.Cast<SubqueryExpression>().Subquery()->node, names, table_in_scope, inner_parameters);
			break;
		default:
			break;
		}
		ParsedExpressionIterator::EnumerateChildren(expr, [&](unique_ptr<ParsedExpression> &child) {
			ProbeFoldedSubquery(*child, names, table_in_scope, inner_parameters);
		});
	}

	bool CallerScopeBindsElsewhere(const ColumnRefExpression &column_ref) {
		auto &parameter = column_ref.ColumnNames()[0];
		vector<DummyBinding> parameter_binding;
		parameter_binding.emplace_back(vector<LogicalType> {LogicalType::ANY}, vector<Identifier> {parameter},
		                               parameter.GetIdentifierName());
		ColumnQualifier qualifier(binder, &parameter_binding);
		ColumnRefExpression probe(column_ref.ColumnNames());
		unique_ptr<ParsedExpression> qualified;
		try {
			ErrorData error;
			qualified = qualifier.QualifyColumnName(probe, error);
		} catch (std::exception &ex) {
			if (ErrorData(ex).Type() != ExceptionType::BINDER) {
				throw;
			}
			// an ambiguous name in the caller's scope is no reading of the parameter either
			return true;
		}
		D_ASSERT(qualified);
		reference<ParsedExpression> base = *qualified;
		while (base.get().GetExpressionType() == ExpressionType::STRUCT_EXTRACT) {
			base = *base.get().Cast<OperatorExpression>().GetChildren()[0];
		}
		return base.get().GetExpressionClass() != ExpressionClass::LAMBDA_REF;
	}

	// A rename can end ReplaceMacroParameters' last name test, so a qualified form counts whatever its last name.
	void CaptureColumnReference(ColumnRefExpression &column_ref) {
		auto &names = column_ref.ColumnNames();
		if (body_is_qualified) {
			CaptureByCallerScope(column_ref);
		}
		if (!column_ref.IsQualified()) {
			if (!IsLambdaParameter(names[0])) {
				CaptureMacroParameter(names[0]);
			}
		} else if (names[names.size() - 2].StartsWith(DummyBinding::DUMMY_NAME)) {
			CaptureMacroParameter(names.back());
		} else if (body_is_qualified && !IsLambdaParameter(names[0])) {
			// the dummy qualification reads a.d on a macro parameter a as a field of the argument
			CaptureMacroParameter(names[0]);
		}
	}

	void CaptureMacroParameter(const Identifier &parameter) {
		column_t index;
		if (scopes.empty() || !macro_binding.TryGetBindingIndex(parameter, index)) {
			return;
		}
		Capture(ArgumentNames(index));
	}

	const identifier_set_t &ArgumentNames(column_t index) {
		auto entry = argument_names.find(index);
		if (entry == argument_names.end()) {
			vector<identifier_set_t> bound;
			entry = argument_names.emplace(index, identifier_set_t()).first;
			CollectFreeNames(*(*macro_binding.arguments)[index], bound, entry->second);
		}
		return entry->second;
	}

	const identifier_set_t &CallerWindowNames() {
		if (!caller_window_names) {
			caller_window_names = make_uniq<identifier_set_t>();
			vector<identifier_set_t> bound;
			CollectFreeNames(*caller_window, bound, *caller_window_names);
		}
		return *caller_window_names;
	}

	// The caller's expressions are qualified by the expanding binder, so ProbeBinder answers for them.
	void CollectFreeNames(ParsedExpression &expr, vector<identifier_set_t> &bound, identifier_set_t &names) {
		switch (expr.GetExpressionClass()) {
		case ExpressionClass::COLUMN_REF: {
			auto &leading_name = expr.Cast<ColumnRefExpression>().ColumnNames()[0];
			if (!LambdaExpression::IsLambdaParameter(bound, leading_name)) {
				names.insert(leading_name);
			}
			return;
		}
		case ExpressionClass::FUNCTION: {
			auto &function = expr.Cast<FunctionExpression>();
			auto &path = function.GetQualifiedName().Path();
			// the receiver of a dot call, x.upper(), is a reference to x
			if (path.size() >= 2 && !LambdaExpression::IsLambdaParameter(bound, path[0]) &&
			    IsDotCall(function, ProbeBinder())) {
				names.insert(path[0]);
			}
			EnumerateCallChildren(
			    function, ProbeBinder(),
			    [&](unique_ptr<ParsedExpression> &child) { CollectFreeNames(*child, bound, names); },
			    [&](LambdaExpression &lambda_expr, vector<reference<ColumnRefExpression>> &parameters) {
				    bound.emplace_back();
				    for (auto &parameter : parameters) {
					    bound.back().insert(parameter.get().GetColumnName());
				    }
				    CollectFreeNames(*lambda_expr.RightMutable(), bound, names);
				    bound.pop_back();
			    });
			return;
		}
		case ExpressionClass::LAMBDA: {
			// an argument that is itself an arrow lands where the body puts it, and reading it as JSON only adds names
			auto &lambda_expr = expr.Cast<LambdaExpression>();
			vector<reference<ColumnRefExpression>> parameters;
			if (lambda_expr.GetLambdaSyntaxType() != LambdaSyntaxType::LAMBDA_KEYWORD ||
			    !GetLambdaParameters(lambda_expr, parameters)) {
				break;
			}
			bound.emplace_back();
			for (auto &parameter : parameters) {
				bound.back().insert(parameter.get().GetColumnName());
			}
			CollectFreeNames(*lambda_expr.RightMutable(), bound, names);
			bound.pop_back();
			return;
		}
		case ExpressionClass::SUBQUERY:
			// an aggregate inside a lambda can hold the argument's subquery, where only its own tables shadow a name
			EnumerateScopedNodeExpressions(*expr.Cast<SubqueryExpression>().Subquery()->node, identifier_set_t(),
			                               [&](unique_ptr<ParsedExpression> &child, const identifier_set_t &declared) {
				                               bound.push_back(declared);
				                               CollectFreeNames(*child, bound, names);
				                               bound.pop_back();
			                               });
			break;
		default:
			break;
		}
		ParsedExpressionIterator::EnumerateChildren(
		    expr, [&](ParsedExpression &child) { CollectFreeNames(child, bound, names); });
	}

	void Capture(const identifier_set_t &names) {
		for (auto &scope : scopes) {
			for (auto &parameter : scope.parameters) {
				auto &name = parameter.get().GetColumnName();
				if (names.find(name) != names.end()) {
					scope.captured.insert(name);
				}
			}
		}
	}

	// Only a quoted table name can collide; one name per original keeps a lambda equal to its GROUP BY copy.
	Identifier GetUnusedName(const Identifier &name) {
		auto known = new_names.find(name);
		if (known != new_names.end()) {
			return known->second;
		}
		if (!used_names) {
			used_names = make_uniq<identifier_set_t>();
			for (auto &expr : body) {
				CollectWrittenNames(*expr.get(), *used_names);
			}
			for (auto &argument : *macro_binding.arguments) {
				CollectWrittenNames(*argument, *used_names);
			}
			if (caller_window) {
				CollectWrittenNames(*caller_window, *used_names);
			}
		}
		for (idx_t suffix = 1;; suffix++) {
			Identifier candidate("0_" + to_string(suffix) + "_" + name.GetIdentifierName());
			if (!BodyTableNames().count(candidate) && used_names->insert(candidate).second) {
				new_names.emplace(name, candidate);
				return candidate;
			}
		}
	}

	// A child of the expanding binder has its search path; without the catalog callback it records no dependency.
	Binder &ProbeBinder() {
		if (!probe_binder) {
			probe_binder = Binder::CreateBinder(binder.context, &binder);
			probe_binder->EntryRetriever().SetCallback(nullptr);
		}
		return *probe_binder;
	}

	// The dummy binder qualifies the top of a scalar macro body; the expanding binder qualifies everything else.
	Binder &QualifyingBinder() {
		return body_is_qualified ? *dummy_binder : ProbeBinder();
	}

	// Asked of the binder that will qualify the call, which decides whether it is a dot call.
	static optional_ptr<CatalogEntry> FindFunction(const FunctionExpression &function, Binder &qualifying_binder,
	                                               bool &dot_call) {
		dot_call = false;
		if (ExpressionBinder::IsUnnestFunction(function.FunctionName())) {
			return nullptr;
		}
		FunctionExpression probe(function.GetQualifiedName(), vector<unique_ptr<ParsedExpression>>());
		auto entry = ColumnQualifier(qualifying_binder).QualifyFunction(probe);
		dot_call = function.GetQualifiedName().Path().size() >= 2 && probe.GetQualifiedName().Path().size() == 1;
		return entry;
	}

	static bool IsDotCall(const FunctionExpression &function, Binder &qualifying_binder) {
		bool dot_call;
		FindFunction(function, qualifying_binder, dot_call);
		return dot_call;
	}

	// As TryBindLambdaOrJson reads it; an arrow the binder may not read as a lambda is the JSON operator.
	bool TakesArrowAsLambda(const LambdaExpression &arrow, const FunctionExpression &function, idx_t index,
	                        Binder &qualifying_binder) {
		if (arrow.GetLambdaSyntaxType() == LambdaSyntaxType::SINGLE_ARROW &&
		    Settings::Get<LambdaSyntaxSetting>(binder.context) != LambdaSyntax::ENABLE_SINGLE_ARROW) {
			return false;
		}
		bool dot_call;
		auto entry = FindFunction(function, qualifying_binder, dot_call);
		if (!entry || entry->type != CatalogType::SCALAR_FUNCTION_ENTRY) {
			return false;
		}
		auto lambda_index = ExpressionBinder::GetLambdaArgumentIndex(entry->Cast<ScalarFunctionCatalogEntry>());
		// the receiver of a dot call is the function's first argument
		return lambda_index.IsValid() && lambda_index.GetIndex() == index + (dot_call ? 1 : 0);
	}

	// A keyword lambda always declares its parameters, an arrow only where the binder binds it as a lambda.
	bool DeclaresParameters(FunctionExpression &function, idx_t index, Binder &qualifying_binder,
	                        vector<reference<ColumnRefExpression>> &parameters) {
		auto &argument = function.GetArgumentsMutable()[index].GetExpressionMutable();
		if (argument->GetExpressionClass() != ExpressionClass::LAMBDA) {
			return false;
		}
		auto &lambda_expr = argument->Cast<LambdaExpression>();
		if (!GetLambdaParameters(lambda_expr, parameters)) {
			return false;
		}
		if (lambda_expr.GetLambdaSyntaxType() == LambdaSyntaxType::LAMBDA_KEYWORD ||
		    TakesArrowAsLambda(lambda_expr, function, index, qualifying_binder)) {
			return true;
		}
		parameters.clear();
		return false;
	}

	void EnumerateCallChildren(FunctionExpression &function, Binder &qualifying_binder,
	                           const std::function<void(unique_ptr<ParsedExpression> &)> &callback,
	                           const lambda_callback_t &lambda_callback) {
		auto &arguments = function.GetArgumentsMutable();
		for (idx_t i = 0; i < arguments.size(); i++) {
			vector<reference<ColumnRefExpression>> parameters;
			if (DeclaresParameters(function, i, qualifying_binder, parameters)) {
				lambda_callback(arguments[i].GetExpressionMutable()->Cast<LambdaExpression>(), parameters);
			} else {
				callback(arguments[i].GetExpressionMutable());
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

	// Where ReplaceMacroParameters substitutes an argument that names one of the lambda's own parameters.
	bool ReadsMacroParameter(ParsedExpression &expr, vector<identifier_set_t> &declared) {
		if (expr.GetExpressionClass() == ExpressionClass::COLUMN_REF) {
			auto &names = expr.Cast<ColumnRefExpression>().ColumnNames();
			bool substituted = names.size() == 1 ||
			                   (names.size() >= 2 && names[names.size() - 2].StartsWith(DummyBinding::DUMMY_NAME));
			column_t index;
			if (!substituted || LambdaExpression::IsLambdaParameter(declared, names.back()) ||
			    !macro_binding.TryGetBindingIndex(names.back(), index)) {
				return false;
			}
			auto &argument_names = ArgumentNames(index);
			for (auto &name : declared.front()) {
				if (argument_names.find(name) != argument_names.end()) {
					return true;
				}
			}
			return false;
		}
		if (expr.GetExpressionClass() == ExpressionClass::LAMBDA) {
			auto &lambda_expr = expr.Cast<LambdaExpression>();
			vector<reference<ColumnRefExpression>> parameters;
			if (GetLambdaParameters(lambda_expr, parameters)) {
				declared.emplace_back();
				for (auto &parameter : parameters) {
					declared.back().insert(parameter.get().GetColumnName());
				}
				auto found = ReadsMacroParameter(*lambda_expr.RightMutable(), declared);
				declared.pop_back();
				return found;
			}
		}
		bool found = false;
		auto visit = [&](unique_ptr<ParsedExpression> &child) {
			found = found || ReadsMacroParameter(*child, declared);
		};
		if (expr.GetExpressionClass() == ExpressionClass::SUBQUERY) {
			ParsedExpressionIterator::EnumerateQueryNodeChildren(*expr.Cast<SubqueryExpression>().Subquery()->node,
			                                                     visit);
		}
		ParsedExpressionIterator::EnumerateChildren(expr, visit);
		return found;
	}

	// The binder names a parameter by its alias, so a declaration takes none; an inner one keeps shadowing.
	void RenameDeclaration(LambdaExpression &lambda_expr, vector<reference<ColumnRefExpression>> &parameters,
	                       const identifier_map_t<Identifier> &renames) {
		// a lambda in a subquery only needs the new name where an argument lands in its body
		vector<identifier_set_t> declared(1);
		for (auto &parameter : parameters) {
			declared.back().insert(parameter.get().GetColumnName());
		}
		if (subquery_depth > 0 && !ReadsMacroParameter(*lambda_expr.RightMutable(), declared)) {
			return;
		}
		identifier_map_t<Identifier> own;
		for (auto &parameter : parameters) {
			auto &name = parameter.get().ColumnNamesMutable()[0];
			auto entry = renames.find(name);
			if (entry != renames.end()) {
				own.emplace(name, entry->second);
				name = entry->second;
			}
		}
		if (subquery_depth == 0) {
			RenameReferences(lambda_expr.RightMutable(), renames);
			return;
		}
		// in a subquery only this lambda's own parameters are renamed in its body
		auto depth = subquery_depth;
		subquery_depth = 0;
		RenameReferences(lambda_expr.RightMutable(), own);
		subquery_depth = depth;
	}

	void RenameReferences(unique_ptr<ParsedExpression> &expr, const identifier_map_t<Identifier> &renames) {
		switch (expr->GetExpressionClass()) {
		case ExpressionClass::COLUMN_REF: {
			auto &column_ref = expr->Cast<ColumnRefExpression>();
			auto &leading_name = column_ref.ColumnNamesMutable()[0];
			auto entry = renames.find(leading_name);
			if (entry == renames.end() || subquery_depth > 0) {
				return;
			}
			// struct_pack(x) names its field after the reference
			if (!column_ref.IsQualified() && column_ref.GetAlias().empty()) {
				column_ref.SetAlias(leading_name);
			}
			leading_name = entry->second;
			return;
		}
		case ExpressionClass::LAMBDA: {
			// without an alias the left side names the new name as a column or a declaration; in a subquery it is JSON
			auto &lambda_expr = expr->Cast<LambdaExpression>();
			vector<reference<ColumnRefExpression>> parameters;
			if (subquery_depth > 0 || !GetLambdaParameters(lambda_expr, parameters)) {
				break;
			}
			RenameDeclaration(lambda_expr, parameters, renames);
			return;
		}
		case ExpressionClass::FUNCTION: {
			auto &function = expr->Cast<FunctionExpression>();
			auto &path = function.GetQualifiedName().Path();
			if (subquery_depth == 0 && path.size() >= 2 && renames.find(path[0]) != renames.end() &&
			    IsDotCall(function, QualifyingBinder())) {
				// rewritten as the qualifier would, so that the receiver is a reference renamed below with its alias
				auto qualified_name = function.GetQualifiedName();
				auto receiver = qualified_name.Catalog().empty()
				                    ? make_uniq<ColumnRefExpression>(qualified_name.Schema())
				                    : make_uniq<ColumnRefExpression>(qualified_name.Schema(), qualified_name.Catalog());
				auto &arguments = function.GetArgumentsMutable();
				arguments.insert(arguments.begin(), FunctionArgument(std::move(receiver)));
				function.SetQualifiedName(QualifiedName(qualified_name.Name()));
			}
			vector<bool> folded;
			if (FoldedArguments(function, folded)) {
				auto &arguments = function.GetArgumentsMutable();
				for (idx_t i = 0; i < arguments.size(); i++) {
					folding += folded[i];
					RenameReferences(arguments[i].GetExpressionMutable(), renames);
					folding -= folded[i];
				}
				if (function.FilterMutable()) {
					RenameReferences(function.FilterMutable(), renames);
				}
				if (function.OrderByMutable()) {
					for (auto &order : function.OrderByMutable()->orders) {
						RenameReferences(order.expression, renames);
					}
				}
				return;
			}
			if (!function.IsLambdaFunction()) {
				break;
			}
			EnumerateCallChildren(
			    function, QualifyingBinder(),
			    [&](unique_ptr<ParsedExpression> &child) { RenameReferences(child, renames); },
			    [&](LambdaExpression &lambda_expr, vector<reference<ColumnRefExpression>> &parameters) {
				    RenameDeclaration(lambda_expr, parameters, renames);
			    });
			return;
		}
		case ExpressionClass::SUBQUERY: {
			auto &node = *expr->Cast<SubqueryExpression>().Subquery()->node;
			if (folding > 0) {
				auto outer_body_is_qualified = body_is_qualified;
				body_is_qualified = false;
				RenameFoldedNode(node, renames, folded_table_in_scope);
				body_is_qualified = outer_body_is_qualified;
			} else {
				RenameSubquery(node, renames);
			}
			break;
		}
		default:
			break;
		}
		ParsedExpressionIterator::EnumerateChildren(
		    *expr, [&](unique_ptr<ParsedExpression> &child) { RenameReferences(child, renames); });
	}

	// Bind callbacks that keep only the type of a subquery argument, and pg_typeof, a built in macro over typeof.
	bool FoldedArguments(FunctionExpression &function, vector<bool> &folded) {
		if (subquery_depth > 0 || !function.HasSubquery()) {
			return false;
		}
		bool dot_call;
		auto entry = FindFunction(function, QualifyingBinder(), dot_call);
		if (!entry || !entry->ParentCatalog().IsSystemCatalog()) {
			return false;
		}
		auto &name = entry->name;
		auto count = function.GetArguments().size();
		folded.assign(count, false);
		if (entry->type == CatalogType::MACRO_ENTRY) {
			if (name == "pg_typeof") {
				folded.assign(count, true);
			}
		} else if (entry->type == CatalogType::SCALAR_FUNCTION_ENTRY) {
			// a value argument of cast_to_type that reads the lambda's parameter already fails at CREATE MACRO
			if (name == "typeof" || name == "get_type" || name == "can_cast_implicitly" || name == "st_crs" ||
			    name == "cast_to_type" || name == "replace_type") {
				folded.assign(count, true);
			}
		}
		for (auto fold : folded) {
			if (fold) {
				return true;
			}
		}
		return false;
	}

	// Renames the references of a folded subquery that may still read the lambda's parameter.
	void RenameFoldedNode(QueryNode &node, const identifier_map_t<Identifier> &renames, bool table_in_scope) {
		identifier_set_t names;
		for (auto &entry : renames) {
			names.insert(entry.first);
		}
		EnumerateFoldedNode(node, names, table_in_scope,
		                    [&](unique_ptr<ParsedExpression> &expr, const identifier_set_t &expr_names, bool in_scope) {
			                    identifier_map_t<Identifier> expr_renames;
			                    for (auto &entry : renames) {
				                    if (expr_names.find(entry.first) != expr_names.end()) {
					                    expr_renames.emplace(entry.first, entry.second);
				                    }
			                    }
			                    auto outer = folded_table_in_scope;
			                    folded_table_in_scope = in_scope;
			                    RenameReferences(expr, expr_renames);
			                    folded_table_in_scope = outer;
		                    });
	}

	// A lambda beside a table of its name reads x.v as that table, as without the macro.
	void RenameSubquery(QueryNode &node, const identifier_map_t<Identifier> &renames) {
		auto outer_body_is_qualified = body_is_qualified;
		body_is_qualified = false;
		auto outer_folding = folding;
		subquery_depth++;
		folding = 0;
		EnumerateScopedNodeExpressions(node, identifier_set_t(),
		                               [&](unique_ptr<ParsedExpression> &child, const identifier_set_t &declared) {
			                               identifier_map_t<Identifier> scope_renames;
			                               for (auto &entry : renames) {
				                               if (declared.find(entry.first) == declared.end()) {
					                               scope_renames.emplace(entry.first, entry.second);
				                               }
			                               }
			                               RenameReferences(child, scope_renames);
		                               });
		subquery_depth--;
		folding = outer_folding;
		body_is_qualified = outer_body_is_qualified;
	}

	// x.v may name a body table x by scopes and columns unknown here, so such an x keeps its name.
	bool ReadsBodyTable(LambdaScope &scope, const Identifier &name) {
		return scope.in_body_query && BodyTableNames().count(name) &&
		       HasQualifiedReference(*scope.lambda_expr.get().RightMutable(), name);
	}

	void RenameCaptured(LambdaScope &scope) {
		if (scope.captured.empty()) {
			return;
		}
		identifier_map_t<Identifier> renames;
		for (auto &parameter : scope.parameters) {
			auto &name = parameter.get().ColumnNamesMutable()[0];
			if (scope.captured.find(name) == scope.captured.end() || ReadsBodyTable(scope, name)) {
				continue;
			}
			// a repeated parameter keeps one name, so the binder still rejects it as a duplicate
			auto entry = renames.find(name);
			if (entry == renames.end()) {
				entry = renames.emplace(name, GetUnusedName(name)).first;
			}
			name = entry->second;
		}
		RenameReferences(scope.lambda_expr.get().RightMutable(), renames);
	}

	const vector<reference<unique_ptr<ParsedExpression>>> &body;
	Binder &binder;
	DummyBinding &macro_binding;
	optional_ptr<Binder> dummy_binder;
	bool body_is_qualified;
	optional_ptr<ParsedExpression> window_aggregate;
	optional_ptr<ParsedExpression> caller_window;
	optional_ptr<QueryNode> body_node;
	vector<LambdaScope> scopes;
	idx_t subquery_depth = 0;
	idx_t folding = 0;
	idx_t caller_scope_folding = 0;
	bool folded_table_in_scope = false;
	unordered_map<column_t, identifier_set_t> argument_names;
	unique_ptr<identifier_set_t> caller_window_names;
	unique_ptr<identifier_set_t> used_names;
	unique_ptr<identifier_set_t> body_table_names;
	identifier_map_t<Identifier> new_names;
	shared_ptr<Binder> probe_binder;
};

} // namespace

void ExpressionBinder::RenameLambdaParametersCapturingArguments(
    const vector<reference<unique_ptr<ParsedExpression>>> &body, Binder &binder, DummyBinding &macro_binding,
    optional_ptr<Binder> dummy_binder, optional_ptr<ParsedExpression> window_aggregate,
    optional_ptr<ParsedExpression> caller_window, optional_ptr<QueryNode> body_node) {
	MacroLambdaRenamer renamer(body, binder, macro_binding, dummy_binder, window_aggregate, caller_window, body_node);
	for (auto &expr : body) {
		renamer.Rename(expr.get());
	}
}

static unique_ptr<ParsedExpression> &FindExpressionSlot(unique_ptr<ParsedExpression> &root,
                                                        const ParsedExpression &target) {
	optional_ptr<unique_ptr<ParsedExpression>> slot;
	std::function<void(unique_ptr<ParsedExpression> &)> find = [&](unique_ptr<ParsedExpression> &child) {
		if (slot) {
			return;
		}
		if (child.get() == &target) {
			slot = &child;
			return;
		}
		ParsedExpressionIterator::EnumerateChildren(*child, find);
	};
	find(root);
	if (!slot) {
		throw InternalException("Window macro aggregate not found in the macro body");
	}
	return *slot;
}

// Find aggregate expression children
void ExpressionBinder::FindAggregateExprs(unique_ptr<ParsedExpression> &expr,
                                          vector<reference<unique_ptr<ParsedExpression>>> &exprs) {
	if (expr->GetExpressionType() == ExpressionType::FUNCTION) {
		auto &fn_expr = expr->Cast<FunctionExpression>();

		// Look up the function in the catalog, check to see if it is actually an aggregate function
		EntryLookupInfo fn_entry(CatalogType::AGGREGATE_FUNCTION_ENTRY, fn_expr.GetQualifiedName());
		auto entry = GetCatalogEntry(fn_entry, OnEntryNotFound::RETURN_NULL);

		if (entry && entry->type == CatalogType::AGGREGATE_FUNCTION_ENTRY) {
			exprs.push_back(expr);
			return;
		}
	}

	ParsedExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<ParsedExpression> &child_expr) { FindAggregateExprs(child_expr, exprs); });
}

void ExpressionBinder::UnfoldWindowMacroExpression(unique_ptr<ParsedExpression> &expr, ScalarMacroFunction &macro_def,
                                                   Binder &dummy_binder) {
	auto macro_copy = macro_def.expression->Copy();
	vector<reference<unique_ptr<ParsedExpression>>> aggregate_exprs;
	FindAggregateExprs(macro_copy, aggregate_exprs);

	// Only allowed if the macro body has a single aggregate expression
	if (aggregate_exprs.size() != 1) {
		throw BinderException("Window function macro bodies must contain exactly one aggregate function");
	}

	// The window spec is pushed down to the aggregate function target within the macro body
	auto &agg_fn_expr = aggregate_exprs[0].get()->Cast<FunctionExpression>();

	// Transfer the macro function attributes
	auto &window_expr = expr->Cast<WindowExpression>();
	window_expr.SetQualifiedName(agg_fn_expr.GetQualifiedName());
	window_expr.GetArgumentsMutable().clear();
	// renamed after the call's arguments are cleared and before the caller's window clause moves into the body
	RenameLambdaParametersCapturingArguments({macro_copy}, binder, *macro_binding, dummy_binder, agg_fn_expr,
	                                         expr.get());
	// renaming can insert a dot call's receiver before the aggregate, which moves the slot holding it
	auto &agg_expr_ref = FindExpressionSlot(macro_copy, agg_fn_expr);
	for (auto &arg : agg_fn_expr.GetArgumentsMutable()) {
		window_expr.GetArgumentsMutable().push_back(std::move(arg));
	}
	if (!window_expr.Distinct()) {
		window_expr.DistinctMutable() = agg_fn_expr.Distinct();
	}
	if (window_expr.Filter() && agg_fn_expr.Filter()) {
		// Two FILTER clauses: combine
		window_expr.FilterMutable() =
		    make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(window_expr.FilterMutable()),
		                                     std::move(agg_fn_expr.FilterMutable()));
	} else if (agg_fn_expr.Filter()) {
		//	One FILTER from the MACRO
		window_expr.FilterMutable() = std::move(agg_fn_expr.FilterMutable());
	}
	// Transfer argument ORDER BYs
	if (agg_fn_expr.OrderBy()) {
		auto clone = agg_fn_expr.OrderBy()->Copy();
		window_expr.ArgOrdersMutable() = std::move(clone->Cast<OrderModifier>().orders);
	}

	// Replace the aggregate expression with the new window expression
	agg_expr_ref = std::move(expr);
	expr = std::move(macro_copy);
}

MacroFunction &ExpressionBinder::UnfoldMacroExpression(FunctionExpression &function,
                                                       ScalarMacroCatalogEntry &macro_func,
                                                       unique_ptr<ParsedExpression> &expr, idx_t depth) {
	// validate the arguments and separate positional and default arguments
	vector<unique_ptr<ParsedExpression>> positional_arguments;
	InsertionOrderPreservingMap<unique_ptr<ParsedExpression>, Identifier, identifier_map_t<idx_t>> named_arguments;
	binder.lambda_bindings = lambda_bindings;
	auto bind_result = MacroFunction::BindMacroFunction(binder, macro_func.macros, macro_func.name, function,
	                                                    positional_arguments, named_arguments, depth);
	if (!bind_result.error.empty()) {
		throw BinderException(*expr, bind_result.error);
	}
	auto &macro_def = macro_func.macros[bind_result.function_idx.GetIndex()]->Cast<ScalarMacroFunction>();

	auto new_macro_binding =
	    MacroFunction::CreateDummyBinding(macro_def, macro_func.name, positional_arguments, named_arguments);
	macro_binding = new_macro_binding.get();

	// a new empty binder that only knows the macro binding qualifies the macro parameters
	auto dummy_binder = Binder::CreateBinder(context);
	dummy_binder->macro_binding = new_macro_binding.get();

	// replace current expression with stored macro expression
	// special case: If this is a window function, then we need to return a window expression
	if (expr->GetExpressionClass() == ExpressionClass::WINDOW) {
		UnfoldWindowMacroExpression(expr, macro_def, *dummy_binder);
	} else {
		expr = macro_def.expression->Copy();
		RenameLambdaParametersCapturingArguments({expr}, binder, *macro_binding, dummy_binder.get());
	}

	ExpressionBinder::QualifyColumnNames(*dummy_binder, expr);

	// now replace the parameters
	vector<identifier_set_t> lambda_params;
	ReplaceMacroParameters(expr, lambda_params);
	return macro_def;
}

BindResult ExpressionBinder::BindMacro(FunctionExpression &function, ScalarMacroCatalogEntry &macro_func, idx_t depth,
                                       unique_ptr<ParsedExpression> &expr) {
	auto stack_checker = StackCheck(*expr, 3);
	// expanding nested macros can take a long time, allow the query to be interrupted
	context.InterruptCheck();

	// unfold the macro expression
	auto &member = UnfoldMacroExpression(function, macro_func, expr, depth);

	// bind the unfolded macro
	auto result = BindExpression(expr, depth);
	if (result.HasError()) {
		return result;
	}
	// enforce a declared RETURNS type by casting the member's result to it
	if (member.return_type.id() != LogicalTypeId::UNKNOWN) {
		result.expression =
		    BoundCastExpression::AddCastToType(context, std::move(result.expression), member.return_type);
	}
	return result;
}

} // namespace duckdb
