#include "duckdb/planner/expression_binder.hpp"

#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/lambda_expression.hpp"
#include "duckdb/parser/expression/lambdaref_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/column_qualifier.hpp"

namespace duckdb {

idx_t ExpressionBinder::ScopeCount() const {
	return binder.GetEnclosingScopes().size() + 1;
}

ExpressionBinder &ExpressionBinder::ScopeAt(idx_t depth) {
	D_ASSERT(depth < ScopeCount());
	if (depth == 0) {
		return *this;
	}
	// the enclosing scopes are stored outermost first, so the index is counted from the back
	auto &enclosing = binder.GetEnclosingScopes();
	return enclosing[enclosing.size() - depth];
}

static QueryLocation ExtractLocation(const unordered_map<string, string> &info) {
	auto pos_entry = info.find("position");
	if (pos_entry == info.end()) {
		return QueryLocation();
	}
	uint64_t start;
	if (!TryCast::Operation<string_t, uint64_t>(string_t(pos_entry->second), start)) {
		return QueryLocation();
	}
	uint64_t length = 0;
	auto location_entry = info.find("location");
	if (location_entry != info.end()) {
		// value is formatted as "[start,length]"
		auto comma = location_entry->second.find(',');
		if (comma != string::npos) {
			auto len_str = location_entry->second.substr(comma + 1);
			if (!len_str.empty() && len_str.back() == ']') {
				len_str.pop_back();
			}
			TryCast::Operation<string_t, uint64_t>(string_t(len_str), length);
		}
	}
	return QueryLocation(start, length);
}

static bool CombineMissingColumns(ErrorData &current, ErrorData new_error) {
	auto &current_info = current.ExtraInfo();
	auto &new_info = new_error.ExtraInfo();
	auto current_entry = current_info.find("error_subtype");
	auto new_entry = new_info.find("error_subtype");
	if (current_entry == current_info.end() || new_entry == new_info.end()) {
		// no subtype info in either expression
		return false;
	}
	if (current_entry->second != "COLUMN_NOT_FOUND" || new_entry->second != "COLUMN_NOT_FOUND") {
		// either info is not a `COLUMN_NOT_FOUND`
		return false;
	}
	current_entry = current_info.find("name");
	new_entry = new_info.find("name");
	if (current_entry == current_info.end() || new_entry == new_info.end()) {
		// no candidate info in either column
		return false;
	}
	if (current_entry->second != new_entry->second) {
		// error does not concern the same name/column
		return false;
	}
	auto column_name = current_entry->second;
	current_entry = current_info.find("candidates");
	new_entry = new_info.find("candidates");
	if (current_entry == current_info.end()) {
		// no current candidates - use new candidates
		current = std::move(new_error);
		return true;
	}
	if (new_entry == new_info.end()) {
		// no new candidates - use current candidates
		return true;
	}
	// both errors have candidates - combine the candidates
	auto current_candidates = StringUtil::Split(current_entry->second, ",");
	auto new_candidates = StringUtil::Split(new_entry->second, ",");
	current_candidates.insert(current_candidates.end(), new_candidates.begin(), new_candidates.end());

	// run the similarity ranking on both sets of candidates
	unordered_set<string> candidates;
	vector<pair<string, double>> scores;
	for (auto &candidate : current_candidates) {
		// split by "." since the candidates might be in the form "table.column"
		auto column_splits = StringUtil::Split(candidate, ".");
		if (column_splits.empty()) {
			continue;
		}
		auto &candidate_column = column_splits.back();
		auto entry = candidates.find(candidate);
		if (entry != candidates.end()) {
			// already found
			continue;
		}
		auto score = StringUtil::SimilarityRating(candidate_column, column_name);
		candidates.insert(candidate);
		scores.emplace_back(std::move(candidate), score);
	}
	// get a new top-n
	auto top_candidates = StringUtil::TopNStrings(scores);
	// get query location (prefer the current error's location, fall back to the new error's)
	auto location = ExtractLocation(current_info);
	if (!location.IsValid()) {
		location = ExtractLocation(new_info);
	}
	QueryErrorContext context(location);
	// generate a new (combined) error
	current = BinderException::ColumnNotFound(Identifier(column_name), StringsToIdentifiers(top_candidates), context);
	return true;
}

void ExpressionBinder::CombineErrors(ErrorData &current, ErrorData new_error) {
	// try to combine missing column exceptions in order to pick the most relevant one
	if (CombineMissingColumns(current, new_error)) {
		// keep the old info
		return;
	}

	// override the error with the new one
	// FIXME: the outermost scope searched always wins here, which can bury a more specific error from
	// the scope the reference was actually written in - e.g. HAVING's "must appear in the GROUP BY
	// clause" is replaced by an enclosing scope's "table not found". Preserving the innermost error
	// unless the outer one is strictly more specific would read better, but it changes many existing
	// error messages, so it is left as is here.
	current = std::move(new_error);
}

ColumnResolution ExpressionBinder::ResolveColumn(ColumnRefExpression &colref, idx_t start) {
	ColumnResolution result;
	for (idx_t depth = start; depth < ScopeCount(); depth++) {
		auto &scope = ScopeAt(depth);
		if (scope.ClaimsAlias(colref)) {
			// the scope has a select-list alias of this name: it owns the reference even though
			// qualification cannot produce a replacement for it
			result.found = true;
			result.depth = depth;
			return result;
		}
		auto qualifier = scope.CreateColumnQualifier();
		ErrorData error;
		auto qualified = qualifier->QualifyColumnName(colref, error);
		if (qualified) {
			result.found = true;
			result.depth = depth;
			result.qualified = std::move(qualified);
			return result;
		}
		if (scope.MatchesGroup(colref)) {
			// the name is one of this scope's groups by alias, which qualification cannot see
			result.found = true;
			result.depth = depth;
			return result;
		}
		if (depth == start) {
			result.error = std::move(error);
		} else {
			CombineErrors(result.error, std::move(error));
		}
	}
	return result;
}

//! Whether `predicate` holds for a column reference of the expression that the expression does not declare itself.
//! An argument declares lambda parameters only where `ColumnQualifier::EnumerateCallChildren` reads it as a lambda,
//! and any other arrow is the JSON operator, whose sides are ordinary expressions. A field access on a parameter is
//! declared too, unless a table of the query the lambda is written in provides the reference. Without
//! `every_binding` a subquery is skipped, since it binds in a chain of its own and for ownership its correlated
//! columns would count as well. With it the walk reads every name the expression may yet bind: the operand and the
//! body of a subquery, the receiver of a dot call and the relation of a star, which a subquery qualifies only when it
//! binds, and both sides of an arrow that a call reads as a lambda only until it expands.
template <class PREDICATE>
static bool AnyFreeColumn(Binder &binder, ColumnQualifier &qualifier, ParsedExpression &expr,
                          vector<identifier_set_t> &lambda_params, bool every_binding, PREDICATE &predicate,
                          bool from_item = false) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::SUBQUERY: {
		if (!every_binding) {
			return false;
		}
		auto &node = *expr.Cast<SubqueryExpression>().SubqueryMutable()->node;
		bool found = false;
		auto walk = [&](unique_ptr<ParsedExpression> &child, bool child_from_item) {
			found = found ||
			        AnyFreeColumn(binder, qualifier, *child, lambda_params, every_binding, predicate, child_from_item);
		};
		ColumnQualifier::EnumerateQueryNodeExpressions(node, walk);
		if (found) {
			return true;
		}
		break;
	}
	case ExpressionClass::COLUMN_REF: {
		auto &col_ref = expr.Cast<ColumnRefExpression>();
		if (qualifier.IsLambdaParameterReference(lambda_params, col_ref)) {
			return false;
		}
		return predicate(col_ref);
	}
	case ExpressionClass::STAR: {
		auto &star = expr.Cast<StarExpression>();
		if (every_binding && !star.RelationName().empty() && !qualifier.TableNameExists(star.RelationName())) {
			ColumnRefExpression relation(star.RelationName());
			if (!qualifier.IsLambdaParameterReference(lambda_params, relation) && predicate(relation)) {
				return true;
			}
		}
		break;
	}
	case ExpressionClass::FUNCTION: {
		auto &function = expr.Cast<FunctionExpression>();
		if (every_binding) {
			auto receiver = ColumnQualifier::DotCallReceiver(binder, function, from_item);
			if (receiver && !qualifier.IsLambdaParameterReference(lambda_params, *receiver) && predicate(*receiver)) {
				return true;
			}
		}
		if (!function.IsLambdaFunction()) {
			break;
		}
		bool found = false;
		ColumnQualifier::EnumerateCallChildren(
		    binder, function,
		    [&](unique_ptr<ParsedExpression> &child) {
			    found = found || AnyFreeColumn(binder, qualifier, *child, lambda_params, every_binding, predicate);
		    },
		    [&](LambdaExpression &lambda, identifier_set_t &parameters) {
			    if (found) {
				    return;
			    }
			    if (every_binding && lambda.GetLambdaSyntaxType() != LambdaSyntaxType::LAMBDA_KEYWORD &&
			        ColumnQualifier::ReadsArrowsProvisionally(binder, function, from_item)) {
				    found = AnyFreeColumn(binder, qualifier, *lambda.LeftMutable(), lambda_params, every_binding,
				                          predicate) ||
				            AnyFreeColumn(binder, qualifier, *lambda.RightMutable(), lambda_params, every_binding,
				                          predicate);
				    return;
			    }
			    lambda_params.push_back(std::move(parameters));
			    found =
			        AnyFreeColumn(binder, qualifier, *lambda.RightMutable(), lambda_params, every_binding, predicate);
			    lambda_params.pop_back();
		    },
		    nullptr, from_item);
		return found;
	}
	default:
		break;
	}
	for (auto &child : expr.ChildrenMutable()) {
		if (AnyFreeColumn(binder, qualifier, *child, lambda_params, every_binding, predicate)) {
			return true;
		}
	}
	return false;
}

//! Collect the column references that decide which scope owns an expression. Lambda parameters declared inside the
//! expression are skipped: they are bound by the lambda rather than by any scope, so a parameter that happens to
//! share a name with an outer column must not pull the expression out to that scope.
static void CollectResolvableColumns(Binder &binder, ColumnQualifier &qualifier, ParsedExpression &expr,
                                     vector<reference<ColumnRefExpression>> &result) {
	vector<identifier_set_t> lambda_params;
	auto collect = [&](ColumnRefExpression &col_ref) {
		result.push_back(col_ref);
		return false;
	};
	AnyFreeColumn(binder, qualifier, expr, lambda_params, false, collect);
}

bool ExpressionBinder::ReferencesLambdaParameter(ParsedExpression &expr) {
	if (!lambda_bindings) {
		return false;
	}
	ColumnQualifier qualifier(binder);
	vector<identifier_set_t> lambda_params;
	auto is_parameter = [&](ColumnRefExpression &col_ref) {
		return LambdaRefExpression::FindMatchingBinding(lambda_bindings, col_ref.ColumnNames()[0]) &&
		       !qualifier.TableProvidesReference(col_ref);
	};
	return AnyFreeColumn(binder, qualifier, expr, lambda_params, true, is_parameter);
}

optional_idx ExpressionBinder::ResolveAggregateOwner(FunctionExpression &aggregate, idx_t start) {
	vector<reference<ColumnRefExpression>> columns;
	ColumnQualifier qualifier(binder);
	// an arrow passed straight to the aggregate is the JSON operator, as the aggregate binds it
	for (auto &child : aggregate.GetArgumentsMutable()) {
		CollectResolvableColumns(binder, qualifier, *child.GetExpressionMutable(), columns);
	}
	if (aggregate.Filter()) {
		CollectResolvableColumns(binder, qualifier, *aggregate.FilterMutable(), columns);
	}
	if (aggregate.OrderBy()) {
		for (auto &order : aggregate.OrderByMutable()->orders) {
			CollectResolvableColumns(binder, qualifier, *order.expression, columns);
		}
	}
	return ResolveOwner(columns, start);
}

optional_idx ExpressionBinder::ResolveExpressionOwner(ParsedExpression &expr, idx_t start) {
	vector<reference<ColumnRefExpression>> columns;
	ColumnQualifier qualifier(binder);
	CollectResolvableColumns(binder, qualifier, expr, columns);
	return ResolveOwner(columns, start);
}

optional_idx ExpressionBinder::ResolveOwner(vector<reference<ColumnRefExpression>> &columns, idx_t start) {
	auto owner = start;
	bool found = false;
	for (auto &colref : columns) {
		auto resolution = ResolveColumn(colref.get(), start);
		if (!resolution.found) {
			// an alias or a column that does not resolve at all - it says nothing about ownership,
			// and the scope we settle on reports the error
			continue;
		}
		if (!found || resolution.depth < owner) {
			owner = resolution.depth;
			found = true;
		}
	}
	return found ? optional_idx(owner) : optional_idx();
}

optional_idx ExpressionBinder::ResolveOuterGroup(vector<reference<ParsedExpression>> &expressions, idx_t start) {
	for (auto &expr : expressions) {
		// a lambda parameter is bound in this scope, so no group of any scope is defined in terms of one
		if (ReferencesLambdaParameter(expr.get())) {
			return optional_idx();
		}
	}
	for (idx_t depth = start; depth < ScopeCount(); depth++) {
		bool all_match = true;
		for (auto &expr : expressions) {
			if (!ScopeAt(depth).MatchesGroup(expr.get())) {
				all_match = false;
				break;
			}
		}
		if (all_match) {
			return depth;
		}
	}
	return optional_idx();
}

//! Expression classes that can never equal a group expression, or that are already matched against
//! the groups elsewhere: a column reference is matched by name and alias in `ResolveColumn`
static bool CanMatchGroup(const ParsedExpression &expr) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::COLUMN_REF:
	case ExpressionClass::CONSTANT:
	case ExpressionClass::PARAMETER:
	case ExpressionClass::POSITIONAL_REFERENCE:
	case ExpressionClass::LAMBDA_REF:
	case ExpressionClass::LAMBDA:
	case ExpressionClass::STAR:
	case ExpressionClass::DEFAULT:
	case ExpressionClass::SUBQUERY:
	case ExpressionClass::TYPE:
	case ExpressionClass::WINDOW:
		return false;
	default:
		return true;
	}
}

optional_idx ExpressionBinder::ResolveOuterGroupOwner(ParsedExpression &expr) {
	if (!CanMatchGroup(expr)) {
		return optional_idx();
	}
	// resolving the owner walks the expression, so only do so when some enclosing scope can match at all -
	// plain column groups are already matched in `ResolveColumn`
	bool any_compound_groups = false;
	for (idx_t scope = 1; scope < ScopeCount() && !any_compound_groups; scope++) {
		any_compound_groups = ScopeAt(scope).HasCompoundGroups();
	}
	if (!any_compound_groups) {
		return optional_idx();
	}
	// the expression only refers to a group of the scope that owns all of its columns, so that e.g. a
	// column of an inner scope that shadows the grouped one keeps the expression in the inner scope
	auto owner = ResolveExpressionOwner(expr, 0);
	if (!owner.IsValid() || owner.GetIndex() == 0) {
		return optional_idx();
	}
	// the columns of a nested subquery can resolve in other scopes than the expression's own columns
	if (!ScopeAt(owner.GetIndex()).MatchesGroupWithoutSubquery(expr)) {
		return optional_idx();
	}
	return owner;
}

BindResult ExpressionBinder::BindGroupInEnclosingScope(unique_ptr<ParsedExpression> &expr_ptr, idx_t owner,
                                                       idx_t depth) {
	// the groups are keyed by the qualified form, so bind a qualified copy and leave the original intact
	auto qualified = expr_ptr->Copy();
	QualifyColumnNames(ScopeAt(owner).binder, qualified);
	return DispatchToScope(owner, qualified, depth);
}

} // namespace duckdb
