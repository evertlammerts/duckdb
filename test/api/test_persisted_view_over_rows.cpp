#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/column_data_ref.hpp"

using namespace duckdb;

TEST_CASE("Test a persisted view over materialized rows", "[api]") {
	auto db_path = TestCreatePath("persisted_view_over_rows.db");
	{
		DuckDB db(db_path);
		Connection con(db);

		REQUIRE_NO_FAIL(con.Query("create table tbl(a varchar);"));

		auto result = con.Query("insert into tbl values ('test') returning *");
		shared_ptr<ColumnDataCollection> collection = result->TakeCollection();
		auto rows = make_uniq<ColumnDataRef>(std::move(collection), result->GetNames());
		rows->alias = Identifier("vw");
		auto node = make_uniq<SelectNode>();
		node->select_list.push_back(make_uniq<StarExpression>());
		node->from_table = std::move(rows);
		auto select = make_uniq<SelectStatement>();
		select->node = std::move(node);
		auto info = make_uniq<CreateViewInfo>();
		info->query = std::move(select);
		info->temporary = false;
		info->SetQualifiedName(QualifiedName(Identifier(), Identifier::InvalidSchema(), Identifier("vw")));
		info->on_conflict = OnCreateConflict::REPLACE_ON_CONFLICT;
		auto create = make_uniq<CreateStatement>();
		create->info = std::move(info);
		con.Query(std::move(create))->ThrowIfError();

		result = con.Query("SELECT * FROM vw");
		REQUIRE(CHECK_COLUMN(result, 0, {"test"}));
	}
	// reset and query again
	{
		DuckDB db(db_path);
		Connection con(db);
		auto result = con.Query("SELECT * FROM vw");
		REQUIRE(CHECK_COLUMN(result, 0, {"test"}));
	}
}
