#include "catch.hpp"

#include <algorithm>
#include "test_helpers.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"

using namespace duckdb;

namespace {

vector<string> seen_at_clauses;

//! A catalog that keeps the snapshot at VERSION => n in the schema named vn
class TestTimeTravelCatalog : public DuckCatalog {
public:
	explicit TestTimeTravelCatalog(AttachedDatabase &db) : DuckCatalog(db) {
	}

public:
	bool SupportsTimeTravel() const override {
		return true;
	}

	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const EntryLookupInfo &schema_lookup,
	                                              OnEntryNotFound if_not_found) override {
		auto at_clause = schema_lookup.GetAtClause();
		if (!at_clause) {
			return DuckCatalog::LookupSchema(transaction, schema_lookup, if_not_found);
		}
		auto value = at_clause->GetValue().ToString();
		seen_at_clauses.push_back(at_clause->Unit().GetIdentifierName() + "=" + value);
		EntryLookupInfo snapshot(schema_lookup, QualifiedName(Identifier("v" + value)));
		return DuckCatalog::LookupSchema(transaction, snapshot, if_not_found);
	}
};

struct TestTimeTravelStorageExtension : StorageExtension {
	TestTimeTravelStorageExtension() {
		attach = [](optional_ptr<StorageExtensionInfo>, ClientContext &, AttachedDatabase &db, const string &,
		            AttachInfo &info, AttachOptions &) -> unique_ptr<Catalog> {
			info.path = IN_MEMORY_PATH;
			return make_uniq_base<Catalog, TestTimeTravelCatalog>(db);
		};
		create_transaction_manager = [](optional_ptr<StorageExtensionInfo>, AttachedDatabase &db,
		                                Catalog &) -> unique_ptr<TransactionManager> {
			return make_uniq<DuckTransactionManager>(db);
		};
	}
};

void RequireSeen(const vector<string> &at_clauses) {
	REQUIRE(seen_at_clauses == at_clauses);
	seen_at_clauses.clear();
}

} // namespace

TEST_CASE("A prepared AT clause is resolved at every EXECUTE with the values given there", "[api]") {
	DBConfig config;
	StorageExtension::Register(config, "test_time_travel", make_shared_ptr<TestTimeTravelStorageExtension>());
	DuckDB db(nullptr, &config);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("ATTACH 'tt' AS tt (TYPE test_time_travel)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE tt.t AS SELECT 0 AS i"));
	for (auto &version : {"1", "2"}) {
		auto schema = string("tt.v") + version;
		REQUIRE_NO_FAIL(con.Query("CREATE SCHEMA " + schema));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE " + schema + ".t AS SELECT " + version + "0 AS i"));
		REQUIRE_NO_FAIL(con.Query("CREATE VIEW " + schema + ".vw AS SELECT i FROM tt.main.t"));
	}
	seen_at_clauses.clear();

	auto result = con.Query("SELECT i FROM tt.t");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	REQUIRE(seen_at_clauses.empty());
	result = con.Query("SELECT i FROM tt.t AT (VERSION => 2)");
	REQUIRE(CHECK_COLUMN(result, 0, {20}));
	RequireSeen({"VERSION=2"});

	REQUIRE_NO_FAIL(con.Query("PREPARE p AS SELECT i FROM tt.t AT (VERSION => $1::BIGINT)"));
	REQUIRE(seen_at_clauses.empty());
	result = con.Query("EXECUTE p(1)");
	REQUIRE(CHECK_COLUMN(result, 0, {10}));
	RequireSeen({"VERSION=1"});
	result = con.Query("EXECUTE p(2)");
	REQUIRE(CHECK_COLUMN(result, 0, {20}));
	RequireSeen({"VERSION=2"});
	result = con.Query("EXECUTE p(1)");
	REQUIRE(CHECK_COLUMN(result, 0, {10}));
	RequireSeen({"VERSION=1"});
	result = con.Query("EXECUTE p(3)");
	REQUIRE(result->HasError());
	REQUIRE(result->GetErrorType() == ExceptionType::CATALOG);
	REQUIRE(!seen_at_clauses.empty());
	REQUIRE(std::all_of(seen_at_clauses.begin(), seen_at_clauses.end(),
	                    [](const string &seen) { return seen == "VERSION=3"; }));
	seen_at_clauses.clear();
	result = con.Query("EXECUTE p(2)");
	REQUIRE(CHECK_COLUMN(result, 0, {20}));
	RequireSeen({"VERSION=2"});

	REQUIRE_NO_FAIL(con.Query("PREPARE e AS SELECT i FROM tt.t AT (VERSION => $1::BIGINT + 1)"));
	REQUIRE(seen_at_clauses.empty());
	result = con.Query("EXECUTE e(1)");
	REQUIRE(CHECK_COLUMN(result, 0, {20}));
	RequireSeen({"VERSION=2"});

	REQUIRE_NO_FAIL(con.Query(
	    "PREPARE j AS SELECT a.i, b.i FROM tt.t a AT (VERSION => $1::BIGINT), tt.t b AT (VERSION => $2::BIGINT)"));
	REQUIRE(seen_at_clauses.empty());
	result = con.Query("EXECUTE j(1, 2)");
	REQUIRE(CHECK_COLUMN(result, 0, {10}));
	REQUIRE(CHECK_COLUMN(result, 1, {20}));
	RequireSeen({"VERSION=1", "VERSION=2"});
	result = con.Query("EXECUTE j(2, 1)");
	REQUIRE(CHECK_COLUMN(result, 0, {20}));
	REQUIRE(CHECK_COLUMN(result, 1, {10}));
	RequireSeen({"VERSION=2", "VERSION=1"});

	REQUIRE_NO_FAIL(con.Query("PREPARE w AS SELECT i FROM tt.vw AT (VERSION => $1::BIGINT)"));
	REQUIRE(seen_at_clauses.empty());
	result = con.Query("EXECUTE w(1)");
	REQUIRE(CHECK_COLUMN(result, 0, {10}));
	RequireSeen({"VERSION=1", "VERSION=1"});
	result = con.Query("EXECUTE w(2)");
	REQUIRE(CHECK_COLUMN(result, 0, {20}));
	RequireSeen({"VERSION=2", "VERSION=2"});

	auto prepared = con.Prepare("SELECT i FROM tt.t AT (VERSION => $1::BIGINT)");
	REQUIRE(!prepared->HasError());
	REQUIRE(seen_at_clauses.empty());
	auto executed = prepared->Execute(2);
	REQUIRE(CHECK_COLUMN(executed, 0, {20}));
	RequireSeen({"VERSION=2"});
	executed = prepared->Execute(1);
	REQUIRE(CHECK_COLUMN(executed, 0, {10}));
	RequireSeen({"VERSION=1"});
}
