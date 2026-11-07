// Arrow sparse unions whose member names repeat, which Arrow allows and a DuckDB UNION type does not.

#include "catch.hpp"

#include "arrow/arrow_test_helper.hpp"
#include "duckdb.h"
#include "duckdb/common/adbc/single_batch_array_stream.hpp"
#include "test_helpers.hpp"

#include <memory>
#include <string>
#include <vector>

using namespace duckdb;

namespace arrow_union_duplicate_member_names_test {

void ReleaseSchema(ArrowSchema *s) {
	s->release = nullptr;
}
void ReleaseArray(ArrowArray *a) {
	a->release = nullptr;
}

// A hand built Arrow field and its array, owning its children and its buffers.
struct Field {
	ArrowSchema schema {};
	ArrowArray array {};
	std::string format;
	std::vector<std::unique_ptr<Field>> children;
	std::vector<int32_t> data;
	std::vector<int8_t> type_ids;
	std::vector<ArrowSchema *> schema_children;
	std::vector<ArrowArray *> array_children;
	std::vector<const void *> buffers;
};
using FieldPtr = std::unique_ptr<Field>;

FieldPtr MakeField(std::string format, const char *name, int64_t length, std::vector<FieldPtr> children,
                   std::vector<const void *> buffers) {
	auto field = std::unique_ptr<Field>(new Field());
	field->format = std::move(format);
	field->schema.format = field->format.c_str();
	field->schema.name = name;
	field->schema.flags = ARROW_FLAG_NULLABLE;
	field->schema.release = ReleaseSchema;
	field->array.length = length;
	field->array.release = ReleaseArray;
	field->children = std::move(children);
	for (auto &child : field->children) {
		field->schema_children.push_back(&child->schema);
		field->array_children.push_back(&child->array);
	}
	field->schema.n_children = int64_t(field->children.size());
	field->schema.children = field->schema_children.empty() ? nullptr : field->schema_children.data();
	field->array.n_children = int64_t(field->children.size());
	field->array.children = field->array_children.empty() ? nullptr : field->array_children.data();
	field->buffers = std::move(buffers);
	field->array.n_buffers = int64_t(field->buffers.size());
	field->array.buffers = field->buffers.data();
	return field;
}

FieldPtr Int32(const char *name, std::vector<int32_t> values) {
	auto length = int64_t(values.size());
	auto field = MakeField("i", name, length, {}, {});
	field->data = std::move(values);
	field->buffers = {nullptr, field->data.data()};
	field->array.n_buffers = 2;
	field->array.buffers = field->buffers.data();
	return field;
}

FieldPtr Struct(const char *name, std::vector<FieldPtr> children) {
	auto length = children[0]->array.length;
	return MakeField("+s", name, length, std::move(children), {nullptr});
}

FieldPtr List(const char *name, std::vector<int32_t> offsets, FieldPtr child) {
	std::vector<FieldPtr> children;
	children.push_back(std::move(child));
	auto field = MakeField("+l", name, int64_t(offsets.size()) - 1, std::move(children), {});
	field->data = std::move(offsets);
	field->buffers = {nullptr, field->data.data()};
	field->array.n_buffers = 2;
	field->array.buffers = field->buffers.data();
	return field;
}

// Members named by `names`, member i holding 10 * i + row; row j selects member type_ids[j].
FieldPtr SparseUnion(const char *name, std::vector<const char *> names, std::vector<int8_t> type_ids) {
	std::vector<FieldPtr> members;
	std::string format = "+us:";
	for (idx_t i = 0; i < names.size(); i++) {
		std::vector<int32_t> values;
		for (idx_t row = 0; row < type_ids.size(); row++) {
			values.push_back(int32_t(10 * i + row));
		}
		members.push_back(Int32(names[i], std::move(values)));
		format += (i ? "," : "") + std::to_string(i);
	}
	auto field = MakeField(format, name, int64_t(type_ids.size()), std::move(members), {});
	field->type_ids = std::move(type_ids);
	field->buffers = {field->type_ids.data()};
	field->array.n_buffers = 1;
	field->array.buffers = field->buffers.data();
	return field;
}

FieldPtr Record(FieldPtr column) {
	std::vector<FieldPtr> columns;
	columns.push_back(std::move(column));
	return Struct("", std::move(columns));
}

class RecordScanFactory : public ArrowScanFactory {
public:
	explicit RecordScanFactory(Field &record_p) : record(record_p) {
	}
	void GetSchema(ArrowSchema &out) override {
		out = record.schema;
	}
	unique_ptr<ArrowArrayStreamWrapper> ProduceStream(ArrowStreamParameters &parameters) override {
		auto schema = record.schema;
		auto array = record.array;
		auto result = make_uniq<ArrowArrayStreamWrapper>();
		AdbcError error {};
		if (duckdb_adbc::BatchToArrayStream(&array, &schema, &result->arrow_array_stream, &error) != ADBC_STATUS_OK) {
			throw InternalException("BatchToArrayStream failed");
		}
		return result;
	}

	Field &record;
};

// The rows of the projection over the scan, or the error.
string Scan(Field &record, const string &projection) {
	DuckDB db(nullptr);
	Connection con(db);
	auto factory = make_shared_ptr<RecordScanFactory>(record);
	try {
		auto result = con.TableFunction("arrow_scan", {}, {}, factory)->Project(projection)->Execute();
		if (result->HasError()) {
			return "error: " + ErrorData(result->GetError()).RawMessage();
		}
		string rows;
		for (auto chunk = result->Fetch(); chunk; chunk = result->Fetch()) {
			for (idx_t row = 0; row < chunk->size(); row++) {
				for (idx_t column = 0; column < chunk->ColumnCount(); column++) {
					rows += (column ? "\t" : "") + chunk->GetValue(column, row).ToString();
				}
				rows += "\n";
			}
		}
		return rows;
	} catch (std::exception &ex) {
		return "error: " + ErrorData(ex).RawMessage();
	}
}

string ScanType(Field &record) {
	return Scan(record, "typeof(u)");
}

// The error of the C API import of the schema, or "imported".
string CApiImport(Field &record) {
	duckdb_database db;
	duckdb_connection con;
	REQUIRE(duckdb_open(nullptr, &db) == DuckDBSuccess);
	REQUIRE(duckdb_connect(db, &con) == DuckDBSuccess);
	string text;
	duckdb_arrow_converted_schema converted = nullptr;
	auto error = duckdb_schema_from_arrow(con, &record.schema, &converted);
	if (error) {
		text = "error: " + ErrorData(duckdb_error_data_message(error)).RawMessage();
		duckdb_destroy_error_data(&error);
	} else {
		text = "imported";
		duckdb_destroy_arrow_converted_schema(&converted);
	}
	duckdb_disconnect(&con);
	duckdb_close(&db);
	return text;
}

TEST_CASE("Arrow scan of a sparse union with duplicate member names", "[arrow]") {
	SECTION("two members named a") {
		auto record = Record(SparseUnion("u", {"a", "a"}, {0, 1}));
		REQUIRE(ScanType(*record) == "error: Duplicate member name \"a\" in UNION(a INTEGER, a INTEGER)");
		REQUIRE(CApiImport(*record) == "error: Duplicate member name \"a\" in UNION(a INTEGER, a INTEGER)");
	}
	SECTION("names that differ only in case") {
		auto record = Record(SparseUnion("u", {"a", "A"}, {0, 1}));
		REQUIRE(ScanType(*record) == "error: Duplicate member name \"A\" in UNION(a INTEGER, A INTEGER)");
		REQUIRE(CApiImport(*record) == "error: Duplicate member name \"A\" in UNION(a INTEGER, A INTEGER)");
	}
	SECTION("a repeat after a distinct member") {
		auto record = Record(SparseUnion("u", {"a", "b", "a"}, {0, 1, 2}));
		REQUIRE(ScanType(*record) == "error: Duplicate member name \"a\" in UNION(a INTEGER, b INTEGER, a INTEGER)");
		REQUIRE(CApiImport(*record) == "error: Duplicate member name \"a\" in UNION(a INTEGER, b INTEGER, a INTEGER)");
	}
	SECTION("two unnamed members") {
		auto record = Record(SparseUnion("u", {"", ""}, {0, 1}));
		REQUIRE(ScanType(*record) == "error: Duplicate member name \"\" in UNION(\"\" INTEGER, \"\" INTEGER)");
		REQUIRE(CApiImport(*record) == "error: Duplicate member name \"\" in UNION(\"\" INTEGER, \"\" INTEGER)");
	}
	SECTION("a union inside a struct") {
		std::vector<FieldPtr> children;
		children.push_back(SparseUnion("x", {"a", "a"}, {0, 1}));
		auto record = Record(Struct("u", std::move(children)));
		REQUIRE(ScanType(*record) == "error: Duplicate member name \"a\" in UNION(a INTEGER, a INTEGER)");
		REQUIRE(CApiImport(*record) == "error: Duplicate member name \"a\" in UNION(a INTEGER, a INTEGER)");
	}
	SECTION("a list of unions") {
		auto record = Record(List("u", {0, 2}, SparseUnion("item", {"", ""}, {1, 0})));
		REQUIRE(ScanType(*record) == "error: Duplicate member name \"\" in UNION(\"\" INTEGER, \"\" INTEGER)");
		REQUIRE(CApiImport(*record) == "error: Duplicate member name \"\" in UNION(\"\" INTEGER, \"\" INTEGER)");
	}
}

TEST_CASE("Arrow scan of a sparse union with unique member names", "[arrow]") {
	SECTION("named members") {
		auto record = Record(SparseUnion("u", {"a", "b"}, {0, 1, 1}));
		REQUIRE(CApiImport(*record) == "imported");
		REQUIRE(Scan(*record, "typeof(u), u::VARCHAR, union_tag(u)::VARCHAR, union_extract(u, 'a'), "
		                      "union_extract(u, 'B')") == "UNION(a INTEGER, b INTEGER)\t0\ta\t0\tNULL\n"
		                                                  "UNION(a INTEGER, b INTEGER)\t11\tb\tNULL\t11\n"
		                                                  "UNION(a INTEGER, b INTEGER)\t12\tb\tNULL\t12\n");
	}
	SECTION("one unnamed member beside a named one") {
		auto record = Record(SparseUnion("u", {"", "b"}, {0, 1}));
		REQUIRE(Scan(*record, "typeof(u), u::VARCHAR, union_tag(u)::VARCHAR, union_extract(u, 'b')") ==
		        "UNION(\"\" INTEGER, b INTEGER)\t0\t\tNULL\n"
		        "UNION(\"\" INTEGER, b INTEGER)\t11\tb\t11\n");
		REQUIRE(Scan(*record, "union_extract(u, '')") == "error: Key name for union_extract must not be empty");
	}
}

} // namespace arrow_union_duplicate_member_names_test
