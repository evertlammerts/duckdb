#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/common/arrow/arrow_query_result.hpp"
#include "duckdb/common/arrow/physical_arrow_collector.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/execution/operator/helper/physical_result_collector.hpp"
#include "duckdb/main/buffered_data/buffered_data.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/main/query_result_stream.hpp"
#include "result_wait_helpers.hpp"

#include <chrono>
#include <thread>

using namespace duckdb;

namespace {

//! Submit a query, leaving the retention undecided
unique_ptr<QueryResult> Submit(Connection &con, const string &query, QueryParameters parameters = {}) {
	auto handle = con.Submit(query, std::move(parameters));
	if (handle->HasError()) {
		FAIL(handle->GetError());
	}
	return handle;
}

idx_t DrainCursor(QueryResult &result) {
	idx_t rows = 0;
	while (auto chunk = result.Fetch()) {
		rows += chunk->size();
	}
	return rows;
}

//! Step a handle a few times, so its execution has started but has not finished
void StepUnfinished(QueryResult &handle) {
	for (idx_t step = 0; step < 5; step++) {
		REQUIRE(!IsTerminal(handle.ExecuteTask()));
	}
}

//! A dynamic PIVOT preprocesses into one CREATE TYPE per pivot column followed by the select
constexpr const char *PIVOT_QUERY = "PIVOT sales ON year USING sum(amount) ORDER BY city";

void CreateSales(Connection &con) {
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE sales (city VARCHAR, year INTEGER, amount INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO sales VALUES ('ams', 2023, 10), ('ams', 2024, 20), "
	                          "('rtm', 2023, 30), ('rtm', 2024, 40)"));
}

//! Stands in for an out-of-tree streaming collector: it builds its own result object and keeps the
//! query open, the combination no in-tree collector has
class TestCollectorState : public GlobalSinkState {
public:
	TestCollectorState(ClientContext &context, const vector<LogicalType> &types)
	    : collection(make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types)),
	      client_properties(context.GetClientProperties()) {
		collection->InitializeAppend(append_state);
	}

	unique_ptr<ColumnDataCollection> collection;
	ColumnDataAppendState append_state;
	ClientProperties client_properties;
};

class TestStreamingCollector : public PhysicalResultCollector {
public:
	TestStreamingCollector(PhysicalPlan &physical_plan, PreparedStatementData &data)
	    : PhysicalResultCollector(physical_plan, data) {
	}

public:
	bool IsStreaming() const override {
		return true;
	}

	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override {
		return make_uniq<TestCollectorState>(context, types);
	}

	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override {
		auto &gstate = input.global_state.Cast<TestCollectorState>();
		gstate.collection->Append(gstate.append_state, chunk);
		return SinkResultType::NEED_MORE_INPUT;
	}

	unique_ptr<QueryResult> GetResult(GlobalSinkState &state) const override {
		auto &gstate = state.Cast<TestCollectorState>();
		return make_uniq<QueryResult>(statement_type, properties, names, std::move(gstate.collection),
		                              gstate.client_properties);
	}
};

ScopedConfigSetting UseTestStreamingCollector(ClientConfig &config) {
	return ScopedConfigSetting(
	    config,
	    [](ClientConfig &config) {
		    config.get_result_collector = [](ClientContext &context,
		                                     PreparedStatementData &data) -> unique_ptr<PhysicalOperator> {
			    return make_uniq<TestStreamingCollector>(*data.physical_plan, data);
		    };
	    },
	    [](ClientConfig &config) { config.get_result_collector = nullptr; });
}

ScopedConfigSetting UseArrowCollector(ClientConfig &config) {
	return ScopedConfigSetting(
	    config,
	    [](ClientConfig &config) {
		    config.get_result_collector = [](ClientContext &context,
		                                     PreparedStatementData &data) -> unique_ptr<PhysicalOperator> {
			    return PhysicalArrowCollector::Create(context, data, STANDARD_VECTOR_SIZE);
		    };
	    },
	    [](ClientConfig &config) { config.get_result_collector = nullptr; });
}

} // namespace

#ifndef DUCKDB_NO_THREADS

TEST_CASE("Query returns a completed retained handle", "[api][query_result]") {
	DuckDB db(nullptr);
	Connection con(db);

	auto result = con.Query("SELECT i FROM range(2000) t(i)");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->RowCount() == 2000);
	REQUIRE(result->GetValue(0, 0).GetValue<int64_t>() == 0);
	REQUIRE(result->GetValue(0, 1999).GetValue<int64_t>() == 1999);
	REQUIRE(result->Collection().Count() == 2000);
	REQUIRE(!result->ToString().empty());
	// The cursor walks the collection the handle already holds
	REQUIRE(DrainCursor(*result) == 2000);
	// Retention was settled at submission, so no producer ever parked
	REQUIRE(result->GetBufferedData().Lifetime() == ResultLifetime::RETAINED);
	REQUIRE(result->GetBufferedData().PeakBufferedBytes() == 0);
}

TEST_CASE("A submitted query parks for the consumer's choice", "[api][query_result]") {
	DuckDB db(nullptr);
	Connection con(db);

	auto handle = Submit(con, "SELECT i FROM range(500000) t(i)");

	Deadline deadline;
	while (handle->Poll() != QueryResultState::READY) {
		REQUIRE(!deadline.Passed());
		std::this_thread::sleep_for(std::chrono::microseconds(100));
	}
	// READY is the engine waiting on the consumer: a producer is parked with its first chunk
	REQUIRE(handle->GetBufferedData().WaitsOnConsumer());
	REQUIRE(handle->GetBufferedData().Lifetime() == ResultLifetime::UNDECIDED);
	REQUIRE(handle->GetBufferedData().PeakBufferedBytes() == 0);
}

TEST_CASE("ExecuteTask on a parked undecided handle reports READY and runs nothing", "[api][query_result]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));

	auto handle = Submit(con, "SELECT i FROM range(500000) t(i)");
	Deadline deadline;
	QueryResultState state;
	while ((state = handle->ExecuteTask()) != QueryResultState::READY) {
		REQUIRE(state == QueryResultState::NOT_READY);
		REQUIRE(!deadline.Passed());
	}
	auto &buffer = handle->GetBufferedData();
	REQUIRE(buffer.WaitsOnConsumer());
	REQUIRE(buffer.Lifetime() == ResultLifetime::UNDECIDED);
	// The engine waits for the consumer's choice: stepping again decides nothing and runs nothing
	REQUIRE(handle->ExecuteTask() == QueryResultState::READY);
	REQUIRE(handle->ExecuteTask() == QueryResultState::READY);
	REQUIRE(handle->Poll() == QueryResultState::READY);
	REQUIRE(buffer.Lifetime() == ResultLifetime::UNDECIDED);
	REQUIRE(buffer.PeakBufferedBytes() == 0);
	// The choice releases the park
	REQUIRE(handle->Collection().Count() == 500000);
}

TEST_CASE("Materialize returns at once and the workers complete the query", "[api][query_result]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(200000)"));

	// The unordered plan uses the simple store, the table scan the batched one
	for (auto query : {"SELECT i FROM range(200000) t(i)", "SELECT i FROM t"}) {
		auto handle = Submit(con, query);

		handle->Materialize();
		// Nothing else is asked of the consumer: the workers run the query to completion
		Deadline deadline;
		while (handle->Poll() != QueryResultState::FINISHED) {
			REQUIRE(!deadline.Passed());
			std::this_thread::sleep_for(std::chrono::microseconds(100));
		}
		REQUIRE(handle->Collection().Count() == 200000);
		REQUIRE(handle->GetBufferedData().Lifetime() == ResultLifetime::RETAINED);
		REQUIRE(handle->GetBufferedData().PeakBufferedBytes() == 0);
		// Collecting finished the query, so the connection is free
		auto next = con.Query("SELECT 42");
		REQUIRE(CHECK_COLUMN(next, 0, {42}));
	}
}

TEST_CASE("Collecting a fresh submission takes the retained path", "[api][query_result]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET max_streaming_buffer_size='16KB'"));

	auto handle = Submit(con, "SELECT i FROM range(500000) t(i)");
	DrainWatchdog watchdog(con);
	auto &collection = handle->Collection();
	REQUIRE(collection.Count() == 500000);
	REQUIRE(handle->GetValue(0, 0).GetValue<int64_t>() == 0);
	REQUIRE(handle->GetValue(0, 499999).GetValue<int64_t>() == 499999);
	// Producers appended into the collection: nothing was ever staged in the streaming buffer
	REQUIRE(handle->GetBufferedData().Lifetime() == ResultLifetime::RETAINED);
	REQUIRE(handle->GetBufferedData().PeakBufferedBytes() == 0);
	// Collection is idempotent once retained
	REQUIRE(&handle->Collection() == &collection);
}

TEST_CASE("TakeCollection hands the collection over exactly once", "[api][query_result]") {
	DuckDB db(nullptr);
	Connection con(db);

	auto handle = Submit(con, "SELECT i FROM range(1000) t(i)");
	DrainWatchdog watchdog(con);
	auto collection = handle->TakeCollection();
	REQUIRE(collection);
	REQUIRE(collection->Count() == 1000);

	REQUIRE_THROWS_AS(handle->TakeCollection(), InvalidInputException);
	REQUIRE_THROWS_AS(handle->Collection(), InvalidInputException);
	REQUIRE_THROWS_AS(handle->Fetch(), InvalidInputException);
	// The collection outlives the handle it came from
	handle.reset();
	REQUIRE(collection->Count() == 1000);
}

TEST_CASE("An execution error surfaces on every retained-side call", "[api][query_result]") {
	DuckDB db(nullptr);
	Connection con(db);

	// The cast fails at a late row, long after the submission succeeded
	auto handle =
	    Submit(con, "SELECT (CASE WHEN i = 40000 THEN 'boom' ELSE i::VARCHAR END)::INT FROM range(50000) t(i)");
	REQUIRE(!handle->HasError());

	handle->Materialize();
	Deadline deadline;
	while (handle->Poll() != QueryResultState::EXECUTION_ERROR) {
		REQUIRE(!deadline.Passed());
		std::this_thread::sleep_for(std::chrono::microseconds(100));
	}
	REQUIRE(handle->HasError());
	REQUIRE(StringUtil::Contains(handle->GetError(), "boom"));
	REQUIRE_THROWS_AS(handle->Collection(), InvalidInputException);
	// GetValue throws the query's own error, not an internal one
	bool threw_query_error = false;
	try {
		handle->GetValue(0, 0);
	} catch (const std::exception &ex) {
		threw_query_error = StringUtil::Contains(ErrorData(ex).Message(), "boom");
	}
	REQUIRE(threw_query_error);
	REQUIRE(handle->RowCount() == 0);

	auto next = con.Query("SELECT 42");
	REQUIRE(CHECK_COLUMN(next, 0, {42}));
}

TEST_CASE("Poll observes an interrupt on a materializing handle", "[api][query_result]") {
	DuckDB db(nullptr);
	Connection con(db);

	auto handle = Submit(con, "SELECT i FROM range(100000000000) t(i) WHERE i % 10 = 0");
	handle->Materialize();
	con.Interrupt();

	Deadline deadline;
	QueryResultState state;
	while (!IsTerminal(state = handle->Poll())) {
		REQUIRE(!deadline.Passed());
		std::this_thread::sleep_for(std::chrono::microseconds(100));
	}
	REQUIRE(state == QueryResultState::EXECUTION_ERROR);
	REQUIRE(StringUtil::Contains(handle->GetError(), "INTERRUPT"));

	con.context->ClearInterrupt();
	auto next = con.Query("SELECT 42");
	REQUIRE(CHECK_COLUMN(next, 0, {42}));
}

TEST_CASE("A side-effecting statement can be streamed and applies its effect on drain", "[api][query_result]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t(i INTEGER)"));

	for (auto query : {"INSERT INTO t VALUES (1), (2) RETURNING i", "CREATE TABLE ctas AS SELECT 42 AS i"}) {
		auto handle = Submit(con, query);
		// Nothing settles the retention at submission: the consumer's first call does
		REQUIRE(handle->GetBufferedData().Lifetime() == ResultLifetime::UNDECIDED);
		QueryResultStream stream(std::move(handle));
		DrainWatchdog watchdog(con);
		REQUIRE(stream.GetBufferedData().Lifetime() == ResultLifetime::DRAINING);
		while (auto chunk = stream.Fetch()) {
		}
		REQUIRE(!stream.HasError());
	}
	auto rows = con.Query("SELECT i FROM t");
	REQUIRE(CHECK_COLUMN(rows, 0, {1, 2}));
	auto created = con.Query("SELECT i FROM ctas");
	REQUIRE(CHECK_COLUMN(created, 0, {42}));
}

TEST_CASE("Multi-statement text chains completed results", "[api][query_result]") {
	DuckDB db(nullptr);
	Connection con(db);

	auto result = con.Query("CREATE TABLE t AS SELECT 42 AS i; SELECT i FROM t; SELECT 84 AS i;");
	REQUIRE_NO_FAIL(*result);
	// Every statement of the chain ran to completion and kept its result
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
	REQUIRE(result->next);
	auto &last = *result->next;
	REQUIRE(CHECK_COLUMN(last, 0, {84}));
	REQUIRE(!last.next);
	REQUIRE(last.RowCount() == 1);

	// A submission takes a single statement, and so does a parameterized eager query
	auto handle = con.Submit("SELECT 1; SELECT 2;");
	REQUIRE(handle->HasError());
	REQUIRE_FAIL(con.Query("SELECT $1; SELECT $1;", 1));
	auto single = con.Query("SELECT $1::INT", 7);
	REQUIRE(CHECK_COLUMN(single, 0, {7}));
}

TEST_CASE("A prepared statement gets a fresh store on every submission", "[api][query_result]") {
	DuckDB db(nullptr);
	Connection con(db);

	auto prepared = con.Prepare("SELECT i FROM range($1) t(i)");
	REQUIRE(!prepared->HasError());

	DrainWatchdog watchdog(con);
	auto first = prepared->Submit(1000);
	auto &first_buffer = first->GetBufferedData();
	REQUIRE(first->Collection().Count() == 1000);

	auto second = prepared->Submit(2000);
	REQUIRE(&second->GetBufferedData() != &first_buffer);
	REQUIRE(second->Collection().Count() == 2000);
	// The first result is still readable: it holds its own collection
	REQUIRE(first->Collection().Count() == 1000);
}

TEST_CASE("A custom collector hands out its own result object", "[api][query_result]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &config = ClientConfig::GetConfig(*con.context);
	DrainWatchdog watchdog(con);

	SECTION("arrow collector, from Query and from Submit") {
		auto setting = UseArrowCollector(config);
		auto queried = con.Query("SELECT i FROM range(3000) t(i)");
		REQUIRE(queried->GetResultType() == QueryResultType::ARROW_RESULT);
		REQUIRE(!queried->HasError());
		REQUIRE(!queried->Cast<ArrowQueryResult>().Arrays().empty());

		auto submitted = con.Submit("SELECT i FROM range(3000) t(i)");
		REQUIRE(submitted->GetResultType() == QueryResultType::ARROW_RESULT);
		REQUIRE(!submitted->HasError());
	}
	SECTION("a streaming collector keeps the query open until its result is dropped") {
		auto setting = UseTestStreamingCollector(config);
		auto result = con.Submit("SELECT i FROM range(3000) t(i)");
		REQUIRE(!result->HasError());
		REQUIRE(result->RowCount() == 3000);
		// The collector's own result holds the query, so the connection takes nothing else until it ends
		auto refused = con.Query("SELECT 42");
		REQUIRE(refused->HasError());
		REQUIRE(refused->GetErrorType() == ExceptionType::RESOURCE_IN_USE);
	}
	// The connection is usable once the collector is gone
	auto next = con.Query("SELECT 42");
	REQUIRE(CHECK_COLUMN(next, 0, {42}));
}

TEST_CASE("A handle destroyed without collecting releases the query", "[api][query_result]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET max_streaming_buffer_size='16KB'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE fanout AS SELECT range i FROM range(200000)"));

	SECTION("a plain submission") {
		auto handle = Submit(con, "SELECT i FROM range(1000000) t(i)");
		REQUIRE(con.context->transaction.HasActiveTransaction());
		handle.reset();
		REQUIRE(!con.context->transaction.HasActiveTransaction());
	}
	SECTION("a fan-out plan with parked producers") {
		// One scan feeds two consumers: dropping the handle must unwind the parked producers
		auto handle = Submit(con, "WITH c AS MATERIALIZED (SELECT i FROM fanout) "
		                          "SELECT t1.i FROM c t1 JOIN c t2 USING (i)");
		Deadline deadline;
		while (!handle->GetBufferedData().WaitsOnConsumer()) {
			REQUIRE(!deadline.Passed());
			std::this_thread::sleep_for(std::chrono::microseconds(100));
		}
		handle.reset();
	}
	auto next = con.Query("SELECT 42");
	REQUIRE(CHECK_COLUMN(next, 0, {42}));
}

TEST_CASE("Closing an unfinished query discards its writes", "[api][query_result]") {
	DuckDB db(nullptr);
	Connection con(db);
	Connection observer(db);
	// Without worker threads the insert only advances when this thread steps it
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t(i BIGINT)"));

	auto handle = Submit(con, "INSERT INTO t SELECT i FROM range(1000000) t(i)");
	StepUnfinished(*handle);
	handle->Close();

	auto count = observer.Query("SELECT count(*) FROM t");
	REQUIRE(CHECK_COLUMN(count, 0, {0}));
}

TEST_CASE("Closing a query that a worker failed discards its writes", "[api][query_result]") {
	DuckDB db(nullptr);
	Connection con(db);
	Connection observer(db);
	REQUIRE_NO_FAIL(con.Query("SET threads=2"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t(i BIGINT)"));

	auto handle = Submit(con, "INSERT INTO t SELECT i FROM range(20000000) t(i)");
	// Let the worker append rows before the interrupt reaches it
	std::this_thread::sleep_for(std::chrono::milliseconds(10));
	con.Interrupt();

	auto &executor = Executor::Get(*con.context);
	Deadline deadline;
	while (!executor.HasError()) {
		REQUIRE(!deadline.Passed());
		std::this_thread::sleep_for(std::chrono::microseconds(100));
	}
	// Nothing observed the failure, so it is on the executor alone
	REQUIRE(!handle->HasError());
	handle->Close();

	auto count = observer.Query("SELECT count(*) FROM t");
	REQUIRE(CHECK_COLUMN(count, 0, {0}));
}

TEST_CASE("Closing an unfinished query invalidates the open transaction", "[api][query_result]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t(i BIGINT)"));
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));

	auto handle = Submit(con, "INSERT INTO t SELECT i FROM range(1000000) t(i)");
	StepUnfinished(*handle);
	handle->Close();

	auto next = con.Query("SELECT 42");
	REQUIRE(next->HasError());
	REQUIRE(StringUtil::Contains(next->GetError(), "aborted"));
	REQUIRE_NO_FAIL(con.Query("ROLLBACK"));
	auto after = con.Query("SELECT 42");
	REQUIRE(CHECK_COLUMN(after, 0, {42}));
}

TEST_CASE("Closing a finished query without reading it commits", "[api][query_result]") {
	DuckDB db(nullptr);
	Connection con(db);
	Connection observer(db);
	REQUIRE_NO_FAIL(con.Query("SET threads=2"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t(i BIGINT)"));

	auto handle = Submit(con, "INSERT INTO t SELECT i FROM range(1000) t(i)");
	handle->Materialize();
	Deadline deadline;
	while (handle->Poll() != QueryResultState::FINISHED) {
		REQUIRE(!deadline.Passed());
		std::this_thread::sleep_for(std::chrono::microseconds(100));
	}
	handle->Close();

	auto count = observer.Query("SELECT count(*) FROM t");
	REQUIRE(CHECK_COLUMN(count, 0, {1000}));
}

TEST_CASE("A DDL submission finishes without a consumer call", "[api][query_result]") {
	DuckDB db(nullptr);
	Connection con(db);
	Connection observer(db);
	REQUIRE_NO_FAIL(con.Query("SET threads=2"));

	auto handle = Submit(con, "CREATE TABLE t(i BIGINT)");
	Deadline deadline;
	while (handle->Poll() != QueryResultState::FINISHED) {
		REQUIRE(!deadline.Passed());
		std::this_thread::sleep_for(std::chrono::microseconds(100));
	}
	handle->Close();

	auto count = observer.Query("SELECT count(*) FROM t");
	REQUIRE(CHECK_COLUMN(count, 0, {0}));
}

TEST_CASE("A submitted insert parks until the consumer chooses", "[api][query_result]") {
	DuckDB db(nullptr);
	Connection con(db);
	Connection observer(db);
	REQUIRE_NO_FAIL(con.Query("SET threads=2"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t(i BIGINT)"));

	auto handle = Submit(con, "INSERT INTO t VALUES (1)");
	Deadline deadline;
	auto state = handle->Poll();
	while (state != QueryResultState::READY && !IsTerminal(state)) {
		REQUIRE(!deadline.Passed());
		std::this_thread::sleep_for(std::chrono::microseconds(100));
		state = handle->Poll();
	}
	// The row count parks for the consumer's choice, so the workers cannot finish the query
	REQUIRE(state == QueryResultState::READY);
	REQUIRE(handle->GetBufferedData().Lifetime() == ResultLifetime::UNDECIDED);

	handle->Materialize();
	while (handle->Poll() != QueryResultState::FINISHED) {
		REQUIRE(!deadline.Passed());
		std::this_thread::sleep_for(std::chrono::microseconds(100));
	}
	handle->Close();

	auto rows = observer.Query("SELECT i FROM t");
	REQUIRE(CHECK_COLUMN(rows, 0, {1}));
}

TEST_CASE("A dynamic PIVOT is one submitted query", "[api][query_result]") {
	DuckDB db(nullptr);
	Connection con(db);
	CreateSales(con);

	auto handle = Submit(con, PIVOT_QUERY);
	// The schema is the set of distinct pivot values, so it is unknown until the fragment that
	// produces the rows has been prepared
	REQUIRE(!handle->MetadataAvailable());
	REQUIRE(!handle->TryGetTypes());
	REQUIRE(!handle->TryGetNames());
	REQUIRE(!handle->TryGetStatementType());
	REQUIRE(!handle->TryGetStatementProperties());
	REQUIRE(!handle->TryColumnCount().IsValid());
	REQUIRE_THROWS_AS(handle->GetTypes(), InvalidInputException);
	REQUIRE_THROWS_AS(handle->ColumnCount(), InvalidInputException);

	DrainWatchdog watchdog(con);
	handle->Complete();
	REQUIRE(!handle->HasError());
	REQUIRE(handle->MetadataAvailable());
	REQUIRE(handle->TryGetTypes());
	REQUIRE(handle->TryColumnCount().GetIndex() == 3);
	REQUIRE(handle->GetStatementType() == StatementType::SELECT_STATEMENT);
	REQUIRE(handle->RowCount() == 2);

	auto expected = con.Query(PIVOT_QUERY);
	REQUIRE_NO_FAIL(*expected);
	REQUIRE(handle->GetNames() == expected->GetNames());
	REQUIRE(handle->GetTypes() == expected->GetTypes());
	// The cursor walks the collection the handle holds
	REQUIRE(handle->Equals(*expected));
}

TEST_CASE("A poll-only consumer reaches the principal fragment of a group", "[api][query_result]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET threads=2"));
	CreateSales(con);

	auto handle = Submit(con, PIVOT_QUERY);
	Deadline deadline;
	while (handle->Poll() != QueryResultState::READY) {
		REQUIRE(!deadline.Passed());
		std::this_thread::sleep_for(std::chrono::microseconds(100));
	}
	// READY is the row-producing fragment parked for the retention decision, so its schema is known
	REQUIRE(handle->MetadataAvailable());
	REQUIRE(handle->ColumnCount() == 3);

	handle->Materialize();
	while (handle->Poll() != QueryResultState::FINISHED) {
		REQUIRE(!deadline.Passed());
		std::this_thread::sleep_for(std::chrono::microseconds(100));
	}
	REQUIRE(handle->Collection().Count() == 2);
}

TEST_CASE("A single-threaded consumer drives a group itself", "[api][query_result]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	CreateSales(con);

	auto handle = Submit(con, PIVOT_QUERY);
	DrainWatchdog watchdog(con);
	Deadline deadline;
	QueryResultState state;
	while ((state = handle->ExecuteTask()) != QueryResultState::READY) {
		REQUIRE(!IsTerminal(state));
		if (state == QueryResultState::BLOCKED) {
			handle->WaitForTask();
		}
		REQUIRE(!deadline.Passed());
	}
	handle->Complete();
	REQUIRE(!handle->HasError());
	REQUIRE(handle->RowCount() == 2);
}

TEST_CASE("A group that produces no rows knows its metadata at submission", "[api][query_result]") {
	DuckDB db(nullptr);
	Connection con(db);
	Connection observer(db);
	REQUIRE_NO_FAIL(con.Query("SET threads=2"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(1000)"));

	auto handle = Submit(con, "ALTER TABLE t ADD COLUMN c INTEGER DEFAULT random()");
	REQUIRE(handle->MetadataAvailable());
	REQUIRE(handle->ColumnCount() == 0);
	REQUIRE(handle->GetStatementProperties().return_type == StatementReturnType::NOTHING);

	Deadline deadline;
	while (handle->Poll() != QueryResultState::FINISHED) {
		REQUIRE(!deadline.Passed());
		std::this_thread::sleep_for(std::chrono::microseconds(100));
	}
	handle->Close();

	auto count = observer.Query("SELECT count(*) FROM t WHERE c IS NOT NULL");
	REQUIRE(CHECK_COLUMN(count, 0, {1000}));
	// The injected wrap left no transaction open
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con.Query("COMMIT"));
}

TEST_CASE("An error inside a wrapped group rolls the group back", "[api][query_result]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(1000)"));

	auto handle = Submit(con, "ALTER TABLE t ADD COLUMN c INTEGER DEFAULT ((random()::VARCHAR || 'x')::INTEGER)");
	DrainWatchdog watchdog(con);
	handle->Complete();
	REQUIRE(handle->HasError());
	REQUIRE(handle->GetErrorType() == ExceptionType::CONVERSION);
	handle.reset();

	auto missing = con.Query("SELECT c FROM t");
	REQUIRE(missing->HasError());
	auto count = con.Query("SELECT count(*) FROM t");
	REQUIRE(CHECK_COLUMN(count, 0, {1000}));
}

TEST_CASE("Abandoning a wrapped group rolls it back", "[api][query_result]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(1000000)"));

	auto handle = Submit(con, "ALTER TABLE t ADD COLUMN c INTEGER DEFAULT random()");
	for (idx_t step = 0; step < 2; step++) {
		REQUIRE(!IsTerminal(handle->ExecuteTask()));
	}
	handle->Close();

	auto missing = con.Query("SELECT c FROM t");
	REQUIRE(missing->HasError());
	auto count = con.Query("SELECT count(*) FROM t");
	REQUIRE(CHECK_COLUMN(count, 0, {1000000}));
}

TEST_CASE("An interrupt during a group cancels it and rolls it back", "[api][query_result]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(2000000)"));

	auto handle = Submit(con, "ALTER TABLE t ADD COLUMN c INTEGER DEFAULT random()");
	handle->Materialize();
	con.Interrupt();

	Deadline deadline;
	QueryResultState state;
	while (!IsTerminal(state = handle->Poll())) {
		REQUIRE(!deadline.Passed());
		std::this_thread::sleep_for(std::chrono::microseconds(100));
	}
	REQUIRE(state == QueryResultState::EXECUTION_ERROR);
	REQUIRE(StringUtil::Contains(handle->GetError(), "INTERRUPT"));
	handle.reset();

	con.context->ClearInterrupt();
	auto missing = con.Query("SELECT c FROM t");
	REQUIRE(missing->HasError());
	auto next = con.Query("SELECT 42");
	REQUIRE(CHECK_COLUMN(next, 0, {42}));
}

TEST_CASE("Parameters on a statement that expands into several are refused", "[api][query_result]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(10)"));

	vector<Value> values {Value::INTEGER(100)};
	auto handle = con.Submit("ALTER TABLE t ADD COLUMN c INTEGER DEFAULT (random() * $1)::INTEGER", values);
	REQUIRE(handle->HasError());
	REQUIRE(StringUtil::Contains(handle->GetError(), "parameters are not supported"));

	auto next = con.Query("SELECT 42");
	REQUIRE(CHECK_COLUMN(next, 0, {42}));
}

TEST_CASE("A submission still takes exactly one user statement", "[api][query_result]") {
	DuckDB db(nullptr);
	Connection con(db);

	auto refused = con.Submit("SELECT 1; SELECT 2;");
	REQUIRE(refused->HasError());
	REQUIRE(StringUtil::Contains(refused->GetError(), "Cannot prepare multiple statements at once!"));

	// A statement that does not expand is submitted exactly as before
	auto handle = Submit(con, "SELECT i FROM range(1000) t(i)");
	REQUIRE(handle->MetadataAvailable());
	REQUIRE(handle->ColumnCount() == 1);
	REQUIRE(handle->GetStatementType() == StatementType::SELECT_STATEMENT);
	REQUIRE(handle->GetStatementProperties().return_type == StatementReturnType::QUERY_RESULT);
	REQUIRE(handle->GetNames()[0] == "i");
	// Nothing settles the retention at submission: the consumer's first call does
	REQUIRE(handle->GetBufferedData().Lifetime() == ResultLifetime::UNDECIDED);
	QueryResultStream stream(std::move(handle));
	DrainWatchdog watchdog(con);
	idx_t rows = 0;
	while (auto chunk = stream.Fetch()) {
		rows += chunk->size();
	}
	REQUIRE(!stream.HasError());
	REQUIRE(rows == 1000);
}

TEST_CASE("Query on a statement that expands is unchanged", "[api][query_result]") {
	DuckDB db(nullptr);
	Connection con(db);
	CreateSales(con);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(1000)"));

	auto pivot = con.Query(PIVOT_QUERY);
	REQUIRE_NO_FAIL(*pivot);
	REQUIRE(pivot->ColumnCount() == 3);
	REQUIRE(pivot->RowCount() == 2);

	auto altered = con.Query("ALTER TABLE t ADD COLUMN c INTEGER DEFAULT random()");
	REQUIRE_NO_FAIL(*altered);
	REQUIRE(altered->GetStatementProperties().return_type == StatementReturnType::NOTHING);
	auto count = con.Query("SELECT count(*) FROM t WHERE c IS NOT NULL");
	REQUIRE(CHECK_COLUMN(count, 0, {1000}));
}

TEST_CASE("IMPORT DATABASE is one submitted query", "[api][query_result]") {
	auto export_dir = TestCreatePath("submitted_import_export");
	TestDeleteDirectory(export_dir);
	{
		DuckDB source(nullptr);
		Connection writer(source);
		REQUIRE_NO_FAIL(writer.Query("CREATE TABLE t AS SELECT range i FROM range(100)"));
		REQUIRE_NO_FAIL(writer.Query("EXPORT DATABASE '" + export_dir + "'"));
	}

	DuckDB db(nullptr);
	Connection con(db);
	auto handle = Submit(con, "IMPORT DATABASE '" + export_dir + "'");
	Deadline deadline;
	while (handle->Poll() != QueryResultState::FINISHED) {
		REQUIRE(!deadline.Passed());
		std::this_thread::sleep_for(std::chrono::microseconds(100));
	}
	handle->Close();

	auto count = con.Query("SELECT count(*) FROM t");
	REQUIRE(CHECK_COLUMN(count, 0, {100}));
	TestDeleteDirectory(export_dir);
}

TEST_CASE("A statement that expands runs inside a user transaction", "[api][query_result]") {
	DuckDB db(nullptr);
	Connection con(db);
	Connection observer(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(1000)"));
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));

	// Inside a transaction the preprocessor brackets the statements with SET instead of wrapping them
	auto handle = Submit(con, "ALTER TABLE t ADD COLUMN c INTEGER DEFAULT random()");
	REQUIRE(handle->MetadataAvailable());
	REQUIRE(handle->ColumnCount() == 0);
	DrainWatchdog watchdog(con);
	handle->Complete();
	REQUIRE(!handle->HasError());
	handle.reset();
	REQUIRE_NO_FAIL(con.Query("COMMIT"));

	auto count = observer.Query("SELECT count(*) FROM t WHERE c IS NOT NULL");
	REQUIRE(CHECK_COLUMN(count, 0, {1000}));
}

TEST_CASE("A statement that expands leaves the user's transaction to the user", "[api][query_result]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(1000)"));
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));

	auto handle = Submit(con, "ALTER TABLE t ADD COLUMN c INTEGER DEFAULT ((random()::VARCHAR || 'x')::INTEGER)");
	DrainWatchdog watchdog(con);
	handle->Complete();
	REQUIRE(handle->HasError());
	handle.reset();

	// The failure invalidated the transaction the user opened, and left ending it to them
	auto next = con.Query("SELECT 42");
	REQUIRE(next->HasError());
	REQUIRE(StringUtil::Contains(next->GetError(), "aborted"));
	REQUIRE_NO_FAIL(con.Query("ROLLBACK"));

	auto missing = con.Query("SELECT c FROM t");
	REQUIRE(missing->HasError());
	auto count = con.Query("SELECT count(*) FROM t");
	REQUIRE(CHECK_COLUMN(count, 0, {1000}));
}

TEST_CASE("A statement that fails to bind after the first one rolls the query back", "[api][query_result]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(1000)"));

	// The first ALTER binds, the UPDATE that materializes the default does not
	auto handle = Submit(con, "ALTER TABLE t ADD COLUMN c INTEGER DEFAULT nonexistent_function()");
	DrainWatchdog watchdog(con);
	handle->Complete();
	REQUIRE(handle->HasError());
	handle.reset();

	auto missing = con.Query("SELECT c FROM t");
	REQUIRE(missing->HasError());
	auto count = con.Query("SELECT count(*) FROM t");
	REQUIRE(CHECK_COLUMN(count, 0, {1000}));
}

TEST_CASE("A single-threaded consumer sees a later statement's bind failure", "[api][query_result]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET threads=1"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(1000)"));

	auto handle = Submit(con, "ALTER TABLE t ADD COLUMN c INTEGER DEFAULT nonexistent_function()");
	DrainWatchdog watchdog(con);
	Deadline deadline;
	QueryResultState state;
	while (!IsTerminal(state = handle->ExecuteTask())) {
		if (state == QueryResultState::BLOCKED || state == QueryResultState::NO_TASKS_AVAILABLE) {
			handle->WaitForTask();
		}
		REQUIRE(!deadline.Passed());
	}
	REQUIRE(state == QueryResultState::EXECUTION_ERROR);
	REQUIRE(handle->HasError());
	handle.reset();

	auto missing = con.Query("SELECT c FROM t");
	REQUIRE(missing->HasError());
	auto count = con.Query("SELECT count(*) FROM t");
	REQUIRE(CHECK_COLUMN(count, 0, {1000}));
}

TEST_CASE("An interrupt on a statement boundary cancels the query", "[api][query_result]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET threads=2"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(2000000)"));

	auto handle = Submit(con, "ALTER TABLE t ADD COLUMN c INTEGER DEFAULT random()");
	// The first statement finishes while nothing steps the query on, so the interrupt lands between
	auto &executor = Executor::Get(*con.context);
	Deadline deadline;
	while (!executor.ExecutionIsFinished()) {
		REQUIRE(!deadline.Passed());
		std::this_thread::sleep_for(std::chrono::microseconds(100));
	}
	con.Interrupt();

	QueryResultState state;
	while (!IsTerminal(state = handle->Poll())) {
		REQUIRE(!deadline.Passed());
		std::this_thread::sleep_for(std::chrono::microseconds(100));
	}
	REQUIRE(state == QueryResultState::EXECUTION_ERROR);
	REQUIRE(StringUtil::Contains(handle->GetError(), "INTERRUPT"));
	handle.reset();

	con.context->ClearInterrupt();
	auto missing = con.Query("SELECT c FROM t");
	REQUIRE(missing->HasError());
	auto next = con.Query("SELECT 42");
	REQUIRE(CHECK_COLUMN(next, 0, {42}));
}

TEST_CASE("A query that returns no rows hands out a collection of its own shape", "[api][query_result]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(100)"));

	auto handle = Submit(con, "ALTER TABLE t ADD COLUMN c INTEGER DEFAULT random()");
	DrainWatchdog watchdog(con);
	handle->Complete();
	REQUIRE(!handle->HasError());
	REQUIRE(handle->ColumnCount() == 0);
	REQUIRE(handle->Collection().ColumnCount() == 0);
	REQUIRE(handle->RowCount() == 0);
	REQUIRE(!handle->Fetch());
}

TEST_CASE("Profiling a statement that expands writes one profile per statement", "[api][query_result]") {
	auto profile = TestCreatePath("submitted_expansion_profile.json");
	LocalFileSystem fs;
	if (fs.FileExists(profile)) {
		fs.RemoveFile(profile);
	}
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET enable_profiling='json'"));
	REQUIRE_NO_FAIL(con.Query("SET profiling_output='" + profile + "'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT range i FROM range(1000)"));
	CreateSales(con);

	SECTION("a statement that returns no rows") {
		auto handle = Submit(con, "ALTER TABLE t ADD COLUMN c INTEGER DEFAULT random()");
		DrainWatchdog watchdog(con);
		handle->Complete();
		REQUIRE(!handle->HasError());
	}
	SECTION("a statement that returns rows from its last statement") {
		auto handle = Submit(con, PIVOT_QUERY);
		DrainWatchdog watchdog(con);
		handle->Complete();
		REQUIRE(!handle->HasError());
		REQUIRE(handle->RowCount() == 2);
	}
	REQUIRE(fs.FileExists(profile));
	fs.RemoveFile(profile);
}

TEST_CASE("An open result holds the connection until it ends", "[api][query_result]") {
	DuckDB db(nullptr);
	Connection con(db);
	Connection observer(db);
	REQUIRE_NO_FAIL(con.Query("SET threads=2"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t(i BIGINT)"));

	auto handle = Submit(con, "INSERT INTO t SELECT i FROM range(1000) t(i)");
	handle->Materialize();
	Deadline deadline;
	while (handle->Poll() != QueryResultState::FINISHED) {
		REQUIRE(!deadline.Passed());
		std::this_thread::sleep_for(std::chrono::microseconds(100));
	}

	// A result that finished still holds the connection until the consumer ends it
	auto refused = con.Query("SELECT 42");
	REQUIRE(refused->HasError());
	REQUIRE(refused->GetErrorType() == ExceptionType::RESOURCE_IN_USE);
	REQUIRE(StringUtil::Contains(refused->GetError(), "connection has an open result"));
	// and it reads exactly as it would have
	REQUIRE(handle->Collection().Count() == 1);
	handle->Close();

	auto next = con.Query("SELECT 42");
	REQUIRE(CHECK_COLUMN(next, 0, {42}));
	auto count = observer.Query("SELECT count(*) FROM t");
	REQUIRE(CHECK_COLUMN(count, 0, {1000}));
}

TEST_CASE("Preparing a statement is refused while a result is open", "[api][query_result]") {
	DuckDB db(nullptr);
	Connection con(db);

	auto handle = Submit(con, "SELECT i FROM range(1000) t(i)");
	auto prepared = con.Prepare("SELECT 42");
	REQUIRE(prepared->HasError());
	REQUIRE(StringUtil::Contains(prepared->GetError(), "connection has an open result"));

	handle->Close();
	prepared = con.Prepare("SELECT 42");
	REQUIRE(!prepared->HasError());
}

TEST_CASE("A result that failed does not hold the connection", "[api][query_result]") {
	DuckDB db(nullptr);
	Connection con(db);

	SECTION("a failure found while binding") {
		auto handle = con.Submit("SELECT * FROM no_such_table");
		REQUIRE(handle->HasError());
	}
	SECTION("a failure found while running") {
		auto handle =
		    Submit(con, "SELECT (CASE WHEN i = 4000 THEN 'boom' ELSE i::VARCHAR END)::INT FROM range(5000) t(i)");
		DrainWatchdog watchdog(con);
		handle->Complete();
		REQUIRE(handle->HasError());
	}
	auto next = con.Query("SELECT 42");
	REQUIRE(CHECK_COLUMN(next, 0, {42}));
}

#endif
