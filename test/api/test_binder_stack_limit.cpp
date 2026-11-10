#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/thread.hpp"

#include <functional>

#ifdef _WIN32
#include "duckdb/common/windows.hpp"
#else
#include <pthread.h>
#include <sys/mman.h>
#include <unistd.h>
#endif
#if defined(__linux__)
#include <sys/resource.h>
#include <sys/syscall.h>
#endif

using namespace duckdb;

namespace {

// A macOS secondary thread gets this much stack by default; the nests below need several MiB on a release build.
constexpr idx_t SMALL_STACK_SIZE = 512 * 1024;
// A scalar macro costs about 1 KiB of stack per level on a release build, so its nest gets a smaller thread.
constexpr idx_t MACRO_STACK_SIZE = 192 * 1024;
constexpr idx_t DEEP_NEST = 400;
// A nest written out in one statement also makes the parsed statement deep, and destroying it recurses too.
constexpr idx_t DEEP_STATEMENT_NEST = 200;
// Five expression levels per macro keep the chain under max_expression_depth.
constexpr idx_t DEEP_MACRO_NEST = 190;
constexpr idx_t SHALLOW_NEST = 3;
// Enough for a macro cycle to reach a lowered max_expression_depth on any build.
constexpr idx_t LARGE_STACK_SIZE = 16 * 1024 * 1024;
// Planning makes three operators per EXCEPT, and walks them with more stack than binding the chain takes.
constexpr idx_t DEEP_EXCEPT_CHAIN = 150;
// Five plan operators per level: deep enough for planning to run out before binding on x86, arm64 and under ASan.
constexpr idx_t DEEP_DENSE_NEST = 150;
// About two nested join order optimizers per EXCEPT ALL, so the chain reaches the cutoff of 128.
constexpr idx_t CORRELATED_EXCEPT_ALL_CHAIN = 100;
// A level that joins makes the parsed statement deeper still, and its destruction overflows 512 KiB under a sanitizer
// at 200 levels.
constexpr idx_t DEEP_JOIN_NEST = 100;
// Below this the binder's error for a nest of 200 can still overflow while the parsed statement is destroyed, under
// a sanitizer.
constexpr idx_t SWEEP_MIN_STACK_SIZE = 512 * 1024;
constexpr idx_t SWEEP_MAX_STACK_SIZE = 2048 * 1024;
constexpr idx_t SWEEP_STEP = 128 * 1024;
constexpr idx_t NARROW_SWEEP_MIN_STACK_SIZE = 384 * 1024;
constexpr idx_t NARROW_SWEEP_MAX_STACK_SIZE = 1024 * 1024;
constexpr idx_t NARROW_SWEEP_STEP = 32 * 1024;
constexpr idx_t FINE_SWEEP_STEP = 8 * 1024;

struct SmallStackQuery {
	Connection &con;
	string sql;
	idx_t stack_size;
	bool stack_reported = false;
	bool has_error = false;
	string error;
	string value;
};

void RunQuery(SmallStackQuery &query) {
	idx_t stack_size;
	idx_t remaining;
	query.stack_reported = ThreadUtil::GetStackSpace(stack_size, remaining);
	try {
		auto result = query.con.Query(query.sql);
		if (result->HasError()) {
			query.has_error = true;
			query.error = result->GetError();
		} else {
			query.value = result->Collection().GetValue(0, 0).ToString();
		}
	} catch (std::exception &ex) {
		query.has_error = true;
		query.error = ex.what();
	}
}

#ifdef _WIN32
DWORD WINAPI RunQueryThread(LPVOID argument) {
	RunQuery(*static_cast<SmallStackQuery *>(argument));
	return 0;
}

bool RunOnSmallStack(SmallStackQuery &query) {
	auto thread =
	    CreateThread(nullptr, query.stack_size, RunQueryThread, &query, STACK_SIZE_PARAM_IS_A_RESERVATION, nullptr);
	if (!thread) {
		return false;
	}
	WaitForSingleObject(thread, INFINITE);
	CloseHandle(thread);
	return true;
}

// Windows rounds a stack reservation up to its allocation granularity, so sizes in between give the same stack.
idx_t PageMultiple(idx_t size) {
	SYSTEM_INFO info;
	GetSystemInfo(&info);
	auto granularity = static_cast<idx_t>(info.dwAllocationGranularity);
	return (size + granularity - 1) / granularity * granularity;
}
#else
void *RunQueryThread(void *argument) {
	RunQuery(*static_cast<SmallStackQuery *>(argument));
	return nullptr;
}

// The thread runs on a stack mapped here, since glibc hands a new thread any cached stack at least as large as the
// one requested.
bool RunOnSmallStack(SmallStackQuery &query) {
	auto page_size = static_cast<idx_t>(sysconf(_SC_PAGESIZE));
	auto mapping_size = query.stack_size + page_size;
	auto mapping = mmap(nullptr, mapping_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0);
	if (mapping == MAP_FAILED) {
		return false;
	}
	// a guard page below the stack turns an overflow into a crash instead of a write into other memory
	auto stack = static_cast<char *>(mapping) + page_size;
	pthread_attr_t attributes;
	auto created = mprotect(mapping, page_size, PROT_NONE) == 0 && pthread_attr_init(&attributes) == 0;
	if (created) {
		pthread_t thread;
		created = pthread_attr_setstack(&attributes, stack, query.stack_size) == 0 &&
		          pthread_create(&thread, &attributes, RunQueryThread, &query) == 0;
		pthread_attr_destroy(&attributes);
		created = created && pthread_join(thread, nullptr) == 0;
	}
	munmap(mapping, mapping_size);
	return created;
}

// pthread_attr_setstack on Apple platforms takes only whole pages, which are 16 KiB on Apple silicon.
idx_t PageMultiple(idx_t size) {
	auto queried = sysconf(_SC_PAGESIZE);
	auto page_size = queried > 0 ? static_cast<idx_t>(queried) : idx_t(4096);
	return (size + page_size - 1) / page_size * page_size;
}
#endif

SmallStackQuery QueryOnSmallStack(Connection &con, const string &sql, idx_t stack_size) {
	SmallStackQuery query {con, sql, PageMultiple(stack_size)};
	REQUIRE(RunOnSmallStack(query));
	return query;
}

bool IsStackError(const string &error, const string &step) {
	return StringUtil::Contains(error, "Binder Error: Max expression depth exceeded: " + step +
	                                       " the query would overflow the");
}

void RequireStackError(Connection &con, const string &sql, idx_t stack_size = SMALL_STACK_SIZE) {
	auto query = QueryOnSmallStack(con, sql, stack_size);
	INFO(query.value);
	INFO(query.error);
	REQUIRE(query.has_error);
	REQUIRE(IsStackError(query.error, "binding"));
}

// Which step runs out of stack first depends on the compiler and the build: binding a FROM subquery takes about 3.8 KiB
// on arm64 with GCC 11 and 1 KiB on x86 with GCC 13, planning it about the same on both. So the nest is planned on a
// range of stacks, from small to large, each of which must give the value or one of the two errors, up to the first
// that gives the value. Returns whether planning ran out.
bool RequireValueOrStackError(Connection &con, const string &sql, const string &expected,
                              idx_t min_stack_size = SWEEP_MIN_STACK_SIZE, idx_t max_stack_size = SWEEP_MAX_STACK_SIZE,
                              idx_t step = SWEEP_STEP) {
	bool planning_error = false;
	idx_t previous_size = 0;
	for (idx_t stack_size = min_stack_size; stack_size <= max_stack_size; stack_size += step) {
		// with pages, or an allocation granularity, larger than the step several steps round to the same stack
		auto rounded_size = PageMultiple(stack_size);
		if (rounded_size == previous_size) {
			continue;
		}
		previous_size = rounded_size;
		auto query = QueryOnSmallStack(con, sql, rounded_size);
		INFO(query.stack_size);
		INFO(query.error);
		if (query.has_error) {
			auto planning = IsStackError(query.error, "planning");
			REQUIRE((planning || IsStackError(query.error, "binding")));
			planning_error = planning_error || planning;
		} else {
			REQUIRE(query.value == expected);
			break;
		}
	}
	return planning_error;
}

void RequireDepthError(Connection &con, const string &sql, const string &expected) {
	auto query = QueryOnSmallStack(con, sql, LARGE_STACK_SIZE);
	INFO(query.error);
	REQUIRE(query.has_error);
	REQUIRE(StringUtil::Contains(query.error, expected));
}

void RequireValue(Connection &con, const string &sql, const string &expected, idx_t stack_size = SMALL_STACK_SIZE) {
	auto query = QueryOnSmallStack(con, sql, stack_size);
	INFO(query.error);
	REQUIRE(!query.has_error);
	REQUIRE(query.value == expected);
}

// The platforms on which the engine asks for the bounds of a thread's stack (QueryStackBounds in thread_util.cpp),
// where not getting them is a defect: Windows, and Linux, Apple platforms and FreeBSD in builds with threads.
// Elsewhere, such as WebAssembly, only max_expression_depth applies.
#if defined(_WIN32) ||                                                                                                 \
    (!defined(DUCKDB_NO_THREADS) && (defined(__linux__) || defined(__APPLE__) || defined(__FreeBSD__)))
constexpr bool STACK_BOUNDS_EXPECTED = true;
#else
constexpr bool STACK_BOUNDS_EXPECTED = false;
#endif

bool StackIsReported(Connection &con) {
	auto stack_reported = QueryOnSmallStack(con, "SELECT 42", SMALL_STACK_SIZE).stack_reported;
	if (STACK_BOUNDS_EXPECTED) {
		REQUIRE(stack_reported);
	} else if (!stack_reported) {
		// without the stack bounds only max_expression_depth applies, and the nests of these tests would overflow
		WARN("The platform does not report the stack of a thread");
	}
	return stack_reported;
}

// Each object first reads a constant and is then replaced from the top down, so no CREATE binds the whole nest.
void CreateChain(Connection &con, const string &object, const string &constant_body, const string &chained_body,
                 idx_t count) {
	for (idx_t i = 0; i <= count; i++) {
		REQUIRE_NO_FAIL(con.Query(StringUtil::Format("CREATE " + object + constant_body, i)));
	}
	for (idx_t i = count; i > 0; i -= 1) {
		REQUIRE_NO_FAIL(con.Query(StringUtil::Format("CREATE OR REPLACE " + object + chained_body, i, i - 1)));
	}
}

// VALUES binds its expressions without first walking each of them whole, so only binding recurses through the nest,
// one expression level per call and no binder level at all.
string NestedCallsInValues(idx_t count) {
	string expression = "42";
	for (idx_t i = 0; i < count; i++) {
		expression = StringUtil::Format("abs(%s)", expression);
	}
	return StringUtil::Format("VALUES (%s)", expression);
}

string NestedCTEs(idx_t count) {
	string query = "SELECT 42 AS a";
	for (idx_t i = 0; i < count; i++) {
		query = StringUtil::Format("WITH c%d AS (%s) SELECT * FROM c%d", i, query, i);
	}
	return query;
}

string NestedFromSubqueries(idx_t count) {
	string query = "SELECT 42 AS a";
	for (idx_t i = 0; i < count; i++) {
		query = StringUtil::Format("SELECT * FROM (%s)", query);
	}
	return query;
}

string NestedFromJoins(idx_t count) {
	string query = "SELECT 42 AS a";
	for (idx_t i = 0; i < count; i++) {
		query = StringUtil::Format("SELECT t%d.a FROM (%s) t%d JOIN (SELECT 42 AS a) u%d USING (a)", i, query, i, i);
	}
	return query;
}

// Each level makes five plan operators: a projection, a filter, a distinct, an order and a limit.
string NestedDenseSubqueries(idx_t count) {
	string query = "SELECT 42 AS a";
	for (idx_t i = 0; i < count; i++) {
		query = StringUtil::Format("SELECT DISTINCT a FROM (%s) WHERE a > 0 ORDER BY a LIMIT 10", query);
	}
	return query;
}

string ExceptChain(idx_t count) {
	string query = "SELECT 42";
	for (idx_t i = 0; i < count; i++) {
		query += StringUtil::Format(" EXCEPT SELECT %d", 100 + i);
	}
	return query;
}

// The chain reads the outer query's column, so decorrelation walks it as the body of a correlated subquery.
string CorrelatedExceptChain(idx_t count, const string &except = "EXCEPT") {
	string chain = "SELECT t.a";
	for (idx_t i = 0; i < count; i++) {
		chain += StringUtil::Format(" %s SELECT %d", except, 100 + i);
	}
	return StringUtil::Format("SELECT (%s) FROM (SELECT 42 AS a) t", chain);
}

} // namespace

TEST_CASE("A nest deeper than the thread's stack raises the depth error", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET memory_limit='4GB'"));
	if (!StackIsReported(con)) {
		return;
	}

	SECTION("views") {
		CreateChain(con, "VIEW v%d AS ", "SELECT 42 AS a", "SELECT * FROM v%d", DEEP_NEST);
		RequireStackError(con, StringUtil::Format("SELECT * FROM v%d", DEEP_NEST));
		RequireValue(con, StringUtil::Format("SELECT * FROM v%d", SHALLOW_NEST), "42");
	}
	SECTION("table macros") {
		CreateChain(con, "MACRO t%d() AS TABLE ", "SELECT 42 AS a", "SELECT * FROM t%d()", DEEP_NEST);
		RequireStackError(con, StringUtil::Format("SELECT * FROM t%d()", DEEP_NEST));
		RequireValue(con, StringUtil::Format("SELECT * FROM t%d()", SHALLOW_NEST), "42");
	}
	SECTION("common table expressions") {
		RequireStackError(con, NestedCTEs(DEEP_STATEMENT_NEST));
		RequireValue(con, NestedCTEs(SHALLOW_NEST), "42");
	}
	SECTION("scalar macros") {
		CreateChain(con, "MACRO m%d(x) AS ", "x", "m%d(x) + 1", DEEP_MACRO_NEST);
		RequireStackError(con, StringUtil::Format("SELECT m%d(42)", DEEP_MACRO_NEST), MACRO_STACK_SIZE);
		RequireValue(con, StringUtil::Format("SELECT m%d(42)", SHALLOW_NEST), "45", MACRO_STACK_SIZE);
	}
	// the connection is still usable on any thread
	RequireValue(con, "SELECT 42", "42");
	auto result = con.Query("SELECT 42");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
}

TEST_CASE("A deep expression raises the depth error on a small stack", "[api]") {
	// under max_expression_depth, and binding the nest takes about 300 KiB of stack on a release build
	constexpr idx_t DEEP_EXPRESSION_NEST = 200;
	// small, so that the nest that overflows it is shallow and destroying it takes little stack
	constexpr idx_t EXPRESSION_STACK_SIZE = 192 * 1024;
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET memory_limit='4GB'"));
	if (!StackIsReported(con)) {
		return;
	}
	RequireStackError(con, NestedCallsInValues(DEEP_EXPRESSION_NEST), EXPRESSION_STACK_SIZE);
	RequireValue(con, NestedCallsInValues(SHALLOW_NEST), "42", EXPRESSION_STACK_SIZE);
	RequireValue(con, NestedCallsInValues(DEEP_EXPRESSION_NEST), "42", LARGE_STACK_SIZE);
}

#if defined(__linux__) && !defined(DUCKDB_NO_THREADS)
namespace {

struct SoftStackLimit {
	explicit SoftStackLimit(const struct rlimit &original) : original(original) {
	}
	~SoftStackLimit() {
		setrlimit(RLIMIT_STACK, &original);
	}
	void Set(rlim_t soft_limit) {
		struct rlimit limit = original;
		limit.rlim_cur = soft_limit;
		REQUIRE(setrlimit(RLIMIT_STACK, &limit) == 0);
	}

	struct rlimit original;
};

} // namespace

// The main thread of a Linux process grows its stack on demand up to the soft stack limit, which can change while the
// process runs.
TEST_CASE("The main thread of a Linux process follows a change of its stack limit", "[api]") {
	constexpr rlim_t LOWERED_STACK_LIMIT = 256 * 1024;
	constexpr rlim_t RAISED_STACK_LIMIT = 8 * 1024 * 1024;
	// binding the nest takes about 1 MiB of stack on a release build
	constexpr idx_t MAIN_THREAD_NEST = 150;
	if (getpid() != static_cast<pid_t>(syscall(SYS_gettid))) {
		WARN("The test does not run on the main thread");
		return;
	}
	struct rlimit original;
	REQUIRE(getrlimit(RLIMIT_STACK, &original) == 0);
	if (original.rlim_max != RLIM_INFINITY && original.rlim_max < RAISED_STACK_LIMIT) {
		WARN("The hard stack limit is below " + to_string(RAISED_STACK_LIMIT) + " bytes");
		return;
	}
	SoftStackLimit stack_limit(original);
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET memory_limit='4GB'"));
	CreateChain(con, "VIEW v%d AS ", "SELECT 42 AS a", "SELECT * FROM v%d", MAIN_THREAD_NEST);
	const auto deep = StringUtil::Format("SELECT * FROM v%d", MAIN_THREAD_NEST);

	stack_limit.Set(LOWERED_STACK_LIMIT);
	auto result = con.Query(deep);
	INFO(result->ToString());
	REQUIRE(result->HasError());
	REQUIRE(StringUtil::Contains(result->GetError(),
	                             "Binder Error: Max expression depth exceeded: binding the query would overflow the"));
	result = con.Query(StringUtil::Format("SELECT * FROM v%d", SHALLOW_NEST));
	REQUIRE(CHECK_COLUMN(result, 0, {42}));

	stack_limit.Set(RAISED_STACK_LIMIT);
	result = con.Query(deep);
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
}
#endif

TEST_CASE("A macro cycle through a USING KEY aggregate reaches the depth limit", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET memory_limit='4GB'"));
	// the aggregates bind on a binder without a parent, which continues the depth of the recursive CTE
	const string body =
	    "WITH RECURSIVE r(k, v) USING KEY (k, max(%s()) AS v) AS (VALUES (1, 1) UNION SELECT k, v FROM r "
	    "WHERE false) SELECT max(v) FROM r";
	REQUIRE_NO_FAIL(con.Query("SET max_expression_depth=100"));

	SECTION("closed by dropping a shadow") {
		REQUIRE_NO_FAIL(con.Query("CREATE TEMP MACRO m() AS 1"));
		REQUIRE_NO_FAIL(con.Query("CREATE MACRO main.m() AS (" + StringUtil::Format(body, "m") + ")"));
		REQUIRE_NO_FAIL(con.Query("DROP MACRO temp.m"));
		RequireDepthError(con, "SELECT m()", "Max expression depth limit of 100 exceeded");
	}
	SECTION("closed by the search path") {
		REQUIRE_NO_FAIL(con.Query("CREATE SCHEMA s"));
		REQUIRE_NO_FAIL(con.Query("CREATE MACRO s.n() AS 1"));
		REQUIRE_NO_FAIL(con.Query("SET search_path='s,main'"));
		REQUIRE_NO_FAIL(con.Query("CREATE MACRO main.n() AS (" + StringUtil::Format(body, "n") + ")"));
		REQUIRE_NO_FAIL(con.Query("SET search_path='main'"));
		RequireDepthError(con, "SELECT n()", "Max expression depth limit of 100 exceeded");
	}
}

TEST_CASE("Join ordering and subquery planning raise the stack error before the stack runs out", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET memory_limit='4GB'"));
	// these passes do not check the stack and need more of it per level than the checked walks for some nests below
	REQUIRE_NO_FAIL(con.Query("SET disabled_optimizers='window_self_join,projection_placement,unused_columns,"
	                          "top_n_window_elimination,filter_pushdown'"));
	if (!StackIsReported(con)) {
		return;
	}

	SECTION("FROM subqueries") {
		RequireValueOrStackError(con, NestedFromSubqueries(DEEP_STATEMENT_NEST), "42");
		RequireValue(con, NestedFromSubqueries(SHALLOW_NEST), "42");
	}
	SECTION("FROM subqueries with joins") {
		// planning a nest of joins takes seconds once it fits, so it runs on one stack only
		RequireValueOrStackError(con, NestedFromJoins(DEEP_JOIN_NEST), "42", SMALL_STACK_SIZE, SMALL_STACK_SIZE);
		RequireValue(con, NestedFromJoins(SHALLOW_NEST), "42");
	}
	SECTION("subqueries with five plan operators per level") {
		// pins the checks of the trigger rewrite, the subquery planner and decorrelation
		REQUIRE(RequireValueOrStackError(con, NestedDenseSubqueries(DEEP_DENSE_NEST), "42"));
		RequireValue(con, NestedDenseSubqueries(SHALLOW_NEST), "42");
	}
	SECTION("set operations") {
		// pins the check of decorrelation
		REQUIRE(RequireValueOrStackError(con, ExceptChain(DEEP_EXCEPT_CHAIN), "42"));
		RequireValue(con, ExceptChain(SHALLOW_NEST), "42");
	}
	SECTION("EXCEPT ALL in a correlated subquery") {
		// pins the check of the join order optimizer; binding runs out first under a sanitizer, so no error is required
		RequireValueOrStackError(con, CorrelatedExceptChain(CORRELATED_EXCEPT_ALL_CHAIN, "EXCEPT ALL"), "42",
		                         NARROW_SWEEP_MIN_STACK_SIZE, NARROW_SWEEP_MAX_STACK_SIZE, NARROW_SWEEP_STEP);
		// a sanitizer's frames make even this short chain's nested join order optimizers take most of 512 KiB
		RequireValue(con, CorrelatedExceptChain(SHALLOW_NEST, "EXCEPT ALL"), "42", NARROW_SWEEP_MAX_STACK_SIZE);
	}
	SECTION("a longer EXCEPT ALL chain in a correlated subquery") {
		// pins the checks of the subquery planner and the correlated push down, on bands narrower than the other steps
		RequireValueOrStackError(con, CorrelatedExceptChain(DEEP_EXCEPT_CHAIN, "EXCEPT ALL"), "42",
		                         NARROW_SWEEP_MIN_STACK_SIZE, NARROW_SWEEP_MAX_STACK_SIZE, FINE_SWEEP_STEP);
	}
	SECTION("set operations in a correlated subquery") {
		// pins the checks of the subquery planner and the correlated push down; binding runs out first under a
		// sanitizer
		RequireValueOrStackError(con, CorrelatedExceptChain(DEEP_EXCEPT_CHAIN), "42", NARROW_SWEEP_MIN_STACK_SIZE,
		                         NARROW_SWEEP_MAX_STACK_SIZE, NARROW_SWEEP_STEP);
		RequireValue(con, CorrelatedExceptChain(SHALLOW_NEST), "42");
	}
	RequireValue(con, "SELECT 42", "42");
}
