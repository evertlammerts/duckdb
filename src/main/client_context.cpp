#include "duckdb/main/client_context.hpp"

#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/common/chrono.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/common/progress_bar/progress_bar.hpp"
#include "duckdb/common/serializer/buffered_file_writer.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/execution/column_binding_resolver.hpp"
#include "duckdb/execution/operator/helper/physical_result_collector.hpp"
#include "duckdb/execution/operator/helper/physical_result_sink.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/buffered_data/batched_buffered_data.hpp"
#include "duckdb/main/buffered_data/simple_buffered_data.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context_file_opener.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/statement_iterator.hpp"
#include "duckdb/main/error_manager.hpp"
#include "duckdb/main/parse_iterator.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/main/relation.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/expression/parameter_expression.hpp"
#include "duckdb/parser/parsed_data/create_function_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/peg/compiled_grammar.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/drop_statement.hpp"
#include "duckdb/parser/statement/execute_statement.hpp"
#include "duckdb/parser/statement/explain_statement.hpp"
#include "duckdb/parser/statement/delete_statement.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/statement/merge_into_statement.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/query_node/delete_query_node.hpp"
#include "duckdb/parser/query_node/insert_query_node.hpp"
#include "duckdb/parser/query_node/merge_query_node.hpp"
#include "duckdb/parser/query_node/update_query_node.hpp"
#include "duckdb/parser/statement/prepare_statement.hpp"
#include "duckdb/parser/statement/relation_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/column_data_ref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/logical_plan_verifier.hpp"
#include "duckdb/planner/operator/logical_execute.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/common/enums/current_transaction_state.hpp"
#include "duckdb/planner/statement_preprocessor.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/transaction/transaction_context.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/logging/log_type.hpp"
#include "duckdb/logging/log_manager.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/main/result_set_manager.hpp"
#include "duckdb/parser/statement/transaction_statement.hpp"
#include "duckdb/main/prepared_statement.hpp"

#ifdef __APPLE__
#include <sys/sysctl.h>

// code adapted from Apple's example
// https://developer.apple.com/documentation/apple-silicon/about-the-rosetta-translation-environment#Determine-Whether-Your-App-Is-Running-as-a-Translated-Binary
static bool OsxRosettaIsActive() {
	int ret = 0;
	size_t size = sizeof(ret);
	if (sysctlbyname("sysctl.proc_translated", &ret, &size, NULL, 0)) {
		return false;
	}
	return ret == 1;
}

#endif

namespace duckdb {

//! The execution state of the statement that is running
struct ActiveFragment {
	//! Prepared statement data
	shared_ptr<PreparedStatementData> prepared;
	//! The query executor
	unique_ptr<Executor> executor;
	//! The progress bar
	unique_ptr<ProgressBar> progress_bar;
};

//! Whether preprocessing bracketed these statements in a transaction of their own: a leading BEGIN
//! paired with a trailing COMMIT, both carrying the auto-rollback flag only the preprocessor sets
static bool CarriesInjectedTransaction(const vector<unique_ptr<SQLStatement>> &statements) {
	if (statements.size() < 2) {
		return false;
	}
	auto &front = *statements.front();
	auto &back = *statements.back();
	if (front.type != StatementType::TRANSACTION_STATEMENT || back.type != StatementType::TRANSACTION_STATEMENT) {
		return false;
	}
	auto &begin_info = *front.Cast<TransactionStatement>().info;
	auto &commit_info = *back.Cast<TransactionStatement>().info;
	return begin_info.type == TransactionType::BEGIN_TRANSACTION && begin_info.auto_rollback &&
	       commit_info.type == TransactionType::COMMIT && commit_info.auto_rollback;
}

//! One user statement for its whole life: the engine statements it preprocessed into, the one that
//! runs now, the result the consumer holds and the decisions the consumer made about it
struct ActiveQueryContext {
public:
	explicit ActiveQueryContext(vector<unique_ptr<SQLStatement>> statements)
	    : fragments(std::move(statements)), owns_transaction(CarriesInjectedTransaction(fragments)) {
		D_ASSERT(!fragments.empty());
	}

public:
	//! The text of the statement that is currently being executed
	string query;
	//! The engine statements the user's statement preprocessed into
	vector<unique_ptr<SQLStatement>> fragments;
	//! The statement that is running
	unique_ptr<ActiveFragment> fragment;

public:
	void SetOpenResult(QueryResult &result) {
		open_result = &result;
	}
	bool IsOpenResult(BaseQueryResult &result) {
		return static_cast<BaseQueryResult *>(open_result) == &result;
	}
	bool HasOpenResult() const {
		return open_result != nullptr;
	}
	QueryResult &GetOpenResult() {
		D_ASSERT(open_result);
		return *open_result;
	}

	//! Whether a statement remains after the one that is running
	bool HasNextFragment() const {
		return next_fragment < fragments.size();
	}
	unique_ptr<SQLStatement> TakeNextFragment() {
		D_ASSERT(HasNextFragment());
		return std::move(fragments[next_fragment++]);
	}
	//! Whether the statement that is running is the one whose rows and schema the consumer reads
	bool RunsPrincipal() const {
		return principal;
	}
	void SetPrincipal(bool principal_p) {
		principal = principal_p;
	}
	//! Whether the transaction around these statements is this query's own to roll back
	bool OwnsTransaction() const {
		return owns_transaction;
	}
	//! Whether the registered states have been told this query began
	bool StatesNotified() const {
		return states_notified;
	}
	void MarkStatesNotified() {
		states_notified = true;
	}
	//! The retention the consumer settled, UNDECIDED until it does. The first decision stands
	ResultLifetime Retention() const {
		return retention;
	}
	void Settle(ResultLifetime lifetime) {
		if (retention == ResultLifetime::UNDECIDED) {
			retention = lifetime;
		}
	}
	QueryResultMemoryType MemoryType() const {
		return memory_type;
	}
	void SetMemoryType(QueryResultMemoryType type) {
		memory_type = type;
	}

private:
	//! The currently open result
	QueryResult *open_result = nullptr;
	idx_t next_fragment = 0;
	bool principal = false;
	bool states_notified = false;
	bool owns_transaction;
	ResultLifetime retention = ResultLifetime::UNDECIDED;
	QueryResultMemoryType memory_type = QueryResultMemoryType::IN_MEMORY;
};

#ifdef DEBUG
struct DebugClientContextState : public ClientContextState {
	~DebugClientContextState() override {
		if (Exception::UncaughtException()) {
			return;
		}
		D_ASSERT(!active_transaction);
		D_ASSERT(!active_query);
	}

	bool active_transaction = false;
	bool active_query = false;

	void QueryBegin(ClientContext &context) override {
		if (active_query) {
			throw InternalException("DebugClientContextState::QueryBegin called when a query is already active");
		}
		active_query = true;
	}
	void QueryEnd(ClientContext &context) override {
		if (!active_query) {
			throw InternalException("DebugClientContextState::QueryEnd called when no query is active");
		}
		active_query = false;
	}
	void TransactionBegin(MetaTransaction &transaction, ClientContext &context) override {
		if (active_transaction) {
			throw InternalException(
			    "DebugClientContextState::TransactionBegin called when a transaction is already active");
		}
		active_transaction = true;
	}
	void TransactionCommit(MetaTransaction &transaction, ClientContext &context) override {
		if (!active_transaction) {
			throw InternalException("DebugClientContextState::TransactionCommit called when no transaction is active");
		}
		active_transaction = false;
	}
	void TransactionRollback(MetaTransaction &transaction, ClientContext &context) override {
		if (!active_transaction) {
			throw InternalException(
			    "DebugClientContextState::TransactionRollback called when no transaction is active");
		}
		active_transaction = false;
	}
#ifdef DUCKDB_DEBUG_REBIND
	RebindQueryInfo OnPlanningError(ClientContext &context, SQLStatement &statement, ErrorData &error) override {
		return RebindQueryInfo::ATTEMPT_TO_REBIND;
	}
	RebindQueryInfo OnFinalizePrepare(ClientContext &context, PreparedStatementData &prepared,
	                                  PreparedStatementMode mode) override {
		if (mode == PreparedStatementMode::PREPARE_AND_EXECUTE) {
			return RebindQueryInfo::ATTEMPT_TO_REBIND;
		}
		return RebindQueryInfo::DO_NOT_REBIND;
	}
	RebindQueryInfo OnRebindPreparedStatement(ClientContext &context, BindPreparedStatementCallbackInfo &info,
	                                          RebindQueryInfo current_rebind) override {
		return RebindQueryInfo::ATTEMPT_TO_REBIND;
	}
#endif
};
#endif

ClientContext::ClientContext(shared_ptr<DatabaseInstance> database)
    : db(std::move(database)), interrupt_state(ClientInterruptState::NOT_INTERRUPTED), transaction(*this),
      connection_id(DConstants::INVALID_INDEX) {
	registered_state = make_uniq<RegisteredStateManager>();
#ifdef DEBUG
	registered_state->GetOrCreate<DebugClientContextState>("debug_client_context_state");
#endif
	LoggingContext context(LogContextScope::CONNECTION);
	logger = db->GetLogManager().CreateLogger(context, true);
	client_data = make_uniq<ClientData>(*this);

#ifdef __APPLE__
	if (OsxRosettaIsActive()) {
		DUCKDB_LOG_WARNING(*this, "OSX binary translation ('Rosetta') detected. Running DuckDB through Rosetta will "
		                          "cause a significant performance degradation. DuckDB is available natively on Apple "
		                          "silicon, please download an appropriate binary here: https://duckdb.org/install/");
	}
#endif
}

ClientContext::~ClientContext() {
	if (Exception::UncaughtException()) {
		return;
	}
	// destroy the client context and rollback if there is an active transaction
	// but only if we are not destroying this client context as part of an exception stack unwind
	Destroy();
}

unique_ptr<ClientContextLock> ClientContext::LockContext() {
	return make_uniq<ClientContextLock>(context_lock);
}

void ClientContext::ConnectToCatalog(const shared_ptr<AttachedDatabase> &target) {
	D_ASSERT(target);
	// Pre-flight: Supports(RemoteCapability::CONNECT) is the capability declaration; catalogs that
	// return true MUST implement RemoteExecute(string). Validation runs before mutation so a throw
	// leaves the client unbound.
	if (!target->GetCatalog().Supports(RemoteCapability::CONNECT)) {
		throw InvalidInputException("Database \"%s\" does not support CONNECT", target->GetName());
	}
	connected_to_database = target;
	is_connected = true;
}

void ClientContext::DisconnectFromCatalog() {
	connected_to_database.reset();
	is_connected = false;
}

shared_ptr<AttachedDatabase> ClientContext::TryGetConnectedCatalog() const {
	if (!is_connected) {
		return nullptr;
	}
	return AttachedDatabase::TryGetReference(connected_to_database);
}

//! True if `type` is a CONNECT control statement that must execute against LOCAL even while a
//! CONNECT binding is active (i.e. the chokepoint must let it fall through, not rewrite it).
static bool IsConnectControlStatement(StatementType type) {
	return type == StatementType::CONNECT_STATEMENT || type == StatementType::DISCONNECT_STATEMENT;
}

//! Wrap a TableRef returned from Catalog::RemoteExecute into `SELECT * FROM <ref>` for the chokepoint.
static unique_ptr<SQLStatement> WrapAsSelect(unique_ptr<TableRef> from_ref) {
	auto select_node = make_uniq<SelectNode>();
	select_node->select_list.push_back(make_uniq<StarExpression>());
	select_node->from_table = std::move(from_ref);

	auto select_stmt = make_uniq<SelectStatement>();
	select_stmt->node = std::move(select_node);
	return std::move(select_stmt);
}

void ClientContext::Destroy() {
	auto lock = LockContext();
	if (transaction.HasActiveTransaction()) {
		transaction.ResetActiveQuery();
		if (!transaction.IsAutoCommit()) {
			transaction.Rollback(nullptr);
		}
	}
	CleanupInternal(*lock);
}

void ClientContext::ProcessError(ErrorData &error, const string &query) const {
	error.FinalizeError();
	if (Settings::Get<ErrorsAsJSONSetting>(*this)) {
		error.ConvertErrorToJSON();
	} else {
		error.AddErrorLocation(query);
	}
}

template <class T>
unique_ptr<T> ClientContext::ErrorResult(ErrorData error, const string &query) {
	ProcessError(error, query);
	return make_uniq<T>(std::move(error));
}

void ClientContext::BeginQueryInternal(ClientContextLock &lock, vector<unique_ptr<SQLStatement>> fragments) {
	D_ASSERT(!active_query);
	auto &db_inst = DatabaseInstance::GetDatabase(*this);
	if (ValidChecker::IsInvalidated(db_inst)) {
		throw ErrorManager::InvalidatedDatabase(*this, ValidChecker::InvalidatedMessage(db_inst));
	}
	active_query = make_uniq<ActiveQueryContext>(std::move(fragments));

	query_progress.Initialize();
	// Set query deadline if max_execution_time is configured
	auto max_execution_time = Settings::Get<MaxExecutionTimeSetting>(*this);
	if (max_execution_time > 0) {
		auto now = steady_clock::now();
		auto deadline_tp = now + milliseconds(max_execution_time);
		query_deadline = NumericCast<idx_t>(duration_cast<milliseconds>(deadline_tp.time_since_epoch()).count());
	} else {
		query_deadline.SetInvalid();
	}
}

ErrorData ClientContext::EndFragmentInternal(ClientContextLock &, bool success, bool invalidate_transaction,
                                             optional_ptr<ErrorData> previous_error, const char *invalidation_reason) {
	D_ASSERT(active_query);
	auto fragment = std::move(active_query->fragment);
	if (fragment && fragment->executor) {
		fragment->executor->CancelTasks();
	}
	fragment.reset();
	ErrorData error;
	try {
		if (transaction.HasActiveTransaction()) {
			transaction.ResetActiveQuery();
			if (transaction.IsAutoCommit()) {
				if (success) {
					transaction.Commit();
				} else {
					transaction.Rollback(previous_error);
				}
			} else if (invalidate_transaction) {
				D_ASSERT(!success);
				ValidChecker::Invalidate(ActiveTransaction(), invalidation_reason);
			}
		}
	} catch (std::exception &ex) {
		error = ErrorData(ex);
		if (Exception::InvalidatesDatabase(error.Type()) || error.Type() == ExceptionType::INTERNAL) {
			auto &db_inst = DatabaseInstance::GetDatabase(*this);
			ValidChecker::Invalidate(db_inst, error.RawMessage());
		}
	} catch (...) { // LCOV_EXCL_START
		error = ErrorData("Unhandled exception!");
	} // LCOV_EXCL_STOP
	// The profiler describes one statement, and its operator tree dies with the statement
	client_data->profiler->EndQuery();
	return error;
}

ErrorData ClientContext::EndQueryInternal(ClientContextLock &lock, bool success, bool invalidate_transaction,
                                          optional_ptr<ErrorData> previous_error, const char *invalidation_reason) {
	D_ASSERT(active_query);
	// A query that opened a transaction of its own and did not finish leaves nothing behind
	const bool rollback_own_transaction = !success && active_query->OwnsTransaction();
	const bool states_notified = active_query->StatesNotified();
	auto error = EndFragmentInternal(lock, success, invalidate_transaction, previous_error, invalidation_reason);
	active_query.reset();
	query_deadline.SetInvalid();
	query_progress.Initialize();
	if (rollback_own_transaction && !transaction.IsAutoCommit() && transaction.HasActiveTransaction()) {
		try {
			transaction.Rollback(previous_error);
		} catch (std::exception &ex) {
			if (!error.HasError()) {
				error = ErrorData(ex);
			}
		} catch (...) { // LCOV_EXCL_START
			if (!error.HasError()) {
				error = ErrorData("Unhandled exception!");
			}
		} // LCOV_EXCL_STOP
	}

	client_data->profiler->EndQuery();

	// Refresh the logger
	logger->Flush();
	LoggingContext context(LogContextScope::CONNECTION);
	context.connection_id = connection_id;
	logger = db->GetLogManager().CreateLogger(context, true);

	// Notify any registered state of query end
	if (states_notified) {
		for (auto const &s : registered_state->States()) {
			if (error.HasError()) {
				s->QueryEnd(*this, &error);
			} else {
				s->QueryEnd(*this, previous_error);
			}
		}
	}
	return error;
}

void ClientContext::CleanupInternal(ClientContextLock &lock, BaseQueryResult *result, bool invalidate_transaction) {
	if (!active_query) {
		// no query currently active
		return;
	}
	bool execution_finished = !active_query->HasNextFragment();
	if (active_query->fragment && active_query->fragment->executor) {
		auto &executor = *active_query->fragment->executor;
		// ExecutionIsFinished also reports true for a failed execution
		execution_finished = execution_finished && executor.ExecutionIsFinished() && !executor.HasError();
		// Read before CancelTasks clears the slot, and while the profiler is still running
		auto buffer = executor.GetResultBuffer();
		if (buffer) {
			QueryProfiler::Get(*this).SetStreamingPeakBufferSize(buffer->PeakBufferedBytes());
		}
		executor.CancelTasks();
	}

	// Relaunch the threads if a SET THREADS command was issued
	auto &scheduler = TaskScheduler::GetScheduler(*this);
	scheduler.RelaunchThreads();

	optional_ptr<ErrorData> passed_error = nullptr;
	if (result && result->HasError()) {
		passed_error = result->GetErrorObject();
	}
	bool success = false;
	const char *invalidation_reason = "Failed to commit";
	if (result && !result->HasError()) {
		success = execution_finished;
		if (!success) {
			// Committing would keep only the part of the statement the workers happened to run
			invalidate_transaction = true;
			invalidation_reason = "Query was closed before it finished";
		}
	}
	auto error = EndQueryInternal(lock, success, invalidate_transaction, passed_error, invalidation_reason);
	if (result && !result->HasError()) {
		// if an error occurred while committing report it in the result
		result->SetError(error);
	}
	D_ASSERT(!active_query);
}

Executor &ClientContext::GetExecutor() {
	D_ASSERT(active_query);
	D_ASSERT(active_query->fragment);
	D_ASSERT(active_query->fragment->executor);
	return *active_query->fragment->executor;
}

Logger &ClientContext::GetLogger() const {
	return *logger;
}

const string &ClientContext::GetCurrentQuery() {
	D_ASSERT(active_query);
	return active_query->query;
}

connection_t ClientContext::GetConnectionId() const {
	return connection_id;
}

static bool IsExplainAnalyze(SQLStatement *statement) {
	if (!statement) {
		return false;
	}
	if (statement->type != StatementType::EXPLAIN_STATEMENT) {
		return false;
	}
	auto &explain = statement->Cast<ExplainStatement>();
	return explain.explain_type == ExplainType::EXPLAIN_ANALYZE;
}

shared_ptr<PreparedStatementData> ClientContext::CreatePreparedStatementInternal(ClientContextLock &lock,
                                                                                 unique_ptr<SQLStatement> statement,
                                                                                 const QueryParameters &parameters) {
	StatementType statement_type = statement->type;
	auto result = make_shared_ptr<PreparedStatementData>(statement_type);

	auto &profiler = QueryProfiler::Get(*this);
	profiler.StartQuery(statement->query, IsExplainAnalyze(statement.get()));
	Planner logical_planner(*this);
	if (parameters.statement_args) {
		auto &parameter_values = *parameters.statement_args;
		for (auto &value : parameter_values) {
			logical_planner.parameter_data.emplace(value.first, BoundParameterData(value.second));
		}
	}

	{
		auto planner_timer = profiler.StartTimer<MetricPlannerTotalTime>();
		logical_planner.CreatePlan(std::move(statement));
		D_ASSERT(logical_planner.plan || !logical_planner.properties.bound_all_parameters);
	}

	auto logical_plan = std::move(logical_planner.plan);
	// extract the result column names from the plan
	result->properties = logical_planner.properties;
	result->names = logical_planner.names;
	result->types = logical_planner.types;
	result->value_map = std::move(logical_planner.value_map);
	if (!logical_planner.properties.bound_all_parameters) {
		// not all parameters were bound - return
		return result;
	}
#ifdef DEBUG
	logical_plan->Verify(*this);
#endif
	bool optimize = Settings::Get<EnableOptimizerSetting>(*this);
	if (Settings::Get<DebugDisableOptimizerSetting>(*this)) {
		// verify disable optimizer - disable EXCEPT for explain, otherwise every single EXPLAIN query breaks
		if (logical_plan->type != LogicalOperatorType::LOGICAL_EXPLAIN) {
			optimize = false;
		}
	}
	if (logical_plan->RequireOptimizer()) {
		{
			auto optimizer_timer = profiler.StartTimer<MetricOptimizerTotalTime>();
			Optimizer optimizer(*logical_planner.binder, *this);
			if (optimize) {
				logical_plan = optimizer.Optimize(std::move(logical_plan));
			} else {
				logical_plan = optimizer.LowerMandatoryAggregateRewrites(std::move(logical_plan));
			}
			D_ASSERT(logical_plan);
		}
#ifdef DEBUG
		logical_plan->Verify(*this);
#endif
	}

	// Convert the logical query plan into a physical query plan.
	{
		auto physical_timer = profiler.StartTimer<MetricPhysicalPlannerTotalTime>();
		PhysicalPlanGenerator physical_planner(*this);
		result->physical_plan = physical_planner.Plan(std::move(logical_plan));
	}
	D_ASSERT(result->physical_plan);
	return result;
}

shared_ptr<PreparedStatementData> ClientContext::CreatePreparedStatement(ClientContextLock &lock,
                                                                         unique_ptr<SQLStatement> statement,
                                                                         const QueryParameters &parameters) {
	// check if any client context state could request a rebind
	bool can_request_rebind = false;
	for (auto &state : registered_state->States()) {
		if (state->CanRequestRebind()) {
			can_request_rebind = true;
		}
	}
	if (can_request_rebind) {
		bool rebind = false;
		// if any registered state can request a rebind we do the binding on a copy first
		shared_ptr<PreparedStatementData> result;
		try {
			result = CreatePreparedStatementInternal(lock, statement->Copy(), parameters);
		} catch (std::exception &ex) {
			ErrorData error(ex);
			// check if any registered client context state wants to try a rebind
			for (auto &state : registered_state->States()) {
				auto info = state->OnPlanningError(*this, *statement, error);
				if (info == RebindQueryInfo::ATTEMPT_TO_REBIND) {
					rebind = true;
				}
			}
			if (!rebind) {
				throw;
			}
		}
		if (result) {
			D_ASSERT(!rebind);
			for (auto &state : registered_state->States()) {
				auto info = state->OnFinalizePrepare(*this, *result, PreparedStatementMode::PREPARE_AND_EXECUTE);
				if (info == RebindQueryInfo::ATTEMPT_TO_REBIND) {
					rebind = true;
				}
			}
		}
		if (!rebind) {
			return result;
		}
		// an extension wants to do a rebind - do it once
	}

	return CreatePreparedStatementInternal(lock, std::move(statement), parameters);
}

QueryProgress ClientContext::GetQueryProgress() {
	return query_progress;
}

void BindPreparedStatementParameters(ClientContext &context, PreparedStatementData &statement,
                                     const QueryParameters &parameters) {
	identifier_map_t<BoundParameterData> owned_values;
	if (parameters.statement_args) {
		auto &params = *parameters.statement_args;
		for (auto &val : params) {
			owned_values.emplace(val);
		}
	}
	statement.Bind(context, owned_values);
}

void ClientContext::CheckIfPreparedStatementIsExecutable(PreparedStatementData &statement) {
	if (ValidChecker::IsInvalidated(ActiveTransaction()) && statement.properties.requires_valid_transaction) {
		throw ErrorManager::InvalidatedTransaction(*this);
	}

	auto &meta_transaction = MetaTransaction::Get(*this);
	auto &manager = DatabaseManager::Get(*this);
	for (auto &it : statement.properties.modified_databases) {
		auto &modified_database = it.first;
		auto entry = manager.GetDatabase(*this, modified_database);
		if (!entry) {
			// database has been detached
			throw InvalidInputException("Database \"%s\" not found", modified_database);
		}
		if (entry->IsReadOnly()) {
			throw InvalidInputException(StringUtil::Format(
			    "Cannot execute statement of type \"%s\" on database %s which is attached in read-only mode!",
			    StatementTypeToString(statement.statement_type), modified_database));
		}
		meta_transaction.ModifyDatabase(*entry, it.second.modifications);
	}
}

ClientContext::FragmentExecution ClientContext::InitializeExecutionInternal(ClientContextLock &,
                                                                            PreparedStatementData &statement_data,
                                                                            const QueryParameters &parameters) {
	D_ASSERT(active_query && !active_query->fragment);
	BindPreparedStatementParameters(*this, statement_data, parameters);

	active_query->fragment = make_uniq<ActiveFragment>();
	// Only the statement whose rows the consumer reads may park for a decision they have not made
	const auto settle_at_submission =
	    active_query->RunsPrincipal() ? active_query->Retention() : ResultLifetime::RETAINED;

	// Create the query executor.
	active_query->fragment->executor = make_uniq<Executor>(*this);
	auto &executor = *active_query->fragment->executor;

	if (config.enable_progress_bar) {
		progress_bar_display_create_func_t display_create_func = nullptr;
		if (config.print_progress_bar) {
			// Use either a custom display function, or the default.
			display_create_func =
			    config.display_create_func ? config.display_create_func : ProgressBar::DefaultProgressBarDisplay;
		}
		active_query->fragment->progress_bar =
		    make_uniq<ProgressBar>(executor, NumericCast<idx_t>(config.wait_time), display_create_func);
		active_query->fragment->progress_bar->Start();
		query_progress.Restart();
	}

	statement_data.memory_type = parameters.memory_type;

	// Decide how to get the result collector.
	get_result_collector_t get_collector = PhysicalResultCollector::GetResultCollector;
	auto &client_config = ClientConfig::GetConfig(*this);
	FragmentExecution execution;
	execution.delegating = client_config.get_result_collector != nullptr;
	if (execution.delegating) {
		get_collector = client_config.get_result_collector;
	}

	// Get the result collector and initialize the executor.
	auto collector = get_collector(*this, statement_data);
	D_ASSERT(collector->type == PhysicalOperatorType::RESULT_COLLECTOR);

	// The buffer is created here, on the client thread, and handed to the sink, the executor and the
	// handle. It carries the retention decision, so it exists for every query the sink serves
	if (!execution.delegating) {
		auto &sink = collector->Cast<PhysicalResultSink>();
		if (sink.ordering == ResultOrdering::BATCH_INDEX_ORDERED) {
			execution.buffer = make_shared_ptr<BatchedBufferedData>(*this, sink.lifetime);
		} else {
			execution.buffer = make_shared_ptr<SimpleBufferedData>(*this, sink.lifetime);
		}
		if (settle_at_submission != ResultLifetime::UNDECIDED) {
			// Settled before execution starts, so no producer ever parks for the decision
			execution.buffer->Decide(settle_at_submission);
		}
		sink.SetResultBuffer(execution.buffer);
	}
	executor.SetResultBuffer(execution.buffer);

	// Read before Initialize starts the workers: a SET statement writes the settings from a task
	execution.client_properties = GetClientProperties();
	execution.types = statement_data.types;
	executor.Initialize(std::move(collector));

	D_ASSERT(executor.GetTypes() == statement_data.types);
	return execution;
}

unique_ptr<QueryResult> ClientContext::CompleteDelegatedInternal(ClientContextLock &lock, QueryResult &result) {
	QueryResultState state;
	while (!IsObservable(state = ExecuteTaskInternal(lock, result))) {
		if (state == QueryResultState::BLOCKED) {
			WaitForTask(lock, result);
		}
	}
	if (result.HasError()) {
		// The error is on the handle, which the caller hands out instead
		return nullptr;
	}
	auto &executor = GetExecutor();
	auto produced = executor.GetResult();
	if (executor.HasStreamingResultCollector()) {
		// The collector's own result is the open one, so it must be able to end the query it holds
		produced->context = shared_from_this();
		active_query->SetOpenResult(*produced);
	} else {
		CleanupInternal(lock, produced.get(), false);
	}
	return produced;
}

//===--------------------------------------------------------------------===//
// The statements of one query
//===--------------------------------------------------------------------===//
//! Whether a parsed statement can hand rows to the consumer. Only the types that never do answer
//! false, so a statement this does not recognise takes its schema from binding
static bool StatementCanReturnRows(const SQLStatement &statement) {
	switch (statement.type) {
	case StatementType::TRANSACTION_STATEMENT:
	case StatementType::SET_STATEMENT:
	case StatementType::ALTER_STATEMENT:
	case StatementType::CREATE_STATEMENT:
	case StatementType::DROP_STATEMENT:
	case StatementType::COPY_STATEMENT:
	case StatementType::LOAD_STATEMENT:
	case StatementType::ATTACH_STATEMENT:
	case StatementType::DETACH_STATEMENT:
		return false;
	case StatementType::INSERT_STATEMENT:
		return !statement.Cast<InsertStatement>().node->returning_list.empty();
	case StatementType::UPDATE_STATEMENT:
		return !statement.Cast<UpdateStatement>().node->returning_list.empty();
	case StatementType::DELETE_STATEMENT:
		return !statement.Cast<DeleteStatement>().node->returning_list.empty();
	case StatementType::MERGE_INTO_STATEMENT:
		return !statement.Cast<MergeIntoStatement>().node->returning_list.empty();
	default:
		return true;
	}
}

//! Whether no statement of the query can hand rows to the consumer, so the result's schema is
//! settled before anything is bound
static bool StatementsAreRowless(const vector<unique_ptr<SQLStatement>> &fragments) {
	for (auto &fragment : fragments) {
		if (StatementCanReturnRows(*fragment)) {
			return false;
		}
	}
	return true;
}

unique_ptr<QueryResult> ClientContext::StartFragmentInternal(ClientContextLock &lock,
                                                             const QueryParameters &parameters) {
	D_ASSERT(active_query && !active_query->fragment);
	auto statement = active_query->TakeNextFragment();
	active_query->query = statement->query;
	LogQueryInternal(lock, active_query->query);
	if (transaction.IsAutoCommit()) {
		transaction.BeginTransaction();
	}
	transaction.SetActiveQuery(db->GetDatabaseManager().GetNewQueryNumber());
	if (!active_query->StatesNotified()) {
		// Once per query, with the transaction the first statement runs in already open
		for (auto &state : registered_state->States()) {
			state->QueryBegin(*this);
		}
		active_query->MarkStatesNotified();
	}

	// Flush the old logger and refresh it to stay in sync with the global log settings
	logger->Flush();
	LoggingContext logging_context(LogContextScope::CONNECTION);
	logging_context.connection_id = connection_id;
	logging_context.transaction_id = transaction.ActiveTransaction().global_transaction_id;
	logging_context.query_id = transaction.GetActiveQuery();
	logger = db->GetLogManager().CreateLogger(logging_context, true);
	DUCKDB_LOG(*this, QueryLogType, active_query->query);

	if (!statement->named_param_map.empty()) {
		if (parameters.statement_args) {
			PreparedStatement::VerifyParameters(*parameters.statement_args, statement->named_param_map, this);
		} else {
			identifier_map_t<BoundParameterData> empty_parameters;
			PreparedStatement::VerifyParameters(empty_parameters, statement->named_param_map, this);
		}
	}
	auto prepared = CreatePreparedStatement(lock, std::move(statement), parameters);
	if (!prepared->properties.bound_all_parameters) {
		throw InvalidInputException("Not all parameters were bound");
	}
	CheckIfPreparedStatementIsExecutable(*prepared);

	auto &handle = active_query->GetOpenResult();
	// The result takes its rows and its schema from the statement that returns rows, or from the last
	// statement when nothing has given it a schema yet
	const bool returns_rows = prepared->properties.return_type == StatementReturnType::QUERY_RESULT;
	if (returns_rows && active_query->HasNextFragment()) {
		throw NotImplementedException("a statement that expands into a row-producing statement followed by "
		                              "others cannot be served as one result");
	}
	active_query->SetPrincipal(returns_rows || (!handle.MetadataAvailable() && !active_query->HasNextFragment()));

	auto execution = InitializeExecutionInternal(lock, *prepared, parameters);
	if (active_query->RunsPrincipal()) {
		handle.client_properties = std::move(execution.client_properties);
		handle.SetMetadata(prepared->statement_type, prepared->properties, std::move(execution.types), prepared->names);
		handle.SetBuffer(std::move(execution.buffer));
	}
	active_query->fragment->prepared = std::move(prepared);
	if (execution.delegating) {
		D_ASSERT(!active_query->HasNextFragment());
		// The collector builds its own result object: run the query and hand that object out. The
		// handle is released first, so destroying it never takes the context lock held here
		handle.context.reset();
		return CompleteDelegatedInternal(lock, handle);
	}
	return nullptr;
}

QueryResultState ClientContext::AdvanceFragmentInternal(ClientContextLock &lock) {
	D_ASSERT(active_query && active_query->fragment);
	QueryParameters parameters;
	parameters.memory_type = active_query->MemoryType();
	while (active_query->HasNextFragment()) {
		auto error = EndFragmentInternal(lock, true, false, nullptr);
		if (error.HasError()) {
			error.Throw();
		}
		// The statement that starts here runs no interrupt check of its own yet, so honour one that
		// landed on the boundary before it begins
		if (IsInterrupted()) {
			throw InterruptException();
		}
		StartFragmentInternal(lock, parameters);
		auto state = active_query->fragment->executor->Poll();
		if (state != QueryResultState::FINISHED) {
			return state;
		}
	}
	// The statement the consumer reads is the last one, and ending it is theirs
	return QueryResultState::FINISHED;
}

ResultLifetime ClientContext::SettleRetention(ClientContextLock &, ResultLifetime lifetime) {
	D_ASSERT(active_query);
	active_query->Settle(lifetime);
	auto &buffer = active_query->GetOpenResult().buffer;
	if (buffer) {
		return buffer->Decide(lifetime);
	}
	return active_query->Retention();
}

bool ClientContext::PrincipalHasStarted(ClientContextLock &) {
	D_ASSERT(active_query);
	return active_query->RunsPrincipal();
}

void ClientContext::WaitForTask(ClientContextLock &lock, BaseQueryResult &result) {
	D_ASSERT(active_query);
	D_ASSERT(active_query->fragment);
	D_ASSERT(active_query->IsOpenResult(result));
	auto &executor = *active_query->fragment->executor;
	if (executor.ExecutionIsFinished() && !executor.HasError()) {
		// Nothing to wait for: the caller's next step moves the query on
		return;
	}
	if (executor.HasTaskInProgress()) {
		// This thread is holding a partially processed task, the next step resumes it without waiting.
		return;
	}
	executor.WaitForTask();
}

bool ClientContext::ErrorInvalidatesTransaction(ExceptionType type) {
	switch (transaction.GetInvalidationPolicy()) {
	case TransactionInvalidationPolicy::STANDARD_POLICY:
	case TransactionInvalidationPolicy::ALL_ERRORS_INVALIDATE_TRANSACTION:
		return true;
	default:
		return Exception::InvalidatesTransaction(type);
	}
}

QueryResultState ClientContext::ExecuteTaskInternal(ClientContextLock &lock, BaseQueryResult &result) {
	D_ASSERT(active_query);
	D_ASSERT(active_query->IsOpenResult(result));
	try {
		// Surface a pending interrupt even when this thread runs no task that reaches InterruptCheck.
		// IsInterrupted() rather than InterruptCheck(): we must not enforce query_deadline here.
		if (IsInterrupted()) {
			throw InterruptException();
		}
		auto state = active_query->fragment->executor->ExecuteTask();
		UpdateProgressInternal(state);
		if (state == QueryResultState::FINISHED) {
			return AdvanceFragmentInternal(lock);
		}
		return state;
	} catch (std::exception &ex) {
		return FailQueryInternal(lock, result, ErrorData(ex));
	} catch (...) { // LCOV_EXCL_START
		return FailQueryInternal(lock, result, ErrorData("Unhandled exception in ExecuteTaskInternal"));
	} // LCOV_EXCL_STOP
}

QueryResultState ClientContext::PollInternal(ClientContextLock &lock, BaseQueryResult &result) {
	D_ASSERT(active_query);
	D_ASSERT(active_query->IsOpenResult(result));
	try {
		auto state = active_query->fragment->executor->Poll();
		UpdateProgressInternal(state);
		if (state == QueryResultState::FINISHED) {
			return AdvanceFragmentInternal(lock);
		}
		return state;
	} catch (std::exception &ex) {
		return FailQueryInternal(lock, result, ErrorData(ex));
	} catch (...) { // LCOV_EXCL_START
		return FailQueryInternal(lock, result, ErrorData("Unhandled exception in PollInternal"));
	} // LCOV_EXCL_STOP
}

void ClientContext::UpdateProgressInternal(QueryResultState state) {
	auto &progress_bar = active_query->fragment->progress_bar;
	if (!progress_bar) {
		return;
	}
	// todo: this is not correct for streaming results
	progress_bar->Update(IsObservable(state));
	query_progress = progress_bar->GetDetailedQueryProgress();
}

QueryResultState ClientContext::FailQueryInternal(ClientContextLock &lock, BaseQueryResult &result, ErrorData error) {
	bool invalidate_transaction = true;
	if (error.Type() == ExceptionType::INTERRUPT) {
		if (active_query->fragment && active_query->fragment->executor->HasError()) {
			// Interrupted by an exception caused in a worker thread
			error = active_query->fragment->executor->GetError();
			invalidate_transaction = ErrorInvalidatesTransaction(error.Type());
		}
	} else if (!ErrorInvalidatesTransaction(error.Type())) {
		invalidate_transaction = false;
	} else if (Exception::InvalidatesDatabase(error.Type()) || error.Type() == ExceptionType::INTERNAL) {
		// fatal exceptions invalidate the entire database
		auto &db_instance = DatabaseInstance::GetDatabase(*this);
		ValidChecker::Invalidate(db_instance, error.RawMessage());
	}
	ProcessError(error, active_query->query);
	result.SetError(std::move(error));
	EndQueryInternal(lock, false, invalidate_transaction, result.GetErrorObject());
	return QueryResultState::EXECUTION_ERROR;
}

void ClientContext::InitialCleanup(ClientContextLock &) {
	if (active_query) {
		throw ResourceInUseException(
		    "connection has an open result; drain, destroy, or interrupt it before starting a new query");
	}
	interrupt_state = ClientInterruptState::NOT_INTERRUPTED;
}

StatementIterator ClientContext::IterateStatements(const string &query) {
	// The iterator yields ready-to-execute (engine-facing) statements: PRAGMA reparse,
	// MULTI_STATEMENT unpack and transaction wrapping per peel — matches the eager API users expect.
	// Callers that want raw parse-facing statements and drive their own preprocessing construct a
	// ParseIterator directly (e.g. Query / ParseStatementsInternal below, which hold the lock).
	return StatementIterator(ParseIterator(*this, query));
}

void ClientContext::PreprocessStatements(vector<unique_ptr<SQLStatement>> &buffer,
                                         optional_ptr<ClientContextLock> lock) {
	// Acquire our own lock if the caller doesn't hold one (e.g. the shell); own_lock keeps it alive
	// for the duration of the preprocess pass.
	unique_ptr<ClientContextLock> own_lock;
	if (!lock) {
		own_lock = LockContext();
		lock = own_lock.get();
	}
	StatementPreprocessor preprocessor(*this);
	const CurrentTransactionState transaction_state =
	    transaction.HasActiveTransaction() ? IN_ACTIVE_TRANSACTION : NOT_IN_ACTIVE_TRANSACTION;
	preprocessor.Preprocess(*lock, buffer, transaction_state);
}

vector<unique_ptr<SQLStatement>> ClientContext::ParseStatementsInternal(ClientContextLock &lock, const string &query) {
	try {
		QueryProfiler::Get(*this).StartQuery(query);

		// Drain the lazy iterator into a vector for callers that want the eager shape.
		StatementIterator iterator {ParseIterator(*this, query)};
		vector<unique_ptr<SQLStatement>> result;
		while (iterator.Peek()) {
			auto stmt = iterator.GetStatementForExecutionWithLock(lock);
			if (!stmt) {
				continue; // a peel that preprocessing swallowed
			}
			result.push_back(std::move(stmt));
		}
		return result;
	} catch (std::exception &ex) {
		auto error = ErrorData(ex);
		ProcessError(error, query);
		error.Throw();
	}
}

unique_ptr<LogicalOperator> ClientContext::ExtractPlan(const string &query) {
	auto lock = LockContext();

	auto statements = ParseStatementsInternal(*lock, query);
	if (statements.size() != 1) {
		throw InvalidInputException("ExtractPlan can only prepare a single statement");
	}

	unique_ptr<LogicalOperator> plan;
	RunFunctionInTransactionInternal(*lock, [&]() {
		Planner planner(*this);
		planner.CreatePlan(std::move(statements[0]));
		D_ASSERT(planner.plan);

		plan = std::move(planner.plan);

		Optimizer optimizer(*planner.binder, *this);
		plan = optimizer.Optimize(std::move(plan));

		ColumnBindingResolver resolver;
		LogicalPlanVerifier::Verify(*this, *plan);
		resolver.VisitOperator(*plan);

		plan->ResolveOperatorTypes();
	});
	return plan;
}

//! Snapshot the metadata that the PreparedStatement handle exposes to the user
static PreparedStatementInfo GetPreparedStatementInfo(PreparedStatementData &data) {
	PreparedStatementInfo info;
	info.names = data.names;
	info.types = data.types;
	info.statement_type = data.statement_type;
	info.properties = data.properties;
	if (data.unbound_statement) {
		info.named_param_map = data.unbound_statement->named_param_map;
	}
	for (auto &entry : data.value_map) {
		LogicalType parameter_type;
		if (data.TryGetType(entry.first, parameter_type)) {
			info.parameter_types.emplace(entry.first, std::move(parameter_type));
		}
	}
	return info;
}

unique_ptr<PreparedStatement> ClientContext::PrepareInternal(ClientContextLock &lock,
                                                             unique_ptr<SQLStatement> statement) {
	auto statement_query = statement->query;
	// prepare the statement under a generated name - the returned PreparedStatement only refers to that name
	auto name = "duckdb_prepare_internal_" + UUID::ToString(UUID::GenerateRandomUUID());
	auto prepare = make_uniq<PrepareStatement>();
	prepare->name = Identifier(name);
	prepare->query = statement_query;
	prepare->stmt_location = statement->stmt_location;
	prepare->statement = std::move(statement);

	QueryParameters parameters;
	auto result = RunStatementInternal(lock, std::move(prepare), parameters, false);
	if (result->HasError()) {
		result->ThrowError();
	}
	auto entry = client_data->prepared_statements.find(Identifier(name));
	if (entry == client_data->prepared_statements.end()) {
		throw InternalException("PREPARE succeeded but the prepared statement was not registered");
	}
	return make_uniq<PreparedStatement>(shared_from_this(), std::move(name), std::move(statement_query),
	                                    GetPreparedStatementInfo(*entry->second));
}

void ClientContext::RemovePreparedStatement(const string &name) {
	auto lock = LockContext();
	client_data->prepared_statements.erase(Identifier(name));
}

unique_ptr<PreparedStatement> ClientContext::Prepare(unique_ptr<SQLStatement> statement) {
	auto lock = LockContext();
	// Store the query in case of an error.
	auto query = statement->query;

	// Try to prepare.
	try {
		InitialCleanup(*lock);
		return PrepareInternal(*lock, std::move(statement));
	} catch (std::exception &ex) {
		return ErrorResult<PreparedStatement>(ErrorData(ex), query);
	}
}

StatementSignature ClientContext::BindStatement(unique_ptr<SQLStatement> statement) {
	auto lock = LockContext();
	auto named_param_map = statement->named_param_map;
	StatementSignature signature;
	ErrorData bind_error;
	RunFunctionInTransactionInternal(
	    *lock,
	    [&]() {
		    try {
			    Planner planner(*this);
			    planner.CreatePlan(std::move(statement));
			    signature.names = planner.names;
			    signature.types = planner.types;
			    signature.properties = std::move(planner.properties);
			    // Parameter types from the bound parameter map (as in PreparedStatementData::TryGetType).
			    // An un-anchored parameter (e.g. SELECT $1) gets no value_map entry, so its
			    // type stays UNKNOWN; do not assume every parameter is present.
			    for (auto &entry : named_param_map) {
				    LogicalType type(LogicalTypeId::UNKNOWN);
				    auto it = planner.value_map.find(entry.first);
				    if (it != planner.value_map.end()) {
					    type = it->second->return_type.id() != LogicalTypeId::INVALID ? it->second->return_type
					                                                                  : it->second->GetValue().type();
				    }
				    signature.parameters.push_back({entry.first, entry.second, std::move(type)});
			    }
		    } catch (const std::exception &ex) {
			    ErrorData error(ex);
			    // Binding is read-only: a recoverable bind error changed nothing, so leave
			    // the caller's transaction intact (rethrow after the wrapper). A database-
			    // fatal error still propagates so invalidation runs as usual.
			    if (Exception::InvalidatesDatabase(error.Type())) {
				    throw;
			    }
			    bind_error = std::move(error);
		    }
	    },
	    false);
	if (bind_error.HasError()) {
		bind_error.Throw();
	}
	return signature;
}

unique_ptr<PreparedStatement> ClientContext::Prepare(const string &query) {
	auto lock = LockContext();
	// prepare the query
	try {
		InitialCleanup(*lock);

		// first parse the query
		auto statements = ParseStatementsInternal(*lock, query);
		if (statements.empty()) {
			throw InvalidInputException("No statement to prepare!");
		}
		if (statements.size() > 1) {
			throw InvalidInputException("Cannot prepare multiple statements at once!");
		}
		return PrepareInternal(*lock, std::move(statements[0]));
	} catch (std::exception &ex) {
		return ErrorResult<PreparedStatement>(ErrorData(ex), query);
	}
}

unique_ptr<QueryResult> ClientContext::RunStatementInternal(ClientContextLock &lock, unique_ptr<SQLStatement> statement,
                                                            const QueryParameters &parameters, bool verify) {
	auto query = statement->query;
	auto result = SubmitStatement(lock, std::move(statement), parameters, ResultLifetime::RETAINED, verify);
	if (!result) {
		return ErrorResult<QueryResult>(ErrorData(InvalidInputException("No statement to prepare!")), query);
	}
	if (result->HasError()) {
		return result;
	}
	return CompleteInternal(lock, std::move(result));
}

unique_ptr<QueryResult> ClientContext::CompleteInternal(ClientContextLock &lock, unique_ptr<QueryResult> result) {
	result->CompleteInternal(lock);
	return result;
}

bool ClientContext::IsActiveResult(ClientContextLock &lock, BaseQueryResult &result) {
	if (!active_query) {
		return false;
	}
	return active_query->IsOpenResult(result);
}

//! Whether this statement executes a prepared statement with parameter values supplied through the C/C++ API
static bool HasBoundParameterValues(const SQLStatement &statement) {
	if (statement.type != StatementType::EXECUTE_STATEMENT) {
		return false;
	}
	return !statement.Cast<ExecuteStatement>().bound_values.empty();
}

void ClientContext::RouteConnectedStatement(unique_ptr<SQLStatement> &statement) {
	// Parameterized prepared statements would need parameter substitution we don't do in v0 - reject.
	// No-param prepared statements have a fully-resolved `query` already, route them.
	if (HasBoundParameterValues(*statement)) {
		throw InvalidInputException("Parameterized prepared statements cannot be executed while "
		                            "CONNECT-ed; DISCONNECT first, or run the SQL as a fresh "
		                            "statement to route through the CONNECT binding");
	}
	auto live = TryGetConnectedCatalog();
	if (!live) {
		// Target was detached elsewhere; user must explicitly DISCONNECT to clear is_connected.
		throw InvalidInputException("The connected database has been detached out from under this connection. Issue "
		                            "DISCONNECT to clear the connection before running further SQL.");
	}
	// Dispatch via the catalog. Supports(CONNECT) was validated at CONNECT time, so RemoteExecute
	// is contracted to be implemented. Wrap the returned TableRef into a SelectStatement.
	auto remote_ref = live->GetCatalog().RemoteExecute(*this, statement->query);
	auto rewritten = WrapAsSelect(std::move(remote_ref));
	// the rewrite is invisible to the user - keep reporting the SQL they issued
	rewritten->query = std::move(statement->query);
	statement = std::move(rewritten);
	AttachedDatabase::InvokeCloseIfLastReference(live, *this);
}

vector<unique_ptr<SQLStatement>> ClientContext::PreprocessStatementInternal(ClientContextLock &lock,
                                                                            unique_ptr<SQLStatement> statement) {
	// CONNECT chokepoint: when connected, non-control SQL is rewritten in place and falls through to
	// the normal pipeline
	if (is_connected && !IsConnectControlStatement(statement->type)) {
		RouteConnectedStatement(statement);
	}
	vector<unique_ptr<SQLStatement>> fragments;
	fragments.push_back(std::move(statement));
	PreprocessStatements(fragments, lock);
	return fragments;
}

unique_ptr<QueryResult> ClientContext::SubmitStatement(ClientContextLock &lock, unique_ptr<SQLStatement> statement,
                                                       const QueryParameters &parameters, ResultLifetime retention,
                                                       bool verify) {
	// the statement is moved into the active query - keep the source text for error reporting
	auto query = statement->query;
	auto statement_type = statement->type;
	vector<unique_ptr<SQLStatement>> fragments;
	try {
		fragments = PreprocessStatementInternal(lock, std::move(statement));
		if (fragments.empty()) {
			return nullptr;
		}
		if (fragments.size() > 1) {
			if (parameters.statement_args && !parameters.statement_args->empty()) {
				// They would bind to the first statement, which is not the one the consumer wrote
				throw InvalidInputException("parameters are not supported for a statement that expands into several");
			}
			if (ClientConfig::GetConfig(*this).get_result_collector) {
				throw NotImplementedException(
				    "a statement that expands into several cannot be served by a custom result collector");
			}
		}
		if (verify) {
			for (auto &fragment : fragments) {
				StatementVerification(lock, fragment, parameters);
			}
		}
	} catch (std::exception &ex) {
		return ErrorResult<QueryResult>(ErrorData(ex), query);
	}

	const bool rowless = fragments.size() > 1 && StatementsAreRowless(fragments);
	auto handle = make_uniq<QueryResult>(shared_from_this(), GetClientProperties());
	try {
		BeginQueryInternal(lock, std::move(fragments));
	} catch (std::exception &ex) {
		ErrorData error(ex);
		if (Exception::InvalidatesDatabase(error.Type())) {
			// fatal exceptions invalidate the entire database
			auto &db_instance = DatabaseInstance::GetDatabase(*this);
			ValidChecker::Invalidate(db_instance, error.RawMessage());
		}
		// Released before it is destroyed, so destroying it never takes the context lock held here
		handle->context.reset();
		return ErrorResult<QueryResult>(std::move(error), query);
	}
	active_query->SetOpenResult(*handle);
	active_query->Settle(retention);
	active_query->SetMemoryType(parameters.memory_type);
	if (rowless) {
		// No statement of this query returns rows, so the result's schema is settled before it runs
		StatementProperties properties;
		properties.return_type = StatementReturnType::NOTHING;
		handle->SetMetadata(statement_type, std::move(properties), vector<LogicalType>(), vector<Identifier>());
	}

	unique_ptr<QueryResult> failure;
	bool invalidate_query = true;
	try {
		auto produced = StartFragmentInternal(lock, parameters);
		if (produced) {
			return produced;
		}
	} catch (std::exception &ex) {
		ErrorData error(ex);
		if (!ErrorInvalidatesTransaction(error.Type())) {
			// standard exceptions do not invalidate the current transaction
			invalidate_query = false;
		} else if (Exception::InvalidatesDatabase(error.Type())) {
			// fatal exceptions invalidate the entire database
			auto &db_instance = DatabaseInstance::GetDatabase(*this);
			ValidChecker::Invalidate(db_instance, error.RawMessage());
		}
		// other types of exceptions do invalidate the current transaction
		failure = ErrorResult<QueryResult>(std::move(error), query);
	}
	if (failure) {
		// query failed: abort now
		EndQueryInternal(lock, false, invalidate_query, failure->GetErrorObject());
		// Released before it is destroyed, so destroying it never takes the context lock held here
		handle->context.reset();
		return failure;
	}
	// A collector that builds its own result object finishes the query inside the submission, so
	// the query it belonged to may already be gone
	D_ASSERT(!active_query || active_query->IsOpenResult(*handle));
	return handle;
}

void ClientContext::LogQueryInternal(ClientContextLock &, const string &query) {
	if (!client_data->log_query_writer) {
#ifdef DUCKDB_FORCE_QUERY_LOG
		try {
			string log_path(DUCKDB_FORCE_QUERY_LOG);
			client_data->log_query_writer = make_uniq<BufferedFileWriter>(FileSystem::GetFileSystem(*this), log_path,
			                                                              BufferedFileWriter::DEFAULT_OPEN_FLAGS);
		} catch (...) {
			return;
		}
#else
		return;
#endif
	}
	// log query path is set: log the query
	client_data->log_query_writer->WriteData(const_data_ptr_cast(query.c_str()), query.size());
	client_data->log_query_writer->WriteData(const_data_ptr_cast("\n"), 1);
	client_data->log_query_writer->Flush();
	client_data->log_query_writer->Sync();
}

unique_ptr<QueryResult> ClientContext::Query(unique_ptr<SQLStatement> statement, QueryParameters parameters) {
	auto lock = LockContext();
	auto query = statement->query;
	try {
		InitialCleanup(*lock);
	} catch (std::exception &ex) {
		return ErrorResult<QueryResult>(ErrorData(ex), query);
	}
	auto result = SubmitStatement(*lock, std::move(statement), parameters, ResultLifetime::RETAINED, true);
	if (!result) {
		return ErrorResult<QueryResult>(ErrorData(InvalidInputException("No statement to prepare!")), query);
	}
	if (result->HasError()) {
		if (transaction.HasActiveTransaction() && transaction.GetAutoRollback()) {
			transaction.Rollback(result->GetErrorObject());
		}
		return result;
	}
	return CompleteInternal(*lock, std::move(result));
}

unique_ptr<QueryResult> ClientContext::Query(const string &query, QueryParameters query_parameters) {
	auto lock = LockContext();
	try {
		InitialCleanup(*lock);
	} catch (std::exception &ex) {
		return ErrorResult<QueryResult>(ErrorData(ex), query);
	}
	auto &profiler = QueryProfiler::Get(*this);
	profiler.StartQuery(query);
	// ParseIterator's constructor runs UTF-8 validation / Unicode-space strip and can throw — route
	// through ErrorResult so the source location attaches the same way Peek failures do.
	optional_ptr<ParseIterator> iterator_ptr;
	unique_ptr<ParseIterator> iterator_storage;
	try {
		iterator_storage = make_uniq<ParseIterator>(*this, query);
		iterator_ptr = *iterator_storage;
	} catch (const std::exception &ex) {
		return ErrorResult<QueryResult>(ErrorData(ex), query);
	}
	auto &iterator = *iterator_ptr;

	Profiler parser_timer;
	auto peek_or_error = [&](bool &has_now) -> unique_ptr<QueryResult> {
		try {
			parser_timer.Start();
			has_now = iterator.Peek();
			parser_timer.End();
			return nullptr;
		} catch (const std::exception &ex) {
			return ErrorResult<QueryResult>(ErrorData(ex), query);
		}
	};

	bool has_current = false;
	if (auto error = peek_or_error(has_current)) {
		return error;
	}
	if (!has_current) {
		// no statements, return empty successful result
		StatementProperties properties;
		vector<Identifier> names;
		auto collection = make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator());
		return make_uniq<QueryResult>(StatementType::INVALID_STATEMENT, properties, std::move(names),
		                              std::move(collection), GetClientProperties());
	}

	unique_ptr<QueryResult> result;
	optional_ptr<QueryResult> last_result;
	bool last_had_result = false;
	while (has_current) {
		auto statement = iterator.GetStatement();
		profiler.StartQuery(statement->query, IsExplainAnalyze(statement.get()));
		profiler.AddParserTime(parser_timer);

		// Look ahead WITHOUT parsing: HasMore() only walks the token cursor, so it never parses (and
		// never throws) the next statement here. The next statement is parsed later, in this loop's
		// next Peek, after the current statement has executed. This lets a statement register grammar
		// (e.g. LOAD an extension) that a following statement then uses.
		bool has_next = iterator.HasMore();

		if (has_next && query_parameters.statement_args && !query_parameters.statement_args->empty()) {
			return ErrorResult<QueryResult>(
			    ErrorData(InvalidInputException("Cannot prepare multiple statements at once!")), query);
		}
		// Every statement of an eager query completes before the call returns, so none of them
		// leaves a producer parked for the consumer's choice. A statement can preprocess to nothing,
		// in which case there is nothing to execute
		auto current_result =
		    SubmitStatement(*lock, std::move(statement), query_parameters, ResultLifetime::RETAINED, true);
		if (current_result) {
			if (!current_result->HasError()) {
				current_result = CompleteInternal(*lock, std::move(current_result));
			}
			if (current_result->HasError()) {
				if (transaction.HasActiveTransaction() && transaction.GetAutoRollback()) {
					transaction.Rollback(current_result->GetErrorObject());
				}
				// Reset the interrupted flag, this was set by the task that found the error
				// Next statements should not be bothered by that interruption
				interrupt_state = ClientInterruptState::NOT_INTERRUPTED;
				return current_result;
			}
			auto has_result = current_result->GetStatementProperties().return_type == StatementReturnType::QUERY_RESULT;
			// now append the result to the list of results
			if (!last_result || !last_had_result) {
				// first result of the query
				result = std::move(current_result);
				last_result = result.get();
				last_had_result = has_result;
			} else if (has_result) {
				// later results; attach to the result chain, but only if there is a result
				last_result->next = std::move(current_result);
				last_result = last_result->next.get();
			}
			D_ASSERT(last_result);
		}

		if (!has_next) {
			break;
		}
		if (auto error = peek_or_error(has_current)) {
			return error;
		}
	}
	return result;
}

unique_ptr<QueryResult> ClientContext::Submit(const string &query, const QueryParameters &parameters) {
	auto lock = LockContext();
	try {
		InitialCleanup(*lock);
		auto &profiler = QueryProfiler::Get(*this);
		profiler.StartQuery(query);

		// Parsed without preprocessing: one user statement is one submission even when it expands
		Profiler parser_timer;
		parser_timer.Start();
		ParseIterator parse_iterator(*this, query);
		const bool has_statement = parse_iterator.Peek();
		parser_timer.End();
		if (!has_statement) {
			throw InvalidInputException("No statement to prepare!");
		}
		auto statement = parse_iterator.GetStatement();
		if (parse_iterator.HasMore()) {
			throw InvalidInputException("Cannot prepare multiple statements at once!");
		}
		profiler.AddParserTime(parser_timer);
		auto result = SubmitStatement(*lock, std::move(statement), parameters, ResultLifetime::UNDECIDED, true);
		if (!result) {
			throw InvalidInputException("No statement to prepare!");
		}
		return result;
	} catch (std::exception &ex) {
		ErrorData error(ex);
		ProcessError(error, query);
		return make_uniq<QueryResult>(std::move(error));
	}
}

unique_ptr<QueryResult> ClientContext::Submit(unique_ptr<SQLStatement> statement, const QueryParameters &parameters) {
	auto lock = LockContext();
	auto query = statement->query;
	try {
		InitialCleanup(*lock);

		auto result = SubmitStatement(*lock, std::move(statement), parameters, ResultLifetime::UNDECIDED, true);
		if (!result) {
			throw InvalidInputException("No statement to prepare!");
		}
		return result;
	} catch (std::exception &ex) {
		return make_uniq<QueryResult>(ErrorData(ex));
	}
}

unique_ptr<QueryResult> ClientContext::Submit(const string &query, identifier_map_t<BoundParameterData> &values,
                                              QueryParameters parameters) {
	parameters.statement_args = values;
	return Submit(query, parameters);
}

unique_ptr<QueryResult> ClientContext::Submit(unique_ptr<SQLStatement> statement,
                                              identifier_map_t<BoundParameterData> &values,
                                              QueryParameters parameters) {
	parameters.statement_args = values;
	return Submit(std::move(statement), parameters);
}

unique_ptr<QueryResult> ClientContext::RunInternalStatement(unique_ptr<SQLStatement> statement,
                                                            const QueryParameters &parameters) {
	auto lock = LockContext();
	try {
		InitialCleanup(*lock);
	} catch (std::exception &ex) {
		return ErrorResult<QueryResult>(ErrorData(ex), statement->query);
	}
	return RunStatementInternal(*lock, std::move(statement), parameters, false);
}

unique_ptr<QueryResult> ClientContext::SubmitInternalStatement(unique_ptr<SQLStatement> statement,
                                                               const QueryParameters &parameters) {
	auto lock = LockContext();
	auto query = statement->query;
	try {
		InitialCleanup(*lock);
	} catch (std::exception &ex) {
		return ErrorResult<QueryResult>(ErrorData(ex), query);
	}
	auto result = SubmitStatement(*lock, std::move(statement), parameters, ResultLifetime::UNDECIDED, false);
	if (!result) {
		return ErrorResult<QueryResult>(ErrorData(InvalidInputException("No statement to prepare!")), query);
	}
	return result;
}

void ClientContext::Interrupt() {
	ClientInterruptState expected = ClientInterruptState::NOT_INTERRUPTED;
	interrupt_state.compare_exchange_strong(expected, ClientInterruptState::INTERRUPTED);
}

bool ClientContext::IsInterrupted() const {
	return interrupt_state.load(std::memory_order_relaxed) == ClientInterruptState::INTERRUPTED;
}

void ClientContext::ClearInterrupt() {
	interrupt_state = ClientInterruptState::NOT_INTERRUPTED;
}

void ClientContext::SuppressInterrupts() {
	interrupt_state = ClientInterruptState::INTERRUPTS_SUPPRESSED;
}

void ClientContext::InterruptCheck() const {
	// Counter for throttling timeout checks - only check every N iterations
	static constexpr uint32_t TIMEOUT_CHECK_INTERVAL = 256;
	thread_local uint32_t timeout_check_counter = 0;

	if (interrupt_state.load(std::memory_order_relaxed) == ClientInterruptState::INTERRUPTED) {
		throw InterruptException();
	}
	// Only check timeout every N calls to avoid expensive steady_clock::now() syscall
	if (query_deadline.IsValid() && ++timeout_check_counter % TIMEOUT_CHECK_INTERVAL == 0) {
		auto now = NumericCast<idx_t>(duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count());
		if (now >= query_deadline.GetIndex()) {
			throw InterruptException("Query exceeded maximum execution time");
		}
	}
}

void ClientContext::CancelTransaction() {
	auto lock = LockContext();
	CleanupInternal(*lock);
	interrupt_state = ClientInterruptState::NOT_INTERRUPTED;
}

void ClientContext::EnableProfiling() {
	auto lock = LockContext();
	auto &client_config = ClientConfig::GetConfig(*this);
	client_config.enable_profiler = true;
}

void ClientContext::DisableProfiling() {
	auto lock = LockContext();
	auto &client_config = ClientConfig::GetConfig(*this);
	client_config.enable_profiler = false;
}

void ClientContext::RegisterFunction(CreateFunctionInfo &info) {
	RunFunctionInTransaction([&]() {
		auto existing_function = Catalog::GetEntry<ScalarFunctionCatalogEntry>(
		    *this,
		    QualifiedName(Identifier::InvalidCatalog(), info.GetQualifiedName().Schema(), info.GetFunctionName()),
		    OnEntryNotFound::RETURN_NULL);
		if (existing_function) {
			auto &new_info = info.Cast<CreateScalarFunctionInfo>();
			if (new_info.functions.MergeFunctionSet(existing_function->functions)) {
				// function info was updated from catalog entry, rewrite is needed
				info.on_conflict = OnCreateConflict::REPLACE_ON_CONFLICT;
			}
		}
		// create function
		auto &catalog = Catalog::GetSystemCatalog(*this);
		catalog.CreateFunction(*this, info);
	});
}

void ClientContext::RunTransactionStatementInternal(const TransactionInfo &info) {
	auto type = info.type;
	if (type == TransactionType::COMMIT && transaction.HasActiveTransaction() &&
	    ValidChecker::IsInvalidated(ActiveTransaction())) {
		// transaction is invalidated - turn COMMIT into ROLLBACK
		type = TransactionType::ROLLBACK;
	}
	switch (type) {
	case TransactionType::BEGIN_TRANSACTION: {
		if (!transaction.IsAutoCommit()) {
			throw TransactionException("cannot start a transaction within a transaction");
		}
		transaction.SetAutoCommit(false);
		if (info.modifier == TransactionModifierType::TRANSACTION_READ_ONLY) {
			transaction.SetReadOnly();
		}
		transaction.SetInvalidationPolicy(info.invalidation_policy);
		transaction.SetAutoRollback(info.auto_rollback);
		if (Settings::Get<ImmediateTransactionModeSetting>(*this)) {
			auto databases = DatabaseManager::Get(*this).GetDatabases(*this);
			for (auto &attached : databases) {
				if (ValidChecker::IsInvalidated(*attached)) {
					continue;
				}
				transaction.ActiveTransaction().GetTransaction(*attached);
			}
		}
		break;
	}
	case TransactionType::COMMIT:
		if (transaction.IsAutoCommit()) {
			throw TransactionException("cannot commit - no transaction is active");
		}
		transaction.Commit();
		// The commit is irreversible, so ignore interrupts until the next query.
		SuppressInterrupts();
		break;
	case TransactionType::ROLLBACK: {
		if (transaction.IsAutoCommit()) {
			throw TransactionException("cannot rollback - no transaction is active");
		}
		auto &valid_checker = ValidChecker::Get(transaction.ActiveTransaction());
		if (valid_checker.IsInvalidated()) {
			ErrorData error(ExceptionType::TRANSACTION, valid_checker.InvalidatedMessage());
			transaction.Rollback(error);
		} else {
			transaction.Rollback(nullptr);
		}
		break;
	}
	default:
		throw NotImplementedException("Unrecognized transaction type!");
	}
}

void ClientContext::RunTransactionStatement(const TransactionInfo &info) {
	auto lock = LockContext();
	InitialCleanup(*lock);
	if (is_connected) {
		auto statement = make_uniq<TransactionStatement>(info.Copy());
		statement->query = info.ToString();
		QueryParameters parameters;
		auto result = RunStatementInternal(*lock, std::move(statement), parameters);
		if (result->HasError()) {
			if (transaction.HasActiveTransaction() && transaction.GetAutoRollback()) {
				transaction.Rollback(result->GetErrorObject());
			}
			ClearInterrupt();
			result->ThrowError();
		}
		return;
	}
	auto &db_instance = DatabaseInstance::GetDatabase(*this);
	if (ValidChecker::IsInvalidated(db_instance)) {
		throw ErrorManager::InvalidatedDatabase(*this, ValidChecker::InvalidatedMessage(db_instance));
	}
	RunTransactionStatementInternal(info);
}

void ClientContext::RunFunctionInTransactionInternal(ClientContextLock &lock, const std::function<void(void)> &fun,
                                                     bool requires_valid_transaction) {
	if (requires_valid_transaction && transaction.HasActiveTransaction() &&
	    ValidChecker::IsInvalidated(ActiveTransaction())) {
		throw TransactionException(ErrorManager::FormatException(*this, ErrorType::INVALIDATED_TRANSACTION));
	}

	// check if we are on AutoCommit. In this case we should start a transaction
	bool require_new_transaction = transaction.IsAutoCommit() && !transaction.HasActiveTransaction();
	if (require_new_transaction) {
		D_ASSERT(!active_query);
		transaction.BeginTransaction();
		interrupt_state = ClientInterruptState::NOT_INTERRUPTED;
	}
	try {
		fun();
	} catch (std::exception &ex) {
		ErrorData error(ex);
		bool invalidates_transaction = true;
		if (!ErrorInvalidatesTransaction(error.Type())) {
			// standard exceptions don't invalidate the transaction
			invalidates_transaction = false;
		} else if (Exception::InvalidatesDatabase(error.Type())) {
			auto &db_instance = DatabaseInstance::GetDatabase(*this);
			ValidChecker::Invalidate(db_instance, error.RawMessage());
		}
		if (require_new_transaction) {
			transaction.Rollback(error);
		} else if (invalidates_transaction) {
			ValidChecker::Invalidate(ActiveTransaction(), error.RawMessage());
		}
		throw;
	}
	if (require_new_transaction) {
		transaction.Commit();
	}
}

void ClientContext::RunFunctionInTransaction(const std::function<void(void)> &fun, bool requires_valid_transaction) {
	auto lock = LockContext();
	RunFunctionInTransactionInternal(*lock, fun, requires_valid_transaction);
}

unique_ptr<TableDescription> ClientContext::TableInfo(const Identifier &database_name, const Identifier &schema_name,
                                                      const Identifier &table_name) {
	unique_ptr<TableDescription> result;
	RunFunctionInTransaction([&]() {
		// Obtain the table from the catalog.
		auto table = Catalog::GetEntry<TableCatalogEntry>(*this, QualifiedName(database_name, schema_name, table_name),
		                                                  OnEntryNotFound::RETURN_NULL);
		if (!table) {
			return;
		}
		// Describe the table at its resolved location, not the input identifiers.
		auto &catalog = table->ParentCatalog();
		result = make_uniq<TableDescription>(QualifiedName(catalog.GetName(), table->ParentSchema().name, table->name));
		result->readonly = catalog.GetAttached().IsReadOnly();
		for (auto &column : table->GetColumns().Logical()) {
			result->columns.emplace_back(column.Copy());
		}
	});
	return result;
}

unique_ptr<TableDescription> ClientContext::TableInfo(const Identifier &schema_name, const Identifier &table_name) {
	return TableInfo(Identifier::InvalidCatalog(), schema_name, table_name);
}

void ClientContext::Append(unique_ptr<SQLStatement> stmt) {
	auto result = Query(std::move(stmt), QueryParameters());
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to append: ");
	}
}

void ClientContext::Append(TableDescription &description, ColumnDataCollection &collection) {
	Identifier table_name("__duckdb_internal_appended_data");
	vector<Identifier> expected_names;
	auto query = Appender::ConstructQuery(description, table_name, expected_names);
	auto table_ref = BaseAppender::GetColumnDataTableRef(collection, table_name, expected_names);
	auto stmt = BaseAppender::ParseStatement(std::move(table_ref), query, table_name.GetIdentifierName());
	Append(std::move(stmt));
}

void ClientContext::InternalTryBindRelation(Relation &relation, vector<ColumnDefinition> &result_columns) {
	// bind the expressions
	auto binder = Binder::CreateBinder(*this);
	auto result = relation.Bind(*binder);
	D_ASSERT(result.names.size() == result.types.size());

	result_columns.reserve(result_columns.size() + result.names.size());
	for (idx_t i = 0; i < result.names.size(); i++) {
		result_columns.emplace_back(result.names[i], result.types[i]);
	}
}

void ClientContext::TryBindRelation(Relation &relation, vector<ColumnDefinition> &result_columns) {
#ifdef DEBUG
	D_ASSERT(!relation.GetAlias().empty());
	D_ASSERT(!relation.ToString().empty());
#endif
	RunFunctionInTransaction([&]() { InternalTryBindRelation(relation, result_columns); });
}

unordered_set<string> ClientContext::GetTableNames(const string &query, const bool qualified) {
	auto lock = LockContext();

	// Preprocess before binding so PRAGMA reparse / macro expansion happens up front — GetTableNames
	// extracts names from the *underlying* query (e.g. `PRAGMA tpch(1)` -> the TPC-H SELECT, whose
	// tables are what the caller wants). A raw, un-preprocessed PRAGMA would never surface them.
	auto statements = ParseStatementsInternal(*lock, query);
	if (statements.size() != 1) {
		throw InvalidInputException("Expected a single statement");
	}

	unordered_set<string> result;
	RunFunctionInTransactionInternal(*lock, [&]() {
		// bind the expressions
		auto binder = Binder::CreateBinder(*this);
		auto mode = qualified ? BindingMode::EXTRACT_QUALIFIED_NAMES : BindingMode::EXTRACT_NAMES;
		binder->SetBindingMode(mode);
		binder->Bind(*statements[0]);
		result = binder->GetTableNames();
	});
	return result;
}

unique_ptr<QueryResult> ClientContext::SubmitInternal(ClientContextLock &lock, const shared_ptr<Relation> &relation,
                                                      const QueryParameters &query_parameters) {
	try {
		InitialCleanup(lock);
	} catch (std::exception &ex) {
		return ErrorResult<QueryResult>(ErrorData(ex), relation->ToString());
	}

#ifdef DEBUG
	// run the ToString method of any relation we run, mostly to ensure it doesn't crash
	relation->ToString();
	relation->GetAlias();
#endif

	auto relation_stmt = make_uniq<RelationStatement>(relation);
	auto result = SubmitStatement(lock, std::move(relation_stmt), query_parameters, ResultLifetime::UNDECIDED, true);
	D_ASSERT(result);
	return result;
}

unique_ptr<QueryResult> ClientContext::Submit(const shared_ptr<Relation> &relation,
                                              const QueryParameters &query_parameters) {
	auto lock = LockContext();
	return SubmitInternal(*lock, relation, query_parameters);
}

unique_ptr<QueryResult> ClientContext::Execute(const shared_ptr<Relation> &relation) {
	auto lock = LockContext();
	auto &expected_columns = relation->Columns();
	try {
		InitialCleanup(*lock);
	} catch (std::exception &ex) {
		return ErrorResult<QueryResult>(ErrorData(ex), relation->ToString());
	}

	auto relation_stmt = make_uniq<RelationStatement>(relation);
	QueryParameters parameters;
	auto result = SubmitStatement(*lock, std::move(relation_stmt), parameters, ResultLifetime::RETAINED, true);
	D_ASSERT(result);
	if (result->HasError()) {
		return result;
	}
	result = CompleteInternal(*lock, std::move(result));
	if (result->HasError()) {
		return result;
	}
	// verify that the result types and result names of the query match the expected result types/names
	if (result->GetTypes().size() == expected_columns.size()) {
		bool mismatch = false;
		for (idx_t i = 0; i < result->GetTypes().size(); i++) {
			if (result->GetTypes()[i] != expected_columns[i].Type() ||
			    result->ColumnName(i) != expected_columns[i].Name()) {
				mismatch = true;
				break;
			}
		}
		if (!mismatch) {
			// all is as expected: return the result
			return result;
		}
	}
	// result mismatch
	string err_str = "Result mismatch in query!\nExpected the following columns: [";
	for (idx_t i = 0; i < expected_columns.size(); i++) {
		if (i > 0) {
			err_str += ", ";
		}
		err_str += expected_columns[i].Name() + " " + expected_columns[i].Type().ToString();
	}
	err_str += "]\nBut result contained the following: ";
	for (idx_t i = 0; i < result->GetTypes().size(); i++) {
		err_str += i == 0 ? "[" : ", ";
		err_str += result->ColumnName(i) + " " + result->GetTypes()[i].ToString();
	}
	err_str += "]";
	return ErrorResult<QueryResult>(ErrorData(err_str));
}

SettingLookupResult ClientContext::TryGetCurrentSetting(const Identifier &key, Value &result) const {
	optional_ptr<const ConfigurationOption> option;
	// try to get the setting index
	auto &db_config = DBConfig::GetConfig(*this);
	auto setting_index = db_config.TryGetSettingIndex(key, option);
	if (setting_index.IsValid()) {
		// generic setting - try to fetch it
		auto lookup_result =
		    config.user_settings.TryGetSetting(db_config.user_settings, setting_index.GetIndex(), result);
		if (lookup_result) {
			return lookup_result;
		}
	}
	if (option && option->get_setting) {
		// legacy callback
		result = option->get_setting(*this);
		return SettingLookupResult(SettingScope::LOCAL);
	}
	// setting is not set - get the default value
	return DBConfig::TryGetDefaultValue(option, result);
}

SettingLookupResult ClientContext::TryGetCurrentUserSetting(idx_t setting_index, Value &result) const {
	auto &db_config = DBConfig::GetConfig(*this);
	return config.user_settings.TryGetSetting(db_config.user_settings, setting_index, result);
}

ParserOptions ClientContext::GetParserOptions() {
	ParserOptions options;
	options.identifier_case_mode = Settings::Get<PreserveIdentifierCaseSetting>(*this);
	options.integer_division = Settings::Get<IntegerDivisionSetting>(*this);
	options.regex_match_operator_semantics = Settings::Get<RegexMatchOperatorSemanticsSetting>(*this);
	options.max_expression_depth = Settings::Get<MaxExpressionDepthSetting>(*this);
	options.extensions = DBConfig::GetConfig(*this).GetCallbackManager();
	options.parser_override_setting = Settings::Get<AllowParserOverrideExtensionSetting>(*this);
	options.compiled_grammar = CompiledGrammar::Get(*this);
	return options;
}

ClientProperties ClientContext::GetClientProperties() {
	string timezone = "UTC";
	Value result;

	if (TryGetCurrentSetting("TimeZone", result)) {
		timezone = result.ToString();
	}
	ArrowOffsetSize arrow_offset_size = ArrowOffsetSize::REGULAR;
	if (Settings::Get<ArrowLargeBufferSizeSetting>(*this)) {
		arrow_offset_size = ArrowOffsetSize::LARGE;
	}
	bool arrow_use_list_view = Settings::Get<ArrowOutputListViewSetting>(*this);
	bool arrow_lossless_conversion = Settings::Get<ArrowLosslessConversionSetting>(*this);
	bool arrow_use_string_view = Settings::Get<ProduceArrowStringViewSetting>(*this);
	auto arrow_format_version = Settings::Get<ArrowOutputVersionSetting>(*this);
	return {timezone,
	        arrow_offset_size,
	        arrow_use_list_view,
	        arrow_use_string_view,
	        arrow_lossless_conversion,
	        arrow_format_version,
	        this};
}

bool ClientContext::ExecutionIsFinished() {
	if (!active_query || !active_query->fragment || !active_query->fragment->executor) {
		return false;
	}
	return !active_query->HasNextFragment() && active_query->fragment->executor->ExecutionIsFinished();
}

LogicalType ClientContext::ParseLogicalType(const string &type) {
	auto lock = LockContext();
	LogicalType logical_type;
	RunFunctionInTransactionInternal(*lock,
	                                 [&]() { logical_type = TypeManager::Get(*db).ParseLogicalType(type, *this); });
	return logical_type;
}

} // namespace duckdb
