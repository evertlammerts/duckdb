#include "duckdb/main/query_result.hpp"

#include "duckdb/common/box_renderer.hpp"
#include "duckdb/common/column_data_collection_render_interface.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/main/buffered_data/buffered_data.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

BaseQueryResult::BaseQueryResult(QueryResultType type, StatementType statement_type, StatementProperties properties_p,
                                 vector<LogicalType> types_p, vector<Identifier> names_p)
    : type(type), statement_type(statement_type), properties(std::move(properties_p)), types(std::move(types_p)),
      names(std::move(names_p)), metadata_available(true), success(true) {
	D_ASSERT(types.size() == names.size());
}

BaseQueryResult::BaseQueryResult(QueryResultType type, ErrorData error)
    : type(type), metadata_available(true), success(false), error(std::move(error)) {
	// Assert that the error object is initialized
	D_ASSERT(this->error.HasError());
}

BaseQueryResult::BaseQueryResult(QueryResultType type)
    : type(type), statement_type(StatementType::INVALID_STATEMENT), metadata_available(false), success(true) {
}

BaseQueryResult::~BaseQueryResult() {
}

void BaseQueryResult::ThrowError(const string &prepended_message) const {
	D_ASSERT(HasError());
	error.Throw(prepended_message);
}

void BaseQueryResult::SetError(ErrorData error) {
	success = !error.HasError();
	this->error = std::move(error);
}

bool BaseQueryResult::HasError() const {
	D_ASSERT(error.HasError() == !success);
	return !success;
}

const ExceptionType &BaseQueryResult::GetErrorType() const {
	return error.Type();
}

const std::string &BaseQueryResult::GetError() const {
	D_ASSERT(HasError());
	return error.Message();
}

ErrorData &BaseQueryResult::GetErrorObject() {
	return error;
}

const ErrorData &BaseQueryResult::GetErrorObject() const {
	return error;
}

idx_t BaseQueryResult::ColumnCount() const {
	RequireMetadata();
	return types.size();
}

bool BaseQueryResult::MetadataAvailable() const {
	return metadata_available;
}

void BaseQueryResult::ThrowMetadataUnavailable() const {
	throw InvalidInputException("The metadata of this query result is not available yet: the statement expands into "
	                            "several, and the one the result takes its schema from has not been prepared");
}

optional_ptr<const StatementType> BaseQueryResult::TryGetStatementType() const {
	return metadata_available ? optional_ptr<const StatementType>(statement_type) : nullptr;
}

optional_ptr<const StatementProperties> BaseQueryResult::TryGetStatementProperties() const {
	return metadata_available ? optional_ptr<const StatementProperties>(properties) : nullptr;
}

optional_ptr<const vector<LogicalType>> BaseQueryResult::TryGetTypes() const {
	return metadata_available ? optional_ptr<const vector<LogicalType>>(types) : nullptr;
}

optional_ptr<const vector<Identifier>> BaseQueryResult::TryGetNames() const {
	return metadata_available ? optional_ptr<const vector<Identifier>>(names) : nullptr;
}

optional_idx BaseQueryResult::TryColumnCount() const {
	return metadata_available ? optional_idx(types.size()) : optional_idx();
}

void BaseQueryResult::SetMetadata(StatementType statement_type_p, StatementProperties properties_p,
                                  vector<LogicalType> types_p, vector<Identifier> names_p) {
	D_ASSERT(types_p.size() == names_p.size());
	statement_type = statement_type_p;
	properties = std::move(properties_p);
	types = std::move(types_p);
	names = std::move(names_p);
	metadata_available = true;
}

QueryResultType BaseQueryResult::GetResultType() const {
	return type;
}

StatementType BaseQueryResult::GetStatementType() const {
	RequireMetadata();
	return statement_type;
}

const StatementProperties &BaseQueryResult::GetStatementProperties() const {
	RequireMetadata();
	return properties;
}

const vector<LogicalType> &BaseQueryResult::GetTypes() const {
	RequireMetadata();
	return types;
}

const vector<Identifier> &BaseQueryResult::GetNames() const {
	RequireMetadata();
	return names;
}

//===--------------------------------------------------------------------===//
// Construction
//===--------------------------------------------------------------------===//
QueryResult::QueryResult(QueryResultType type, StatementType statement_type, StatementProperties properties,
                         vector<LogicalType> types_p, vector<Identifier> names_p, ClientProperties client_properties_p)
    : BaseQueryResult(type, statement_type, std::move(properties), std::move(types_p), std::move(names_p)),
      client_properties(std::move(client_properties_p)) {
}

QueryResult::QueryResult(QueryResultType type, ErrorData error)
    : BaseQueryResult(type, std::move(error)),
      client_properties("UTC", ArrowOffsetSize::REGULAR, false, false, false, ArrowFormatVersion::V1_0, nullptr) {
}

QueryResult::QueryResult(shared_ptr<ClientContext> context_p, ClientProperties client_properties_p)
    : BaseQueryResult(QueryResultType::MATERIALIZED_RESULT), client_properties(std::move(client_properties_p)),
      context(std::move(context_p)) {
}

QueryResult::QueryResult(StatementType statement_type, StatementProperties properties, vector<Identifier> names_p,
                         unique_ptr<ColumnDataCollection> collection_p, ClientProperties client_properties_p)
    : BaseQueryResult(QueryResultType::MATERIALIZED_RESULT, statement_type, std::move(properties),
                      collection_p->Types(), std::move(names_p)),
      client_properties(std::move(client_properties_p)), collection(std::move(collection_p)) {
}

QueryResult::QueryResult(ErrorData error) : QueryResult(QueryResultType::MATERIALIZED_RESULT, std::move(error)) {
}

QueryResult::~QueryResult() {
	Close();
}

void QueryResult::DeduplicateColumns(vector<string> &names) {
	auto identifiers = StringsToIdentifiers(names);
	DeduplicateColumns(identifiers);
	names = IdentifiersToStrings(identifiers);
}

void QueryResult::DeduplicateColumns(vector<Identifier> &names) {
	identifier_map_t<idx_t> name_map;
	for (auto &column_name : names) {
		if (name_map.find(column_name) == name_map.end()) {
			// Name does not exist yet
			name_map[column_name]++;
		} else {
			// Name already exists, we add _x where x is the repetition number
			Identifier new_column_name(column_name + "_" + std::to_string(name_map[column_name]));
			while (name_map.find(new_column_name) != name_map.end()) {
				// This name is already here due to a previous definition
				name_map[column_name]++;
				new_column_name = Identifier(column_name + "_" + std::to_string(name_map[column_name]));
			}
			column_name = new_column_name;
			name_map[new_column_name]++;
		}
	}
}

const Identifier &QueryResult::ColumnName(idx_t index) const {
	auto &names = GetNames();
	D_ASSERT(index < names.size());
	return names[index];
}

//===--------------------------------------------------------------------===//
// Execution
//===--------------------------------------------------------------------===//
unique_ptr<ClientContextLock> QueryResult::LockContext() {
	if (!context) {
		string error_str = "Attempting to execute an unsuccessful or closed query result";
		if (HasError()) {
			error_str += StringUtil::Format("\nError: %s", GetError());
		}
		throw InvalidInputException(error_str);
	}
	return context->LockContext();
}

bool QueryResult::IsOpenInternal(ClientContextLock &lock) {
	if (HasError() || !context) {
		return false;
	}
	return context->IsActiveResult(lock, *this);
}

void QueryResult::CheckExecutableInternal(ClientContextLock &lock) {
	if (!IsOpenInternal(lock)) {
		string error_str = "Attempting to execute an unsuccessful or closed query result";
		if (HasError()) {
			error_str += StringUtil::Format("\nError: %s", GetError());
		}
		throw InvalidInputException(error_str);
	}
}

bool QueryResult::IsOpen() {
	if (HasError() || !context) {
		return false;
	}
	auto lock = LockContext();
	return IsOpenInternal(*lock);
}

QueryResultState QueryResult::Cancelled() {
	if (!HasError()) {
		SetError(ErrorData(ExceptionType::INTERRUPT,
		                   "The execution of the query was cancelled before it could finish, likely caused by "
		                   "executing a different query"));
	}
	return QueryResultState::EXECUTION_ERROR;
}

QueryResultState QueryResult::Poll() {
	if (HasError()) {
		return QueryResultState::EXECUTION_ERROR;
	}
	if (collection || !context) {
		// The result was collected, or the query already ended: keep reporting the terminal state
		return QueryResultState::FINISHED;
	}
	auto lock = LockContext();
	if (!IsOpenInternal(*lock)) {
		return Cancelled();
	}
	return context->PollInternal(*lock, *this);
}

QueryResultState QueryResult::ExecuteTask() {
	auto lock = LockContext();
	CheckExecutableInternal(*lock);
	return context->ExecuteTaskInternal(*lock, *this);
}

void QueryResult::WaitForTask() {
	if (!context) {
		return;
	}
	auto lock = LockContext();
	if (!IsOpenInternal(*lock)) {
		return;
	}
	context->WaitForTask(*lock, *this);
}

void QueryResult::Close() {
	if (buffer) {
		buffer->Close();
	}
	if (context) {
		auto lock = context->LockContext();
		if (context->IsActiveResult(*lock, *this)) {
			// Abandoned before the result was consumed: release the active-query state now (matching
			// InitialCleanup) instead of leaking it until the next query or context teardown
			context->CleanupInternal(*lock, this, false);
		}
	}
	context.reset();
}

void QueryResult::SetBuffer(shared_ptr<BufferedData> buffer_p) {
	D_ASSERT(!buffer);
	buffer = std::move(buffer_p);
}

//===--------------------------------------------------------------------===//
// Retention
//===--------------------------------------------------------------------===//
void QueryResult::Materialize() {
	if (collection || HasError() || !context) {
		return;
	}
	auto lock = LockContext();
	if (!IsOpenInternal(*lock)) {
		Cancelled();
		context.reset();
		return;
	}
	context->SettleRetention(*lock, ResultLifetime::RETAINED);
}

void QueryResult::Complete() {
	if (collection || HasError() || !context) {
		return;
	}
	// The handle may hold the last reference to the context, which the lock below outlives
	auto keep_alive = context;
	auto lock = keep_alive->LockContext();
	CompleteInternal(*lock);
}

void QueryResult::CompleteInternal(ClientContextLock &lock) {
	if (collection || HasError() || !context) {
		return;
	}
	if (!IsOpenInternal(lock)) {
		Cancelled();
		context.reset();
		return;
	}
	try {
		context->SettleRetention(lock, ResultLifetime::RETAINED);
		QueryResultState state;
		while (!IsTerminal(state = context->ExecuteTaskInternal(lock, *this))) {
			if (state == QueryResultState::BLOCKED || state == QueryResultState::READY) {
				context->WaitForTask(lock, *this);
			}
		}
		if (state == QueryResultState::FINISHED) {
			// A statement the consumer does not read keeps none of its rows, not even its shape
			const bool reads_rows = context->PrincipalHasStarted(lock);
			auto produced = context->GetExecutor().GetResult();
			// Cleanup can fail on an autocommit commit; it records the error on this result
			context->CleanupInternal(lock, this, false);
			if (!HasError()) {
				D_ASSERT(reads_rows || GetTypes().empty());
				collection = reads_rows ? produced->TakeCollection()
				                        : make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator());
			}
		}
	} catch (...) {
		// Destroying this result while the caller still holds the context lock would take it again
		context.reset();
		throw;
	}
	context.reset();
}

void QueryResult::ThrowNoCollection() const {
	throw InvalidInputException("This query result no longer holds a collection: it was taken with TakeCollection, or "
	                            "the result was closed before it was collected");
}

ColumnDataCollection &QueryResult::Collection() {
	Complete();
	if (HasError()) {
		throw InvalidInputException("Attempting to get collection from an unsuccessful query result\n: Error %s",
		                            GetError());
	}
	if (!collection) {
		ThrowNoCollection();
	}
	return *collection;
}

unique_ptr<ColumnDataCollection> QueryResult::TakeCollection() {
	Complete();
	if (HasError()) {
		throw InvalidInputException("Attempting to get collection from an unsuccessful query result\n: Error %s",
		                            GetError());
	}
	if (!collection) {
		ThrowNoCollection();
	}
	return std::move(collection);
}

Value QueryResult::GetValue(idx_t column_idx, idx_t row_idx) {
	Complete();
	if (HasError()) {
		ThrowError();
	}
	if (!row_collection) {
		if (!collection) {
			ThrowNoCollection();
		}
		row_collection = make_uniq<ColumnDataRowCollection>(collection->GetRows());
	}
	return row_collection->GetValue(column_idx, row_idx);
}

idx_t QueryResult::RowCount() {
	Complete();
	return collection ? collection->Count() : 0;
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
void QueryResult::EndQuery(ClientContextLock &lock, bool invalidate_transaction) {
	context->CleanupInternal(lock, this, invalidate_transaction);
}

void QueryResult::HandleFetchFailure(ClientContextLock &lock, ErrorData error) {
	bool invalidate_query = true;
	if (!context->ErrorInvalidatesTransaction(error.Type())) {
		// standard exceptions do not invalidate the current transaction
		invalidate_query = false;
	} else if (Exception::InvalidatesDatabase(error.Type())) {
		// fatal exceptions invalidate the entire database
		auto &db_instance = DatabaseInstance::GetDatabase(*context);
		ValidChecker::Invalidate(db_instance, error.RawMessage());
	}
	context->ProcessError(error, context->GetCurrentQuery());
	SetError(std::move(error));
	context->CleanupInternal(lock, this, invalidate_query);
}

unique_ptr<DataChunk> QueryResult::FetchInternal() {
	Complete();
	if (HasError()) {
		throw InvalidInputException("Attempting to fetch from an unsuccessful query result\nError: %s", GetError());
	}
	if (!collection) {
		ThrowNoCollection();
	}
	auto result = make_uniq<DataChunk>();
	collection->InitializeScanChunk(*result);
	if (!scan_initialized) {
		// we disallow zero copy so the chunk is independently usable even after the result is destroyed
		collection->InitializeScan(scan_state, ColumnDataScanProperties::DISALLOW_ZERO_COPY);
		scan_initialized = true;
	}
	collection->Scan(scan_state, *result);
	if (result->size() == 0) {
		return nullptr;
	}
	return result;
}

unique_ptr<DataChunk> QueryResult::Fetch() {
	auto chunk = FetchRaw();
	if (!chunk) {
		return nullptr;
	}
	chunk->Flatten();
	return chunk;
}

unique_ptr<DataChunk> QueryResult::FetchRaw() {
	return FetchInternal();
}

//===--------------------------------------------------------------------===//
// Rendering
//===--------------------------------------------------------------------===//
string QueryResult::ToString() {
	if (HasError()) {
		return GetError() + "\n";
	}
	string result = HeaderToString();
	auto &coll = Collection();
	result += "[ Rows: " + to_string(coll.Count()) + "]\n";
	for (auto &row : coll.Rows()) {
		for (idx_t col_idx = 0; col_idx < coll.ColumnCount(); col_idx++) {
			if (col_idx > 0) {
				result += "\t";
			}
			auto val = row.GetValue(col_idx);
			result += val.IsNull() ? "NULL" : StringUtil::Replace(val.ToString(), string("\0", 1), "\\0");
		}
		result += "\n";
	}
	result += "\n";
	return result;
}

string QueryResult::ToBox(BoxRendererContext &context_p, const BoxRendererConfig &config) {
	if (HasError()) {
		return GetError() + "\n";
	}
	BoxRenderer renderer(config);
	ColumnDataCollectionWrapper wrapper(Collection());
	return renderer.ToString(context_p, IdentifiersToStrings(GetNames()), wrapper);
}

bool QueryResult::Equals(QueryResult &other, bool compare_names) { // LCOV_EXCL_START
	// first compare the success state of the results
	if (HasError() != other.HasError()) {
		return false;
	}
	if (HasError()) {
		return GetErrorObject() == other.GetErrorObject();
	}
	// compare names
	if (compare_names && GetNames() != other.GetNames()) {
		return false;
	}
	// compare types
	if (GetTypes() != other.GetTypes()) {
		return false;
	}
	// now compare the actual values
	// fetch chunks
	unique_ptr<DataChunk> lchunk, rchunk;
	idx_t lindex = 0, rindex = 0;
	while (true) {
		if (!lchunk || lindex == lchunk->size()) {
			lchunk = Fetch();
			lindex = 0;
		}
		if (!rchunk || rindex == rchunk->size()) {
			rchunk = other.Fetch();
			rindex = 0;
		}
		if (!lchunk && !rchunk) {
			return true;
		}
		if (!lchunk || !rchunk) {
			return false;
		}
		if (lchunk->size() == 0 && rchunk->size() == 0) {
			return true;
		}
		D_ASSERT(lchunk->ColumnCount() == rchunk->ColumnCount());
		for (; lindex < lchunk->size() && rindex < rchunk->size(); lindex++, rindex++) {
			for (idx_t col = 0; col < rchunk->ColumnCount(); col++) {
				auto lvalue = lchunk->GetValue(col, lindex);
				auto rvalue = rchunk->GetValue(col, rindex);
				if (lvalue.IsNull() && rvalue.IsNull()) {
					continue;
				}
				if (lvalue.IsNull() != rvalue.IsNull()) {
					return false;
				}
				if (lvalue != rvalue) {
					return false;
				}
			}
		}
	}
} // LCOV_EXCL_STOP

void QueryResult::Print() {
	Printer::Print(ToString());
}

string QueryResult::HeaderToString() {
	string result;
	for (auto &name : GetNames()) {
		result += name + "\t";
	}
	result += "\n";
	for (auto &type : GetTypes()) {
		result += type.ToString() + "\t";
	}
	result += "\n";
	return result;
}

} // namespace duckdb
