#include "raw_functions.hpp"
#include "raw_json.hpp"
#include "raw_write_settings.hpp"

#include <atomic>

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/enums/database_modification_type.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/sql_identifier.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/valid_checker.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/optimistic_data_writer.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "duckdb/storage/partial_block_manager.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/meta_transaction.hpp"

#include <chrono>
#include <condition_variable>
#include <deque>
#include <thread>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Schema-shape cache: high-rate small inserts mostly repeat the same payload
// shape. Hash the (column, type) signature per payload; shapes already
// absorbed by a table skip schema-delta work entirely. The cache validates
// against the table's storage pointer, which changes on any ALTER.
//===--------------------------------------------------------------------===//

class RawSchemaCache : public ObjectCacheEntry {
public:
	static string ObjectType() {
		return "rawduck_schema_shapes";
	}
	string GetObjectType() override {
		return ObjectType();
	}
	optional_idx GetEstimatedCacheMemory() const override {
		return optional_idx();
	}

	struct Entry {
		// validity token: the table's DataTable address at absorption time
		void *storage_token = nullptr;
		unordered_set<uint64_t> absorbed_shapes;
	};
	mutex lock;
	unordered_map<string, Entry> tables;
};

//===--------------------------------------------------------------------===//
// Table-creation serialization: concurrent HTTP/programmatic requests each
// open their own Connection/transaction. DuckDB's catalog allows only one
// of them to CREATE a given table name — the rest see a TransactionException
// ("write-write conflict") even though their payload is perfectly valid.
// `known_to_exist` is the steady-state fast path (checked, never locked,
// once a table has been seen): normal ingestion after a table's first-ever
// insert never touches `creation_lock` at all. `creation_lock` is only
// acquired for a table's *first* insert in this process, and held across
// the whole attempt through commit — a losing racer's catalog lookup can't
// see the winner's CREATE until that transaction actually commits, so
// releasing any earlier would just move the race, not remove it. One
// process-wide lock (not per-table): creating brand-new tables is rare and
// one-time per table, so serializing unrelated tables' first inserts against
// each other is an acceptable, simple tradeoff for never serializing
// steady-state appends to already-existing tables.
class RawTableCreationCache : public ObjectCacheEntry {
public:
	static string ObjectType() {
		return "rawduck_table_creation";
	}
	string GetObjectType() override {
		return ObjectType();
	}
	optional_idx GetEstimatedCacheMemory() const override {
		return optional_idx();
	}

	mutex lock;
	case_insensitive_set_t known_to_exist;
	mutex creation_lock;
};

//===--------------------------------------------------------------------===//
// Group commit: coalesces concurrent HTTP/programmatic requests to the same
// table into a single underlying transaction. DuckDB's single-writer WAL
// serializes every commit's fsync regardless of how many independent
// Connections are involved -- N concurrent requests each committing on their
// own pay N times the WAL-write/fsync latency, and under high fan-in that
// tail can exceed a client's read timeout even though no single commit is
// slow in isolation (observed directly: 16-way concurrent first-time OTLP
// ingest occasionally exceeded a 10s client timeout with every individual
// commit under half a second). One request becomes the "leader" for a
// table's currently-queued batch, merges every queued request's
// already-parsed payload (MergeParsedPayloads — the same merge already used
// for small-batch coalescing on the bulk-file path) and runs ONE
// RawIngestSerialized call for all of them, then wakes every waiter with its
// own row/error count against the batch's shared result.
//===--------------------------------------------------------------------===//

class RawCommitCoordinator : public ObjectCacheEntry {
public:
	static string ObjectType() {
		return "rawduck_commit_coordinator";
	}
	string GetObjectType() override {
		return ObjectType();
	}
	optional_idx GetEstimatedCacheMemory() const override {
		return optional_idx();
	}

	struct PendingItem {
		shared_ptr<RawParsedPayload> parsed;
		string payload_text;
		idx_t rows = 0;
		idx_t parse_errors = 0;
		bool done = false;
		bool failed = false;
		string error_message;
		RawIngestStats stats;
	};

	mutex lock;
	std::condition_variable cv;
	// FIFO of requests waiting for the next combined commit, per table.
	unordered_map<string, std::deque<shared_ptr<PendingItem>>> pending;
	// tables currently being drained/committed by a leader
	case_insensitive_set_t committing;
	// requests currently inside RawIngestGroupCommit for this table (from
	// the moment they enter to the moment they return/throw) — the signal
	// used to decide whether lingering is worth it at all. Unlike
	// `pending`, this is incremented before the mutex serializes anything,
	// so a request that becomes leader can see that siblings are already
	// under way even though none of them have reached `pending` yet.
	unordered_map<string, idx_t> active;
};

//===--------------------------------------------------------------------===//
// Direct (non-SQL) transaction control. Connection::BeginTransaction() /
// Commit() / Rollback() each run a full SQL statement ("BEGIN TRANSACTION" /
// "COMMIT" / "ROLLBACK") through the parser, binder, optimizer, and the
// task-scheduled executor -- real, measured overhead (profiling showed
// Commit() alone accounting for a substantial share of per-request cost
// under concurrent HTTP ingest). These call the same ClientContext::
// transaction primitives PhysicalTransaction's operator uses, skipping the
// SQL front end entirely. Semantically identical for RawDuck's usage: no
// read-only/custom-invalidation-policy modifiers (matching a plain "BEGIN
// TRANSACTION"), and immediate_transaction_mode defaults false and RawDuck
// never sets it, so that setting's extra eager-attach behavior never
// applies to these transactions either way.
//===--------------------------------------------------------------------===//

void RawBeginTransaction(ClientContext &context) {
	if (!context.transaction.IsAutoCommit()) {
		throw TransactionException("cannot start a transaction within a transaction");
	}
	context.transaction.SetAutoCommit(false);
}

void RawRollbackTransaction(ClientContext &context) {
	auto &txn = context.transaction;
	if (txn.IsAutoCommit()) {
		throw TransactionException("cannot rollback - no transaction is active");
	}
	auto &valid_checker = ValidChecker::Get(txn.ActiveTransaction());
	if (valid_checker.IsInvalidated()) {
		ErrorData error(ExceptionType::TRANSACTION, valid_checker.InvalidatedMessage());
		txn.Rollback(error);
	} else {
		txn.Rollback(nullptr);
	}
}

void RawCommitTransaction(ClientContext &context) {
	auto &txn = context.transaction;
	if (txn.IsAutoCommit()) {
		throw TransactionException("cannot commit - no transaction is active");
	}
	if (ValidChecker::IsInvalidated(txn.ActiveTransaction())) {
		// mirrors PhysicalTransaction: an invalidated transaction can't be
		// committed, so treat this exactly like a ROLLBACK instead.
		RawRollbackTransaction(context);
		return;
	}
	txn.Commit();
}

static uint64_t HashPayloadShape(const vector<RawColumn> &columns) {
	uint64_t shape = 0xcbf29ce484222325ULL;
	for (auto &column : columns) {
		for (auto c : column.name) {
			shape = (shape ^ NumericCast<uint64_t>(StringUtil::CharacterToLower(c))) * 0x100000001b3ULL;
		}
		shape = (shape ^ static_cast<uint64_t>(column.type.id())) * 0x100000001b3ULL;
	}
	return shape;
}

static void MergeParsedPayloads(RawParsedPayload &into, RawParsedPayload &&from) {
	into.payload.rows.insert(into.payload.rows.end(), from.payload.rows.begin(), from.payload.rows.end());
	for (auto *doc : from.payload.docs) {
		into.payload.docs.push_back(doc);
	}
	from.payload.docs.clear();
	from.payload.rows.clear();
	from.payload.parse_errors = 0;
	// the moved docs' yyjson_alc still points at from's pool buffer(s): that
	// ownership must move with them, or freeing `from` frees memory `into`'s
	// docs still reference.
	for (auto buffer : from.payload.pool_buffers) {
		into.payload.pool_buffers.push_back(buffer);
	}
	from.payload.pool_buffers.clear();
}

//===--------------------------------------------------------------------===//
// Type widening between an existing table column and incoming data
//===--------------------------------------------------------------------===//

static bool IsIntegerType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
		return true;
	default:
		return false;
	}
}

static bool IsPlainScalar(const LogicalType &type) {
	if (IsRawOverflowType(type)) {
		return false;
	}
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::VARCHAR:
		return true;
	default:
		return IsIntegerType(type);
	}
}

// Returns the type the table column must have to hold both existing and
// incoming values. Falls back to the existing type (no ALTER) for exotic
// existing types: the append cast will surface any true incompatibility.
static LogicalType JoinColumnTypes(const LogicalType &existing, const LogicalType &incoming) {
	if (existing == incoming) {
		return existing;
	}
	auto existing_overflow = IsRawOverflowType(existing);
	auto incoming_overflow = IsRawOverflowType(incoming);
	if (existing_overflow || incoming_overflow) {
		// JSON (legacy) and VARIANT both widen to the current overflow sink
		return RawOverflowType();
	}
	if (existing.id() == LogicalTypeId::LIST && incoming.id() == LogicalTypeId::LIST) {
		return LogicalType::LIST(JoinColumnTypes(ListType::GetChildType(existing), ListType::GetChildType(incoming)));
	}
	if (existing.id() == LogicalTypeId::LIST || incoming.id() == LogicalTypeId::LIST) {
		if (!IsPlainScalar(existing) && !IsPlainScalar(incoming)) {
			return existing;
		}
		return RawOverflowType();
	}
	if (!IsPlainScalar(existing)) {
		return existing;
	}
	if (IsIntegerType(existing) && IsIntegerType(incoming)) {
		// incoming integers are always inferred as BIGINT; only widen upwards
		return existing.id() == LogicalTypeId::HUGEINT ? existing : incoming;
	}
	auto is_numeric = [](const LogicalType &t) {
		return IsIntegerType(t) || t.id() == LogicalTypeId::FLOAT || t.id() == LogicalTypeId::DOUBLE;
	};
	auto is_temporal = [](const LogicalType &t) {
		return t.id() == LogicalTypeId::DATE || t.id() == LogicalTypeId::TIMESTAMP;
	};
	if (is_numeric(existing) && is_numeric(incoming)) {
		return LogicalType::DOUBLE;
	}
	if (is_temporal(existing) && is_temporal(incoming)) {
		return LogicalType::TIMESTAMP;
	}
	return LogicalType::VARCHAR;
}

//===--------------------------------------------------------------------===//
// SQL generation helpers (used by the non-native fallback and raw_optimize)
//===--------------------------------------------------------------------===//

string RawQuoteIdentifier(const string &name) {
	return SQLQuotedIdentifier::ToString(name);
}

static string QuoteLiteral(const string &value) {
	return SQLString::ToString(value);
}

string RawQualifiedTarget(const string &target) {
	auto qualified = QualifiedName::Parse(target);
	string result;
	if (!qualified.Catalog().empty()) {
		result += SQLQuotedIdentifier::ToString(qualified.Catalog()) + ".";
	}
	if (!qualified.Schema().empty()) {
		result += SQLQuotedIdentifier::ToString(qualified.Schema()) + ".";
	}
	result += SQLQuotedIdentifier::ToString(qualified.Name());
	return result;
}

// Parses a user-supplied target into a fully qualified [catalog, schema, name]
// path: the QualifiedName accessors only report a catalog for a three-part path.
QualifiedName RawResolveQualifiedName(ClientContext &context, const string &target) {
	auto parsed_name = QualifiedName::Parse(target);
	auto catalog_name = parsed_name.Catalog();
	auto schema_name = parsed_name.Schema();
	if (catalog_name.empty() && !schema_name.empty()) {
		// two-part names: the first part may be a catalog (raw.events)
		if (DatabaseManager::Get(context).GetDatabase(context, schema_name)) {
			catalog_name = schema_name;
			schema_name = Identifier();
		}
	}
	if (catalog_name.empty()) {
		catalog_name = DatabaseManager::GetDefaultDatabase(context);
	}
	if (schema_name.empty()) {
		schema_name = Identifier::DefaultSchema();
	}
	return QualifiedName(catalog_name, schema_name, parsed_name.Name());
}

static unique_ptr<MaterializedQueryResult> RunQuery(Connection &conn, const string &sql) {
	auto result = conn.Query(sql);
	if (result->HasError()) {
		result->ThrowError("RawDuck: ");
	}
	return result;
}

//===--------------------------------------------------------------------===//
// RawAppendPool: multi-threaded appends
//
// Worker threads extract payload batches into private optimistic row-group
// collections (the same mechanism as DuckDB's parallel INSERT sink), flushing
// completed row groups to disk as they go so parse/compression/I/O overlap;
// the collections merge into transaction-local storage on the main thread when
// the pool drains. Schema evolution is barrier-free: new columns are published
// to an append-only list and each worker pads its own collection (and rebuilds
// its flush writer against the evolved storage) when it next picks up a batch.
// Only type widening drains the pool.
//===--------------------------------------------------------------------===//

class RawAppendPool {
public:
	RawAppendPool(ClientContext &context, TableCatalogEntry &table, idx_t worker_count, bool overlap_flush,
	              idx_t max_queue_depth)
	    : context(context), block_manager_ref(table.GetStorage().GetTableIOManager().GetBlockManagerForRowData()),
	      flush_storage(&table.GetStorage()), overlap_flush(overlap_flush), max_queue_depth(max_queue_depth),
	      types(table.GetTypes()) {
		auto &storage = table.GetStorage();
		idx_t physical_index = 0;
		for (auto &column : table.GetColumns().Physical()) {
			slots[column.Name().GetIdentifierName()] = physical_index++;
		}
		workers = vector<Worker>(worker_count);
		for (auto &worker : workers) {
			worker.local_types = types;
			worker.local_slots = slots;
			worker.writer = make_uniq<OptimisticDataWriter>(context, storage);
			auto collection = worker.writer->CreateCollection(storage, types, OptimisticWritePartialManagers::GLOBAL);
			collection->collection->InitializeEmpty();
			worker.append_state = make_uniq<TableAppendState>();
			collection->collection->InitializeAppend(*worker.append_state);
			auto index = storage.CreateOptimisticCollection(context, std::move(collection));
			worker.collection = &storage.GetOptimisticCollection(context, index);
		}
		for (idx_t i = 0; i < worker_count; i++) {
			threads.emplace_back([this, i] { WorkerLoop(workers[i]); });
		}
	}
	~RawAppendPool() {
		// abnormal teardown: stop workers, merge nothing
		Stop();
	}

	void Submit(shared_ptr<RawParsedPayload> parsed) {
		std::unique_lock<mutex> guard(lock);
		producer_cv.wait(guard, [&] { return queue.size() < max_queue_depth || stopped; });
		if (stopped) {
			return;
		}
		queue.push_back(std::move(parsed));
		worker_cv.notify_one();
	}

	// Drain-free, barrier-free schema evolution: the consumer publishes new
	// columns to an append-only list; each worker pads its own collection to
	// the published prefix when it next picks up a batch. Worker layouts are
	// always a prefix of the table layout, so merges stay positional.
	void PublishColumns(const vector<pair<string, LogicalType>> &new_columns, DataTable &current_storage) {
		std::unique_lock<mutex> guard(lock);
		// the ALTER swapped the table's storage object; flush writers rebuilt
		// after this point must target it (its column count covers every
		// published column, so it is always a valid superset of any worker's
		// prefix layout)
		flush_storage = &current_storage;
		for (auto &new_column : new_columns) {
			slots[new_column.first] = types.size();
			types.push_back(new_column.second);
		}
		for (auto &worker : workers) {
			worker.layout_cache.shape = 0;
		}
	}

	// stops the workers and merges their collections; returns appended rows.
	// ALTERs swap the table's storage object, so the merge target must be
	// the CURRENT catalog entry, not the one captured at pool creation.
	idx_t Drain(TableCatalogEntry &current_table) {
		auto &duck_table = current_table.Cast<DuckTableEntry>();
		auto &storage = current_table.GetStorage();
		Stop();
		if (error) {
			std::rethrow_exception(error);
		}
		idx_t total = 0;
		auto merge_threshold = storage.GetRowGroupSize() / 8;
		const bool flush_now = overlap_flush;
		// phase 1, parallel: finalize each worker. Mid-append overlap_flush
		// already wrote complete row groups; finish those to disk here. The
		// default (in-memory until drain) skips that write: LocalMerge keeps
		// uncompressed collections in local storage and CHECKPOINT packs them
		// once — same GLOBAL partial-block path as DuckDB INSERT, without a
		// second allocation that leaves free-list holes.
		vector<std::thread> flushers;
		for (auto &worker : workers) {
			// pad to the full published schema so layouts match the table
			vector<pair<string, LogicalType>> missing;
			for (auto &slot : slots) {
				if (slot.second >= worker.local_types.size()) {
					missing.resize(MaxValue<idx_t>(missing.size(), slot.second - worker.local_types.size() + 1));
					missing[slot.second - worker.local_types.size()] = {slot.first, types[slot.second]};
				}
			}
			if (!missing.empty()) {
				ExtendWorker(worker, missing, storage);
			}
			// the worker's writer already targets the current storage: it was
			// created against it (no evolution) or rebuilt by ExtendWorker, and
			// it holds the partial blocks from incremental flushes, so reuse it
			// rather than dropping that state on the floor.
			flushers.emplace_back([&worker, merge_threshold, flush_now] {
				auto &collection = *worker.collection->collection;
				collection.FinalizeAppend(TransactionData(0, 0), *worker.append_state);
				if (flush_now && collection.GetTotalRows() >= merge_threshold) {
					worker.writer->WriteUnflushedRowGroups(*worker.collection);
					worker.writer->FinalFlush();
				}
			});
		}
		for (auto &flusher : flushers) {
			flusher.join();
		}
		// phase 2, serial: cheap pointer-level merges into local storage
		vector<unique_ptr<BoundConstraint>> no_constraints;
		for (auto &worker : workers) {
			auto &collection = *worker.collection->collection;
			auto count = collection.GetTotalRows();
			if (count == 0) {
				continue;
			}
			total += count;
			if (count < merge_threshold) {
				// genuinely small remainders: append to local storage directly
				LocalAppendState append_state;
				storage.InitializeLocalAppend(append_state, duck_table, context, no_constraints);
				auto &transaction = DuckTransaction::Get(context, current_table.catalog);
				for (auto &chunk : collection.Chunks(transaction)) {
					storage.LocalAppend(append_state, duck_table, context, chunk, false);
				}
				storage.FinalizeLocalAppend(append_state);
			} else {
				storage.LocalMerge(context, duck_table, *worker.collection);
				if (flush_now) {
					storage.GetOptimisticWriter(context).Merge(*worker.writer);
				}
			}
		}
		workers.clear();
		return total;
	}

private:
	struct BatchLayout {
		uint64_t shape = 0;
		vector<idx_t> slot_of;
		vector<bool> covered;
	};

	struct Worker {
		unique_ptr<OptimisticDataWriter> writer;
		optional_ptr<OptimisticWriteCollection> collection;
		unique_ptr<TableAppendState> append_state;
		// this worker's collection layout: a prefix of the published schema
		vector<LogicalType> local_types;
		case_insensitive_map_t<idx_t> local_slots;
		BatchLayout layout_cache;
	};

	void Stop() {
		{
			std::unique_lock<mutex> guard(lock);
			stopped = true;
			worker_cv.notify_all();
			producer_cv.notify_all();
		}
		for (auto &thread : threads) {
			thread.join();
		}
		threads.clear();
	}

	void WorkerLoop(Worker &worker) {
		try {
			DataChunk chunk;
			chunk.Initialize(Allocator::Get(context), worker.local_types);
			while (true) {
				shared_ptr<RawParsedPayload> parsed;
				vector<pair<string, LogicalType>> pending_columns;
				DataTable *pending_storage = nullptr;
				{
					std::unique_lock<mutex> guard(lock);
					worker_cv.wait(guard, [&] { return !queue.empty() || stopped; });
					if (queue.empty()) {
						return;
					}
					parsed = std::move(queue.front());
					queue.pop_front();
					// snapshot published columns this worker has not added yet
					for (idx_t i = worker.local_types.size(); i < types.size(); i++) {
						pending_columns.emplace_back(string(), types[i]);
					}
					for (auto &slot : slots) {
						if (slot.second >= worker.local_types.size()) {
							pending_columns[slot.second - worker.local_types.size()].first = slot.first;
						}
					}
					pending_storage = flush_storage;
					producer_cv.notify_one();
				}
				// flush completed row groups during append only when overlap is
				// enabled AND the schema is stable for this worker. A batch that
				// still carries new columns means the stream is churning:
				// deferring the flush keeps the drain-free behaviour (one flush
				// at the final width) and avoids re-extending freshly
				// checkpointed row groups, the expensive interaction on wide
				// evolving payloads.
				bool schema_stable = overlap_flush && pending_columns.empty();
				if (!pending_columns.empty()) {
					ExtendWorker(worker, pending_columns, *pending_storage);
					chunk.Destroy();
					chunk.Initialize(Allocator::Get(context), worker.local_types);
				}
				AppendBatch(worker, *parsed, chunk, schema_stable);
			}
		} catch (...) {
			std::unique_lock<mutex> guard(lock);
			if (!error) {
				error = std::current_exception();
			}
			stopped = true;
			worker_cv.notify_all();
			producer_cv.notify_all();
		}
	}

	// pads this worker's collection with NULL-filled columns: metadata-only
	// work through RowGroupCollection::AddColumn. GLOBAL partial-block packing
	// (same as DuckDB LocalStorage INSERT) leaves this vector empty: leftover
	// space is shared across columns so CHECKPOINT can truncate. PER_COLUMN
	// managers (one nearly-empty tail block per column) were the VARIANT-flat
	// file-size gap — same codecs, twice the file.
	// flushed complete row groups to disk (incremental WriteNewRowGroup);
	// AddColumn extends those checkpointed row groups too (existing columns
	// stay on disk, the new column is materialized NULL in memory). The flush
	// writer is then rebuilt against the post-ALTER storage so subsequent
	// WriteNewRowGroup calls see the evolved column/compression layout, while
	// the partial blocks written so far are carried forward via Merge.
	void ExtendWorker(Worker &worker, const vector<pair<string, LogicalType>> &new_columns, DataTable &flush_storage) {
		worker.collection->collection->FinalizeAppend(TransactionData(0, 0), *worker.append_state);
		for (auto &new_column : new_columns) {
			ColumnDefinition definition(Identifier(new_column.first), new_column.second);
			BoundConstantExpression null_default(Value(new_column.second));
			ExpressionExecutor default_executor(context, null_default);
			worker.collection->collection =
			    worker.collection->collection->AddColumn(context, definition, default_executor);
			if (!worker.collection->partial_block_managers.empty()) {
				auto &block_manager = block_manager_ref;
				worker.collection->partial_block_managers.push_back(make_uniq<PartialBlockManager>(
				    QueryContext(context), block_manager, PartialBlockType::APPEND_TO_TABLE));
			}
			worker.local_slots[new_column.first] = worker.local_types.size();
			worker.local_types.push_back(new_column.second);
		}
		worker.layout_cache.shape = 0;
		auto rebuilt = make_uniq<OptimisticDataWriter>(context, flush_storage);
		rebuilt->Merge(*worker.writer);
		worker.writer = std::move(rebuilt);
		worker.append_state = make_uniq<TableAppendState>();
		worker.collection->collection->InitializeAppend(*worker.append_state);
	}

	void AppendBatch(Worker &worker, RawParsedPayload &parsed, DataChunk &chunk, bool allow_flush) {
		auto &types = worker.local_types;
		auto &slots = worker.local_slots;
		RawExtractor extractor(*parsed.root, parsed.columns);
		auto shape = HashPayloadShape(parsed.columns);
		vector<idx_t> slot_of;
		vector<bool> covered;
		if (worker.layout_cache.shape == shape && worker.layout_cache.slot_of.size() == parsed.columns.size()) {
			slot_of = worker.layout_cache.slot_of;
			covered = worker.layout_cache.covered;
		} else {
			slot_of.resize(parsed.columns.size());
			covered.assign(types.size(), false);
			for (idx_t col = 0; col < parsed.columns.size(); col++) {
				auto entry = slots.find(parsed.columns[col].name);
				if (entry == slots.end()) {
					throw InternalException("RawDuck: column %s missing from append pool schema",
					                        parsed.columns[col].name);
				}
				slot_of[col] = entry->second;
				covered[entry->second] = true;
			}
			worker.layout_cache.shape = shape;
			worker.layout_cache.slot_of = slot_of;
			worker.layout_cache.covered = covered;
		}
		auto &payload_rows = parsed.payload.rows;
		for (idx_t start = 0; start < payload_rows.size(); start += STANDARD_VECTOR_SIZE) {
			auto count = MinValue<idx_t>(payload_rows.size() - start, STANDARD_VECTOR_SIZE);
			chunk.Reset();
			extractor.Reset(count);
			for (idx_t i = 0; i < count; i++) {
				extractor.AssignRow(payload_rows[start + i], i);
			}
			for (idx_t col = 0; col < parsed.columns.size(); col++) {
				auto slot = slot_of[col];
				if (RawFillSupported(types[slot])) {
					FillVector(extractor.ColumnValues(col), types[slot], chunk.data[slot], 0);
				} else {
					Vector source(parsed.columns[col].type, count);
					FillVector(extractor.ColumnValues(col), parsed.columns[col].type, source, 0);
					VectorOperations::DefaultCast(source, chunk.data[slot], count);
				}
			}
			for (idx_t slot = 0; slot < types.size(); slot++) {
				if (!covered[slot]) {
					chunk.data[slot].SetVectorType(VectorType::CONSTANT_VECTOR);
					ConstantVector::SetNull(chunk.data[slot], true);
				}
			}
			chunk.SetChildCardinality(count);
			// flush completed row groups to disk as we go, overlapping parse +
			// compression + I/O instead of deferring every byte to drain. The
			// writer targets the current storage layout; schema evolution
			// rebuilds it (ExtendWorker) so AddColumn stays safe across flushes.
			auto flushed_row_group_idx = collection_of(worker).Append(chunk, *worker.append_state);
			if (flushed_row_group_idx.IsValid() && allow_flush) {
				worker.writer->WriteNewRowGroup(*worker.collection, flushed_row_group_idx.GetIndex());
			}
		}
	}

	static RowGroupCollection &collection_of(Worker &worker) {
		return *worker.collection->collection;
	}

	ClientContext &context;
	BlockManager &block_manager_ref;
	// the DataTable that flush writers must target: ALTERs swap the storage
	// object, so this is republished (with the new columns) on every evolution
	// and read by workers when they rebuild their writer. Guarded by `lock`.
	DataTable *flush_storage;
	// opt-in (rawduck_overlap_flush): overlap parse with compression/IO by
	// flushing completed row groups mid-append. Off by default: the pool stays
	// drain-free (one flush burst at the final width), which is lower peak
	// memory and optimal for schema-churn payloads.
	bool overlap_flush;
	idx_t max_queue_depth;
	vector<LogicalType> types;
	case_insensitive_map_t<idx_t> slots;
	vector<Worker> workers;
	vector<std::thread> threads;
	mutex lock;
	std::condition_variable worker_cv;
	std::condition_variable producer_cv;
	std::deque<shared_ptr<RawParsedPayload>> queue;
	bool stopped = false;
	std::exception_ptr error;
};

//===--------------------------------------------------------------------===//
// RawIngestor: schema evolution + append for one target table
//===--------------------------------------------------------------------===//

// Drives one or more payload batches into a target table, evolving its schema
// as batches arrive. For native DuckDB catalogs everything runs inside the
// calling query's transaction through catalog and storage APIs directly; for
// other catalogs (e.g. DuckLake) a SQL fallback on a dedicated connection is
// used instead.
class RawIngestor {
public:
	RawIngestor(ClientContext &context, string target_p, RawParseOptions options_p)
	    : context(context), target(std::move(target_p)), options(std::move(options_p)) {
		qname = RawResolveQualifiedName(context, target);
		auto &catalog = Catalog::GetCatalog(context, qname.Catalog());
		native = catalog.IsDuckCatalog();
		write_settings = RawWriteSettings::Get(context);
	}

	void Ingest(const string &payload_str) {
		auto parsed = RawParsedPayload::Process(payload_str, options);
		IngestParsed(std::move(parsed), payload_str);
	}

	void IngestParsed(shared_ptr<RawParsedPayload> parsed, const string &payload_str) {
		errors += parsed->payload.parse_errors;
		if (native) {
			IngestNative(std::move(parsed));
		} else {
			IngestFallback(*parsed, payload_str);
		}
	}

	// merges any outstanding parallel appends; must be called before the
	// ingestion result is read
	void Finish() {
		if (pool) {
			auto table = LookupTable();
			if (!table) {
				throw InternalException("RawDuck: table %s disappeared during ingestion", target);
			}
			rows += pool->Drain(*table);
			pool.reset();
		}
	}

	// Schema evolution (serial). Returns true when the append pool owns the batch.
	bool PrepareForAppend(RawParsedPayload &parsed) {
		write_settings = RawWriteSettings::Get(context);
		auto table = LookupTableCached();
		if (!table || parsed.columns.empty()) {
			return false;
		}
		auto cache_key = qname.Catalog() + "." + qname.Schema() + "." + qname.Name();
		auto shape = HashPayloadShape(parsed.columns);
		auto &cache = *ObjectCache::GetObjectCache(context).GetOrCreate<RawSchemaCache>(RawSchemaCache::ObjectType());
		bool absorbed = false;
		{
			lock_guard<mutex> guard(cache.lock);
			auto entry = cache.tables.find(cache_key);
			absorbed = entry != cache.tables.end() && entry->second.storage_token == &table->GetStorage() &&
			           entry->second.absorbed_shapes.count(shape) > 0;
		}
		last_batch_shape_absorbed = absorbed;
		if (!absorbed) {
			auto &catalog = Catalog::GetCatalog(context, qname.Catalog());
			MetaTransaction::Get(context).ModifyDatabase(
			    catalog.GetAttached(), DatabaseModificationType::ALTER_TABLE | DatabaseModificationType::INSERT_DATA);
			vector<pair<string, LogicalType>> adds;
			bool widens = false;
			ComputeDelta(*table, parsed, adds, widens);
			if (pool && widens) {
				rows += pool->Drain(*table);
				pool.reset();
			}
			EvolveNative(catalog, *table, parsed);
			InvalidateTableCache();
			table = LookupTableCached();
			if (!table) {
				throw InternalException("RawDuck: table %s disappeared during ingestion", target);
			}
			if (pool && !adds.empty()) {
				pool->PublishColumns(adds, table->GetStorage());
			}
			lock_guard<mutex> guard(cache.lock);
			auto &entry = cache.tables[cache_key];
			if (entry.storage_token != &table->GetStorage()) {
				entry.storage_token = &table->GetStorage();
				entry.absorbed_shapes.clear();
			}
			entry.absorbed_shapes.insert(shape);
			last_batch_shape_absorbed = true;
		}
		if (parsed.payload.rows.empty()) {
			return false;
		}
		MetaTransaction::Get(context).ModifyDatabase(Catalog::GetCatalog(context, qname.Catalog()).GetAttached(),
		                                             DatabaseModificationType::INSERT_DATA);
		EnsurePool(*table, parsed, last_batch_shape_absorbed);
		return pool != nullptr;
	}

	void SubmitPreparedBatch(shared_ptr<RawParsedPayload> parsed_ptr, bool use_pool) {
		auto &parsed = *parsed_ptr;
		if (parsed.payload.rows.empty()) {
			return;
		}
		if (use_pool && pool) {
			pool->Submit(std::move(parsed_ptr));
			return;
		}
		auto table = LookupTableCached();
		if (!table) {
			throw InternalException("RawDuck: table %s disappeared during ingestion", target);
		}
		AppendNative(*table, parsed);
	}

	optional_ptr<TableCatalogEntry> LookupTableForAppend() {
		return LookupTable();
	}

	bool created = false;
	idx_t columns_added = 0;
	idx_t columns_widened = 0;
	idx_t rows = 0;
	idx_t errors = 0;

private:
	//===----------------------------------------------------------------===//
	// Native path: catalog + storage APIs in the caller's transaction
	//===----------------------------------------------------------------===//

	optional_ptr<TableCatalogEntry> LookupTable() {
		// non-template lookup: the template ODR-uses CatalogEntry::Name
		// statics, which duplicate-define against core on some toolchains
		EntryLookupInfo lookup_info(CatalogType::TABLE_ENTRY, qname);
		auto entry = Catalog::GetEntry(context, lookup_info, OnEntryNotFound::RETURN_NULL);
		if (!entry) {
			return nullptr;
		}
		return &entry->Cast<TableCatalogEntry>();
	}

	void InvalidateTableCache() {
		cached_table = nullptr;
		cached_storage = nullptr;
		append_layout.shape = 0;
	}

	optional_ptr<TableCatalogEntry> LookupTableCached() {
		auto table = LookupTable();
		if (!table) {
			InvalidateTableCache();
			return nullptr;
		}
		auto *storage = &table->GetStorage();
		if (cached_table && cached_storage == storage) {
			return cached_table;
		}
		cached_table = table;
		cached_storage = storage;
		append_layout.shape = 0;
		return table;
	}

	struct AppendLayoutCache {
		void *storage_token = nullptr;
		uint64_t shape = 0;
		vector<LogicalType> types;
		vector<idx_t> slot_of;
		vector<bool> covered;
		vector<unique_ptr<BoundConstraint>> bound_constraints;
	};

	bool ResolveAppendLayout(TableCatalogEntry &table, RawParsedPayload &parsed, AppendLayoutCache &layout) {
		auto shape = HashPayloadShape(parsed.columns);
		auto *storage = &table.GetStorage();
		if (layout.storage_token == storage && layout.shape == shape && !layout.slot_of.empty()) {
			return true;
		}
		auto &columns_list = table.GetColumns();
		case_insensitive_map_t<idx_t> table_index;
		idx_t physical_index = 0;
		for (auto &column : columns_list.Physical()) {
			table_index[column.Name().GetIdentifierName()] = physical_index++;
		}
		layout.types = table.GetTypes();
		layout.slot_of.resize(parsed.columns.size());
		layout.covered.assign(layout.types.size(), false);
		for (idx_t col = 0; col < parsed.columns.size(); col++) {
			auto entry = table_index.find(parsed.columns[col].name);
			if (entry == table_index.end()) {
				throw InternalException("RawDuck: column %s missing after evolution", parsed.columns[col].name);
			}
			layout.slot_of[col] = entry->second;
			layout.covered[entry->second] = true;
		}
		auto binder = Binder::CreateBinder(context);
		layout.bound_constraints = binder->BindConstraints(table);
		layout.storage_token = storage;
		layout.shape = shape;
		return true;
	}

	// classifies the DDL this payload would require
	void ComputeDelta(TableCatalogEntry &table, RawParsedPayload &parsed, vector<pair<string, LogicalType>> &adds,
	                  bool &widens) {
		auto &existing_columns = table.GetColumns();
		for (auto &column : parsed.columns) {
			Identifier column_name(column.name);
			if (!existing_columns.ColumnExists(column_name)) {
				adds.emplace_back(column.name, column.type);
				continue;
			}
			auto &existing = existing_columns.GetColumn(column_name);
			if (JoinColumnTypes(existing.Type(), column.type) != existing.Type()) {
				widens = true;
			}
		}
	}

	bool PoolEligible(TableCatalogEntry &table, RawParsedPayload &parsed) {
		if (parsed.payload.rows.size() < write_settings.pool_min_rows) {
			return false;
		}
		auto &columns_list = table.GetColumns();
		if (columns_list.PhysicalColumnCount() != columns_list.LogicalColumnCount()) {
			return false;
		}
		if (table.GetStorage().HasIndexes()) {
			return false;
		}
		auto binder = Binder::CreateBinder(context);
		return binder->BindConstraints(table).empty();
	}

	void EnsurePool(TableCatalogEntry &table, RawParsedPayload &parsed, bool shape_absorbed) {
		if (pool || parsed.payload.rows.empty() || !PoolEligible(table, parsed)) {
			return;
		}
		write_settings = RawWriteSettings::Get(context);
		auto worker_count = write_settings.PoolThreadCount(parsed.payload.rows.size());
		auto overlap = write_settings.OverlapFlushForBatch(context, shape_absorbed, parsed.payload.rows.size());
		pool = make_uniq<RawAppendPool>(context, table, worker_count, overlap, write_settings.pipeline_depth);
	}

	void IngestNative(shared_ptr<RawParsedPayload> parsed_ptr) {
		auto &parsed = *parsed_ptr;
		auto table = LookupTableCached();
		if (!table || parsed.columns.empty()) {
			LegacyIngestNative(std::move(parsed_ptr), table);
			return;
		}
		auto cache_key = qname.Catalog() + "." + qname.Schema() + "." + qname.Name();
		auto shape = HashPayloadShape(parsed.columns);
		auto &cache = *ObjectCache::GetObjectCache(context).GetOrCreate<RawSchemaCache>(RawSchemaCache::ObjectType());
		bool absorbed;
		{
			lock_guard<mutex> guard(cache.lock);
			auto entry = cache.tables.find(cache_key);
			absorbed = entry != cache.tables.end() && entry->second.storage_token == &table->GetStorage() &&
			           entry->second.absorbed_shapes.count(shape) > 0;
		}
		bool shape_absorbed = absorbed;
		if (!absorbed) {
			// schema-delta slow path, then absorb the shape
			auto &catalog = Catalog::GetCatalog(context, qname.Catalog());
			MetaTransaction::Get(context).ModifyDatabase(
			    catalog.GetAttached(), DatabaseModificationType::ALTER_TABLE | DatabaseModificationType::INSERT_DATA);
			vector<pair<string, LogicalType>> adds;
			bool widens = false;
			ComputeDelta(*table, parsed, adds, widens);
			if (pool && widens) {
				rows += pool->Drain(*table);
				pool.reset();
			}
			EvolveNative(catalog, *table, parsed);
			InvalidateTableCache();
			table = LookupTableCached();
			if (!table) {
				throw InternalException("RawDuck: table %s disappeared during ingestion", target);
			}
			if (pool && !adds.empty()) {
				pool->PublishColumns(adds, table->GetStorage());
			}
			lock_guard<mutex> guard(cache.lock);
			auto &entry = cache.tables[cache_key];
			if (entry.storage_token != &table->GetStorage()) {
				entry.storage_token = &table->GetStorage();
				entry.absorbed_shapes.clear();
			}
			entry.absorbed_shapes.insert(shape);
			shape_absorbed = true;
		}
		if (parsed.payload.rows.empty()) {
			return;
		}
		MetaTransaction::Get(context).ModifyDatabase(Catalog::GetCatalog(context, qname.Catalog()).GetAttached(),
		                                             DatabaseModificationType::INSERT_DATA);
		EnsurePool(*table, parsed, shape_absorbed);
		if (pool) {
			pool->Submit(std::move(parsed_ptr));
			return;
		}
		AppendNative(*table, parsed);
	}

	void LegacyIngestNative(shared_ptr<RawParsedPayload> parsed_ptr, optional_ptr<TableCatalogEntry> table) {
		auto &parsed = *parsed_ptr;
		if (!table && parsed.columns.empty()) {
			throw InvalidInputException("RawDuck: cannot create table %s from an empty payload", target);
		}
		if (parsed.columns.empty()) {
			return;
		}
		auto &catalog = Catalog::GetCatalog(context, qname.Catalog());
		// the binder marks modified databases for write statements; we write
		// from within a table function, so mark the target ourselves
		MetaTransaction::Get(context).ModifyDatabase(
		    catalog.GetAttached(), DatabaseModificationType::CREATE_CATALOG_ENTRY |
		                               DatabaseModificationType::ALTER_TABLE | DatabaseModificationType::INSERT_DATA);
		if (!table) {
			CreateNative(catalog, parsed);
			created = true;
			columns_added += parsed.columns.size();
			InvalidateTableCache();
		} else {
			vector<pair<string, LogicalType>> adds;
			bool widens = false;
			ComputeDelta(*table, parsed, adds, widens);
			if (pool && widens) {
				// type rewrites cannot be applied to outstanding collections:
				// merge the pool's work before altering
				rows += pool->Drain(*table);
				pool.reset();
			}
			EvolveNative(catalog, *table, parsed);
			InvalidateTableCache();
			if (pool && !adds.empty()) {
				// drain-free, barrier-free: workers pad their own collections
				// and rebuild their flush writers against the post-ALTER storage
				auto evolved = LookupTable();
				if (!evolved) {
					throw InternalException("RawDuck: table %s disappeared during ingestion", target);
				}
				pool->PublishColumns(adds, evolved->GetStorage());
			}
		}
		if (parsed.payload.rows.empty()) {
			return;
		}
		// re-resolve: DDL replaces the catalog entry
		table = LookupTableCached();
		if (!table) {
			throw InternalException("RawDuck: table %s disappeared during ingestion", target);
		}
		EnsurePool(*table, parsed, true);
		if (pool) {
			pool->Submit(std::move(parsed_ptr));
			return;
		}
		AppendNative(*table, parsed);
	}

	void CreateNative(Catalog &catalog, RawParsedPayload &parsed) {
		auto info = make_uniq<CreateTableInfo>(qname);
		for (auto &column : parsed.columns) {
			info->columns.AddColumn(ColumnDefinition(Identifier(column.name), column.type));
		}
		auto &schema_entry = catalog.GetSchema(context, qname.Schema());
		auto binder = Binder::CreateBinder(context);
		auto bound_info = binder->BindCreateTableInfo(std::move(info), schema_entry);
		catalog.CreateTable(context, *bound_info);
	}

	void EvolveNative(Catalog &catalog, TableCatalogEntry &table, RawParsedPayload &parsed) {
		auto &existing_columns = table.GetColumns();
		for (auto &column : parsed.columns) {
			AlterEntryData data(qname, OnEntryNotFound::THROW_EXCEPTION);
			Identifier column_name(column.name);
			if (!existing_columns.ColumnExists(column_name)) {
				AddColumnInfo add_column(data, ColumnDefinition(column_name, column.type), false);
				catalog.Alter(context, add_column);
				columns_added++;
				continue;
			}
			auto &existing = existing_columns.GetColumn(column_name);
			auto target_type = JoinColumnTypes(existing.Type(), column.type);
			if (target_type == existing.Type()) {
				continue;
			}
			auto column_ref = make_uniq<ColumnRefExpression>(Identifier(column.name));
			unique_ptr<ParsedExpression> expression;
			if (IsRawVariantType(target_type) && !IsRawVariantType(existing.Type())) {
				// JSON→VARIANT is a plain cast; other types go through to_json first
				// (same rationale as the JSON sink rewrite — bare strings reject).
				if (IsRawJSONType(existing.Type())) {
					expression = make_uniq<CastExpression>(target_type, std::move(column_ref));
				} else {
					vector<unique_ptr<ParsedExpression>> children;
					children.push_back(std::move(column_ref));
					auto to_json = make_uniq<FunctionExpression>("to_json", std::move(children));
					expression = make_uniq<CastExpression>(target_type, std::move(to_json));
				}
			} else if (IsRawJSONType(target_type) && !IsRawJSONType(existing.Type())) {
				// a plain cast to JSON would reject bare strings
				vector<unique_ptr<ParsedExpression>> children;
				children.push_back(std::move(column_ref));
				expression = make_uniq<FunctionExpression>("to_json", std::move(children));
			} else {
				expression = make_uniq<CastExpression>(target_type, std::move(column_ref));
			}
			ChangeColumnTypeInfo change_type(data, column_name, target_type, std::move(expression));
			catalog.Alter(context, change_type);
			columns_widened++;
		}
	}

	void AppendNative(TableCatalogEntry &table, RawParsedPayload &parsed) {
		if (table.GetColumns().PhysicalColumnCount() != table.GetColumns().LogicalColumnCount()) {
			throw NotImplementedException("RawDuck: cannot ingest into tables with generated columns");
		}
		ResolveAppendLayout(table, parsed, append_layout);
		auto &types = append_layout.types;
		auto &slot_of = append_layout.slot_of;
		auto &covered = append_layout.covered;
		auto &bound_constraints = append_layout.bound_constraints;

		DataChunk chunk;
		chunk.Initialize(Allocator::Get(context), types);
		RawExtractor extractor(*parsed.root, parsed.columns);
		auto &storage = table.GetStorage();
		auto &payload_rows = parsed.payload.rows;

		for (idx_t start = 0; start < payload_rows.size(); start += STANDARD_VECTOR_SIZE) {
			auto count = MinValue<idx_t>(payload_rows.size() - start, STANDARD_VECTOR_SIZE);
			chunk.Reset();
			extractor.Reset(count);
			for (idx_t i = 0; i < count; i++) {
				extractor.AssignRow(payload_rows[start + i], i);
			}
			for (idx_t col = 0; col < parsed.columns.size(); col++) {
				auto slot = slot_of[col];
				if (RawFillSupported(types[slot])) {
					FillVector(extractor.ColumnValues(col), types[slot], chunk.data[slot], 0);
				} else {
					// extract in the payload's inferred type, then cast
					Vector source(parsed.columns[col].type, count);
					FillVector(extractor.ColumnValues(col), parsed.columns[col].type, source, 0);
					VectorOperations::DefaultCast(source, chunk.data[slot], count);
				}
			}
			for (idx_t slot = 0; slot < types.size(); slot++) {
				if (!covered[slot]) {
					chunk.data[slot].SetVectorType(VectorType::CONSTANT_VECTOR);
					ConstantVector::SetNull(chunk.data[slot], true);
				}
			}
			chunk.SetChildCardinality(count);
			storage.LocalAppend(table.Cast<DuckTableEntry>(), context, chunk, bound_constraints);
			rows += count;
		}
	}

	//===----------------------------------------------------------------===//
	// Fallback path: SQL over a dedicated connection (DuckLake et al.)
	//===----------------------------------------------------------------===//

	void EnsureFallbackConnection() {
		if (fallback_conn) {
			return;
		}
		fallback_conn = make_uniq<Connection>(*context.db);
		auto qualified = RawQualifiedTarget(target);
		auto probe = fallback_conn->Query("SELECT * FROM " + qualified + " LIMIT 0");
		fallback_exists = !probe->HasError();
		if (!fallback_exists) {
			auto &error = probe->GetError();
			if (!StringUtil::Contains(error, "does not exist") && !StringUtil::Contains(error, "not found")) {
				throw InvalidInputException("RawDuck: cannot describe %s: %s", target, error);
			}
			return;
		}
		for (idx_t col = 0; col < probe->ColumnCount(); col++) {
			fallback_types[probe->GetNames()[col].GetIdentifierName()] = probe->GetTypes()[col];
		}
	}

	void IngestFallback(RawParsedPayload &parsed, const string &payload_str) {
		EnsureFallbackConnection();
		if (!fallback_exists && parsed.columns.empty()) {
			throw InvalidInputException("RawDuck: cannot create table %s from an empty payload", target);
		}
		if (parsed.columns.empty()) {
			return;
		}
		// Catalogs like DuckLake cannot rewrite a column with an expression
		// (ALTER ... USING), which overflow widening (JSON/VARIANT) needs; when
		// that fails we retry once with widening disabled, converting incoming
		// values to the existing column type instead.
		try {
			FallbackBatch(parsed, payload_str, true);
		} catch (std::exception &ex) {
			ErrorData error(ex);
			if (StringUtil::Contains(error.RawMessage(), "cannot be modified using an expression")) {
				FallbackBatch(parsed, payload_str, false);
			} else {
				throw;
			}
		}
	}

	void FallbackBatch(RawParsedPayload &parsed, const string &payload_str, bool allow_overflow_widening) {
		auto &conn = *fallback_conn;
		auto qualified = RawQualifiedTarget(target);
		auto &columns = parsed.columns;
		vector<string> statements;
		// final table type for every incoming column, to know which need to_json()
		vector<LogicalType> final_types;
		bool batch_creates = false;
		idx_t batch_added = 0;
		idx_t batch_widened = 0;

		if (!fallback_exists) {
			string create = "CREATE TABLE " + qualified + " (";
			for (idx_t i = 0; i < columns.size(); i++) {
				create += (i ? ", " : "") + RawQuoteIdentifier(columns[i].name) + " " + columns[i].type.ToString();
				final_types.push_back(columns[i].type);
			}
			create += ")";
			statements.push_back(create);
			batch_creates = true;
			batch_added = columns.size();
		} else {
			for (auto &column : columns) {
				auto entry = fallback_types.find(column.name);
				if (entry == fallback_types.end()) {
					statements.push_back("ALTER TABLE " + qualified + " ADD COLUMN " + RawQuoteIdentifier(column.name) +
					                     " " + column.type.ToString());
					final_types.push_back(column.type);
					batch_added++;
					continue;
				}
				auto target_type = JoinColumnTypes(entry->second, column.type);
				if (target_type == entry->second) {
					final_types.push_back(target_type);
					continue;
				}
				bool needs_rewrite = IsRawOverflowType(target_type) && !IsRawOverflowType(entry->second);
				if (needs_rewrite && !allow_overflow_widening) {
					final_types.push_back(entry->second);
					continue;
				}
				string alter = "ALTER TABLE " + qualified + " ALTER COLUMN " + RawQuoteIdentifier(column.name) +
				               " SET DATA TYPE " + target_type.ToString();
				if (needs_rewrite) {
					if (IsRawVariantType(target_type)) {
						alter += " USING to_json(" + RawQuoteIdentifier(column.name) + ")::VARIANT";
					} else {
						alter += " USING to_json(" + RawQuoteIdentifier(column.name) + ")";
					}
				} else if (IsRawVariantType(target_type) && IsRawJSONType(entry->second)) {
					alter += " USING " + RawQuoteIdentifier(column.name) + "::VARIANT";
				}
				statements.push_back(alter);
				final_types.push_back(target_type);
				batch_widened++;
			}
		}

		idx_t batch_rows = 0;
		RunQuery(conn, "BEGIN TRANSACTION");
		try {
			for (auto &statement : statements) {
				RunQuery(conn, statement);
			}
			if (!parsed.payload.rows.empty()) {
				string insert = "INSERT INTO " + qualified + " (";
				string select_list;
				for (idx_t i = 0; i < columns.size(); i++) {
					insert += (i ? ", " : "") + RawQuoteIdentifier(columns[i].name);
					auto expr = RawQuoteIdentifier(columns[i].name);
					auto incoming_structural =
					    IsRawOverflowType(columns[i].type) || columns[i].type.id() == LogicalTypeId::LIST;
					if (IsRawVariantType(final_types[i]) && !IsRawVariantType(columns[i].type)) {
						if (IsRawJSONType(columns[i].type)) {
							expr = expr + "::VARIANT";
						} else {
							expr = "to_json(" + expr + ")::VARIANT";
						}
					} else if (IsRawJSONType(final_types[i]) && !IsRawJSONType(columns[i].type)) {
						expr = "to_json(" + expr + ")";
					} else if (!IsRawOverflowType(final_types[i]) && incoming_structural &&
					           final_types[i] != columns[i].type) {
						// degraded widening: render structural values as JSON
						// text and cast to the existing column type
						expr = "to_json(" + expr + ")::" + final_types[i].ToString();
					}
					select_list += (i ? ", " : "") + expr;
				}
				insert += ") SELECT " + select_list + " FROM raw_records(" + QuoteLiteral(payload_str) + ")";
				auto result = RunQuery(conn, insert);
				batch_rows = NumericCast<idx_t>(result->GetValue(0, 0).GetValue<int64_t>());
			}
			RunQuery(conn, "COMMIT");
		} catch (...) {
			conn.Query("ROLLBACK");
			throw;
		}

		created = created || batch_creates;
		columns_added += batch_added;
		columns_widened += batch_widened;
		rows += batch_rows;
		fallback_exists = true;
		for (idx_t i = 0; i < columns.size(); i++) {
			fallback_types[columns[i].name] = final_types[i];
		}
	}

	ClientContext &context;
	string target;
	RawParseOptions options;
	QualifiedName qname;
	bool native = false;
	RawWriteSettings write_settings;
	bool last_batch_shape_absorbed = false;
	optional_ptr<TableCatalogEntry> cached_table;
	void *cached_storage = nullptr;
	AppendLayoutCache append_layout;
	unique_ptr<RawAppendPool> pool;
	// fallback state
	unique_ptr<Connection> fallback_conn;
	bool fallback_exists = false;
	case_insensitive_map_t<LogicalType> fallback_types;
};

// Programmatic entry point (used by the HTTP API): runs a full ingest within
// the caller's active transaction.
RawIngestStats RawIngestPayload(ClientContext &context, const string &target, const string &payload,
                                const RawParseOptions &options) {
	auto parsed = RawParsedPayload::Process(payload, options);
	return RawIngestParsedPayload(context, target, std::move(parsed), payload, options);
}

// Split out of RawIngestPayload so a retrying caller can reuse the same
// already-parsed payload across attempts instead of re-parsing and
// re-shredding the same bytes every time — parsing isn't what conflicts,
// only the catalog step is.
RawIngestStats RawIngestParsedPayload(ClientContext &context, const string &target, shared_ptr<RawParsedPayload> parsed,
                                      const string &payload, const RawParseOptions &options) {
	RawIngestor ingestor(context, target, options);
	ingestor.IngestParsed(std::move(parsed), payload);
	ingestor.Finish();
	RawIngestStats stats;
	stats.created = ingestor.created;
	stats.columns_added = ingestor.columns_added;
	stats.columns_widened = ingestor.columns_widened;
	stats.rows = ingestor.rows;
	stats.errors = ingestor.errors;
	return stats;
}

// Entry point for the HTTP/programmatic ingest path: manages `conn`'s
// transaction itself (a table's first insert must hold the creation lock
// across the whole attempt through commit; see RawTableCreationCache) and
// falls back to a single retry for the rarer conflict class the lock
// doesn't cover — concurrent ALTER on an already-existing table (e.g. two
// requests simultaneously discovering the same new column).
RawIngestStats RawIngestSerialized(Connection &conn, const string &target, shared_ptr<RawParsedPayload> parsed,
                                   const string &payload, const RawParseOptions &options) {
	auto &cache = *ObjectCache::GetObjectCache(*conn.context)
	                   .GetOrCreate<RawTableCreationCache>(RawTableCreationCache::ObjectType());
	auto known_to_exist = [&]() {
		lock_guard<mutex> guard(cache.lock);
		return cache.known_to_exist.count(target) > 0;
	};
	auto mark_known = [&]() {
		lock_guard<mutex> guard(cache.lock);
		cache.known_to_exist.insert(target);
	};

	unique_lock<mutex> creation_guard(cache.creation_lock, std::defer_lock);
	if (!known_to_exist()) {
		creation_guard.lock();
		if (known_to_exist()) {
			// someone else created (and committed) it while we waited
			creation_guard.unlock();
		}
	}

	RawBeginTransaction(*conn.context);
	try {
		auto stats = RawIngestParsedPayload(*conn.context, target, parsed, payload, options);
		RawCommitTransaction(*conn.context);
		mark_known();
		return stats;
	} catch (TransactionException &) {
		RawRollbackTransaction(*conn.context);
	}
	// final attempt: no catch here. Any failure is left open for the
	// caller's own catch/rollback, same as every other error path.
	RawBeginTransaction(*conn.context);
	auto stats = RawIngestParsedPayload(*conn.context, target, parsed, payload, options);
	RawCommitTransaction(*conn.context);
	mark_known();
	return stats;
}

// HTTP/programmatic ingest entry point (replaces calling RawIngestSerialized
// directly): coalesces this request with any others concurrently targeting
// the same table into one shared commit. See RawCommitCoordinator.
//
// Waiters never touch their own Connection's transaction state at all — the
// leader runs the entire Begin/Ingest/Commit/Rollback sequence on its own
// `conn`, so a failed batch is rolled back exactly once by the leader.
// Callers must not call Rollback themselves after this throws.
RawIngestStats RawIngestGroupCommit(Connection &conn, const string &target, shared_ptr<RawParsedPayload> parsed,
                                    const string &payload, const RawParseOptions &options) {
	auto &coord = *ObjectCache::GetObjectCache(*conn.context)
	                   .GetOrCreate<RawCommitCoordinator>(RawCommitCoordinator::ObjectType());

	auto item = make_shared_ptr<RawCommitCoordinator::PendingItem>();
	item->parsed = parsed;
	item->payload_text = payload;
	item->rows = parsed->payload.rows.size();
	item->parse_errors = parsed->payload.parse_errors;

	unique_lock<mutex> guard(coord.lock);
	// Mark ourselves "in flight" for this target before anything else: this
	// is what lets a brand-new leader tell a genuine concurrent burst apart
	// from a lone request with nobody to batch with, and it's set before
	// `pending` is touched so it catches siblings that haven't reached
	// `pending` yet either (all of it happens under the same lock, so the
	// order within this one line doesn't race).
	auto active_at_entry = ++coord.active[target];
	struct ActiveGuard {
		RawCommitCoordinator &coord;
		const string &target;
		~ActiveGuard() {
			lock_guard<mutex> g(coord.lock);
			auto it = coord.active.find(target);
			if (it != coord.active.end() && --it->second == 0) {
				coord.active.erase(it);
			}
		}
	} active_guard {coord, target};

	coord.pending[target].push_back(item);
	coord.cv.notify_all(); // wake a lingering leader if the batch just grew
	if (coord.committing.count(target) > 0) {
		// someone else is already leading a batch for this table: wait for
		// OUR item to be resolved by that (or a subsequent) leader round
		coord.cv.wait(guard, [&] { return item->done; });
	} else {
		coord.committing.insert(target);
		// Only linger if a sibling request for this table is *already*
		// under way (active_at_entry > 1, counting ourselves): a lone,
		// uncontended request has nothing to batch with, and waiting here
		// regardless was a measured ~7x latency regression for that common
		// case (single-digit ms -> ~30ms per request) for no throughput
		// benefit — nobody was going to join anyway. When contention is
		// real, wake immediately once a decent batch has formed rather
		// than always paying the full window, bounded so a slow-to-arrive
		// burst still resolves promptly.
		if (active_at_entry > 1) {
			constexpr idx_t LINGER_TARGET_BATCH = 3;
			coord.cv.wait_for(guard, std::chrono::milliseconds(25),
			                  [&] { return coord.pending[target].size() >= LINGER_TARGET_BATCH; });
		}
		while (!coord.pending[target].empty()) {
			auto batch = std::move(coord.pending[target]);
			coord.pending[target].clear();
			guard.unlock();

			// merge every queued request's payload into one combined ingest.
			// A representative payload TEXT (NDJSON: one request's text per
			// line) rides along for the non-native (DuckLake) fallback path,
			// which re-parses raw text via raw_records() rather than reusing
			// the already-parsed structure — an empty string there would
			// silently insert zero rows for a merged batch.
			shared_ptr<RawParsedPayload> combined = batch.front()->parsed;
			string combined_text = batch.front()->payload_text;
			for (idx_t i = 1; i < batch.size(); i++) {
				MergeParsedPayloads(*combined, std::move(*batch[i]->parsed));
				combined_text += "\n";
				combined_text += batch[i]->payload_text;
			}

			RawIngestStats batch_stats;
			bool ok = true;
			string error_message;
			try {
				batch_stats = RawIngestSerialized(conn, target, combined, combined_text, options);
			} catch (std::exception &ex) {
				ok = false;
				error_message = ErrorData(ex).RawMessage();
				// RawIngestSerialized always leaves a failed attempt's
				// transaction open for the caller to roll back exactly once
				// (matching every other ingest error path) — this IS that
				// caller now that the leader owns the whole batch's commit.
				RawRollbackTransaction(*conn.context);
			}
			for (auto &b : batch) {
				b->done = true;
				b->failed = !ok;
				b->error_message = error_message;
				if (ok) {
					// aggregate stats (created/columns_added/columns_widened)
					// are batch-wide by nature; rows/errors are reported
					// per-request against each request's own input, matching
					// what a non-batched call would have returned.
					b->stats = batch_stats;
					b->stats.rows = b->rows;
					b->stats.errors = b->parse_errors;
				}
			}

			guard.lock();
			coord.cv.notify_all();
			// loop: more requests may have queued up while we were
			// committing: drain them too instead of waking a new leader.
		}
		coord.committing.erase(target);
	}
	guard.unlock();

	if (item->failed) {
		throw InvalidInputException(item->error_message);
	}
	return item->stats;
}

// Streaming handle over RawIngestor for the INSERT-syntax path
namespace {
class RawStreamIngestorImpl : public RawStreamIngestor {
public:
	RawStreamIngestorImpl(ClientContext &context, const string &target, RawParseOptions options)
	    : parse_options(options), ingestor(context, target, std::move(options)) {
	}
	void Ingest(const string &payload) override {
		ingestor.Ingest(payload);
	}
	void IngestParsedConcurrent(shared_ptr<RawParsedPayload> parsed) override {
		ingestor.errors += parsed->payload.parse_errors;
		bool use_pool = false;
		{
			lock_guard<mutex> guard(schema_lock);
			auto table = ingestor.LookupTableForAppend();
			if (!table || parsed->columns.empty()) {
				ingestor.IngestParsed(std::move(parsed), empty_payload);
				return;
			}
			use_pool = ingestor.PrepareForAppend(*parsed);
		}
		if (use_pool) {
			ingestor.SubmitPreparedBatch(std::move(parsed), true);
			return;
		}
		lock_guard<mutex> guard(schema_lock);
		ingestor.SubmitPreparedBatch(std::move(parsed), false);
	}
	void Finish() override {
		lock_guard<mutex> guard(schema_lock);
		ingestor.Finish();
	}
	idx_t Rows() const override {
		return ingestor.rows;
	}
	RawIngestStats GetStats() const override {
		RawIngestStats stats;
		stats.created = ingestor.created;
		stats.columns_added = ingestor.columns_added;
		stats.columns_widened = ingestor.columns_widened;
		stats.rows = ingestor.rows;
		stats.errors = ingestor.errors;
		return stats;
	}

private:
	RawParseOptions parse_options;
	string empty_payload;
	mutex schema_lock;
	RawIngestor ingestor;
};
} // namespace

unique_ptr<RawStreamIngestor> RawCreateStreamIngestor(ClientContext &context, const string &target,
                                                      RawParseOptions options) {
	return make_uniq<RawStreamIngestorImpl>(context, target, std::move(options));
}

//===--------------------------------------------------------------------===//
// raw_ingest(table, payload)
//===--------------------------------------------------------------------===//

struct RawIngestBindData : public TableFunctionData {
	string target;
	string payload;
	RawParseOptions options;
	// raw_ingest_file
	string path;
	idx_t batch_size = 0;
};

struct RawIngestState : public GlobalTableFunctionState {
	bool done = false;
};

static void SetIngestSchema(vector<LogicalType> &return_types, vector<Identifier> &names) {
	return_types = {LogicalType::VARCHAR, LogicalType::BOOLEAN, LogicalType::BIGINT,
	                LogicalType::BIGINT,  LogicalType::BIGINT,  LogicalType::BIGINT};
	names = {"table", "created", "columns_added", "columns_widened", "rows", "errors"};
}

static unique_ptr<FunctionData> RawIngestBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<RawIngestBindData>();
	if (input.inputs[0].IsNull() || input.inputs[1].IsNull()) {
		throw InvalidInputException("RawDuck: raw_ingest(table, payload) arguments may not be NULL");
	}
	result->target = input.inputs[0].GetValue<string>();
	result->payload = input.inputs[1].GetValue<string>();
	result->options = RawBindParseOptions(context, input.named_parameters);
	SetIngestSchema(return_types, names);
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> RawIngestInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<RawIngestState>();
}

// Emits the six shared result columns. Callers own the cardinality: raw_ingest_file
// appends its extra `batches` column afterwards, so the chunk is only complete
// (and only verifiable) once every column has been written.
static void EmitIngestRow(DataChunk &output, const string &target, const RawIngestStats &stats) {
	output.data[0].Append(Value(target));
	output.data[1].Append(Value::BOOLEAN(stats.created));
	output.data[2].Append(Value::BIGINT(NumericCast<int64_t>(stats.columns_added)));
	output.data[3].Append(Value::BIGINT(NumericCast<int64_t>(stats.columns_widened)));
	output.data[4].Append(Value::BIGINT(NumericCast<int64_t>(stats.rows)));
	output.data[5].Append(Value::BIGINT(NumericCast<int64_t>(stats.errors)));
}

static void EmitIngestRow(DataChunk &output, const string &target, const RawIngestor &ingestor) {
	RawIngestStats stats;
	stats.created = ingestor.created;
	stats.columns_added = ingestor.columns_added;
	stats.columns_widened = ingestor.columns_widened;
	stats.rows = ingestor.rows;
	stats.errors = ingestor.errors;
	EmitIngestRow(output, target, stats);
}

static void RawIngestFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<RawIngestBindData>();
	auto &state = data.global_state->Cast<RawIngestState>();
	if (state.done) {
		output.SetChildCardinality(0);
		return;
	}
	state.done = true;

	if (RawAsyncEnabled(context)) {
		// fire-and-forget: enqueue and return; the background flusher ingests
		RawAsyncEnqueue(context, bind_data.target, bind_data.payload, bind_data.options);
		EmitIngestRow(output, bind_data.target, RawIngestStats());
		output.CheckCardinality(1);
		return;
	}
	RawIngestor ingestor(context, bind_data.target, bind_data.options);
	ingestor.Ingest(bind_data.payload);
	ingestor.Finish();
	EmitIngestRow(output, bind_data.target, ingestor);
	output.CheckCardinality(1);
}

TableFunction GetRawIngestFunction() {
	TableFunction function("raw_ingest", {LogicalType::VARCHAR, LogicalType::VARCHAR}, RawIngestFunction, RawIngestBind,
	                       RawIngestInit);
	RawAddIngestParameters(function);
	return function;
}

//===--------------------------------------------------------------------===//
// raw_ingest_file(table, path): streaming NDJSON ingestion
//===--------------------------------------------------------------------===//

static constexpr idx_t RAW_DEFAULT_BATCH_SIZE = 30000;
static constexpr idx_t RAW_READ_BUFFER_SIZE = 16ULL * 1024ULL * 1024ULL;
// A batch is closed at whichever comes first: batch_size *lines* or this many
// *bytes*. The byte cap is what keeps the parse threads fed when each NDJSON
// line is a fat container that explodes into many records (OTLP export
// envelopes, CloudWatch log groups, ...): without it the whole file can land in
// a single line-count batch and parsing serializes onto one thread. Sized well
// above a flat-record batch (30k records of typical JSON) so plain NDJSON still
// batches by line count and schema-evolution rounds are unaffected.
static constexpr idx_t RAW_BATCH_BYTE_TARGET = 8ULL * 1024ULL * 1024ULL;

static unique_ptr<FunctionData> RawIngestFileBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<RawIngestBindData>();
	if (input.inputs[0].IsNull() || input.inputs[1].IsNull()) {
		throw InvalidInputException("RawDuck: raw_ingest_file(table, path) arguments may not be NULL");
	}
	result->target = input.inputs[0].GetValue<string>();
	result->path = input.inputs[1].GetValue<string>();
	result->options = RawBindParseOptions(context, input.named_parameters);
	result->batch_size = RAW_DEFAULT_BATCH_SIZE;
	auto batch_entry = input.named_parameters.find("batch_size");
	if (batch_entry != input.named_parameters.end() && !batch_entry->second.IsNull()) {
		auto batch_size = batch_entry->second.GetValue<int64_t>();
		if (batch_size <= 0) {
			throw InvalidInputException("RawDuck: batch_size must be positive");
		}
		result->batch_size = NumericCast<idx_t>(batch_size);
	}
	SetIngestSchema(return_types, names);
	return_types.push_back(LogicalType::BIGINT);
	names.push_back("batches");
	return std::move(result);
}

// Bounded handoff between the parse thread and the appending thread: parsing
// and inference are pure and overlap with catalog/storage work.
struct RawIngestPipeline {
	explicit RawIngestPipeline(idx_t capacity_p) : capacity(capacity_p) {
	}

	struct Item {
		shared_ptr<RawParsedPayload> parsed;
		string payload;
	};

	idx_t capacity;

	mutex lock;
	std::condition_variable producer_cv;
	std::condition_variable consumer_cv;
	std::condition_variable parser_cv;
	// raw NDJSON batches awaiting parsing
	std::deque<string> raw_queue;
	// parsed batches awaiting ingestion
	std::deque<Item> queue;
	idx_t active_parsers = 0;
	bool reader_done = false;
	bool aborted = false;
	std::exception_ptr error;

	void PushRaw(string batch) {
		std::unique_lock<mutex> guard(lock);
		producer_cv.wait(guard, [&] { return raw_queue.size() < capacity || aborted; });
		if (aborted) {
			return;
		}
		raw_queue.push_back(std::move(batch));
		parser_cv.notify_one();
	}
	bool PopRaw(string &batch) {
		std::unique_lock<mutex> guard(lock);
		parser_cv.wait(guard, [&] { return !raw_queue.empty() || reader_done || aborted; });
		if (raw_queue.empty() || aborted) {
			return false;
		}
		batch = std::move(raw_queue.front());
		raw_queue.pop_front();
		producer_cv.notify_one();
		return true;
	}
	void Push(Item item) {
		std::unique_lock<mutex> guard(lock);
		producer_cv.wait(guard, [&] { return queue.size() < capacity || aborted; });
		if (aborted) {
			return;
		}
		queue.push_back(std::move(item));
		consumer_cv.notify_one();
	}
	bool Pop(Item &item) {
		std::unique_lock<mutex> guard(lock);
		consumer_cv.wait(guard, [&] { return !queue.empty() || aborted || (reader_done && active_parsers == 0); });
		if (aborted || queue.empty()) {
			return false;
		}
		item = std::move(queue.front());
		queue.pop_front();
		producer_cv.notify_one();
		return true;
	}
	void ReaderDone(std::exception_ptr reader_error) {
		std::unique_lock<mutex> guard(lock);
		if (reader_error && !error) {
			error = reader_error;
		}
		reader_done = true;
		parser_cv.notify_all();
		consumer_cv.notify_all();
	}
	void ParserDone(std::exception_ptr parser_error) {
		std::unique_lock<mutex> guard(lock);
		if (parser_error && !error) {
			error = parser_error;
			aborted = true;
			parser_cv.notify_all();
			producer_cv.notify_all();
		}
		active_parsers--;
		consumer_cv.notify_all();
	}
	void Abort() {
		std::unique_lock<mutex> guard(lock);
		aborted = true;
		reader_done = true;
		parser_cv.notify_all();
		producer_cv.notify_all();
	}
};

static void RawIngestFileFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<RawIngestBindData>();
	auto &state = data.global_state->Cast<RawIngestState>();
	if (state.done) {
		output.SetChildCardinality(0);
		return;
	}
	state.done = true;

	auto &fs = FileSystem::GetFileSystem(context);
	auto handle = fs.OpenFile(bind_data.path, FileOpenFlags(FileFlags::FILE_FLAGS_READ) |
	                                              FileOpenFlags(FileCompressionType::AUTO_DETECT));

	auto write_settings = RawWriteSettings::Get(context);
	auto batch_size = bind_data.batch_size;
	RawIngestPipeline pipeline(write_settings.pipeline_depth);
	auto options = bind_data.options;

	// reader thread: owns the file handle, splits NDJSON into raw batches
	std::thread reader([&pipeline, &handle, batch_size] {
		std::exception_ptr reader_error;
		try {
			auto buffer = make_unsafe_uniq_array_uninitialized<char>(RAW_READ_BUFFER_SIZE);
			string pending;
			idx_t pending_lines = 0;
			auto emit = [&](string batch) {
				if (batch.find_first_not_of(" \t\r\n") == string::npos) {
					return;
				}
				pipeline.PushRaw(std::move(batch));
			};
			while (true) {
				auto read = handle->Read(buffer.get(), RAW_READ_BUFFER_SIZE);
				if (read <= 0) {
					break;
				}
				for (int64_t i = 0; i < read; i++) {
					if (buffer[i] == '\n') {
						pending_lines++;
					}
				}
				pending.append(buffer.get(), NumericCast<idx_t>(read));
				// close a batch on the line cap, or earlier on the byte cap when
				// lines are fat (so OTLP/container payloads still parallelize)
				while (pending_lines >= batch_size || (pending.size() >= RAW_BATCH_BYTE_TARGET && pending_lines > 0)) {
					idx_t take = MinValue<idx_t>(pending_lines, batch_size);
					idx_t split = 0;
					for (idx_t line = 0; line < take; line++) {
						split = pending.find('\n', split) + 1;
					}
					// batches run several MB: copy the (small) unconsumed tail and move
					// the (large) batch prefix out via resize-then-move, instead of
					// copying the large prefix via substr on every batch boundary.
					if (split >= pending.size()) {
						emit(std::move(pending));
						pending.clear();
					} else {
						string remainder(pending.data() + split, pending.size() - split);
						pending.resize(split);
						emit(std::move(pending));
						pending = std::move(remainder);
					}
					pending_lines -= take;
				}
			}
			emit(std::move(pending));
		} catch (...) {
			reader_error = std::current_exception();
		}
		pipeline.ReaderDone(reader_error);
	});

	// parse workers: parsing and inference are pure and scale with cores;
	// batch order is irrelevant to ingestion
	auto parser_count = write_settings.PipelineThreadCount();
	pipeline.active_parsers = parser_count;
	vector<std::thread> parsers;
	for (idx_t i = 0; i < parser_count; i++) {
		parsers.emplace_back([&pipeline, options] {
			std::exception_ptr parser_error;
			try {
				string batch;
				while (pipeline.PopRaw(batch)) {
					auto parsed = RawParsedPayload::Process(batch, options);
					pipeline.Push(RawIngestPipeline::Item {std::move(parsed), std::move(batch)});
				}
			} catch (...) {
				parser_error = std::current_exception();
			}
			pipeline.ParserDone(parser_error);
		});
	}
	auto join_all = [&] {
		reader.join();
		for (auto &parser : parsers) {
			parser.join();
		}
	};

	auto consumer_count = write_settings.PipelineConsumerCount();
	std::atomic<idx_t> batches {0};
	mutex consumer_error_lock;
	std::exception_ptr consumer_error;

	auto coalesce_and_ingest = [&](auto ingest_fn, RawIngestPipeline::Item &pending_storage, bool &has_pending,
	                               RawIngestPipeline::Item item) {
		if (!has_pending) {
			pending_storage = std::move(item);
			has_pending = true;
			return;
		}
		auto pending_shape = HashPayloadShape(pending_storage.parsed->columns);
		auto item_shape = HashPayloadShape(item.parsed->columns);
		if (pending_shape == item_shape &&
		    pending_storage.parsed->payload.rows.size() + item.parsed->payload.rows.size() <
		        write_settings.pool_min_rows) {
			MergeParsedPayloads(*pending_storage.parsed, std::move(*item.parsed));
			return;
		}
		ingest_fn(std::move(pending_storage));
		pending_storage = std::move(item);
		has_pending = true;
	};

	idx_t batches_result = 0;
	try {
		if (consumer_count > 1) {
			auto stream = RawCreateStreamIngestor(context, bind_data.target, bind_data.options);
			auto consumer_loop = [&] {
				try {
					RawIngestPipeline::Item pending_storage;
					bool has_pending = false;
					RawIngestPipeline::Item item;
					while (pipeline.Pop(item)) {
						batches.fetch_add(1);
						coalesce_and_ingest(
						    [&](RawIngestPipeline::Item pending) {
							    stream->IngestParsedConcurrent(std::move(pending.parsed));
						    },
						    pending_storage, has_pending, std::move(item));
					}
					if (has_pending) {
						stream->IngestParsedConcurrent(std::move(pending_storage.parsed));
					}
				} catch (...) {
					lock_guard<mutex> guard(consumer_error_lock);
					if (!consumer_error) {
						consumer_error = std::current_exception();
					}
					pipeline.Abort();
				}
			};
			vector<std::thread> consumers;
			consumers.reserve(consumer_count);
			for (idx_t i = 0; i < consumer_count; i++) {
				consumers.emplace_back(consumer_loop);
			}
			for (auto &consumer : consumers) {
				consumer.join();
			}
			if (consumer_error) {
				std::rethrow_exception(consumer_error);
			}
			stream->Finish();
			batches_result = batches.load();
			EmitIngestRow(output, bind_data.target, stream->GetStats());
		} else {
			RawIngestor ingestor(context, bind_data.target, bind_data.options);
			RawIngestPipeline::Item pending_storage;
			bool has_pending = false;
			RawIngestPipeline::Item item;
			while (pipeline.Pop(item)) {
				batches_result++;
				coalesce_and_ingest(
				    [&](RawIngestPipeline::Item pending) {
					    ingestor.IngestParsed(std::move(pending.parsed), pending.payload);
				    },
				    pending_storage, has_pending, std::move(item));
			}
			if (has_pending) {
				ingestor.IngestParsed(std::move(pending_storage.parsed), pending_storage.payload);
			}
			ingestor.Finish();
			EmitIngestRow(output, bind_data.target, ingestor);
			output.data[6].Append(Value::BIGINT(NumericCast<int64_t>(batches_result)));
		}
	} catch (...) {
		pipeline.Abort();
		join_all();
		throw;
	}
	join_all();
	if (pipeline.error) {
		std::rethrow_exception(pipeline.error);
	}
	if (consumer_count > 1) {
		output.data[6].Append(Value::BIGINT(NumericCast<int64_t>(batches_result)));
	}
	output.CheckCardinality(1);
	return;
}

TableFunction GetRawIngestFileFunction() {
	TableFunction function("raw_ingest_file", {LogicalType::VARCHAR, LogicalType::VARCHAR}, RawIngestFileFunction,
	                       RawIngestFileBind, RawIngestInit);
	function.named_parameters["batch_size"] = LogicalType::BIGINT;
	RawAddIngestParameters(function);
	return function;
}

} // namespace duckdb
