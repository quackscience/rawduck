#include "raw_functions.hpp"
#include "raw_json.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/enums/database_modification_type.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/keyword_helper.hpp"
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
	if (IsRawJSONType(type)) {
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
	auto existing_json = IsRawJSONType(existing);
	auto incoming_json = IsRawJSONType(incoming);
	if (existing_json || incoming_json) {
		return LogicalType::JSON();
	}
	if (existing.id() == LogicalTypeId::LIST && incoming.id() == LogicalTypeId::LIST) {
		return LogicalType::LIST(JoinColumnTypes(ListType::GetChildType(existing), ListType::GetChildType(incoming)));
	}
	if (existing.id() == LogicalTypeId::LIST || incoming.id() == LogicalTypeId::LIST) {
		if (!IsPlainScalar(existing) && !IsPlainScalar(incoming)) {
			return existing;
		}
		return LogicalType::JSON();
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
	return KeywordHelper::WriteQuoted(name, '"');
}

static string QuoteLiteral(const string &value) {
	return KeywordHelper::WriteQuoted(value, '\'');
}

string RawQualifiedTarget(const string &target) {
	auto qualified = QualifiedName::Parse(target);
	string result;
	if (!qualified.catalog.empty()) {
		result += RawQuoteIdentifier(qualified.catalog) + ".";
	}
	if (!qualified.schema.empty()) {
		result += RawQuoteIdentifier(qualified.schema) + ".";
	}
	result += RawQuoteIdentifier(qualified.name);
	return result;
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
	RawAppendPool(ClientContext &context, TableCatalogEntry &table, idx_t worker_count, bool overlap_flush)
	    : context(context), block_manager_ref(table.GetStorage().GetTableIOManager().GetBlockManagerForRowData()),
	      flush_storage(&table.GetStorage()), overlap_flush(overlap_flush), types(table.GetTypes()) {
		auto &storage = table.GetStorage();
		idx_t physical_index = 0;
		for (auto &column : table.GetColumns().Physical()) {
			slots[column.Name()] = physical_index++;
		}
		workers = vector<Worker>(worker_count);
		for (auto &worker : workers) {
			worker.local_types = types;
			worker.local_slots = slots;
			worker.writer = make_uniq<OptimisticDataWriter>(context, storage);
			auto collection = worker.writer->CreateCollection(storage, types);
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
		producer_cv.wait(guard, [&] { return queue.size() < 4 || stopped; });
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
	}

	// stops the workers and merges their collections; returns appended rows.
	// ALTERs swap the table's storage object, so the merge target must be
	// the CURRENT catalog entry, not the one captured at pool creation.
	idx_t Drain(TableCatalogEntry &current_table) {
		auto &storage = current_table.GetStorage();
		Stop();
		if (error) {
			std::rethrow_exception(error);
		}
		idx_t total = 0;
		auto merge_threshold = storage.GetRowGroupSize() / 8;
		// phase 1, parallel: finalize and flush each worker's collection.
		// Writers capture the table's storage for compression metadata, so
		// they are rebuilt against the CURRENT (possibly evolved) storage.
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
			flushers.emplace_back([&worker, merge_threshold] {
				auto &collection = *worker.collection->collection;
				collection.FinalizeAppend(TransactionData(0, 0), *worker.append_state);
				if (collection.GetTotalRows() >= merge_threshold) {
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
				storage.InitializeLocalAppend(append_state, current_table, context, no_constraints);
				auto &transaction = DuckTransaction::Get(context, current_table.catalog);
				for (auto &chunk : collection.Chunks(transaction)) {
					storage.LocalAppend(append_state, context, chunk, false);
				}
				storage.FinalizeLocalAppend(append_state);
			} else {
				storage.LocalMerge(context, *worker.collection);
				storage.GetOptimisticWriter(context).Merge(*worker.writer);
			}
		}
		workers.clear();
		return total;
	}

private:
	struct Worker {
		unique_ptr<OptimisticDataWriter> writer;
		optional_ptr<OptimisticWriteCollection> collection;
		unique_ptr<TableAppendState> append_state;
		// this worker's collection layout: a prefix of the published schema
		vector<LogicalType> local_types;
		case_insensitive_map_t<idx_t> local_slots;
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
	// work through RowGroupCollection::AddColumn. The worker may have already
	// flushed complete row groups to disk (incremental WriteNewRowGroup);
	// AddColumn extends those checkpointed row groups too (existing columns
	// stay on disk, the new column is materialized NULL in memory). The flush
	// writer is then rebuilt against the post-ALTER storage so subsequent
	// WriteNewRowGroup calls see the evolved column/compression layout, while
	// the partial blocks written so far are carried forward via Merge.
	void ExtendWorker(Worker &worker, const vector<pair<string, LogicalType>> &new_columns, DataTable &flush_storage) {
		worker.collection->collection->FinalizeAppend(TransactionData(0, 0), *worker.append_state);
		for (auto &new_column : new_columns) {
			ColumnDefinition definition(new_column.first, new_column.second);
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
		vector<idx_t> slot_of(parsed.columns.size());
		vector<bool> covered(types.size(), false);
		for (idx_t col = 0; col < parsed.columns.size(); col++) {
			auto entry = slots.find(parsed.columns[col].name);
			if (entry == slots.end()) {
				throw InternalException("RawDuck: column %s missing from append pool schema", parsed.columns[col].name);
			}
			slot_of[col] = entry->second;
			covered[entry->second] = true;
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
			chunk.SetCardinality(count);
			// flush completed row groups to disk as we go, overlapping parse +
			// compression + I/O instead of deferring every byte to drain. The
			// writer targets the current storage layout; schema evolution
			// rebuilds it (ExtendWorker) so AddColumn stays safe across flushes.
			bool row_group_complete = collection_of(worker).Append(chunk, *worker.append_state);
			if (row_group_complete && allow_flush) {
				worker.writer->WriteNewRowGroup(*worker.collection);
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
		qname = QualifiedName::Parse(target);
		if (qname.catalog.empty() && !qname.schema.empty()) {
			// two-part names: the first part may be a catalog (raw.events)
			if (DatabaseManager::Get(context).GetDatabase(context, qname.schema)) {
				qname.catalog = qname.schema;
				qname.schema = string();
			}
		}
		if (qname.catalog.empty()) {
			qname.catalog = DatabaseManager::GetDefaultDatabase(context);
		}
		if (qname.schema.empty()) {
			qname.schema = DEFAULT_SCHEMA;
		}
		auto &catalog = Catalog::GetCatalog(context, qname.catalog);
		native = catalog.IsDuckCatalog();
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
		EntryLookupInfo lookup_info(CatalogType::TABLE_ENTRY, qname.name);
		auto entry = Catalog::GetEntry(context, qname.catalog, qname.schema, lookup_info, OnEntryNotFound::RETURN_NULL);
		if (!entry) {
			return nullptr;
		}
		return &entry->Cast<TableCatalogEntry>();
	}

	// classifies the DDL this payload would require
	void ComputeDelta(TableCatalogEntry &table, RawParsedPayload &parsed, vector<pair<string, LogicalType>> &adds,
	                  bool &widens) {
		auto &existing_columns = table.GetColumns();
		for (auto &column : parsed.columns) {
			if (!existing_columns.ColumnExists(column.name)) {
				adds.emplace_back(column.name, column.type);
				continue;
			}
			auto &existing = existing_columns.GetColumn(column.name);
			if (JoinColumnTypes(existing.Type(), column.type) != existing.Type()) {
				widens = true;
			}
		}
	}

	bool PoolEligible(TableCatalogEntry &table, RawParsedPayload &parsed) {
		if (parsed.payload.rows.size() < 4096) {
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

	void IngestNative(shared_ptr<RawParsedPayload> parsed_ptr) {
		auto &parsed = *parsed_ptr;
		auto table = LookupTable();
		if (!table || parsed.columns.empty()) {
			LegacyIngestNative(std::move(parsed_ptr), table);
			return;
		}
		auto cache_key = qname.catalog + "." + qname.schema + "." + qname.name;
		auto shape = HashPayloadShape(parsed.columns);
		auto &cache = *ObjectCache::GetObjectCache(context).GetOrCreate<RawSchemaCache>(RawSchemaCache::ObjectType());
		bool absorbed;
		{
			lock_guard<mutex> guard(cache.lock);
			auto entry = cache.tables.find(cache_key);
			absorbed = entry != cache.tables.end() && entry->second.storage_token == &table->GetStorage() &&
			           entry->second.absorbed_shapes.count(shape) > 0;
		}
		if (!absorbed) {
			// schema-delta slow path, then absorb the shape
			auto &catalog = Catalog::GetCatalog(context, qname.catalog);
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
			table = LookupTable();
			if (!table) {
				throw InternalException("RawDuck: table %s disappeared during ingestion", target);
			}
			if (pool && !adds.empty()) {
				// publish against the post-ALTER storage so workers rebuild
				// their flush writers to the evolved layout
				pool->PublishColumns(adds, table->GetStorage());
			}
			lock_guard<mutex> guard(cache.lock);
			auto &entry = cache.tables[cache_key];
			if (entry.storage_token != &table->GetStorage()) {
				entry.storage_token = &table->GetStorage();
				entry.absorbed_shapes.clear();
			}
			entry.absorbed_shapes.insert(shape);
		}
		if (parsed.payload.rows.empty()) {
			return;
		}
		MetaTransaction::Get(context).ModifyDatabase(Catalog::GetCatalog(context, qname.catalog).GetAttached(),
		                                             DatabaseModificationType::INSERT_DATA);
		if (!pool && PoolEligible(*table, parsed)) {
			auto worker_count = MaxValue<idx_t>(1, MinValue<idx_t>(std::thread::hardware_concurrency() / 2, 4));
			pool = make_uniq<RawAppendPool>(context, *table, worker_count, OverlapFlushEnabled());
		}
		if (pool) {
			pool->Submit(std::move(parsed_ptr));
			return;
		}
		AppendNative(*table, parsed);
	}

	// rawduck_overlap_flush: opt-in parse/flush overlap for the append pool
	bool OverlapFlushEnabled() {
		Value enabled;
		return context.TryGetCurrentSetting("rawduck_overlap_flush", enabled) && !enabled.IsNull() &&
		       enabled.GetValue<bool>();
	}

	void LegacyIngestNative(shared_ptr<RawParsedPayload> parsed_ptr, optional_ptr<TableCatalogEntry> table) {
		auto &parsed = *parsed_ptr;
		if (!table && parsed.columns.empty()) {
			throw InvalidInputException("RawDuck: cannot create table %s from an empty payload", target);
		}
		if (parsed.columns.empty()) {
			return;
		}
		auto &catalog = Catalog::GetCatalog(context, qname.catalog);
		// the binder marks modified databases for write statements; we write
		// from within a table function, so mark the target ourselves
		MetaTransaction::Get(context).ModifyDatabase(
		    catalog.GetAttached(), DatabaseModificationType::CREATE_CATALOG_ENTRY |
		                               DatabaseModificationType::ALTER_TABLE | DatabaseModificationType::INSERT_DATA);
		if (!table) {
			CreateNative(catalog, parsed);
			created = true;
			columns_added += parsed.columns.size();
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
		table = LookupTable();
		if (!table) {
			throw InternalException("RawDuck: table %s disappeared during ingestion", target);
		}
		if (!pool && PoolEligible(*table, parsed)) {
			auto worker_count = MaxValue<idx_t>(1, MinValue<idx_t>(std::thread::hardware_concurrency() / 2, 4));
			pool = make_uniq<RawAppendPool>(context, *table, worker_count, OverlapFlushEnabled());
		}
		if (pool) {
			pool->Submit(std::move(parsed_ptr));
			return;
		}
		AppendNative(*table, parsed);
	}

	void CreateNative(Catalog &catalog, RawParsedPayload &parsed) {
		auto info = make_uniq<CreateTableInfo>(qname.catalog, qname.schema, qname.name);
		for (auto &column : parsed.columns) {
			info->columns.AddColumn(ColumnDefinition(column.name, column.type));
		}
		auto &schema_entry = catalog.GetSchema(context, qname.schema);
		auto binder = Binder::CreateBinder(context);
		auto bound_info = binder->BindCreateTableInfo(std::move(info), schema_entry);
		catalog.CreateTable(context, *bound_info);
	}

	void EvolveNative(Catalog &catalog, TableCatalogEntry &table, RawParsedPayload &parsed) {
		auto &existing_columns = table.GetColumns();
		for (auto &column : parsed.columns) {
			AlterEntryData data(qname.catalog, qname.schema, qname.name, OnEntryNotFound::THROW_EXCEPTION);
			if (!existing_columns.ColumnExists(column.name)) {
				AddColumnInfo add_column(std::move(data), ColumnDefinition(column.name, column.type), false);
				catalog.Alter(context, add_column);
				columns_added++;
				continue;
			}
			auto &existing = existing_columns.GetColumn(column.name);
			auto target_type = JoinColumnTypes(existing.Type(), column.type);
			if (target_type == existing.Type()) {
				continue;
			}
			auto column_ref = make_uniq<ColumnRefExpression>(column.name);
			unique_ptr<ParsedExpression> expression;
			if (IsRawJSONType(target_type) && !IsRawJSONType(existing.Type())) {
				// a plain cast to JSON would reject bare strings
				vector<unique_ptr<ParsedExpression>> children;
				children.push_back(std::move(column_ref));
				expression = make_uniq<FunctionExpression>("to_json", std::move(children));
			} else {
				expression = make_uniq<CastExpression>(target_type, std::move(column_ref));
			}
			ChangeColumnTypeInfo change_type(std::move(data), column.name, target_type, std::move(expression));
			catalog.Alter(context, change_type);
			columns_widened++;
		}
	}

	void AppendNative(TableCatalogEntry &table, RawParsedPayload &parsed) {
		auto &columns_list = table.GetColumns();
		if (columns_list.PhysicalColumnCount() != columns_list.LogicalColumnCount()) {
			throw NotImplementedException("RawDuck: cannot ingest into tables with generated columns");
		}
		case_insensitive_map_t<idx_t> table_index;
		idx_t physical_index = 0;
		for (auto &column : columns_list.Physical()) {
			table_index[column.Name()] = physical_index++;
		}
		auto types = table.GetTypes();

		DataChunk chunk;
		chunk.Initialize(Allocator::Get(context), types);
		RawExtractor extractor(*parsed.root, parsed.columns);
		// payload column -> table column slot
		vector<idx_t> slot_of(parsed.columns.size());
		vector<bool> covered(types.size(), false);
		for (idx_t col = 0; col < parsed.columns.size(); col++) {
			auto entry = table_index.find(parsed.columns[col].name);
			if (entry == table_index.end()) {
				throw InternalException("RawDuck: column %s missing after evolution", parsed.columns[col].name);
			}
			slot_of[col] = entry->second;
			covered[entry->second] = true;
		}

		auto binder = Binder::CreateBinder(context);
		auto bound_constraints = binder->BindConstraints(table);
		auto &storage = table.GetStorage();
		auto &payload_rows = parsed.payload.rows;

		// types FillVector writes directly; others go through a cast
		Vector intermediate(LogicalType::VARCHAR);
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
			chunk.SetCardinality(count);
			storage.LocalAppend(table, context, chunk, bound_constraints);
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
			fallback_types[probe->names[col]] = probe->types[col];
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
		// (ALTER ... USING), which JSON-widening needs; when that fails we
		// retry once with widening to JSON disabled, converting incoming
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

	void FallbackBatch(RawParsedPayload &parsed, const string &payload_str, bool allow_json_widening) {
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
				bool needs_rewrite = IsRawJSONType(target_type) && !IsRawJSONType(entry->second);
				if (needs_rewrite && !allow_json_widening) {
					final_types.push_back(entry->second);
					continue;
				}
				string alter = "ALTER TABLE " + qualified + " ALTER COLUMN " + RawQuoteIdentifier(column.name) +
				               " SET DATA TYPE " + target_type.ToString();
				if (needs_rewrite) {
					alter += " USING to_json(" + RawQuoteIdentifier(column.name) + ")";
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
					    IsRawJSONType(columns[i].type) || columns[i].type.id() == LogicalTypeId::LIST;
					if (IsRawJSONType(final_types[i]) && !IsRawJSONType(columns[i].type)) {
						expr = "to_json(" + expr + ")";
					} else if (!IsRawJSONType(final_types[i]) && incoming_structural &&
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
	RawIngestor ingestor(context, target, options);
	ingestor.Ingest(payload);
	ingestor.Finish();
	RawIngestStats stats;
	stats.created = ingestor.created;
	stats.columns_added = ingestor.columns_added;
	stats.columns_widened = ingestor.columns_widened;
	stats.rows = ingestor.rows;
	stats.errors = ingestor.errors;
	return stats;
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
		// parsing already happened on the calling thread; the schema/append
		// handoff serializes and appends fan out through the pool
		lock_guard<mutex> guard(lock);
		ingestor.IngestParsed(std::move(parsed), empty_payload);
	}
	void Finish() override {
		ingestor.Finish();
	}
	idx_t Rows() const override {
		return ingestor.rows;
	}

private:
	RawParseOptions parse_options;
	string empty_payload;
	mutex lock;
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

static void SetIngestSchema(vector<LogicalType> &return_types, vector<string> &names) {
	return_types = {LogicalType::VARCHAR, LogicalType::BOOLEAN, LogicalType::BIGINT,
	                LogicalType::BIGINT,  LogicalType::BIGINT,  LogicalType::BIGINT};
	names = {"table", "created", "columns_added", "columns_widened", "rows", "errors"};
}

static unique_ptr<FunctionData> RawIngestBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
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

static void EmitIngestRow(DataChunk &output, const string &target, const RawIngestor &ingestor) {
	output.SetValue(0, 0, Value(target));
	output.SetValue(1, 0, Value::BOOLEAN(ingestor.created));
	output.SetValue(2, 0, Value::BIGINT(NumericCast<int64_t>(ingestor.columns_added)));
	output.SetValue(3, 0, Value::BIGINT(NumericCast<int64_t>(ingestor.columns_widened)));
	output.SetValue(4, 0, Value::BIGINT(NumericCast<int64_t>(ingestor.rows)));
	output.SetValue(5, 0, Value::BIGINT(NumericCast<int64_t>(ingestor.errors)));
	output.SetCardinality(1);
}

static void RawIngestFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<RawIngestBindData>();
	auto &state = data.global_state->Cast<RawIngestState>();
	if (state.done) {
		output.SetCardinality(0);
		return;
	}
	state.done = true;

	if (RawAsyncEnabled(context)) {
		// fire-and-forget: enqueue and return; the background flusher ingests
		RawAsyncEnqueue(context, bind_data.target, bind_data.payload, bind_data.options);
		output.SetValue(0, 0, Value(bind_data.target));
		output.SetValue(1, 0, Value::BOOLEAN(false));
		output.SetValue(2, 0, Value::BIGINT(0));
		output.SetValue(3, 0, Value::BIGINT(0));
		output.SetValue(4, 0, Value::BIGINT(0));
		output.SetValue(5, 0, Value::BIGINT(0));
		output.SetCardinality(1);
		return;
	}
	RawIngestor ingestor(context, bind_data.target, bind_data.options);
	ingestor.Ingest(bind_data.payload);
	ingestor.Finish();
	EmitIngestRow(output, bind_data.target, ingestor);
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
                                                  vector<LogicalType> &return_types, vector<string> &names) {
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
	struct Item {
		shared_ptr<RawParsedPayload> parsed;
		string payload;
	};
	static constexpr idx_t CAPACITY = 4;

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
		producer_cv.wait(guard, [&] { return raw_queue.size() < CAPACITY || aborted; });
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
		producer_cv.wait(guard, [&] { return queue.size() < CAPACITY || aborted; });
		if (aborted) {
			return;
		}
		queue.push_back(std::move(item));
		consumer_cv.notify_one();
	}
	bool Pop(Item &item) {
		std::unique_lock<mutex> guard(lock);
		consumer_cv.wait(guard, [&] { return !queue.empty() || (reader_done && active_parsers == 0); });
		if (queue.empty()) {
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
		output.SetCardinality(0);
		return;
	}
	state.done = true;

	auto &fs = FileSystem::GetFileSystem(context);
	auto handle = fs.OpenFile(bind_data.path, FileOpenFlags(FileFlags::FILE_FLAGS_READ) |
	                                              FileOpenFlags(FileCompressionType::AUTO_DETECT));

	RawIngestor ingestor(context, bind_data.target, bind_data.options);
	RawIngestPipeline pipeline;
	auto options = bind_data.options;
	auto batch_size = bind_data.batch_size;

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
					emit(pending.substr(0, split));
					pending.erase(0, split);
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
	auto parser_count = MaxValue<idx_t>(1, MinValue<idx_t>(std::thread::hardware_concurrency() / 3, 4));
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

	idx_t batches = 0;
	try {
		RawIngestPipeline::Item item;
		while (pipeline.Pop(item)) {
			ingestor.IngestParsed(std::move(item.parsed), item.payload);
			batches++;
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
	ingestor.Finish();

	EmitIngestRow(output, bind_data.target, ingestor);
	output.SetValue(6, 0, Value::BIGINT(NumericCast<int64_t>(batches)));
}

TableFunction GetRawIngestFileFunction() {
	TableFunction function("raw_ingest_file", {LogicalType::VARCHAR, LogicalType::VARCHAR}, RawIngestFileFunction,
	                       RawIngestFileBind, RawIngestInit);
	function.named_parameters["batch_size"] = LogicalType::BIGINT;
	RawAddIngestParameters(function);
	return function;
}

} // namespace duckdb
