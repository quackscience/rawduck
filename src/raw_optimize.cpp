#include "raw_functions.hpp"
#include "raw_json.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/storage/object_cache.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Predicate statistics, RawMergeTree style: every optimized plan is scanned
// for filters pushed into base table scans and for grouping columns, and the
// (table, column) usage counts feed raw_optimize()'s ORDER BY selection.
//===--------------------------------------------------------------------===//

struct RawColumnUsage {
	idx_t filters = 0;
	idx_t groups = 0;
};

class RawStatsCache : public ObjectCacheEntry {
public:
	static string ObjectType() {
		return "rawduck_filter_stats";
	}
	string GetObjectType() override {
		return ObjectType();
	}
	optional_idx GetEstimatedCacheMemory() const override {
		// statistics must never be evicted
		return optional_idx();
	}

	mutex lock;
	// fully qualified table key -> column name -> observed usage
	map<string, map<string, RawColumnUsage>> usage;
	// fully qualified table key -> sorted group-by column set -> times observed
	map<string, map<string, idx_t>> group_sets;
	struct Projection {
		string catalog;
		string schema;
		string name;
		string group_set;
		// staleness token: the base table's physical row count at materialization
		idx_t base_rows = 0;
	};
	// fully qualified table key -> materialized projection
	map<string, Projection> projections;

	struct OptimizeState {
		string order_by;
		idx_t row_count = 0;
	};
	// fully qualified table key -> last raw_optimize outcome
	map<string, OptimizeState> optimized;
};

static RawStatsCache &GetStatsCache(ClientContext &context) {
	return *ObjectCache::GetObjectCache(context).GetOrCreate<RawStatsCache>(RawStatsCache::ObjectType());
}

static string TableKey(const TableCatalogEntry &table) {
	return StringUtil::Lower(table.ParentCatalog().GetName()) + "." + StringUtil::Lower(table.ParentSchema().name) +
	       "." + StringUtil::Lower(table.name);
}

// Resolves a scan output column (binding into column_ids) back to its name.
static bool ResolveGetColumn(const LogicalGet &get, idx_t column_index, string &name) {
	auto &column_ids = get.GetColumnIds();
	if (column_index >= column_ids.size()) {
		return false;
	}
	auto table_column = column_ids[column_index].GetPrimaryIndex();
	if (table_column >= get.names.size()) {
		return false;
	}
	name = get.names[table_column];
	return true;
}

static void CollectGets(LogicalOperator &op, unordered_map<idx_t, optional_ptr<LogicalOperator>> &operators) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		operators[op.Cast<LogicalGet>().table_index] = &op;
	} else if (op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
		operators[op.Cast<LogicalProjection>().table_index] = &op;
	}
	for (auto &child : op.children) {
		CollectGets(*child, operators);
	}
}

// Traces a column binding through projections down to a base table scan.
static optional_ptr<LogicalGet> TraceBinding(unordered_map<idx_t, optional_ptr<LogicalOperator>> &operators,
                                             ColumnBinding &binding) {
	for (idx_t hops = 0; hops < 16; hops++) {
		auto entry = operators.find(binding.table_index);
		if (entry == operators.end() || !entry->second) {
			return nullptr;
		}
		auto &op = *entry->second;
		if (op.type == LogicalOperatorType::LOGICAL_GET) {
			return op.Cast<LogicalGet>();
		}
		auto &projection = op.Cast<LogicalProjection>();
		if (binding.column_index >= projection.expressions.size()) {
			return nullptr;
		}
		auto expression = projection.expressions[binding.column_index].get();
		// unwrap compressed-materialization wrappers (__internal_compress_*)
		if (expression->GetExpressionType() == ExpressionType::BOUND_FUNCTION) {
			auto &function = expression->Cast<BoundFunctionExpression>();
			if (StringUtil::StartsWith(function.function.name, "__internal_") && !function.children.empty()) {
				expression = function.children[0].get();
			}
		}
		if (expression->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
			return nullptr;
		}
		binding = expression->Cast<BoundColumnRefExpression>().binding;
	}
	return nullptr;
}

static void CollectStats(ClientContext &context, LogicalOperator &op,
                         unordered_map<idx_t, optional_ptr<LogicalOperator>> &operators) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.Cast<LogicalGet>();
		auto table = get.GetTable();
		if (table && !get.table_filters.filters.empty()) {
			auto key = TableKey(*table);
			auto &stats = GetStatsCache(context);
			lock_guard<mutex> guard(stats.lock);
			for (auto &filter : get.table_filters.filters) {
				// table filters are keyed by the table's logical column index,
				// and LogicalGet::names covers the full table schema
				if (filter.first >= get.names.size()) {
					continue;
				}
				stats.usage[key][get.names[filter.first]].filters++;
			}
		}
	} else if (op.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		// grouping columns that reference a base table scan directly
		auto &aggregate = op.Cast<LogicalAggregate>();
		map<string, vector<string>> table_group_sets;
		for (auto &group : aggregate.groups) {
			if (group->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
				continue;
			}
			auto binding = group->Cast<BoundColumnRefExpression>().binding;
			auto resolved = TraceBinding(operators, binding);
			if (!resolved) {
				continue;
			}
			auto &get = *resolved;
			auto table = get.GetTable();
			string column_name;
			if (!table || !ResolveGetColumn(get, binding.column_index, column_name)) {
				continue;
			}
			auto &stats = GetStatsCache(context);
			lock_guard<mutex> guard(stats.lock);
			stats.usage[TableKey(*table)][column_name].groups++;
			table_group_sets[TableKey(*table)].push_back(column_name);
		}
		for (auto &entry : table_group_sets) {
			auto columns = entry.second;
			std::sort(columns.begin(), columns.end());
			auto &stats = GetStatsCache(context);
			lock_guard<mutex> guard(stats.lock);
			stats.group_sets[entry.first][StringUtil::Join(columns, ", ")]++;
		}
	}
	for (auto &child : op.children) {
		CollectStats(context, *child, operators);
	}
}

//===--------------------------------------------------------------------===//
// Automatic aggregate rewriting onto materialized projections
//
// Runs pre-optimization (clean bound plans): PROJECTION -> AGGREGATE
// (count(*)) -> GET(base table) becomes an aggregation over the projection
// table with CAST(sum(count) AS BIGINT), so result types are unchanged.
// Guarded by the rawduck_use_projections setting and a physical-row-count
// staleness token; any mismatch falls back to the base table.
//===--------------------------------------------------------------------===//

static bool TryRewriteToProjection(ClientContext &context, LogicalOperator &parent) {
	if (parent.type != LogicalOperatorType::LOGICAL_PROJECTION || parent.children.size() != 1) {
		return false;
	}
	auto &child = *parent.children[0];
	if (child.type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY || child.children.size() != 1) {
		return false;
	}
	auto &aggregate = child.Cast<LogicalAggregate>();
	auto &grandchild = *child.children[0];
	if (grandchild.type != LogicalOperatorType::LOGICAL_GET) {
		return false;
	}
	auto &get = grandchild.Cast<LogicalGet>();
	auto table = get.GetTable();
	if (!table || !get.table_filters.filters.empty()) {
		return false;
	}
	// exactly one ungrouped-set count(*) aggregate
	if (!aggregate.grouping_functions.empty() || aggregate.grouping_sets.size() > 1 ||
	    aggregate.expressions.size() != 1 || aggregate.groups.empty()) {
		return false;
	}
	if (aggregate.expressions[0]->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
		return false;
	}
	auto &count_aggregate = aggregate.expressions[0]->Cast<BoundAggregateExpression>();
	if (count_aggregate.function.name != "count_star" || !count_aggregate.children.empty() ||
	    count_aggregate.IsDistinct()) {
		return false;
	}
	// all groups are plain scanned columns
	vector<string> group_names;
	for (auto &group : aggregate.groups) {
		if (group->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
			return false;
		}
		auto &column_ref = group->Cast<BoundColumnRefExpression>();
		string name;
		if (column_ref.binding.table_index != get.table_index ||
		    !ResolveGetColumn(get, column_ref.binding.column_index, name)) {
			return false;
		}
		group_names.push_back(name);
	}
	auto sorted_names = group_names;
	std::sort(sorted_names.begin(), sorted_names.end());

	// a fresh projection for exactly this group set must be registered
	RawStatsCache::Projection projection;
	{
		auto &stats = GetStatsCache(context);
		lock_guard<mutex> guard(stats.lock);
		auto entry = stats.projections.find(TableKey(*table));
		if (entry == stats.projections.end() || entry->second.group_set != StringUtil::Join(sorted_names, ", ")) {
			return false;
		}
		projection = entry->second;
	}
	if (table->GetStorage().GetTotalRows() != projection.base_rows) {
		// the base table changed since materialization: never use stale data
		return false;
	}
	EntryLookupInfo projection_lookup(CatalogType::TABLE_ENTRY, projection.name);
	auto projection_entry = Catalog::GetEntry(context, projection.catalog, projection.schema, projection_lookup,
	                                          OnEntryNotFound::RETURN_NULL);
	if (!projection_entry) {
		return false;
	}
	auto &projection_table = projection_entry->Cast<TableCatalogEntry>();
	// the parent projection must reference aggregate outputs as plain columns
	for (auto &expression : parent.expressions) {
		if (expression->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
			return false;
		}
	}
	// map group columns and the count column into the projection table
	case_insensitive_map_t<idx_t> projection_columns;
	idx_t physical_index = 0;
	for (auto &column : projection_table.GetColumns().Physical()) {
		projection_columns[column.Name()] = physical_index++;
	}
	vector<ColumnIndex> new_column_ids;
	for (auto &name : group_names) {
		auto entry = projection_columns.find(name);
		if (entry == projection_columns.end()) {
			return false;
		}
		new_column_ids.emplace_back(entry->second);
	}
	auto count_entry = projection_columns.find("count");
	if (count_entry == projection_columns.end()) {
		return false;
	}
	new_column_ids.emplace_back(count_entry->second);

	// build the replacement scan over the projection table
	unique_ptr<FunctionData> bind_data;
	auto scan_function = projection_table.GetScanFunction(context, bind_data);
	vector<LogicalType> scan_types;
	vector<string> scan_names;
	for (auto &column : projection_table.GetColumns().Physical()) {
		scan_types.push_back(column.Type());
		scan_names.push_back(column.Name());
	}
	auto new_get = make_uniq<LogicalGet>(get.table_index, scan_function, std::move(bind_data), std::move(scan_types),
	                                     std::move(scan_names));
	new_get->GetMutableColumnIds() = std::move(new_column_ids);

	// retarget the groups and turn count(*) into CAST(sum(count) AS BIGINT)
	for (idx_t i = 0; i < aggregate.groups.size(); i++) {
		aggregate.groups[i]->Cast<BoundColumnRefExpression>().binding = ColumnBinding(get.table_index, i);
	}
	// EntryLookupInfo holds the name by reference: keep it alive
	string sum_name = "sum";
	EntryLookupInfo sum_lookup(CatalogType::AGGREGATE_FUNCTION_ENTRY, sum_name);
	auto &sum_entry =
	    Catalog::GetEntry(context, INVALID_CATALOG, DEFAULT_SCHEMA, sum_lookup).Cast<AggregateFunctionCatalogEntry>();
	FunctionBinder function_binder(context);
	vector<LogicalType> sum_arguments {LogicalType::BIGINT};
	ErrorData bind_error;
	auto sum_offset = function_binder.BindFunction(sum_entry.name, sum_entry.functions, sum_arguments, bind_error);
	if (!sum_offset.IsValid()) {
		return false;
	}
	auto sum_function = sum_entry.functions.GetFunctionByOffset(sum_offset.GetIndex());
	vector<unique_ptr<Expression>> sum_children;
	sum_children.push_back(
	    make_uniq<BoundColumnRefExpression>(LogicalType::BIGINT, ColumnBinding(get.table_index, group_names.size())));
	auto bound_sum = function_binder.BindAggregateFunction(sum_function, std::move(sum_children), nullptr,
	                                                       AggregateType::NON_DISTINCT);
	auto sum_type = bound_sum->return_type;
	auto count_binding = ColumnBinding(aggregate.aggregate_index, 0);
	aggregate.expressions[0] = std::move(bound_sum);
	child.children[0] = std::move(new_get);

	// the parent keeps producing BIGINT for the count column
	for (auto &expression : parent.expressions) {
		auto &column_ref = expression->Cast<BoundColumnRefExpression>();
		if (column_ref.binding == count_binding) {
			auto original_type = column_ref.return_type;
			column_ref.return_type = sum_type;
			expression = BoundCastExpression::AddCastToType(context, std::move(expression), original_type);
		}
	}
	child.children[0]->ResolveOperatorTypes();
	child.ResolveOperatorTypes();
	return true;
}

static void RewriteAggregates(ClientContext &context, LogicalOperator &op) {
	TryRewriteToProjection(context, op);
	for (auto &child : op.children) {
		RewriteAggregates(context, *child);
	}
}

static void RawDuckPreOptimizeHook(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	if (!plan) {
		return;
	}
	Value enabled;
	if (!input.context.TryGetCurrentSetting("rawduck_use_projections", enabled) || !enabled.GetValue<bool>()) {
		return;
	}
	{
		// fast bail-out when nothing is materialized
		auto &stats = GetStatsCache(input.context);
		lock_guard<mutex> guard(stats.lock);
		if (stats.projections.empty()) {
			return;
		}
	}
	RewriteAggregates(input.context, *plan);
}

static void RawDuckOptimizeHook(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	if (!plan) {
		return;
	}
	// observation must never break a query
	try {
		unordered_map<idx_t, optional_ptr<LogicalOperator>> operators;
		CollectGets(*plan, operators);
		CollectStats(input.context, *plan, operators);
	} catch (...) { // NOLINT: best-effort statistics collection
	}
}

OptimizerExtension GetRawDuckOptimizerExtension() {
	OptimizerExtension extension;
	extension.optimize_function = RawDuckOptimizeHook;
	extension.pre_optimize_function = RawDuckPreOptimizeHook;
	return extension;
}

//===--------------------------------------------------------------------===//
// raw_stats(): dump observed predicate statistics
//===--------------------------------------------------------------------===//

struct RawStatsState : public GlobalTableFunctionState {
	vector<std::tuple<string, string, idx_t, idx_t>> entries;
	idx_t next = 0;
};

static unique_ptr<FunctionData> RawStatsBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::BIGINT};
	names = {"table", "column", "filter_count", "group_count"};
	return make_uniq<TableFunctionData>();
}

static unique_ptr<GlobalTableFunctionState> RawStatsInit(ClientContext &context, TableFunctionInitInput &input) {
	auto state = make_uniq<RawStatsState>();
	auto &stats = GetStatsCache(context);
	lock_guard<mutex> guard(stats.lock);
	for (auto &table : stats.usage) {
		for (auto &column : table.second) {
			state->entries.emplace_back(table.first, column.first, column.second.filters, column.second.groups);
		}
	}
	return std::move(state);
}

static void RawStatsFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<RawStatsState>();
	idx_t count = 0;
	while (state.next < state.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = state.entries[state.next++];
		output.SetValue(0, count, Value(std::get<0>(entry)));
		output.SetValue(1, count, Value(std::get<1>(entry)));
		output.SetValue(2, count, Value::BIGINT(NumericCast<int64_t>(std::get<2>(entry))));
		output.SetValue(3, count, Value::BIGINT(NumericCast<int64_t>(std::get<3>(entry))));
		count++;
	}
	output.SetCardinality(count);
}

TableFunction GetRawStatsFunction() {
	return TableFunction("raw_stats", {}, RawStatsFunction, RawStatsBind, RawStatsInit);
}

//===--------------------------------------------------------------------===//
// raw_optimize(table): physically reorder by the hottest predicate columns
//===--------------------------------------------------------------------===//

struct RawOptimizeBindData : public TableFunctionData {
	string target;
};

struct RawOptimizeState : public GlobalTableFunctionState {
	bool done = false;
};

static unique_ptr<FunctionData> RawOptimizeBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<RawOptimizeBindData>();
	if (input.inputs[0].IsNull()) {
		throw InvalidInputException("RawDuck: raw_optimize(table) argument may not be NULL");
	}
	result->target = input.inputs[0].GetValue<string>();
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::VARCHAR};
	names = {"table", "order_by", "rows", "mode"};
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> RawOptimizeInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<RawOptimizeState>();
}

static void RawOptimizeFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<RawOptimizeBindData>();
	auto &state = data.global_state->Cast<RawOptimizeState>();
	if (state.done) {
		output.SetCardinality(0);
		return;
	}
	state.done = true;

	// resolve the table within the calling query's transaction
	auto qname = QualifiedName::Parse(bind_data.target);
	auto catalog_name = qname.catalog.empty() ? INVALID_CATALOG : qname.catalog;
	auto schema_name = qname.schema.empty() ? DEFAULT_SCHEMA : qname.schema;
	EntryLookupInfo table_lookup(CatalogType::TABLE_ENTRY, qname.name);
	auto &table = Catalog::GetEntry(context, catalog_name, schema_name, table_lookup).Cast<TableCatalogEntry>();

	// rank columns by how often queries filtered on them
	vector<pair<string, idx_t>> ranked;
	{
		auto &stats = GetStatsCache(context);
		lock_guard<mutex> guard(stats.lock);
		auto entry = stats.usage.find(TableKey(table));
		if (entry != stats.usage.end()) {
			for (auto &column : entry->second) {
				// filters benefit most from zone-map pruning; grouping
				// locality is a secondary win
				ranked.emplace_back(column.first, 2 * column.second.filters + column.second.groups);
			}
		}
	}
	std::stable_sort(ranked.begin(), ranked.end(),
	                 [](const pair<string, idx_t> &a, const pair<string, idx_t> &b) { return a.second > b.second; });

	output.SetValue(0, 0, Value(bind_data.target));
	if (ranked.empty()) {
		// nothing observed yet: no-op
		output.SetValue(1, 0, Value(LogicalType::VARCHAR));
		output.SetValue(2, 0, Value::BIGINT(0));
		output.SetValue(3, 0, Value("noop"));
		output.SetCardinality(1);
		return;
	}

	string order_by;
	for (idx_t i = 0; i < MinValue<idx_t>(ranked.size(), 2); i++) {
		order_by += (i ? ", " : "") + RawQuoteIdentifier(ranked[i].first);
	}

	auto table_key = TableKey(table);
	auto catalog = table.ParentCatalog().GetName();
	auto schema = table.ParentSchema().name;
	auto table_name = table.name;
	auto qualified = RawQualifiedTarget(bind_data.target);
	string tmp_name = "__rawduck_optimize_tmp";
	string tmp_qualified =
	    RawQuoteIdentifier(catalog) + "." + RawQuoteIdentifier(schema) + "." + RawQuoteIdentifier(tmp_name);

	Connection conn(*context.db);
	auto run = [&](const string &sql) {
		auto result = conn.Query(sql);
		if (result->HasError()) {
			result->ThrowError("RawDuck: ");
		}
		return result;
	};

	// decide between a full rewrite and an incremental tail sort: if the
	// table only grew since the last optimize with the same key, the sorted
	// prefix can stay in place and only the new rows form a new sorted run
	// (zone maps prune per row group, RawMergeTree-parts style)
	idx_t last_count = 0;
	{
		auto &stats = GetStatsCache(context);
		lock_guard<mutex> guard(stats.lock);
		auto entry = stats.optimized.find(table_key);
		if (entry != stats.optimized.end() && entry->second.order_by == order_by) {
			last_count = entry->second.row_count;
		}
	}
	auto shape = run("SELECT count(*), coalesce(max(rowid) + 1, 0) FROM " + qualified);
	auto current_count = NumericCast<idx_t>(shape->GetValue(0, 0).GetValue<int64_t>());
	auto rowid_end = NumericCast<idx_t>(shape->GetValue(1, 0).GetValue<int64_t>());
	// rowid continuity proves the table is append-only since the last rewrite
	bool append_only = current_count == rowid_end;

	string mode;
	int64_t rows = 0;
	if (last_count > 0 && append_only && current_count == last_count) {
		mode = "noop";
	} else if (last_count > 0 && append_only && current_count > last_count) {
		mode = "incremental";
		run("BEGIN TRANSACTION");
		try {
			auto tail = to_string(last_count);
			run("CREATE TEMPORARY TABLE __rawduck_optimize_tail AS SELECT * FROM " + qualified +
			    " WHERE rowid >= " + tail);
			run("DELETE FROM " + qualified + " WHERE rowid >= " + tail);
			auto inserted =
			    run("INSERT INTO " + qualified + " SELECT * FROM __rawduck_optimize_tail ORDER BY " + order_by);
			rows = inserted->GetValue(0, 0).GetValue<int64_t>();
			run("DROP TABLE __rawduck_optimize_tail");
			run("COMMIT");
		} catch (...) {
			conn.Query("ROLLBACK");
			throw;
		}
	} else {
		mode = "full";
		run("BEGIN TRANSACTION");
		try {
			auto created =
			    run("CREATE TABLE " + tmp_qualified + " AS SELECT * FROM " + qualified + " ORDER BY " + order_by);
			rows = created->GetValue(0, 0).GetValue<int64_t>();
			run("DROP TABLE " + qualified);
			run("ALTER TABLE " + tmp_qualified + " RENAME TO " + RawQuoteIdentifier(table_name));
			run("COMMIT");
		} catch (...) {
			conn.Query("ROLLBACK");
			throw;
		}
	}
	{
		auto &stats = GetStatsCache(context);
		lock_guard<mutex> guard(stats.lock);
		RawStatsCache::OptimizeState optimize_state;
		optimize_state.order_by = order_by;
		optimize_state.row_count = current_count;
		stats.optimized[table_key] = optimize_state;
	}

	output.SetValue(1, 0, Value(order_by));
	output.SetValue(2, 0, Value::BIGINT(rows));
	output.SetValue(3, 0, Value(mode));
	output.SetCardinality(1);
}

//===--------------------------------------------------------------------===//
// raw_projections(): the projection advisor's view of observed aggregations
//===--------------------------------------------------------------------===//

struct RawProjectionsState : public GlobalTableFunctionState {
	vector<std::tuple<string, string, idx_t, string>> entries;
	idx_t next = 0;
};

static unique_ptr<FunctionData> RawProjectionsBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::VARCHAR};
	names = {"table", "group_columns", "observed", "projection"};
	return make_uniq<TableFunctionData>();
}

static unique_ptr<GlobalTableFunctionState> RawProjectionsInit(ClientContext &context, TableFunctionInitInput &input) {
	auto state = make_uniq<RawProjectionsState>();
	auto &stats = GetStatsCache(context);
	lock_guard<mutex> guard(stats.lock);
	for (auto &table : stats.group_sets) {
		auto materialized = stats.projections.find(table.first);
		for (auto &group_set : table.second) {
			string projection;
			if (materialized != stats.projections.end() && materialized->second.group_set == group_set.first) {
				projection = materialized->second.name;
			}
			state->entries.emplace_back(table.first, group_set.first, group_set.second, projection);
		}
	}
	return std::move(state);
}

static void RawProjectionsFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<RawProjectionsState>();
	idx_t count = 0;
	while (state.next < state.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = state.entries[state.next++];
		output.SetValue(0, count, Value(std::get<0>(entry)));
		output.SetValue(1, count, Value(std::get<1>(entry)));
		output.SetValue(2, count, Value::BIGINT(NumericCast<int64_t>(std::get<2>(entry))));
		output.SetValue(3, count, std::get<3>(entry).empty() ? Value(LogicalType::VARCHAR) : Value(std::get<3>(entry)));
		count++;
	}
	output.SetCardinality(count);
}

TableFunction GetRawProjectionsFunction() {
	return TableFunction("raw_projections", {}, RawProjectionsFunction, RawProjectionsBind, RawProjectionsInit);
}

//===--------------------------------------------------------------------===//
// raw_project(table): materialize the hottest observed aggregation as a
// lightweight summary table (RawMergeTree auto-projections)
//===--------------------------------------------------------------------===//

static unique_ptr<FunctionData> RawProjectBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<RawOptimizeBindData>();
	if (input.inputs[0].IsNull()) {
		throw InvalidInputException("RawDuck: raw_project(table) argument may not be NULL");
	}
	result->target = input.inputs[0].GetValue<string>();
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT};
	names = {"table", "projection", "group_columns", "rows"};
	return std::move(result);
}

static void RawProjectFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<RawOptimizeBindData>();
	auto &state = data.global_state->Cast<RawOptimizeState>();
	if (state.done) {
		output.SetCardinality(0);
		return;
	}
	state.done = true;

	auto qname = QualifiedName::Parse(bind_data.target);
	auto catalog_name = qname.catalog.empty() ? INVALID_CATALOG : qname.catalog;
	auto schema_name = qname.schema.empty() ? DEFAULT_SCHEMA : qname.schema;
	EntryLookupInfo table_lookup(CatalogType::TABLE_ENTRY, qname.name);
	auto &table = Catalog::GetEntry(context, catalog_name, schema_name, table_lookup).Cast<TableCatalogEntry>();

	// the hottest observed group set wins
	string group_set;
	idx_t observed = 0;
	{
		auto &stats = GetStatsCache(context);
		lock_guard<mutex> guard(stats.lock);
		auto entry = stats.group_sets.find(TableKey(table));
		if (entry != stats.group_sets.end()) {
			for (auto &candidate : entry->second) {
				if (candidate.second > observed) {
					group_set = candidate.first;
					observed = candidate.second;
				}
			}
		}
	}
	output.SetValue(0, 0, Value(bind_data.target));
	if (group_set.empty()) {
		// no aggregations observed yet: no-op
		output.SetValue(1, 0, Value(LogicalType::VARCHAR));
		output.SetValue(2, 0, Value(LogicalType::VARCHAR));
		output.SetValue(3, 0, Value::BIGINT(0));
		output.SetCardinality(1);
		return;
	}

	string select_list;
	for (auto &column : StringUtil::Split(group_set, ", ")) {
		select_list += (select_list.empty() ? "" : ", ") + RawQuoteIdentifier(column);
	}
	auto projection_name = table.name + "__proj";
	auto projection_qualified = RawQuoteIdentifier(table.ParentCatalog().GetName()) + "." +
	                            RawQuoteIdentifier(table.ParentSchema().name) + "." +
	                            RawQuoteIdentifier(projection_name);
	auto qualified = RawQualifiedTarget(bind_data.target);

	Connection conn(*context.db);
	auto result = conn.Query("CREATE OR REPLACE TABLE " + projection_qualified + " AS SELECT " + select_list +
	                         ", count(*) AS count FROM " + qualified + " GROUP BY ALL");
	if (result->HasError()) {
		result->ThrowError("RawDuck: ");
	}
	auto probe = conn.Query("SELECT count(*) FROM " + projection_qualified);
	auto projection_rows = probe->HasError() ? 0 : probe->GetValue(0, 0).GetValue<int64_t>();

	{
		auto &stats = GetStatsCache(context);
		lock_guard<mutex> guard(stats.lock);
		RawStatsCache::Projection registered;
		registered.catalog = table.ParentCatalog().GetName();
		registered.schema = table.ParentSchema().name;
		registered.name = projection_name;
		registered.group_set = group_set;
		registered.base_rows = table.GetStorage().GetTotalRows();
		stats.projections[TableKey(table)] = registered;
	}
	output.SetValue(1, 0, Value(projection_name));
	output.SetValue(2, 0, Value(group_set));
	output.SetValue(3, 0, Value(projection_rows));
	output.SetCardinality(1);
}

TableFunction GetRawProjectFunction() {
	return TableFunction("raw_project", {LogicalType::VARCHAR}, RawProjectFunction, RawProjectBind, RawOptimizeInit);
}

//===--------------------------------------------------------------------===//
// raw_stats_save / raw_stats_load: persist observed statistics in a store
//===--------------------------------------------------------------------===//

struct RawStatsIOBindData : public TableFunctionData {
	string catalog;
};

static unique_ptr<FunctionData> RawStatsIOBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<RawStatsIOBindData>();
	if (!input.inputs.empty() && !input.inputs[0].IsNull()) {
		result->catalog = input.inputs[0].GetValue<string>();
	}
	return_types = {LogicalType::VARCHAR, LogicalType::BIGINT};
	names = {"catalog", "entries"};
	return std::move(result);
}

static string StatsTable(ClientContext &context, const string &catalog) {
	auto name = catalog.empty() ? DatabaseManager::GetDefaultDatabase(context) : catalog;
	return RawQuoteIdentifier(name) + ".main.__rawduck_stats";
}

static string SQLString(const string &value) {
	return KeywordHelper::WriteQuoted(value, '\'');
}

static void RawStatsSaveFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<RawStatsIOBindData>();
	auto &state = data.global_state->Cast<RawOptimizeState>();
	if (state.done) {
		output.SetCardinality(0);
		return;
	}
	state.done = true;

	// snapshot the in-memory statistics
	vector<string> rows;
	{
		auto &stats = GetStatsCache(context);
		lock_guard<mutex> guard(stats.lock);
		for (auto &table : stats.usage) {
			for (auto &column : table.second) {
				rows.push_back("('column', " + SQLString(table.first) + ", " + SQLString(column.first) + ", " +
				               to_string(column.second.filters) + ", " + to_string(column.second.groups) + ")");
			}
		}
		for (auto &table : stats.group_sets) {
			for (auto &group_set : table.second) {
				rows.push_back("('group_set', " + SQLString(table.first) + ", " + SQLString(group_set.first) + ", " +
				               to_string(group_set.second) + ", 0)");
			}
		}
	}
	auto stats_table = StatsTable(context, bind_data.catalog);
	Connection conn(*context.db);
	auto run = [&](const string &sql) {
		auto result = conn.Query(sql);
		if (result->HasError()) {
			result->ThrowError("RawDuck: ");
		}
	};
	run("CREATE OR REPLACE TABLE " + stats_table +
	    " (kind VARCHAR, tbl VARCHAR, item VARCHAR, filters BIGINT, groups BIGINT)");
	if (!rows.empty()) {
		run("INSERT INTO " + stats_table + " VALUES " + StringUtil::Join(rows, ", "));
	}
	output.SetValue(
	    0, 0, Value(bind_data.catalog.empty() ? DatabaseManager::GetDefaultDatabase(context) : bind_data.catalog));
	output.SetValue(1, 0, Value::BIGINT(NumericCast<int64_t>(rows.size())));
	output.SetCardinality(1);
}

static void RawStatsLoadFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<RawStatsIOBindData>();
	auto &state = data.global_state->Cast<RawOptimizeState>();
	if (state.done) {
		output.SetCardinality(0);
		return;
	}
	state.done = true;

	auto stats_table = StatsTable(context, bind_data.catalog);
	Connection conn(*context.db);
	auto result = conn.Query("SELECT kind, tbl, item, filters, groups FROM " + stats_table);
	if (result->HasError()) {
		result->ThrowError("RawDuck: ");
	}
	idx_t entries = 0;
	{
		auto &stats = GetStatsCache(context);
		lock_guard<mutex> guard(stats.lock);
		for (idx_t row = 0; row < result->RowCount(); row++) {
			auto kind = result->GetValue(0, row).ToString();
			auto table = result->GetValue(1, row).ToString();
			auto item = result->GetValue(2, row).ToString();
			auto filters = NumericCast<idx_t>(result->GetValue(3, row).GetValue<int64_t>());
			auto groups = NumericCast<idx_t>(result->GetValue(4, row).GetValue<int64_t>());
			if (kind == "column") {
				stats.usage[table][item].filters += filters;
				stats.usage[table][item].groups += groups;
			} else if (kind == "group_set") {
				stats.group_sets[table][item] += filters;
			}
			entries++;
		}
	}
	output.SetValue(
	    0, 0, Value(bind_data.catalog.empty() ? DatabaseManager::GetDefaultDatabase(context) : bind_data.catalog));
	output.SetValue(1, 0, Value::BIGINT(NumericCast<int64_t>(entries)));
	output.SetCardinality(1);
}

TableFunctionSet GetRawStatsSaveFunction() {
	TableFunctionSet set("raw_stats_save");
	set.AddFunction(TableFunction("raw_stats_save", {}, RawStatsSaveFunction, RawStatsIOBind, RawOptimizeInit));
	set.AddFunction(
	    TableFunction("raw_stats_save", {LogicalType::VARCHAR}, RawStatsSaveFunction, RawStatsIOBind, RawOptimizeInit));
	return set;
}

TableFunctionSet GetRawStatsLoadFunction() {
	TableFunctionSet set("raw_stats_load");
	set.AddFunction(TableFunction("raw_stats_load", {}, RawStatsLoadFunction, RawStatsIOBind, RawOptimizeInit));
	set.AddFunction(
	    TableFunction("raw_stats_load", {LogicalType::VARCHAR}, RawStatsLoadFunction, RawStatsIOBind, RawOptimizeInit));
	return set;
}

TableFunction GetRawOptimizeFunction() {
	return TableFunction("raw_optimize", {LogicalType::VARCHAR}, RawOptimizeFunction, RawOptimizeBind, RawOptimizeInit);
}

} // namespace duckdb
