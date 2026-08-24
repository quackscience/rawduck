#include "raw_functions.hpp"
#include "raw_json.hpp"

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/storage/object_cache.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// User-defined transforms
//
// Transforms are data: raw_transform_define() is a scalar function, so
// definitions compose with any DuckDB source —
//   SELECT raw_transform_define(name, explode) FROM read_json('transforms.json');
//   SELECT raw_transform_define(name, explode) FROM my_transforms_table;
//===--------------------------------------------------------------------===//

class RawTransformRegistry : public ObjectCacheEntry {
public:
	static string ObjectType() {
		return "rawduck_transforms";
	}
	string GetObjectType() override {
		return ObjectType();
	}
	optional_idx GetEstimatedCacheMemory() const override {
		// configuration must never be evicted
		return optional_idx();
	}

	mutex lock;
	// transform name (lowercase) -> dotted explode path
	map<string, string> transforms;
};

static RawTransformRegistry &GetTransformRegistry(ClientContext &context) {
	return *ObjectCache::GetObjectCache(context).GetOrCreate<RawTransformRegistry>(RawTransformRegistry::ObjectType());
}

RawParseOptions ResolveTransform(ClientContext &context, const string &transform, const string &explode) {
	if (!transform.empty() && !explode.empty()) {
		throw InvalidInputException("RawDuck: specify either transform or explode, not both");
	}
	string path = explode;
	bool otlp = !transform.empty() && RawTransformIsOtlp(transform);
	if (!transform.empty() && !ResolveBuiltinTransform(transform, path)) {
		auto &registry = GetTransformRegistry(context);
		lock_guard<mutex> guard(registry.lock);
		auto entry = registry.transforms.find(StringUtil::Lower(transform));
		if (entry == registry.transforms.end()) {
			throw InvalidInputException("RawDuck: unknown transform '%s'; list available transforms with "
			                            "raw_transforms(), define new ones with raw_transform_define(name, path), "
			                            "or use explode := 'dotted.path'",
			                            transform);
		}
		path = entry->second;
	}
	auto options = RawExplodeOptions(path);
	options.otlp = otlp;
	return options;
}

//===--------------------------------------------------------------------===//
// raw_transform_define(name, explode_path)
//===--------------------------------------------------------------------===//

static void RawTransformDefineFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	BinaryExecutor::Execute<string_t, string_t, string_t>(
	    args.data[0], args.data[1], result, args.size(), [&](string_t name_t, string_t path_t) {
		    auto name = name_t.GetString();
		    auto path = path_t.GetString();
		    if (name.empty() || path.empty()) {
			    throw InvalidInputException("RawDuck: transform name and explode path may not be empty");
		    }
		    string builtin_path;
		    if (ResolveBuiltinTransform(name, builtin_path)) {
			    throw InvalidInputException("RawDuck: '%s' is a built-in transform and cannot be redefined", name);
		    }
		    auto &registry = GetTransformRegistry(context);
		    lock_guard<mutex> guard(registry.lock);
		    registry.transforms[StringUtil::Lower(name)] = path;
		    return StringVector::AddString(result, name);
	    });
}

ScalarFunction GetRawTransformDefineFunction() {
	ScalarFunction function("raw_transform_define", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                        RawTransformDefineFunction);
	function.SetStability(FunctionStability::VOLATILE);
	// rejects empty names/paths and redefinitions of built-in transforms
	function.SetFallible();
	return function;
}

//===--------------------------------------------------------------------===//
// raw_transforms(): list built-in and user-defined transforms
//===--------------------------------------------------------------------===//

struct RawTransformsState : public GlobalTableFunctionState {
	vector<std::tuple<string, string, bool>> entries;
	idx_t next = 0;
};

static unique_ptr<FunctionData> RawTransformsBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<Identifier> &names) {
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BOOLEAN};
	names = {"name", "explode", "builtin"};
	return make_uniq<TableFunctionData>();
}

static unique_ptr<GlobalTableFunctionState> RawTransformsInit(ClientContext &context, TableFunctionInitInput &input) {
	auto state = make_uniq<RawTransformsState>();
	for (auto &builtin : RawBuiltinTransforms()) {
		state->entries.emplace_back(builtin.first, builtin.second, true);
	}
	auto &registry = GetTransformRegistry(context);
	lock_guard<mutex> guard(registry.lock);
	for (auto &transform : registry.transforms) {
		state->entries.emplace_back(transform.first, transform.second, false);
	}
	return std::move(state);
}

static void RawTransformsFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<RawTransformsState>();
	idx_t count = 0;
	while (state.next < state.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = state.entries[state.next++];
		output.data[0].Append(Value(std::get<0>(entry)));
		output.data[1].Append(Value(std::get<1>(entry)));
		output.data[2].Append(Value::BOOLEAN(std::get<2>(entry)));
		count++;
	}
	output.CheckCardinality(count);
}

TableFunction GetRawTransformsFunction() {
	return TableFunction("raw_transforms", {}, RawTransformsFunction, RawTransformsBind, RawTransformsInit);
}

} // namespace duckdb
