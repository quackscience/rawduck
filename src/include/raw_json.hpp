#pragma once

#include "duckdb.hpp"
#include "yyjson.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// RawDuck schema inference
//
// Implements the RawMergeTree-style "ingest first, schema later" model:
// every JSON payload is merged into a schema tree where scalar types widen
// along a lattice and structural conflicts degrade to JSON. Nested objects
// are flattened into dotted column paths so values land in real typed
// columns instead of opaque JSON strings.
//===--------------------------------------------------------------------===//

// Scalar widening lattice: UNSET < BOOLEAN | BIGINT < DOUBLE | DATE < TIMESTAMP,
// any other combination joins to VARCHAR (the universal scalar sink).
enum class RawScalarKind : uint8_t { UNSET = 0, BOOLEAN, BIGINT, DOUBLE, DATE, TIMESTAMP, VARCHAR };

enum class RawNodeClass : uint8_t {
	UNSET = 0, // only nulls seen so far
	SCALAR,    // scalar values, type tracked in `scalar`
	OBJECT,    // nested object, children flattened into dotted columns
	ARRAY,     // homogeneous array, element type tracked in `element`
	JSON       // structural conflict or unflattenable value: stored as JSON text
};

struct RawNode {
	RawNodeClass node_class = RawNodeClass::UNSET;
	RawScalarKind scalar = RawScalarKind::UNSET;
	// object children in first-seen order, so inferred column order is stable
	vector<pair<string, unique_ptr<RawNode>>> children;
	unordered_map<string, idx_t> child_lookup;
	unique_ptr<RawNode> element;

	RawNode &GetOrCreateChild(const string &key);
};

// A flattened output column: `name` is the dotted path, `path` the segments to
// walk in each JSON row, `node` points into the schema tree owned by the caller.
struct RawColumn {
	string name;
	vector<string> path;
	LogicalType type;
	const RawNode *node;
};

struct RawParseOptions {
	// skip and count unparseable NDJSON lines instead of failing
	bool ignore_errors = false;
	// ingest-time transform: explode the array at this dotted path into one
	// row per element, merging the envelope fields into each row
	vector<string> explode_path;
	// apply OTLP semantics after exploding: spread KeyValue attribute lists
	// into their parent, unwrap AnyValue, numeric *UnixNano fields
	bool otlp = false;
	// original bind parameters for replay through raw_records in SQL fallback paths
	string source_transform;
	string source_explode;
};

// A parsed payload: one or more JSON documents and the row roots within them.
struct RawPayload {
	vector<duckdb_yyjson::yyjson_doc *> docs;
	vector<duckdb_yyjson::yyjson_val *> rows;
	// exploded rows kept in mut form to skip a bulk imut materialization copy
	vector<duckdb_yyjson::yyjson_mut_doc *> mut_docs;
	vector<duckdb_yyjson::yyjson_mut_val *> mut_rows;
	// true when rows are bare scalars/arrays: they map to a single "value" column
	bool scalar_rows = false;
	// NDJSON lines skipped because they failed to parse (ignore_errors only)
	idx_t parse_errors = 0;
	// apply OTLP semantic normalization during Explode
	bool otlp_semantics = false;

	RawPayload() = default;
	RawPayload(const RawPayload &) = delete;
	RawPayload &operator=(const RawPayload &) = delete;
	~RawPayload();

	idx_t RowCount() const {
		return mut_rows.empty() ? rows.size() : mut_rows.size();
	}
	bool HasRows() const {
		return RowCount() > 0;
	}
	bool UsesMutRows() const {
		return !mut_rows.empty();
	}

	// Accepts a JSON array of objects, a single object, or NDJSON; throws
	// InvalidInputException otherwise.
	void Parse(const string &payload, const RawParseOptions &options = RawParseOptions());
	unique_ptr<RawNode> InferSchema() const;
	void Explode(const vector<string> &path);
};

// A fully processed payload: parsed rows plus the inferred flattened schema.
// Parsed exactly once and shared by inference and extraction.
struct RawParsedPayload {
	RawPayload payload;
	unique_ptr<RawNode> root;
	vector<RawColumn> columns;

	static shared_ptr<RawParsedPayload> Process(const string &payload_text,
	                                            const RawParseOptions &options = RawParseOptions());
	// Parse + explode only; schema filled by InferColumns() or AttachCachedRoot().
	static shared_ptr<RawParsedPayload> ParsePayload(const string &payload_text,
	                                                 const RawParseOptions &options = RawParseOptions());
	void InferColumns();
	// Rebuild columns from a cached schema tree (warm ingest fast path).
	void AttachCachedRoot(const RawNode &cached_root);
};

// Incremental payload assembly (zero-copy INSERT sink): parse documents one
// at a time straight from source memory, then finalize (uniformity check +
// inference + flattening) once per batch.
void RawPayloadAddDocument(RawParsedPayload &payload, const char *data, idx_t size, bool ignore_errors = false);
void RawPayloadFinalize(RawParsedPayload &payload, const RawParseOptions &options = RawParseOptions());

// Built-in ingest-time transforms (name -> dotted explode path); users can
// register additional ones via raw_transform_define().
const vector<pair<string, string>> &RawBuiltinTransforms();
bool ResolveBuiltinTransform(const string &name, string &path);
bool RawTransformIsOtlp(const string &name);
RawParseOptions RawExplodeOptions(const string &path);

RawScalarKind JoinScalarKinds(RawScalarKind a, RawScalarKind b);
RawScalarKind SniffScalarKind(duckdb_yyjson::yyjson_val *val);
void MergeValue(RawNode &node, duckdb_yyjson::yyjson_val *val);
void MergeValueMut(RawNode &node, duckdb_yyjson::yyjson_mut_val *val);

LogicalType NodeToType(const RawNode &node);
vector<RawColumn> FlattenSchema(const RawNode &root, bool scalar_rows);
unique_ptr<RawNode> CloneRawNode(const RawNode &node);

bool IsRawJSONType(const LogicalType &type);

// Vectorized extraction. Rows are typically sparse compared to the unified
// schema, so extraction is row-major: each row's JSON tree is traversed once
// and values are routed to their column slot via the schema-tree nodes.
class RawExtractor {
public:
	RawExtractor(const RawNode &root, const vector<RawColumn> &columns, bool mut_rows_p = false);

	// Routes one row's values into per-column slots at `row_idx`. Slots for
	// absent paths keep their nullptr from Reset().
	void AssignRow(duckdb_yyjson::yyjson_val *row, idx_t row_idx);
	void AssignMutRow(duckdb_yyjson::yyjson_mut_val *row, idx_t row_idx);
	void Reset(idx_t row_count);

	bool UsesMutRows() const {
		return mut_rows;
	}

	const vector<duckdb_yyjson::yyjson_val *> &ColumnValues(idx_t col) const {
		return values[col];
	}
	const vector<duckdb_yyjson::yyjson_mut_val *> &ColumnMutValues(idx_t col) const {
		return mut_values[col];
	}

private:
	void Traverse(duckdb_yyjson::yyjson_val *val, const RawNode &node, idx_t row_idx);
	void TraverseMut(duckdb_yyjson::yyjson_mut_val *val, const RawNode &node, idx_t row_idx);

	const RawNode &root;
	// schema-tree leaf -> column slot
	unordered_map<const RawNode *, idx_t> column_of;
	bool root_is_column = false;
	bool mut_rows = false;
	vector<vector<duckdb_yyjson::yyjson_val *>> values;
	vector<vector<duckdb_yyjson::yyjson_mut_val *>> mut_values;
};

void FillVector(const vector<duckdb_yyjson::yyjson_val *> &vals, const LogicalType &type, Vector &result, idx_t offset);
void FillVectorMut(const vector<duckdb_yyjson::yyjson_mut_val *> &vals, const LogicalType &type, Vector &result,
                   idx_t offset);
// whether FillVector can write this type directly (otherwise extract in the
// inferred type and cast)
bool RawFillSupported(const LogicalType &type);

} // namespace duckdb
