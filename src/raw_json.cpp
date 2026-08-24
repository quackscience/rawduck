#include "raw_json.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <cstdlib>

namespace duckdb {

using duckdb_yyjson::yyjson_arr_iter;
using duckdb_yyjson::yyjson_doc;
using duckdb_yyjson::yyjson_obj_iter;
using duckdb_yyjson::yyjson_val;

// Objects nested deeper than this are kept as overflow (VARIANT) columns instead of being
// flattened further.
static constexpr idx_t RAW_MAX_FLATTEN_DEPTH = 16;
// guard against pathological payloads exploding the table schema
static constexpr idx_t RAW_MAX_COLUMNS = 10000;
// recursion guard: nesting beyond this is preserved verbatim as overflow instead
// of risking stack exhaustion on hostile inputs
static constexpr idx_t RAW_MAX_NESTING = 128;
static constexpr auto RAW_READ_FLAGS = duckdb_yyjson::YYJSON_READ_ALLOW_INF_AND_NAN;
// Below this fanout, a linear scan over `children` beats `child_lookup`: no
// temporary std::string, no hashing, and the scan stays within a couple of
// cache lines. Real-world objects (OTLP resource/span attrs, log records)
// overwhelmingly fall under this; wide flat schemas (GH Archive-style) still
// fall back to the hash map so a single object with hundreds of keys stays
// O(1) per lookup instead of O(n).
static constexpr idx_t RAW_CHILD_LINEAR_SCAN_LIMIT = 24;

//===--------------------------------------------------------------------===//
// Payload parsing
//===--------------------------------------------------------------------===//

RawPayload::~RawPayload() {
	for (auto doc : docs) {
		duckdb_yyjson::yyjson_doc_free(doc);
	}
	// must run after the doc frees above: those write into their pool
	// buffer's own free-list metadata (pool_free()), so a buffer can only be
	// released once every doc allocated from it has been freed.
	for (auto buffer : pool_buffers) {
		free(buffer);
	}
}

static void CollectRows(yyjson_val *root, vector<yyjson_val *> &rows) {
	if (duckdb_yyjson::yyjson_is_arr(root)) {
		yyjson_arr_iter iter;
		duckdb_yyjson::yyjson_arr_iter_init(root, &iter);
		while (auto element = duckdb_yyjson::yyjson_arr_iter_next(&iter)) {
			rows.push_back(element);
		}
	} else {
		rows.push_back(root);
	}
}

static void CheckRowUniformity(const vector<yyjson_val *> &rows, bool &scalar_rows) {
	// rows must be uniformly objects, or uniformly non-objects ("value" column)
	idx_t object_rows = 0;
	for (auto row : rows) {
		if (duckdb_yyjson::yyjson_is_obj(row)) {
			object_rows++;
		}
	}
	if (object_rows > 0 && object_rows != rows.size()) {
		throw InvalidInputException("RawDuck: payload mixes JSON objects with non-object values");
	}
	scalar_rows = !rows.empty() && object_rows == 0;
}

// One malloc for the whole payload instead of one per yyjson_read call
// (the common NDJSON case: one call per line). Sized via yyjson's own
// worst-case estimator, which is linear in input length, so estimating once
// for the whole payload safely covers the sum of however it's split into
// documents. Returns nullptr (use the default allocator) on overflow or if
// the single malloc fails — pooling is purely opportunistic, never required.
static void *RawInitReadPool(const string &payload, duckdb_yyjson::yyjson_alc &pool_alc) {
	if (payload.empty()) {
		return nullptr;
	}
	auto pool_size = duckdb_yyjson::yyjson_read_max_memory_usage(payload.size(), RAW_READ_FLAGS);
	if (pool_size == 0) {
		return nullptr;
	}
	auto buffer = malloc(pool_size);
	if (!buffer) {
		return nullptr;
	}
	if (!duckdb_yyjson::yyjson_alc_pool_init(&pool_alc, buffer, pool_size)) {
		free(buffer);
		return nullptr;
	}
	return buffer;
}

// Falls back to the default allocator if the pool is exhausted (a sizing
// edge case, not expected in practice) so pooling can never turn a payload
// that would otherwise parse successfully into a parse error.
static duckdb_yyjson::yyjson_doc *RawPoolRead(const char *data, size_t len, const duckdb_yyjson::yyjson_alc *alc) {
	auto doc = duckdb_yyjson::yyjson_read_opts(const_cast<char *>(data), len, RAW_READ_FLAGS, alc, nullptr);
	if (!doc && alc) {
		doc = duckdb_yyjson::yyjson_read_opts(const_cast<char *>(data), len, RAW_READ_FLAGS, nullptr, nullptr);
	}
	return doc;
}

void RawPayload::Parse(const string &payload, const RawParseOptions &options) {
	duckdb_yyjson::yyjson_alc pool_alc;
	auto pool_buffer = RawInitReadPool(payload, pool_alc);
	if (pool_buffer) {
		pool_buffers.push_back(pool_buffer);
	}
	auto alc = pool_buffer ? &pool_alc : nullptr;

	auto doc = RawPoolRead(payload.c_str(), payload.size(), alc);
	if (doc) {
		docs.push_back(doc);
		CollectRows(duckdb_yyjson::yyjson_doc_get_root(doc), rows);
	} else {
		// not a single JSON document: try newline-delimited JSON
		idx_t line_number = 0;
		size_t pos = 0;
		while (pos <= payload.size()) {
			auto end = payload.find('\n', pos);
			if (end == string::npos) {
				end = payload.size();
			}
			line_number++;
			auto line_begin = payload.data() + pos;
			auto line_len = end - pos;
			pos = end + 1;
			// skip blank lines
			bool blank = true;
			for (idx_t i = 0; i < line_len; i++) {
				if (!StringUtil::CharacterIsSpace(line_begin[i])) {
					blank = false;
					break;
				}
			}
			if (blank) {
				continue;
			}
			auto line_doc = RawPoolRead(line_begin, line_len, alc);
			if (!line_doc) {
				if (options.ignore_errors) {
					parse_errors++;
					continue;
				}
				throw InvalidInputException("RawDuck: payload is not valid JSON or NDJSON (parse error on line %llu)",
				                            line_number);
			}
			docs.push_back(line_doc);
			rows.push_back(duckdb_yyjson::yyjson_doc_get_root(line_doc));
		}
	}
	CheckRowUniformity(rows, scalar_rows);
	if (!options.explode_path.empty()) {
		otlp_semantics = options.otlp;
		Explode(options.explode_path);
	}
}

// One envelope per traversed level: the object plus the key that was
// descended into (excluded when merging).
struct RawExplodeEnvelope {
	yyjson_val *object;
	const string *descended_key;
};

static void AddObjectFields(duckdb_yyjson::yyjson_mut_doc *doc, duckdb_yyjson::yyjson_mut_val *merged,
                            yyjson_val *object, const string *skip_key) {
	yyjson_obj_iter iter;
	duckdb_yyjson::yyjson_obj_iter_init(object, &iter);
	while (auto key = duckdb_yyjson::yyjson_obj_iter_next(&iter)) {
		auto key_str = duckdb_yyjson::yyjson_get_str(key);
		auto key_len = duckdb_yyjson::yyjson_get_len(key);
		if (skip_key && skip_key->size() == key_len && memcmp(skip_key->data(), key_str, key_len) == 0) {
			continue;
		}
		// first writer wins: deeper levels are added before shallower ones
		if (duckdb_yyjson::yyjson_mut_obj_getn(merged, key_str, key_len)) {
			continue;
		}
		auto key_copy = duckdb_yyjson::yyjson_mut_strncpy(doc, key_str, key_len);
		duckdb_yyjson::yyjson_mut_obj_add(
		    merged, key_copy, duckdb_yyjson::yyjson_val_mut_copy(doc, duckdb_yyjson::yyjson_obj_iter_get_val(key)));
	}
}

static void AddMutObjectFields(duckdb_yyjson::yyjson_mut_doc *doc, duckdb_yyjson::yyjson_mut_val *merged,
                               duckdb_yyjson::yyjson_mut_val *object, const string *skip_key) {
	duckdb_yyjson::yyjson_mut_obj_iter iter;
	duckdb_yyjson::yyjson_mut_obj_iter_init(object, &iter);
	while (auto key = duckdb_yyjson::yyjson_mut_obj_iter_next(&iter)) {
		auto key_str = duckdb_yyjson::yyjson_mut_get_str(key);
		auto key_len = duckdb_yyjson::yyjson_mut_get_len(key);
		if (skip_key && skip_key->size() == key_len && memcmp(skip_key->data(), key_str, key_len) == 0) {
			continue;
		}
		if (duckdb_yyjson::yyjson_mut_obj_getn(merged, key_str, key_len)) {
			continue;
		}
		auto key_copy = duckdb_yyjson::yyjson_mut_strncpy(doc, key_str, key_len);
		duckdb_yyjson::yyjson_mut_obj_add(
		    merged, key_copy,
		    duckdb_yyjson::yyjson_mut_val_mut_copy(doc, duckdb_yyjson::yyjson_mut_obj_iter_get_val(key)));
	}
}

static void EmitMergedRow(duckdb_yyjson::yyjson_mut_doc *doc, duckdb_yyjson::yyjson_mut_val *out_rows, yyjson_val *leaf,
                          const vector<RawExplodeEnvelope> &envelopes) {
	if (!duckdb_yyjson::yyjson_is_obj(leaf)) {
		// scalar leaf elements cannot be merged: pass them through
		duckdb_yyjson::yyjson_mut_arr_append(out_rows, duckdb_yyjson::yyjson_val_mut_copy(doc, leaf));
		return;
	}
	auto merged = duckdb_yyjson::yyjson_mut_obj(doc);
	AddObjectFields(doc, merged, leaf, nullptr);
	// envelopes from deepest to shallowest: deeper fields win on collision
	for (idx_t i = envelopes.size(); i > 0; i--) {
		AddObjectFields(doc, merged, envelopes[i - 1].object, envelopes[i - 1].descended_key);
	}
	duckdb_yyjson::yyjson_mut_arr_append(out_rows, merged);
}

// Recursive multi-level explode: each path segment descends into an object
// key; arrays along the way fan out into one row per element. This handles
// both flat envelopes (cloudwatch-logs: logEvents) and nested ones
// (otlp-traces: resourceSpans.scopeSpans.spans).
static void ExplodeInto(duckdb_yyjson::yyjson_mut_doc *doc, duckdb_yyjson::yyjson_mut_val *out_rows, yyjson_val *node,
                        const vector<string> &path, idx_t depth, vector<RawExplodeEnvelope> &envelopes) {
	if (depth == path.size()) {
		EmitMergedRow(doc, out_rows, node, envelopes);
		return;
	}
	if (!duckdb_yyjson::yyjson_is_obj(node)) {
		EmitMergedRow(doc, out_rows, node, envelopes);
		return;
	}
	auto &segment = path[depth];
	auto child = duckdb_yyjson::yyjson_obj_getn(node, segment.c_str(), segment.size());
	if (!child) {
		// the hop is absent: pass the row through unchanged (merged with any
		// envelopes accumulated so far)
		EmitMergedRow(doc, out_rows, node, envelopes);
		return;
	}
	envelopes.push_back(RawExplodeEnvelope {node, &segment});
	if (duckdb_yyjson::yyjson_is_arr(child)) {
		duckdb_yyjson::yyjson_arr_iter elements;
		duckdb_yyjson::yyjson_arr_iter_init(child, &elements);
		while (auto element = duckdb_yyjson::yyjson_arr_iter_next(&elements)) {
			ExplodeInto(doc, out_rows, element, path, depth + 1, envelopes);
		}
	} else {
		ExplodeInto(doc, out_rows, child, path, depth + 1, envelopes);
	}
	envelopes.pop_back();
}

//===--------------------------------------------------------------------===//
// OTLP semantic normalization (applied per exploded row when options.otlp):
// - KeyValue arrays under an "attributes" member spread into the parent
//   object as real fields (existing fields win on collision)
// - AnyValue wrappers unwrap: {"stringValue": s} -> s, {"intValue": "1"} -> 1,
//   arrayValue/kvlistValue recurse
// - all-digit *UnixNano strings become integers
// - traceId/spanId/parentSpanId base64 strings become lowercase hex: the
//   OTLP/JSON mapping requires hex for these two bytes fields, but protobuf's
//   generic JSON conversion (the gRPC and OTLP/HTTP-protobuf decode paths)
//   emits base64
//===--------------------------------------------------------------------===//

static bool OtlpIsHex(const char *str, size_t len) {
	for (size_t i = 0; i < len; i++) {
		auto c = str[i];
		if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'))) {
			return false;
		}
	}
	return true;
}

// strict standard base64 (padded), as produced by protobuf's JSON conversion
static bool OtlpBase64Decode(const char *str, size_t len, string &out) {
	if (len == 0 || len % 4 != 0) {
		return false;
	}
	auto decode = [](char c) -> int {
		if (c >= 'A' && c <= 'Z') {
			return c - 'A';
		}
		if (c >= 'a' && c <= 'z') {
			return c - 'a' + 26;
		}
		if (c >= '0' && c <= '9') {
			return c - '0' + 52;
		}
		if (c == '+') {
			return 62;
		}
		if (c == '/') {
			return 63;
		}
		return -1;
	};
	size_t padding = str[len - 1] == '=' ? (str[len - 2] == '=' ? 2 : 1) : 0;
	out.reserve(len / 4 * 3 - padding);
	for (size_t i = 0; i < len; i += 4) {
		int values[4];
		for (size_t j = 0; j < 4; j++) {
			auto c = str[i + j];
			if (c == '=') {
				// padding is only valid at the very end
				if (i + 4 != len || j < 4 - padding) {
					return false;
				}
				values[j] = 0;
				continue;
			}
			values[j] = decode(c);
			if (values[j] < 0) {
				return false;
			}
		}
		out.push_back(static_cast<char>((values[0] << 2) | (values[1] >> 4)));
		if (i + 4 != len || padding < 2) {
			out.push_back(static_cast<char>((values[1] << 4) | (values[2] >> 2)));
		}
		if (i + 4 != len || padding < 1) {
			out.push_back(static_cast<char>((values[2] << 6) | values[3]));
		}
	}
	return true;
}

// returns the expected decoded byte width for OTLP id fields, 0 otherwise
static size_t OtlpIdWidth(const char *key, size_t key_len) {
	if (key_len == 7 && memcmp(key, "traceId", 7) == 0) {
		return 16;
	}
	if ((key_len == 6 && memcmp(key, "spanId", 6) == 0) || (key_len == 12 && memcmp(key, "parentSpanId", 12) == 0)) {
		return 8;
	}
	return 0;
}

static duckdb_yyjson::yyjson_mut_val *OtlpNormalizeValue(duckdb_yyjson::yyjson_mut_doc *doc,
                                                         duckdb_yyjson::yyjson_mut_val *value, const char *key,
                                                         size_t key_len, idx_t depth);

static bool OtlpIsKeyValueArray(duckdb_yyjson::yyjson_mut_val *value) {
	if (!duckdb_yyjson::yyjson_mut_is_arr(value) || duckdb_yyjson::yyjson_mut_arr_size(value) == 0) {
		return false;
	}
	duckdb_yyjson::yyjson_mut_arr_iter iter;
	duckdb_yyjson::yyjson_mut_arr_iter_init(value, &iter);
	while (auto element = duckdb_yyjson::yyjson_mut_arr_iter_next(&iter)) {
		if (!duckdb_yyjson::yyjson_mut_is_obj(element) || !duckdb_yyjson::yyjson_mut_obj_get(element, "key")) {
			return false;
		}
	}
	return true;
}

// {"stringValue": ...} / {"intValue": "123"} / {"arrayValue": {"values": [..]}} ...
static duckdb_yyjson::yyjson_mut_val *OtlpUnwrapAnyValue(duckdb_yyjson::yyjson_mut_doc *doc,
                                                         duckdb_yyjson::yyjson_mut_val *value) {
	if (!duckdb_yyjson::yyjson_mut_is_obj(value) || duckdb_yyjson::yyjson_mut_obj_size(value) != 1) {
		return nullptr;
	}
	duckdb_yyjson::yyjson_mut_obj_iter iter;
	duckdb_yyjson::yyjson_mut_obj_iter_init(value, &iter);
	auto key = duckdb_yyjson::yyjson_mut_obj_iter_next(&iter);
	auto inner = duckdb_yyjson::yyjson_mut_obj_iter_get_val(key);
	string name(duckdb_yyjson::yyjson_mut_get_str(key), duckdb_yyjson::yyjson_mut_get_len(key));
	if (name == "stringValue" || name == "boolValue" || name == "doubleValue" || name == "bytesValue") {
		return inner;
	}
	if (name == "intValue") {
		if (duckdb_yyjson::yyjson_mut_is_str(inner)) {
			auto str = duckdb_yyjson::yyjson_mut_get_str(inner);
			int64_t parsed;
			if (TryCast::Operation<string_t, int64_t>(string_t(str, duckdb_yyjson::yyjson_mut_get_len(inner)),
			                                          parsed)) {
				return duckdb_yyjson::yyjson_mut_sint(doc, parsed);
			}
		}
		return inner;
	}
	if (name == "arrayValue" || name == "kvlistValue") {
		auto values =
		    duckdb_yyjson::yyjson_mut_is_obj(inner) ? duckdb_yyjson::yyjson_mut_obj_get(inner, "values") : nullptr;
		if (!values) {
			return nullptr;
		}
		return OtlpNormalizeValue(doc, values, name == "kvlistValue" ? "attributes" : "", 0, 0);
	}
	return nullptr;
}

// Objects rebuilt per row (merged span rows, nested "status" etc.) almost
// always fit this inline capacity: only a pathologically wide object spills
// to the heap. Avoids a malloc/free per object per row on the OTLP path.
static constexpr idx_t OTLP_NORMALIZE_INLINE_MEMBERS = 32;

static void OtlpNormalizeObject(duckdb_yyjson::yyjson_mut_doc *doc, duckdb_yyjson::yyjson_mut_val *object,
                                idx_t depth) {
	using MemberPair = pair<duckdb_yyjson::yyjson_mut_val *, duckdb_yyjson::yyjson_mut_val *>;
	// rebuild members so attribute lists can spread into the parent
	MemberPair inline_members[OTLP_NORMALIZE_INLINE_MEMBERS];
	idx_t inline_count = 0;
	vector<MemberPair> overflow_members;
	duckdb_yyjson::yyjson_mut_obj_iter iter;
	duckdb_yyjson::yyjson_mut_obj_iter_init(object, &iter);
	while (auto key = duckdb_yyjson::yyjson_mut_obj_iter_next(&iter)) {
		auto value = duckdb_yyjson::yyjson_mut_obj_iter_get_val(key);
		if (inline_count < OTLP_NORMALIZE_INLINE_MEMBERS) {
			inline_members[inline_count++] = MemberPair(key, value);
		} else {
			overflow_members.emplace_back(key, value);
		}
	}
	duckdb_yyjson::yyjson_mut_obj_clear(object);

	auto process_member = [&](const MemberPair &member) {
		auto key_str = duckdb_yyjson::yyjson_mut_get_str(member.first);
		auto key_len = duckdb_yyjson::yyjson_mut_get_len(member.first);
		if (key_len == 10 && memcmp(key_str, "attributes", 10) == 0 && OtlpIsKeyValueArray(member.second)) {
			// spread the KeyValue list into this object; real fields win
			duckdb_yyjson::yyjson_mut_arr_iter elements;
			duckdb_yyjson::yyjson_mut_arr_iter_init(member.second, &elements);
			while (auto element = duckdb_yyjson::yyjson_mut_arr_iter_next(&elements)) {
				auto name = duckdb_yyjson::yyjson_mut_obj_get(element, "key");
				auto value = duckdb_yyjson::yyjson_mut_obj_get(element, "value");
				if (!name || !duckdb_yyjson::yyjson_mut_is_str(name)) {
					continue;
				}
				auto normalized = value ? OtlpNormalizeValue(doc, value, "", 0, depth + 1) : nullptr;
				if (normalized && !duckdb_yyjson::yyjson_mut_obj_getn(object, duckdb_yyjson::yyjson_mut_get_str(name),
				                                                      duckdb_yyjson::yyjson_mut_get_len(name))) {
					duckdb_yyjson::yyjson_mut_obj_add(object, name, normalized);
				}
			}
			return;
		}
		auto normalized = OtlpNormalizeValue(doc, member.second, key_str, key_len, depth + 1);
		if (!duckdb_yyjson::yyjson_mut_obj_getn(object, key_str, key_len)) {
			duckdb_yyjson::yyjson_mut_obj_add(object, member.first, normalized ? normalized : member.second);
		}
	};
	for (idx_t i = 0; i < inline_count; i++) {
		process_member(inline_members[i]);
	}
	for (auto &member : overflow_members) {
		process_member(member);
	}
}

static duckdb_yyjson::yyjson_mut_val *OtlpNormalizeValue(duckdb_yyjson::yyjson_mut_doc *doc,
                                                         duckdb_yyjson::yyjson_mut_val *value, const char *key,
                                                         size_t key_len, idx_t depth) {
	if (depth > RAW_MAX_NESTING) {
		// too deep to normalize safely: keep the value verbatim
		return value;
	}
	if (duckdb_yyjson::yyjson_mut_is_obj(value)) {
		auto unwrapped = OtlpUnwrapAnyValue(doc, value);
		if (unwrapped) {
			return unwrapped;
		}
		OtlpNormalizeObject(doc, value, depth + 1);
		return value;
	}
	if (OtlpIsKeyValueArray(value)) {
		// bare KeyValue list (kvlistValue payloads): becomes an object
		auto converted = duckdb_yyjson::yyjson_mut_obj(doc);
		duckdb_yyjson::yyjson_mut_arr_iter elements;
		duckdb_yyjson::yyjson_mut_arr_iter_init(value, &elements);
		while (auto element = duckdb_yyjson::yyjson_mut_arr_iter_next(&elements)) {
			auto name = duckdb_yyjson::yyjson_mut_obj_get(element, "key");
			auto inner = duckdb_yyjson::yyjson_mut_obj_get(element, "value");
			if (name && duckdb_yyjson::yyjson_mut_is_str(name) && inner) {
				auto normalized = OtlpNormalizeValue(doc, inner, "", 0, depth + 1);
				duckdb_yyjson::yyjson_mut_obj_add(converted, name, normalized ? normalized : inner);
			}
		}
		return converted;
	}
	if (duckdb_yyjson::yyjson_mut_is_arr(value)) {
		duckdb_yyjson::yyjson_mut_arr_iter elements;
		duckdb_yyjson::yyjson_mut_arr_iter_init(value, &elements);
		vector<duckdb_yyjson::yyjson_mut_val *> originals;
		while (auto element = duckdb_yyjson::yyjson_mut_arr_iter_next(&elements)) {
			originals.push_back(element);
		}
		duckdb_yyjson::yyjson_mut_arr_clear(value);
		for (auto element : originals) {
			auto normalized = OtlpNormalizeValue(doc, element, "", 0, depth + 1);
			duckdb_yyjson::yyjson_mut_arr_append(value, normalized ? normalized : element);
		}
		return value;
	}
	// protobuf JSON conversion emits bytes id fields as base64; OTLP/JSON
	// mandates hex, so normalize (already-hex values pass through untouched)
	auto id_width = duckdb_yyjson::yyjson_mut_is_str(value) ? OtlpIdWidth(key, key_len) : 0;
	if (id_width) {
		auto str = duckdb_yyjson::yyjson_mut_get_str(value);
		auto len = duckdb_yyjson::yyjson_mut_get_len(value);
		if (!(len == id_width * 2 && OtlpIsHex(str, len))) {
			string decoded;
			if (OtlpBase64Decode(str, len, decoded) && decoded.size() == id_width) {
				static const char *hex_digits = "0123456789abcdef";
				string hex;
				hex.reserve(decoded.size() * 2);
				for (auto byte : decoded) {
					hex.push_back(hex_digits[(static_cast<unsigned char>(byte) >> 4) & 0xF]);
					hex.push_back(hex_digits[static_cast<unsigned char>(byte) & 0xF]);
				}
				return duckdb_yyjson::yyjson_mut_strncpy(doc, hex.c_str(), hex.size());
			}
		}
		return value;
	}
	// protobuf uint64 timestamps arrive as digit strings
	if (duckdb_yyjson::yyjson_mut_is_str(value) && key_len >= 8 && memcmp(key + key_len - 8, "UnixNano", 8) == 0) {
		auto str = duckdb_yyjson::yyjson_mut_get_str(value);
		int64_t parsed;
		if (TryCast::Operation<string_t, int64_t>(string_t(str, duckdb_yyjson::yyjson_mut_get_len(value)), parsed)) {
			return duckdb_yyjson::yyjson_mut_sint(doc, parsed);
		}
	}
	return value;
}

// OTLP metrics stop at the Metric message after the builtin path
// (…metrics). The hot grain is NumberDataPoint / HistogramDataPoint — same
// role spans play for traces. Without a second fan-out, sum/gauge/…dataPoints
// land as a single overflow column (VARIANT on v2). Walk each metric type
// field and explode its dataPoints array so attributes shred to typed columns.
static void ExplodeOtlpMetricDataPoints(duckdb_yyjson::yyjson_mut_doc *doc, duckdb_yyjson::yyjson_mut_val *&out_rows) {
	static const char *METRIC_TYPES[] = {"sum", "gauge", "histogram", "exponentialHistogram", "summary"};
	auto next_rows = duckdb_yyjson::yyjson_mut_arr(doc);
	duckdb_yyjson::yyjson_mut_arr_iter metrics;
	duckdb_yyjson::yyjson_mut_arr_iter_init(out_rows, &metrics);
	while (auto metric = duckdb_yyjson::yyjson_mut_arr_iter_next(&metrics)) {
		if (!duckdb_yyjson::yyjson_mut_is_obj(metric)) {
			duckdb_yyjson::yyjson_mut_arr_append(next_rows, metric);
			continue;
		}
		duckdb_yyjson::yyjson_mut_val *type_obj = nullptr;
		const char *type_key = nullptr;
		for (auto key : METRIC_TYPES) {
			auto candidate = duckdb_yyjson::yyjson_mut_obj_get(metric, key);
			if (candidate && duckdb_yyjson::yyjson_mut_is_obj(candidate)) {
				type_obj = candidate;
				type_key = key;
				break;
			}
		}
		auto data_points = type_obj ? duckdb_yyjson::yyjson_mut_obj_get(type_obj, "dataPoints") : nullptr;
		if (!data_points || !duckdb_yyjson::yyjson_mut_is_arr(data_points) ||
		    duckdb_yyjson::yyjson_mut_arr_size(data_points) == 0) {
			duckdb_yyjson::yyjson_mut_arr_append(next_rows, metric);
			continue;
		}
		string type_key_str(type_key);
		string data_points_key("dataPoints");
		duckdb_yyjson::yyjson_mut_arr_iter points;
		duckdb_yyjson::yyjson_mut_arr_iter_init(data_points, &points);
		while (auto point = duckdb_yyjson::yyjson_mut_arr_iter_next(&points)) {
			auto merged = duckdb_yyjson::yyjson_mut_obj(doc);
			// data point fields first, then type fields, then metric envelope
			if (duckdb_yyjson::yyjson_mut_is_obj(point)) {
				AddMutObjectFields(doc, merged, point, nullptr);
			}
			AddMutObjectFields(doc, merged, type_obj, &data_points_key);
			AddMutObjectFields(doc, merged, metric, &type_key_str);
			duckdb_yyjson::yyjson_mut_arr_append(next_rows, merged);
		}
	}
	out_rows = next_rows;
	duckdb_yyjson::yyjson_mut_doc_set_root(doc, out_rows);
}

void RawPayload::Explode(const vector<string> &path) {
	auto mut_doc = duckdb_yyjson::yyjson_mut_doc_new(nullptr);
	auto out_rows = duckdb_yyjson::yyjson_mut_arr(mut_doc);
	duckdb_yyjson::yyjson_mut_doc_set_root(mut_doc, out_rows);

	vector<RawExplodeEnvelope> envelopes;
	for (auto row : rows) {
		ExplodeInto(mut_doc, out_rows, row, path, 0, envelopes);
	}
	// otlp-metrics path ends in "metrics": shred to data-point grain next
	if (otlp_semantics && !path.empty() && path.back() == "metrics") {
		ExplodeOtlpMetricDataPoints(mut_doc, out_rows);
	}
	if (otlp_semantics) {
		duckdb_yyjson::yyjson_mut_arr_iter normalized_rows;
		duckdb_yyjson::yyjson_mut_arr_iter_init(out_rows, &normalized_rows);
		while (auto row = duckdb_yyjson::yyjson_mut_arr_iter_next(&normalized_rows)) {
			OtlpNormalizeValue(mut_doc, row, "", 0, 0);
		}
	}

	auto exploded = duckdb_yyjson::yyjson_mut_doc_imut_copy(mut_doc, nullptr);
	duckdb_yyjson::yyjson_mut_doc_free(mut_doc);
	if (!exploded) {
		throw InternalException("RawDuck: failed to materialize exploded payload");
	}
	for (auto old_doc : docs) {
		duckdb_yyjson::yyjson_doc_free(old_doc);
	}
	docs.clear();
	rows.clear();
	docs.push_back(exploded);
	CollectRows(duckdb_yyjson::yyjson_doc_get_root(exploded), rows);
	CheckRowUniformity(rows, scalar_rows);
}

unique_ptr<RawNode> RawPayload::InferSchema() const {
	auto root = make_uniq<RawNode>();
	for (auto row : rows) {
		MergeValue(*root, row);
	}
	return root;
}

void RawPayloadAddDocument(RawParsedPayload &parsed, const char *data, idx_t size, bool ignore_errors) {
	auto doc = duckdb_yyjson::yyjson_read(data, size, RAW_READ_FLAGS);
	if (!doc) {
		if (ignore_errors) {
			parsed.payload.parse_errors++;
			return;
		}
		throw InvalidInputException("RawDuck: payload is not valid JSON");
	}
	parsed.payload.docs.push_back(doc);
	CollectRows(duckdb_yyjson::yyjson_doc_get_root(doc), parsed.payload.rows);
}

void RawPayloadFinalize(RawParsedPayload &parsed, const RawParseOptions &options) {
	CheckRowUniformity(parsed.payload.rows, parsed.payload.scalar_rows);
	if (!options.explode_path.empty()) {
		parsed.payload.otlp_semantics = options.otlp;
		parsed.payload.Explode(options.explode_path);
	}
	parsed.root = parsed.payload.InferSchema();
	parsed.columns = FlattenSchema(*parsed.root, parsed.payload.scalar_rows);
}

shared_ptr<RawParsedPayload> RawParsedPayload::Process(const string &payload_text, const RawParseOptions &options) {
	auto result = make_shared_ptr<RawParsedPayload>();
	result->payload.Parse(payload_text, options);
	result->root = result->payload.InferSchema();
	result->columns = FlattenSchema(*result->root, result->payload.scalar_rows);
	return result;
}

const vector<pair<string, string>> &RawBuiltinTransforms() {
	static const vector<pair<string, string>> builtins = {
	    {"cloudwatch-logs", "logEvents"},
	    {"cloudtrail", "Records"},
	    {"firehose", "records"},
	    {"otlp-traces", "resourceSpans.scopeSpans.spans"},
	    {"otlp-logs", "resourceLogs.scopeLogs.logRecords"},
	    {"otlp-metrics", "resourceMetrics.scopeMetrics.metrics"},
	};
	return builtins;
}

bool ResolveBuiltinTransform(const string &name, string &path) {
	auto lower = StringUtil::Lower(name);
	for (auto &builtin : RawBuiltinTransforms()) {
		if (builtin.first == lower) {
			path = builtin.second;
			return true;
		}
	}
	return false;
}

bool RawTransformIsOtlp(const string &name) {
	return StringUtil::StartsWith(StringUtil::Lower(name), "otlp-");
}

RawParseOptions RawExplodeOptions(const string &path) {
	RawParseOptions options;
	if (!path.empty()) {
		options.explode_path = StringUtil::Split(path, '.');
	}
	return options;
}

//===--------------------------------------------------------------------===//
// Inference
//===--------------------------------------------------------------------===//

const RawNode *RawNode::FindChild(const char *key, idx_t key_len) const {
	if (children.size() <= RAW_CHILD_LINEAR_SCAN_LIMIT) {
		for (auto &entry : children) {
			auto &name = entry.first;
			if (name.size() == key_len && memcmp(name.data(), key, key_len) == 0) {
				return entry.second.get();
			}
		}
		return nullptr;
	}
	auto entry = child_lookup.find(string(key, key_len));
	return entry == child_lookup.end() ? nullptr : children[entry->second].second.get();
}

RawNode &RawNode::GetOrCreateChild(const char *key, idx_t key_len) {
	if (auto existing = FindChild(key, key_len)) {
		return *const_cast<RawNode *>(existing);
	}
	string key_str(key, key_len);
	// keep child_lookup populated from the start so it's already correct once
	// fanout crosses the linear-scan threshold and FindChild starts using it.
	child_lookup[key_str] = children.size();
	children.emplace_back(std::move(key_str), make_uniq<RawNode>());
	return *children.back().second;
}

RawScalarKind JoinScalarKinds(RawScalarKind a, RawScalarKind b) {
	if (a == RawScalarKind::UNSET) {
		return b;
	}
	if (b == RawScalarKind::UNSET || a == b) {
		return a;
	}
	auto is_numeric = [](RawScalarKind k) {
		return k == RawScalarKind::BIGINT || k == RawScalarKind::DOUBLE;
	};
	auto is_temporal = [](RawScalarKind k) {
		return k == RawScalarKind::DATE || k == RawScalarKind::TIMESTAMP;
	};
	if (is_numeric(a) && is_numeric(b)) {
		return RawScalarKind::DOUBLE;
	}
	if (is_temporal(a) && is_temporal(b)) {
		return RawScalarKind::TIMESTAMP;
	}
	return RawScalarKind::VARCHAR;
}

// Detect ISO dates/timestamps in strings so they land in temporal columns.
static RawScalarKind SniffString(const char *str, idx_t len) {
	// cheap pre-filter: temporal strings start with a digit and contain '-'
	if (len < 8 || len > 40 || !StringUtil::CharacterIsDigit(str[0])) {
		return RawScalarKind::VARCHAR;
	}
	idx_t pos = 0;
	date_t date_result;
	bool special = false;
	if (Date::TryConvertDate(str, len, pos, date_result, special, true) == DateCastResult::SUCCESS && pos == len &&
	    !special) {
		return RawScalarKind::DATE;
	}
	timestamp_t ts_result;
	if (Timestamp::TryConvertTimestamp(str, len, ts_result, true) == TimestampCastResult::SUCCESS) {
		return RawScalarKind::TIMESTAMP;
	}
	return RawScalarKind::VARCHAR;
}

RawScalarKind SniffScalarKind(yyjson_val *val) {
	switch (duckdb_yyjson::yyjson_get_type(val)) {
	case YYJSON_TYPE_BOOL:
		return RawScalarKind::BOOLEAN;
	case YYJSON_TYPE_NUM:
		if (duckdb_yyjson::yyjson_is_real(val)) {
			return RawScalarKind::DOUBLE;
		}
		if (duckdb_yyjson::yyjson_is_uint(val) &&
		    duckdb_yyjson::yyjson_get_uint(val) > static_cast<uint64_t>(NumericLimits<int64_t>::Maximum())) {
			// out of BIGINT range: degrade to DOUBLE rather than fail
			return RawScalarKind::DOUBLE;
		}
		return RawScalarKind::BIGINT;
	case YYJSON_TYPE_STR:
		return SniffString(duckdb_yyjson::yyjson_get_str(val), duckdb_yyjson::yyjson_get_len(val));
	default:
		return RawScalarKind::VARCHAR;
	}
}

static void MergeValueInternal(RawNode &node, yyjson_val *val, idx_t depth);

static void DemoteToOverflow(RawNode &node) {
	node.node_class = RawNodeClass::VARIANT;
	node.children.clear();
	node.child_lookup.clear();
	node.element.reset();
}

void MergeValue(RawNode &node, yyjson_val *val) {
	MergeValueInternal(node, val, 0);
}

static void MergeValueInternal(RawNode &node, yyjson_val *val, idx_t depth) {
	if (depth > RAW_MAX_NESTING) {
		DemoteToOverflow(node);
		return;
	}
	if (!val || duckdb_yyjson::yyjson_is_null(val)) {
		return;
	}
	if (node.node_class == RawNodeClass::VARIANT) {
		return;
	}
	if (duckdb_yyjson::yyjson_is_obj(val)) {
		if (node.node_class == RawNodeClass::UNSET) {
			node.node_class = RawNodeClass::OBJECT;
		}
		if (node.node_class != RawNodeClass::OBJECT) {
			DemoteToOverflow(node);
			return;
		}
		yyjson_obj_iter iter;
		duckdb_yyjson::yyjson_obj_iter_init(val, &iter);
		while (auto key = duckdb_yyjson::yyjson_obj_iter_next(&iter)) {
			auto child_val = duckdb_yyjson::yyjson_obj_iter_get_val(key);
			MergeValueInternal(
			    node.GetOrCreateChild(duckdb_yyjson::yyjson_get_str(key), duckdb_yyjson::yyjson_get_len(key)),
			    child_val, depth + 1);
		}
		return;
	}
	if (duckdb_yyjson::yyjson_is_arr(val)) {
		if (node.node_class == RawNodeClass::UNSET) {
			node.node_class = RawNodeClass::ARRAY;
			node.element = make_uniq<RawNode>();
		}
		if (node.node_class != RawNodeClass::ARRAY) {
			DemoteToOverflow(node);
			return;
		}
		yyjson_arr_iter iter;
		duckdb_yyjson::yyjson_arr_iter_init(val, &iter);
		while (auto element = duckdb_yyjson::yyjson_arr_iter_next(&iter)) {
			MergeValueInternal(*node.element, element, depth + 1);
		}
		return;
	}
	// scalar
	if (node.node_class == RawNodeClass::SCALAR && node.scalar == RawScalarKind::VARCHAR) {
		// already the scalar sink: no sniffing can change it
		return;
	}
	auto kind = SniffScalarKind(val);
	if (node.node_class == RawNodeClass::UNSET) {
		node.node_class = RawNodeClass::SCALAR;
		node.scalar = kind;
		return;
	}
	if (node.node_class != RawNodeClass::SCALAR) {
		DemoteToOverflow(node);
		return;
	}
	node.scalar = JoinScalarKinds(node.scalar, kind);
}

//===--------------------------------------------------------------------===//
// Schema flattening
//===--------------------------------------------------------------------===//

static LogicalType ScalarKindToType(RawScalarKind kind) {
	switch (kind) {
	case RawScalarKind::BOOLEAN:
		return LogicalType::BOOLEAN;
	case RawScalarKind::BIGINT:
		return LogicalType::BIGINT;
	case RawScalarKind::DOUBLE:
		return LogicalType::DOUBLE;
	case RawScalarKind::DATE:
		return LogicalType::DATE;
	case RawScalarKind::TIMESTAMP:
		return LogicalType::TIMESTAMP;
	default:
		return LogicalType::VARCHAR;
	}
}

LogicalType NodeToType(const RawNode &node) {
	switch (node.node_class) {
	case RawNodeClass::UNSET:
		return LogicalType::VARCHAR;
	case RawNodeClass::SCALAR:
		return ScalarKindToType(node.scalar);
	case RawNodeClass::ARRAY: {
		auto &element = *node.element;
		switch (element.node_class) {
		case RawNodeClass::UNSET:
			return LogicalType::LIST(LogicalType::VARCHAR);
		case RawNodeClass::SCALAR:
			return LogicalType::LIST(ScalarKindToType(element.scalar));
		case RawNodeClass::ARRAY:
			return LogicalType::LIST(NodeToType(element));
		default:
			// arrays of objects (or mixed) stay as a single overflow value
			return RawOverflowType();
		}
	}
	default:
		return RawOverflowType();
	}
}

static void FlattenInto(const RawNode &node, const string &prefix, vector<string> &path, idx_t depth,
                        vector<RawColumn> &result) {
	for (auto &entry : node.children) {
		auto &name = entry.first;
		auto &child = *entry.second;
		auto column_name = prefix.empty() ? name : prefix + "." + name;
		path.push_back(name);
		if (child.node_class == RawNodeClass::OBJECT && !child.children.empty() && depth < RAW_MAX_FLATTEN_DEPTH) {
			FlattenInto(child, column_name, path, depth + 1, result);
		} else {
			result.push_back(RawColumn {column_name, path, NodeToType(child), &child});
		}
		path.pop_back();
	}
}

vector<RawColumn> FlattenSchema(const RawNode &root, bool scalar_rows) {
	vector<RawColumn> result;
	if (scalar_rows || root.node_class != RawNodeClass::OBJECT) {
		if (root.node_class == RawNodeClass::UNSET) {
			return result;
		}
		result.push_back(RawColumn {"value", {}, NodeToType(root), &root});
		return result;
	}
	vector<string> path;
	FlattenInto(root, "", path, 0, result);
	if (result.size() > RAW_MAX_COLUMNS) {
		throw InvalidInputException(
		    "RawDuck: payload flattens to %llu columns, exceeding the limit of %llu; restructure the payload or "
		    "reduce key cardinality",
		    result.size(), RAW_MAX_COLUMNS);
	}
	// flattening can collide (e.g. key "a.b" vs nested a->b): disambiguate
	case_insensitive_set_t seen;
	for (auto &column : result) {
		auto base = column.name;
		idx_t suffix = 1;
		while (seen.count(column.name)) {
			column.name = base + "_" + to_string(suffix++);
		}
		seen.insert(column.name);
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Value extraction
//===--------------------------------------------------------------------===//

bool IsRawJSONType(const LogicalType &type) {
	return type.id() == LogicalTypeId::VARCHAR && type.GetAlias() == LogicalType::JSON_TYPE_NAME;
}

LogicalType RawOverflowType() {
	return LogicalType::VARIANT();
}

bool IsRawVariantType(const LogicalType &type) {
	return type.id() == LogicalTypeId::VARIANT;
}

bool IsRawOverflowType(const LogicalType &type) {
	return IsRawVariantType(type) || IsRawJSONType(type);
}

bool RawFillSupported(const LogicalType &type) {
	if (IsRawOverflowType(type)) {
		return true;
	}
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::VARCHAR:
		return true;
	case LogicalTypeId::LIST:
		return RawFillSupported(ListType::GetChildType(type));
	default:
		return false;
	}
}

RawExtractor::RawExtractor(const RawNode &root_p, const vector<RawColumn> &columns) : root(root_p) {
	values.resize(columns.size());
	for (idx_t col = 0; col < columns.size(); col++) {
		if (columns[col].path.empty()) {
			// "value" column: the row itself is the value
			root_is_column = true;
		}
		column_of[columns[col].node] = col;
	}
}

void RawExtractor::Reset(idx_t row_count) {
	for (auto &column_values : values) {
		column_values.assign(row_count, nullptr);
	}
}

void RawExtractor::Traverse(yyjson_val *val, const RawNode &node, idx_t row_idx) {
	auto leaf = column_of.find(&node);
	if (leaf != column_of.end()) {
		values[leaf->second][row_idx] = val;
		return;
	}
	if (node.node_class != RawNodeClass::OBJECT || !duckdb_yyjson::yyjson_is_obj(val)) {
		// value at a path that is neither a column nor a flattened object
		// (e.g. a structural mismatch): nothing to route
		return;
	}
	yyjson_obj_iter iter;
	duckdb_yyjson::yyjson_obj_iter_init(val, &iter);
	while (auto key = duckdb_yyjson::yyjson_obj_iter_next(&iter)) {
		auto child = node.FindChild(duckdb_yyjson::yyjson_get_str(key), duckdb_yyjson::yyjson_get_len(key));
		if (!child) {
			continue;
		}
		Traverse(duckdb_yyjson::yyjson_obj_iter_get_val(key), *child, row_idx);
	}
}

void RawExtractor::AssignRow(yyjson_val *row, idx_t row_idx) {
	if (root_is_column) {
		values[column_of.at(&root)][row_idx] = row;
		return;
	}
	Traverse(row, root, row_idx);
}

static string_t WriteJSONString(yyjson_val *val, Vector &result) {
	size_t len = 0;
	auto data = duckdb_yyjson::yyjson_val_write(val, 0, &len);
	if (!data) {
		throw InternalException("RawDuck: failed to serialize JSON value");
	}
	auto str = StringVector::AddString(result, data, len);
	free(data);
	return str;
}

// Writes a batch of resolved JSON values into `result` starting at `offset`.
// Inference guarantees values fit the column type; anything that does not
// (or is null/missing) becomes NULL.
void FillVector(const vector<yyjson_val *> &vals, const LogicalType &type, Vector &result, idx_t offset) {
	// vectors carry their own size in DuckDB v2: writing through the raw data
	// pointer does not grow them, so declare the rows we are about to write
	FlatVector::SetSize(result, offset + vals.size());
	auto &validity = FlatVector::ValidityMutable(result);
	auto set_null = [&](idx_t i) {
		validity.SetInvalid(offset + i);
	};
	if (IsRawJSONType(type)) {
		auto data = FlatVector::GetDataMutable<string_t>(result);
		for (idx_t i = 0; i < vals.size(); i++) {
			auto val = vals[i];
			if (!val || duckdb_yyjson::yyjson_is_null(val)) {
				set_null(i);
				continue;
			}
			data[offset + i] = WriteJSONString(val, result);
		}
		return;
	}
	if (IsRawVariantType(type)) {
		// DuckDB's idiomatic path is JSON text → VARIANT (json_cast.test /
		// VARIANT storage tests). Serialize yyjson → JSON, then cast.
		Vector json_source(LogicalType::JSON(), vals.size());
		FlatVector::SetSize(json_source, vals.size());
		auto json_data = FlatVector::GetDataMutable<string_t>(json_source);
		auto &json_validity = FlatVector::ValidityMutable(json_source);
		for (idx_t i = 0; i < vals.size(); i++) {
			auto val = vals[i];
			if (!val || duckdb_yyjson::yyjson_is_null(val)) {
				json_validity.SetInvalid(i);
				continue;
			}
			json_data[i] = WriteJSONString(val, json_source);
		}
		if (offset == 0) {
			VectorOperations::DefaultCast(json_source, result, vals.size());
			return;
		}
		Vector variant_tmp(LogicalType::VARIANT(), vals.size());
		VectorOperations::DefaultCast(json_source, variant_tmp, vals.size());
		VectorOperations::Copy(variant_tmp, result, vals.size(), 0, offset);
		return;
	}
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN: {
		auto data = FlatVector::GetDataMutable<bool>(result);
		for (idx_t i = 0; i < vals.size(); i++) {
			auto val = vals[i];
			if (val && duckdb_yyjson::yyjson_is_bool(val)) {
				data[offset + i] = duckdb_yyjson::yyjson_get_bool(val);
			} else {
				set_null(i);
			}
		}
		break;
	}
	case LogicalTypeId::BIGINT: {
		auto data = FlatVector::GetDataMutable<int64_t>(result);
		for (idx_t i = 0; i < vals.size(); i++) {
			auto val = vals[i];
			if (val && duckdb_yyjson::yyjson_is_sint(val)) {
				data[offset + i] = duckdb_yyjson::yyjson_get_sint(val);
			} else if (val && duckdb_yyjson::yyjson_is_uint(val)) {
				data[offset + i] = static_cast<int64_t>(duckdb_yyjson::yyjson_get_uint(val));
			} else {
				set_null(i);
			}
		}
		break;
	}
	case LogicalTypeId::DOUBLE: {
		auto data = FlatVector::GetDataMutable<double>(result);
		for (idx_t i = 0; i < vals.size(); i++) {
			auto val = vals[i];
			if (val && duckdb_yyjson::yyjson_is_num(val)) {
				data[offset + i] = duckdb_yyjson::yyjson_get_num(val);
			} else {
				set_null(i);
			}
		}
		break;
	}
	case LogicalTypeId::DATE: {
		auto data = FlatVector::GetDataMutable<date_t>(result);
		for (idx_t i = 0; i < vals.size(); i++) {
			auto val = vals[i];
			idx_t pos = 0;
			bool special = false;
			date_t converted;
			if (!val || !duckdb_yyjson::yyjson_is_str(val) ||
			    Date::TryConvertDate(duckdb_yyjson::yyjson_get_str(val), duckdb_yyjson::yyjson_get_len(val), pos,
			                         converted, special, true) != DateCastResult::SUCCESS) {
				set_null(i);
			} else {
				data[offset + i] = converted;
			}
		}
		break;
	}
	case LogicalTypeId::TIMESTAMP: {
		auto data = FlatVector::GetDataMutable<timestamp_t>(result);
		for (idx_t i = 0; i < vals.size(); i++) {
			auto val = vals[i];
			timestamp_t converted;
			if (!val || !duckdb_yyjson::yyjson_is_str(val) ||
			    Timestamp::TryConvertTimestamp(duckdb_yyjson::yyjson_get_str(val), duckdb_yyjson::yyjson_get_len(val),
			                                   converted, true) != TimestampCastResult::SUCCESS) {
				set_null(i);
			} else {
				data[offset + i] = converted;
			}
		}
		break;
	}
	case LogicalTypeId::VARCHAR: {
		auto data = FlatVector::GetDataMutable<string_t>(result);
		for (idx_t i = 0; i < vals.size(); i++) {
			auto val = vals[i];
			if (!val || duckdb_yyjson::yyjson_is_null(val)) {
				set_null(i);
			} else if (duckdb_yyjson::yyjson_is_str(val)) {
				data[offset + i] = StringVector::AddString(result, duckdb_yyjson::yyjson_get_str(val),
				                                           duckdb_yyjson::yyjson_get_len(val));
			} else {
				// mixed-type column: render non-strings as their JSON literal
				data[offset + i] = WriteJSONString(val, result);
			}
		}
		break;
	}
	case LogicalTypeId::LIST: {
		auto entries = FlatVector::GetDataMutable<list_entry_t>(result);
		auto child_offset = ListVector::GetListSize(result);
		vector<yyjson_val *> child_vals;
		for (idx_t i = 0; i < vals.size(); i++) {
			auto val = vals[i];
			if (!val || !duckdb_yyjson::yyjson_is_arr(val)) {
				set_null(i);
				entries[offset + i] = list_entry_t {child_offset + child_vals.size(), 0};
				continue;
			}
			auto length = duckdb_yyjson::yyjson_arr_size(val);
			entries[offset + i] = list_entry_t {child_offset + child_vals.size(), length};
			yyjson_arr_iter iter;
			duckdb_yyjson::yyjson_arr_iter_init(val, &iter);
			while (auto element = duckdb_yyjson::yyjson_arr_iter_next(&iter)) {
				child_vals.push_back(element);
			}
		}
		ListVector::Reserve(result, child_offset + child_vals.size());
		ListVector::SetListSize(result, child_offset + child_vals.size());
		FillVector(child_vals, ListType::GetChildType(type), ListVector::GetChildMutable(result), child_offset);
		break;
	}
	default:
		for (idx_t i = 0; i < vals.size(); i++) {
			set_null(i);
		}
		break;
	}
}

} // namespace duckdb
