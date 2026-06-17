#include "raw_functions.hpp"
#include "raw_json.hpp"

#include "duckdb/function/table_function.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"

#include "httplib.hpp"

#include <thread>
#include <unordered_map>

namespace duckdb {

//===--------------------------------------------------------------------===//
// raw_serve: an in-process HTTP API for RawMergeTree-style ingestion and querying.
//
//   GET    /health                     {"status":"ok"}
//   POST   /v1/query                   {"sql": "..."} -> meta/data/rows/statistics
//   GET    /v1/tables                  list tables
//   GET    /v1/tables/{table}          describe schema
//   POST   /v1/tables/{table}          schema-less ingest (?transform=&explode=&ignore_errors=)
//   DELETE /v1/tables/{table}          drop table
//   POST   /otlp/v1/{traces|logs|metrics}   OTLP ingest, JSON or protobuf body
//                                           (x-rawduck-table header routes)
//
// Server lifecycle is owned by a background thread; every request gets its
// own Connection (and so its own transaction) against the database instance.
//===--------------------------------------------------------------------===//

namespace {

struct RawApiServer {
	mutex lock;
	unique_ptr<duckdb_httplib::Server> server;
	std::thread thread;
	weak_ptr<DatabaseInstance> db;
	string host;
	int port = 0;
	string token;
	bool async = false;
	bool running = false;

	// stop the listener and join the thread; safe to call repeatedly
	void Shutdown() {
		if (running) {
			server->stop();
			thread.join();
			server.reset();
			running = false;
		}
	}
	~RawApiServer() {
		// a joinable thread in a static destructor would terminate the process
		Shutdown();
	}
};

RawApiServer &GetApiServer() {
	static RawApiServer server;
	return server;
}

//===----------------------------------------------------------------===//
// JSON helpers (yyjson mutable API)
//===----------------------------------------------------------------===//

struct JsonDoc {
	duckdb_yyjson::yyjson_mut_doc *doc;
	JsonDoc() : doc(duckdb_yyjson::yyjson_mut_doc_new(nullptr)) {
	}
	~JsonDoc() {
		duckdb_yyjson::yyjson_mut_doc_free(doc);
	}
	string Write(duckdb_yyjson::yyjson_mut_val *root) {
		duckdb_yyjson::yyjson_mut_doc_set_root(doc, root);
		size_t len = 0;
		auto data = duckdb_yyjson::yyjson_mut_write(doc, 0, &len);
		string result = data ? string(data, len) : string("{}");
		free(data);
		return result;
	}
};

void Respond(duckdb_httplib::Response &res, int status, JsonDoc &json, duckdb_yyjson::yyjson_mut_val *root) {
	res.status = status;
	res.set_content(json.Write(root), "application/json");
}

void RespondError(duckdb_httplib::Response &res, int status, const string &message, const string &hint = "") {
	JsonDoc json;
	auto root = duckdb_yyjson::yyjson_mut_obj(json.doc);
	duckdb_yyjson::yyjson_mut_obj_add_strncpy(json.doc, root, "error", message.c_str(), message.size());
	duckdb_yyjson::yyjson_mut_obj_add_strncpy(json.doc, root, "message", message.c_str(), message.size());
	duckdb_yyjson::yyjson_mut_obj_add_strncpy(json.doc, root, "hint", hint.c_str(), hint.size());
	Respond(res, status, json, root);
}

static string ApiTypeName(const string &duckdb_type) {
	static const unordered_map<string, string> mapping = {
	    {"VARCHAR", "String"}, {"BIGINT", "Int64"}, {"INTEGER", "Int32"},      {"INT32", "Int32"},
	    {"DOUBLE", "Float64"}, {"BOOLEAN", "Bool"}, {"TIMESTAMP", "DateTime"}, {"TIMESTAMP WITH TIME ZONE", "DateTime"},
	    {"DATE", "Date"},      {"JSON", "Json"},    {"UBIGINT", "UInt64"},     {"UINTEGER", "UInt32"},
	    {"FLOAT", "Float32"},  {"BLOB", "Bytes"},
	};
	auto it = mapping.find(duckdb_type);
	return it != mapping.end() ? it->second : duckdb_type;
}

static constexpr const char *API_CREATED_AT_PLACEHOLDER = "1970-01-01T00:00:00Z";

static pair<idx_t, idx_t> ApiTableStats(Connection &conn, const string &table) {
	idx_t total_rows = 0;
	auto count = conn.Query("SELECT COUNT(*)::UBIGINT FROM " + RawQualifiedTarget(table));
	if (count && !count->HasError() && count->RowCount() > 0) {
		total_rows = count->GetValue(0, 0).GetValue<idx_t>();
	}
	// hosted APIs expose byte counts; we approximate from row count.
	return {total_rows, total_rows * 64};
}

static void AddProjectOrganization(JsonDoc &json, duckdb_yyjson::yyjson_mut_val *root) {
	auto project = duckdb_yyjson::yyjson_mut_obj(json.doc);
	duckdb_yyjson::yyjson_mut_obj_add_str(json.doc, project, "name", "default");
	duckdb_yyjson::yyjson_mut_obj_add_val(json.doc, root, "project", project);
	auto organization = duckdb_yyjson::yyjson_mut_obj(json.doc);
	duckdb_yyjson::yyjson_mut_obj_add_str(json.doc, organization, "name", "default");
	duckdb_yyjson::yyjson_mut_obj_add_val(json.doc, root, "organization", organization);
}

static string InferOtlpTransform(const string &body) {
	if (body.find("\"resourceSpans\"") != string::npos) {
		return "otlp-traces";
	}
	if (body.find("\"resourceLogs\"") != string::npos) {
		return "otlp-logs";
	}
	if (body.find("\"resourceMetrics\"") != string::npos) {
		return "otlp-metrics";
	}
	return "";
}

static string RequestHeader(const duckdb_httplib::Request &req, std::initializer_list<const char *> names) {
	for (auto name : names) {
		if (req.has_header(name)) {
			return req.get_header_value(name);
		}
	}
	return "";
}

duckdb_yyjson::yyjson_mut_val *ValueToJson(JsonDoc &json, const Value &value) {
	if (value.IsNull()) {
		return duckdb_yyjson::yyjson_mut_null(json.doc);
	}
	switch (value.type().id()) {
	case LogicalTypeId::BOOLEAN:
		return duckdb_yyjson::yyjson_mut_bool(json.doc, value.GetValue<bool>());
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
		return duckdb_yyjson::yyjson_mut_sint(json.doc, value.GetValue<int64_t>());
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
		return duckdb_yyjson::yyjson_mut_real(json.doc, value.GetValue<double>());
	default: {
		auto text = value.ToString();
		return duckdb_yyjson::yyjson_mut_strncpy(json.doc, text.c_str(), text.size());
	}
	}
}

//===----------------------------------------------------------------===//
// request handling
//===----------------------------------------------------------------===//

shared_ptr<DatabaseInstance> RequireDatabase(duckdb_httplib::Response &res) {
	auto db = GetApiServer().db.lock();
	if (!db) {
		RespondError(res, 503, "database is no longer available");
	}
	return db;
}

bool Authorized(const duckdb_httplib::Request &req, duckdb_httplib::Response &res) {
	auto &token = GetApiServer().token;
	if (token.empty()) {
		return true;
	}
	if (RawTokenEquals(req.get_header_value("Authorization"), "Bearer " + token)) {
		return true;
	}
	RespondError(res, 401, "missing or invalid bearer token");
	return false;
}

RawParseOptions RequestParseOptions(ClientContext &context, const duckdb_httplib::Request &req,
                                    const string &body = "") {
	auto transform = req.get_param_value("transform");
	if (transform.empty() && !body.empty()) {
		transform = InferOtlpTransform(body);
	}
	auto options = ResolveTransform(context, transform, req.get_param_value("explode"));
	options.ignore_errors = req.get_param_value("ignore_errors") == "true";
	return options;
}

// OTLP responses use partialSuccess with a signal-specific rejected count
const char *OtlpRejectedField(const string &signal) {
	if (signal == "traces") {
		return "rejectedSpans";
	}
	if (signal == "logs") {
		return "rejectedLogRecords";
	}
	return "rejectedDataPoints";
}

void HandleIngest(const duckdb_httplib::Request &req, duckdb_httplib::Response &res, const string &table,
                  const string &otlp_signal) {
	auto db = RequireDatabase(res);
	if (!db) {
		return;
	}
	// gzip request bodies are decompressed by the server layer (zlib builds)
	auto &body = req.body;
	Connection conn(*db);
	{
		// async mode (rawduck_async_insert): enqueue and acknowledge, exactly
		// like SQL-level ingestion - the scaling answer for many concurrent
		// small-insert clients (batched commits, single-threaded evolution)
		auto options = otlp_signal.empty() ? RequestParseOptions(*conn.context, req, body)
		                                   : ResolveTransform(*conn.context, "otlp-" + otlp_signal, "");
		if (GetApiServer().async || RawAsyncEnabled(*conn.context)) {
			RawAsyncEnqueue(*conn.context, table, body, options);
			JsonDoc json;
			auto root = duckdb_yyjson::yyjson_mut_obj(json.doc);
			if (!otlp_signal.empty()) {
				duckdb_yyjson::yyjson_mut_obj_add_val(json.doc, root, "partialSuccess",
				                                      duckdb_yyjson::yyjson_mut_obj(json.doc));
				Respond(res, 200, json, root);
			} else {
				duckdb_yyjson::yyjson_mut_obj_add_strncpy(json.doc, root, "table", table.c_str(), table.size());
				duckdb_yyjson::yyjson_mut_obj_add_bool(json.doc, root, "queued", true);
				Respond(res, 202, json, root);
			}
			return;
		}
	}
	conn.BeginTransaction();
	try {
		auto options = otlp_signal.empty() ? RequestParseOptions(*conn.context, req, body)
		                                   : ResolveTransform(*conn.context, "otlp-" + otlp_signal, "");
		auto stats = RawIngestPayload(*conn.context, table, body, options);
		conn.Commit();
		JsonDoc json;
		auto root = duckdb_yyjson::yyjson_mut_obj(json.doc);
		if (!otlp_signal.empty()) {
			// OTLP/HTTP success response: empty partialSuccess means fully
			// accepted; rejected counts are reported per signal
			auto partial = duckdb_yyjson::yyjson_mut_obj(json.doc);
			if (stats.errors > 0) {
				duckdb_yyjson::yyjson_mut_obj_add_uint(json.doc, partial, OtlpRejectedField(otlp_signal), stats.errors);
				duckdb_yyjson::yyjson_mut_obj_add_str(json.doc, partial, "errorMessage",
				                                      "some records could not be parsed");
			}
			duckdb_yyjson::yyjson_mut_obj_add_val(json.doc, root, "partialSuccess", partial);
			Respond(res, 200, json, root);
			return;
		}
		duckdb_yyjson::yyjson_mut_obj_add_uint(json.doc, root, "inserted", stats.rows);
		Respond(res, 200, json, root);
	} catch (std::exception &ex) {
		conn.Rollback();
		RespondError(res, 400, ErrorData(ex).RawMessage());
	}
}

// OTLP/HTTP protobuf requests answer with protobuf bodies: an
// Export*ServiceResponse on success, a google.rpc.Status on failure
void RespondOtlpStatus(duckdb_httplib::Response &res, int status, const string &message) {
	res.status = status;
	res.set_content(RawOtlpStatusBytes(message), "application/x-protobuf");
}

void HandleOtlpProtobuf(const duckdb_httplib::Request &req, duckdb_httplib::Response &res, const string &table,
                        const string &signal) {
	if (!RawOtlpProtobufSupported()) {
		RespondError(res, 415,
		             "this build does not include OTLP/protobuf support; configure the SDK with "
		             "OTEL_EXPORTER_OTLP_PROTOCOL=http/json or use a build with the protobuf feature");
		return;
	}
	auto db = RequireDatabase(res);
	if (!db) {
		return;
	}
	// decode through the same protobuf -> JSON conversion the gRPC server
	// uses, then ingest exactly like an OTLP/JSON request
	string payload, error;
	if (!RawOtlpProtobufToJson(signal, req.body, payload, error)) {
		RespondOtlpStatus(res, 400, error);
		return;
	}
	Connection conn(*db);
	auto options = ResolveTransform(*conn.context, "otlp-" + signal, "");
	if (GetApiServer().async || RawAsyncEnabled(*conn.context)) {
		RawAsyncEnqueue(*conn.context, table, std::move(payload), options);
		res.status = 200;
		res.set_content(RawOtlpProtobufResponse(signal, 0, ""), "application/x-protobuf");
		return;
	}
	conn.BeginTransaction();
	try {
		auto stats = RawIngestPayload(*conn.context, table, payload, options);
		conn.Commit();
		res.status = 200;
		res.set_content(
		    RawOtlpProtobufResponse(signal, stats.errors, stats.errors ? "some records could not be parsed" : ""),
		    "application/x-protobuf");
	} catch (std::exception &ex) {
		conn.Rollback();
		RespondOtlpStatus(res, 400, ErrorData(ex).RawMessage());
	}
}

void HandleQuery(const duckdb_httplib::Request &req, duckdb_httplib::Response &res) {
	auto db = RequireDatabase(res);
	if (!db) {
		return;
	}
	// body: {"sql": "..."}
	auto body = duckdb_yyjson::yyjson_read(req.body.c_str(), req.body.size(), 0);
	if (!body) {
		RespondError(res, 400, "body must be JSON: {\"sql\": \"...\"}");
		return;
	}
	auto sql_val = duckdb_yyjson::yyjson_obj_get(duckdb_yyjson::yyjson_doc_get_root(body), "sql");
	string sql = duckdb_yyjson::yyjson_is_str(sql_val)
	                 ? string(duckdb_yyjson::yyjson_get_str(sql_val), duckdb_yyjson::yyjson_get_len(sql_val))
	                 : string();
	duckdb_yyjson::yyjson_doc_free(body);
	if (sql.empty()) {
		RespondError(res, 400, "body must be JSON: {\"sql\": \"...\"}");
		return;
	}

	Connection conn(*db);
	auto start = std::chrono::steady_clock::now();
	auto result = conn.Query(sql);
	if (result->HasError()) {
		RespondError(res, 400, result->GetError());
		return;
	}
	auto elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();

	JsonDoc json;
	auto root = duckdb_yyjson::yyjson_mut_obj(json.doc);
	auto meta = duckdb_yyjson::yyjson_mut_arr(json.doc);
	for (idx_t col = 0; col < result->ColumnCount(); col++) {
		auto column = duckdb_yyjson::yyjson_mut_obj(json.doc);
		duckdb_yyjson::yyjson_mut_obj_add_strncpy(json.doc, column, "name", result->names[col].c_str(),
		                                          result->names[col].size());
		auto type = ApiTypeName(result->types[col].ToString());
		duckdb_yyjson::yyjson_mut_obj_add_strncpy(json.doc, column, "type", type.c_str(), type.size());
		duckdb_yyjson::yyjson_mut_arr_append(meta, column);
	}
	duckdb_yyjson::yyjson_mut_obj_add_val(json.doc, root, "meta", meta);
	auto data = duckdb_yyjson::yyjson_mut_arr(json.doc);
	idx_t row_count = 0;
	for (auto &row : *result) {
		auto row_obj = duckdb_yyjson::yyjson_mut_obj(json.doc);
		for (idx_t col = 0; col < result->ColumnCount(); col++) {
			auto &name = result->names[col];
			duckdb_yyjson::yyjson_mut_obj_add_val(json.doc, row_obj, name.c_str(),
			                                      ValueToJson(json, row.GetValue<Value>(col)));
		}
		duckdb_yyjson::yyjson_mut_arr_append(data, row_obj);
		row_count++;
	}
	duckdb_yyjson::yyjson_mut_obj_add_val(json.doc, root, "data", data);
	duckdb_yyjson::yyjson_mut_obj_add_uint(json.doc, root, "rows", row_count);
	auto statistics = duckdb_yyjson::yyjson_mut_obj(json.doc);
	duckdb_yyjson::yyjson_mut_obj_add_real(json.doc, statistics, "elapsed", elapsed);
	duckdb_yyjson::yyjson_mut_obj_add_uint(json.doc, statistics, "rows_read", row_count);
	duckdb_yyjson::yyjson_mut_obj_add_uint(json.doc, statistics, "bytes_read", row_count * result->ColumnCount() * 8);
	duckdb_yyjson::yyjson_mut_obj_add_val(json.doc, root, "statistics", statistics);
	duckdb_yyjson::yyjson_mut_obj_add_val(json.doc, root, "hints", duckdb_yyjson::yyjson_mut_arr(json.doc));
	Respond(res, 200, json, root);
}

void HandleListTables(duckdb_httplib::Response &res) {
	auto db = RequireDatabase(res);
	if (!db) {
		return;
	}
	Connection conn(*db);
	auto result = conn.Query("SELECT table_name FROM duckdb_tables() WHERE NOT internal ORDER BY table_name");
	if (result->HasError()) {
		RespondError(res, 500, result->GetError());
		return;
	}
	JsonDoc json;
	auto root = duckdb_yyjson::yyjson_mut_obj(json.doc);
	auto tables = duckdb_yyjson::yyjson_mut_arr(json.doc);
	for (auto &row : *result) {
		auto name = row.GetValue<Value>(0).ToString();
		auto stats = ApiTableStats(conn, name);
		auto table = duckdb_yyjson::yyjson_mut_obj(json.doc);
		duckdb_yyjson::yyjson_mut_obj_add_strncpy(json.doc, table, "name", name.c_str(), name.size());
		duckdb_yyjson::yyjson_mut_obj_add_str(json.doc, table, "created_at", API_CREATED_AT_PLACEHOLDER);
		duckdb_yyjson::yyjson_mut_obj_add_uint(json.doc, table, "total_rows", stats.first);
		duckdb_yyjson::yyjson_mut_obj_add_uint(json.doc, table, "total_bytes", stats.second);
		duckdb_yyjson::yyjson_mut_arr_append(tables, table);
	}
	duckdb_yyjson::yyjson_mut_obj_add_val(json.doc, root, "tables", tables);
	AddProjectOrganization(json, root);
	Respond(res, 200, json, root);
}

void HandleDescribe(duckdb_httplib::Response &res, const string &table) {
	auto db = RequireDatabase(res);
	if (!db) {
		return;
	}
	Connection conn(*db);
	auto result = conn.Query("DESCRIBE " + RawQualifiedTarget(table));
	if (result->HasError()) {
		RespondError(res, 404, result->GetError());
		return;
	}
	auto stats = ApiTableStats(conn, table);
	JsonDoc json;
	auto root = duckdb_yyjson::yyjson_mut_obj(json.doc);
	auto table_obj = duckdb_yyjson::yyjson_mut_obj(json.doc);
	duckdb_yyjson::yyjson_mut_obj_add_strncpy(json.doc, table_obj, "name", table.c_str(), table.size());
	duckdb_yyjson::yyjson_mut_obj_add_str(json.doc, table_obj, "created_at", API_CREATED_AT_PLACEHOLDER);
	duckdb_yyjson::yyjson_mut_obj_add_uint(json.doc, table_obj, "total_rows", stats.first);
	duckdb_yyjson::yyjson_mut_obj_add_uint(json.doc, table_obj, "total_bytes", stats.second);
	auto columns = duckdb_yyjson::yyjson_mut_arr(json.doc);
	for (auto &row : *result) {
		auto column = duckdb_yyjson::yyjson_mut_obj(json.doc);
		auto name = row.GetValue<Value>(0).ToString();
		auto type = ApiTypeName(row.GetValue<Value>(1).ToString());
		duckdb_yyjson::yyjson_mut_obj_add_strncpy(json.doc, column, "name", name.c_str(), name.size());
		duckdb_yyjson::yyjson_mut_obj_add_strncpy(json.doc, column, "type", type.c_str(), type.size());
		duckdb_yyjson::yyjson_mut_arr_append(columns, column);
	}
	duckdb_yyjson::yyjson_mut_obj_add_val(json.doc, table_obj, "columns", columns);
	duckdb_yyjson::yyjson_mut_obj_add_val(json.doc, root, "table", table_obj);
	AddProjectOrganization(json, root);
	Respond(res, 200, json, root);
}

void HandleDrop(duckdb_httplib::Response &res, const string &table) {
	auto db = RequireDatabase(res);
	if (!db) {
		return;
	}
	Connection conn(*db);
	auto result = conn.Query("DROP TABLE IF EXISTS " + RawQualifiedTarget(table));
	if (result->HasError()) {
		RespondError(res, 400, result->GetError());
		return;
	}
	JsonDoc json;
	auto root = duckdb_yyjson::yyjson_mut_obj(json.doc);
	duckdb_yyjson::yyjson_mut_obj_add_strncpy(json.doc, root, "dropped", table.c_str(), table.size());
	Respond(res, 200, json, root);
}

} // namespace

// constant-time comparison: token checks must not leak length/prefix timing
bool RawTokenEquals(const string &provided, const string &expected) {
	if (provided.size() != expected.size()) {
		return false;
	}
	unsigned char acc = 0;
	for (idx_t i = 0; i < provided.size(); i++) {
		acc |= static_cast<unsigned char>(provided[i]) ^ static_cast<unsigned char>(expected[i]);
	}
	return acc == 0;
}

namespace {

void RegisterRoutes(duckdb_httplib::Server &server) {
	// browser clients: permissive CORS, preflight handled globally
	server.set_default_headers(
	    {{"Access-Control-Allow-Origin", "*"},
	     {"Access-Control-Allow-Headers",
	      "Authorization, Content-Type, x-rawduck-table, x-rawduck-traces-table, x-rawduck-logs-table, "
	      "x-rawduck-metrics-table, x-rawtree-table, x-rawtree-traces-table, x-rawtree-logs-table, "
	      "x-rawtree-metrics-table"},
	     {"Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS"}});
	// cap request bodies: ingest payloads should be batched, not unbounded
	server.set_payload_max_length(256ULL * 1024ULL * 1024ULL);
	server.Options(R"(/.*)", [](const duckdb_httplib::Request &, duckdb_httplib::Response &res) { res.status = 204; });
	server.Get("/health", [](const duckdb_httplib::Request &, duckdb_httplib::Response &res) {
		res.set_content("{\"status\":\"ok\"}", "application/json");
	});
	server.Post("/v1/query", [](const duckdb_httplib::Request &req, duckdb_httplib::Response &res) {
		if (Authorized(req, res)) {
			HandleQuery(req, res);
		}
	});
	server.Get("/v1/tables", [](const duckdb_httplib::Request &req, duckdb_httplib::Response &res) {
		if (Authorized(req, res)) {
			HandleListTables(res);
		}
	});
	server.Get(R"(/v1/tables/(.+))", [](const duckdb_httplib::Request &req, duckdb_httplib::Response &res) {
		if (Authorized(req, res)) {
			HandleDescribe(res, req.matches.groups[1].text);
		}
	});
	server.Post(R"(/v1/tables/(.+))", [](const duckdb_httplib::Request &req, duckdb_httplib::Response &res) {
		if (Authorized(req, res)) {
			HandleIngest(req, res, req.matches.groups[1].text, "");
		}
	});
	server.Delete(R"(/v1/tables/(.+))", [](const duckdb_httplib::Request &req, duckdb_httplib::Response &res) {
		if (Authorized(req, res)) {
			HandleDrop(res, req.matches.groups[1].text);
		}
	});
	server.Post(R"(/otlp/v1/(traces|logs|metrics))",
	            [](const duckdb_httplib::Request &req, duckdb_httplib::Response &res) {
		            if (!Authorized(req, res)) {
			            return;
		            }
		            string signal = req.matches.groups[1].text;
		            // signal-specific table header, generic header, then default
		            auto duck_signal_header = "x-rawduck-" + signal + "-table";
		            auto tree_signal_header = "x-rawtree-" + signal + "-table";
		            auto table = RequestHeader(req, {duck_signal_header.c_str(), tree_signal_header.c_str(),
		                                             "x-rawduck-table", "x-rawtree-table"});
		            if (table.empty()) {
			            table = "otel_" + signal;
		            }
		            if (req.get_header_value("Content-Type").find("protobuf") != string::npos) {
			            HandleOtlpProtobuf(req, res, table, signal);
			            return;
		            }
		            HandleIngest(req, res, table, signal);
	            });
}

} // namespace

//===--------------------------------------------------------------------===//
// raw_serve(host := '127.0.0.1', port := 9999, token := '')
//===--------------------------------------------------------------------===//

struct RawServeBindData : public TableFunctionData {
	string host = "127.0.0.1";
	int32_t port = 9999;
	string token;
	// Synchronous ingestion by default: insert-then-query clients (MCP, the
	// TypeScript SDK, curl scripts) expect a row to be visible the moment the
	// insert call returns. Opt into async batching with async := true when the
	// workload is many concurrent fire-and-forget producers.
	bool async = false;
};

struct RawServeState : public GlobalTableFunctionState {
	bool done = false;
};

static unique_ptr<FunctionData> RawServeBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<RawServeBindData>();
	auto host = input.named_parameters.find("host");
	if (host != input.named_parameters.end() && !host->second.IsNull()) {
		result->host = host->second.GetValue<string>();
	}
	auto port = input.named_parameters.find("port");
	if (port != input.named_parameters.end() && !port->second.IsNull()) {
		result->port = port->second.GetValue<int32_t>();
	}
	auto token = input.named_parameters.find("token");
	if (token != input.named_parameters.end() && !token->second.IsNull()) {
		result->token = token->second.GetValue<string>();
	}
	auto async = input.named_parameters.find("async");
	if (async != input.named_parameters.end() && !async->second.IsNull()) {
		result->async = async->second.GetValue<bool>();
	}
	return_types = {LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::BOOLEAN};
	names = {"host", "port", "auth"};
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> RawServeInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<RawServeState>();
}

static void RawServeFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<RawServeBindData>();
	auto &state = data.global_state->Cast<RawServeState>();
	if (state.done) {
		output.SetCardinality(0);
		return;
	}
	state.done = true;

	auto &api = GetApiServer();
	lock_guard<mutex> guard(api.lock);
	if (api.running) {
		throw InvalidInputException("RawDuck: the API server is already running on %s:%d; call raw_serve_stop() first",
		                            api.host, api.port);
	}
	api.db = context.db;
	api.host = bind_data.host;
	api.port = bind_data.port;
	api.token = bind_data.token;
	api.async = bind_data.async;
	api.server = make_uniq<duckdb_httplib::Server>();
	RegisterRoutes(*api.server);
	if (!api.server->bind_to_port(bind_data.host.c_str(), bind_data.port)) {
		api.server.reset();
		throw IOException("RawDuck: cannot bind API server to %s:%d", bind_data.host, bind_data.port);
	}
	auto server = api.server.get();
	api.thread = std::thread([server] { server->listen_after_bind(); });
	api.running = true;

	output.SetValue(0, 0, Value(bind_data.host));
	output.SetValue(1, 0, Value::INTEGER(bind_data.port));
	output.SetValue(2, 0, Value::BOOLEAN(!bind_data.token.empty()));
	output.SetCardinality(1);
}

static void RawServeStopFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<RawServeState>();
	if (state.done) {
		output.SetCardinality(0);
		return;
	}
	state.done = true;

	auto &api = GetApiServer();
	lock_guard<mutex> guard(api.lock);
	bool was_running = api.running;
	api.Shutdown();
	output.SetValue(0, 0, Value::BOOLEAN(was_running));
	output.SetCardinality(1);
}

static unique_ptr<FunctionData> RawServeStopBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	return_types = {LogicalType::BOOLEAN};
	names = {"stopped"};
	return make_uniq<TableFunctionData>();
}

TableFunction GetRawServeFunction() {
	TableFunction function("raw_serve", {}, RawServeFunction, RawServeBind, RawServeInit);
	function.named_parameters["host"] = LogicalType::VARCHAR;
	function.named_parameters["port"] = LogicalType::INTEGER;
	function.named_parameters["token"] = LogicalType::VARCHAR;
	function.named_parameters["async"] = LogicalType::BOOLEAN;
	return function;
}

TableFunction GetRawServeStopFunction() {
	return TableFunction("raw_serve_stop", {}, RawServeStopFunction, RawServeStopBind, RawServeInit);
}

} // namespace duckdb
