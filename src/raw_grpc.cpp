#include "raw_functions.hpp"
#include "raw_json.hpp"

#include "duckdb/function/table_function.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"

#ifdef RAWDUCK_WITH_GRPC
#include "opentelemetry/proto/collector/logs/v1/logs_service.grpc.pb.h"
#include "opentelemetry/proto/collector/metrics/v1/metrics_service.grpc.pb.h"
#include "opentelemetry/proto/collector/trace/v1/trace_service.grpc.pb.h"

#include <google/protobuf/util/json_util.h>
#include <grpcpp/grpcpp.h>
#endif

namespace duckdb {

//===--------------------------------------------------------------------===//
// raw_serve_grpc: an in-process OTLP/gRPC collector endpoint
//
// Implements the standard OpenTelemetry collector services (TraceService,
// LogsService, MetricsService). Requests are converted to OTLP/JSON through
// protobuf's canonical JSON mapping and flow through the exact same native
// ingestion path as the HTTP API, so the two transports cannot diverge.
// Compiled only when gRPC + protobuf are available (never for wasm); the
// functions exist regardless and explain themselves when support is absent.
//===--------------------------------------------------------------------===//

#ifdef RAWDUCK_WITH_GRPC

namespace {

struct RawGrpcState {
	mutex lock;
	std::unique_ptr<grpc::Server> server;
	// the service objects must outlive the server
	vector<shared_ptr<void>> services;
	weak_ptr<DatabaseInstance> db;
	string host;
	int port = 0;
	string token;
	bool running = false;

	void Shutdown() {
		if (running) {
			server->Shutdown();
			server.reset();
			services.clear();
			running = false;
		}
	}
	~RawGrpcState() {
		Shutdown();
	}
};

RawGrpcState &GetGrpcState() {
	static RawGrpcState state;
	return state;
}

grpc::Status Unauthenticated() {
	return grpc::Status(grpc::StatusCode::UNAUTHENTICATED, "missing or invalid bearer token");
}

string MetadataValue(const grpc::ServerContext &context, const string &key) {
	auto &metadata = context.client_metadata();
	auto entry = metadata.find(key);
	if (entry == metadata.end()) {
		return string();
	}
	return string(entry->second.data(), entry->second.size());
}

// Shared export path: protobuf request -> OTLP/JSON -> native ingestion.
// On success fills rejected/error for the partialSuccess response.
grpc::Status ExportSignal(const grpc::ServerContext &context, const google::protobuf::Message &request,
                          const string &signal, uint64_t &rejected, string &error_message) {
	auto &state = GetGrpcState();
	if (!state.token.empty() && MetadataValue(context, "authorization") != "Bearer " + state.token) {
		return Unauthenticated();
	}
	auto db = state.db.lock();
	if (!db) {
		return grpc::Status(grpc::StatusCode::UNAVAILABLE, "database is no longer available");
	}
	auto table = MetadataValue(context, "x-rawduck-" + signal + "-table");
	if (table.empty()) {
		table = MetadataValue(context, "x-rawduck-table");
	}
	if (table.empty()) {
		table = "otel_" + signal;
	}

	string payload;
	google::protobuf::util::JsonPrintOptions options;
	auto converted = google::protobuf::util::MessageToJsonString(request, &payload, options);
	if (!converted.ok()) {
		return grpc::Status(grpc::StatusCode::INTERNAL, "failed to convert OTLP request to JSON");
	}

	Connection conn(*db);
	conn.BeginTransaction();
	try {
		auto parse_options = ResolveTransform(*conn.context, "otlp-" + signal, "");
		auto stats = RawIngestPayload(*conn.context, table, payload, parse_options);
		conn.Commit();
		rejected = stats.errors;
		if (rejected > 0) {
			error_message = "some records could not be parsed";
		}
		return grpc::Status::OK;
	} catch (std::exception &ex) {
		conn.Rollback();
		return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, ErrorData(ex).RawMessage());
	}
}

class RawTraceService final : public opentelemetry::proto::collector::trace::v1::TraceService::Service {
	grpc::Status Export(grpc::ServerContext *context,
	                    const opentelemetry::proto::collector::trace::v1::ExportTraceServiceRequest *request,
	                    opentelemetry::proto::collector::trace::v1::ExportTraceServiceResponse *response) override {
		uint64_t rejected = 0;
		string error_message;
		auto status = ExportSignal(*context, *request, "traces", rejected, error_message);
		if (status.ok() && rejected > 0) {
			response->mutable_partial_success()->set_rejected_spans(NumericCast<int64_t>(rejected));
			response->mutable_partial_success()->set_error_message(error_message);
		}
		return status;
	}
};

class RawLogsService final : public opentelemetry::proto::collector::logs::v1::LogsService::Service {
	grpc::Status Export(grpc::ServerContext *context,
	                    const opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest *request,
	                    opentelemetry::proto::collector::logs::v1::ExportLogsServiceResponse *response) override {
		uint64_t rejected = 0;
		string error_message;
		auto status = ExportSignal(*context, *request, "logs", rejected, error_message);
		if (status.ok() && rejected > 0) {
			response->mutable_partial_success()->set_rejected_log_records(NumericCast<int64_t>(rejected));
			response->mutable_partial_success()->set_error_message(error_message);
		}
		return status;
	}
};

class RawMetricsService final : public opentelemetry::proto::collector::metrics::v1::MetricsService::Service {
	grpc::Status Export(grpc::ServerContext *context,
	                    const opentelemetry::proto::collector::metrics::v1::ExportMetricsServiceRequest *request,
	                    opentelemetry::proto::collector::metrics::v1::ExportMetricsServiceResponse *response) override {
		uint64_t rejected = 0;
		string error_message;
		auto status = ExportSignal(*context, *request, "metrics", rejected, error_message);
		if (status.ok() && rejected > 0) {
			response->mutable_partial_success()->set_rejected_data_points(NumericCast<int64_t>(rejected));
			response->mutable_partial_success()->set_error_message(error_message);
		}
		return status;
	}
};

} // namespace

#endif // RAWDUCK_WITH_GRPC

//===--------------------------------------------------------------------===//
// raw_serve_grpc(host := '127.0.0.1', port := 4317, token := '')
//===--------------------------------------------------------------------===//

struct RawServeGrpcBindData : public TableFunctionData {
	string host = "127.0.0.1";
	int32_t port = 4317;
	string token;
};

struct RawServeGrpcState : public GlobalTableFunctionState {
	bool done = false;
};

static unique_ptr<FunctionData> RawServeGrpcBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<RawServeGrpcBindData>();
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
	return_types = {LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::BOOLEAN};
	names = {"host", "port", "auth"};
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> RawServeGrpcInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<RawServeGrpcState>();
}

static void RawServeGrpcFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<RawServeGrpcBindData>();
	auto &state = data.global_state->Cast<RawServeGrpcState>();
	if (state.done) {
		output.SetCardinality(0);
		return;
	}
	state.done = true;
#ifndef RAWDUCK_WITH_GRPC
	throw NotImplementedException(
	    "RawDuck: this build does not include OTLP/gRPC support (gRPC was unavailable at build time, this is a "
	    "wasm build, or it was disabled via RAWDUCK_DISABLE_GRPC); use the OTLP/HTTP endpoints instead");
#else
	auto &grpc_state = GetGrpcState();
	lock_guard<mutex> guard(grpc_state.lock);
	if (grpc_state.running) {
		throw InvalidInputException("RawDuck: the OTLP/gRPC server is already running on %s:%d; call "
		                            "raw_serve_grpc_stop() first",
		                            grpc_state.host, grpc_state.port);
	}
	grpc_state.db = context.db;
	grpc_state.host = bind_data.host;
	grpc_state.token = bind_data.token;

	auto traces = make_shared_ptr<RawTraceService>();
	auto logs = make_shared_ptr<RawLogsService>();
	auto metrics = make_shared_ptr<RawMetricsService>();
	grpc::ServerBuilder builder;
	int bound_port = 0;
	builder.AddListeningPort(bind_data.host + ":" + to_string(bind_data.port), grpc::InsecureServerCredentials(),
	                         &bound_port);
	builder.RegisterService(traces.get());
	builder.RegisterService(logs.get());
	builder.RegisterService(metrics.get());
	auto server = builder.BuildAndStart();
	if (!server || bound_port == 0) {
		throw IOException("RawDuck: cannot bind OTLP/gRPC server to %s:%d", bind_data.host, bind_data.port);
	}
	grpc_state.server = std::move(server);
	grpc_state.services = {traces, logs, metrics};
	grpc_state.port = bound_port;
	grpc_state.running = true;

	output.SetValue(0, 0, Value(bind_data.host));
	output.SetValue(1, 0, Value::INTEGER(bound_port));
	output.SetValue(2, 0, Value::BOOLEAN(!bind_data.token.empty()));
	output.SetCardinality(1);
#endif
}

static void RawServeGrpcStopFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<RawServeGrpcState>();
	if (state.done) {
		output.SetCardinality(0);
		return;
	}
	state.done = true;
#ifndef RAWDUCK_WITH_GRPC
	output.SetValue(0, 0, Value::BOOLEAN(false));
	output.SetCardinality(1);
#else
	auto &grpc_state = GetGrpcState();
	lock_guard<mutex> guard(grpc_state.lock);
	bool was_running = grpc_state.running;
	grpc_state.Shutdown();
	output.SetValue(0, 0, Value::BOOLEAN(was_running));
	output.SetCardinality(1);
#endif
}

static unique_ptr<FunctionData> RawServeGrpcStopBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
	return_types = {LogicalType::BOOLEAN};
	names = {"stopped"};
	return make_uniq<TableFunctionData>();
}

TableFunction GetRawServeGrpcFunction() {
	TableFunction function("raw_serve_grpc", {}, RawServeGrpcFunction, RawServeGrpcBind, RawServeGrpcInit);
	function.named_parameters["host"] = LogicalType::VARCHAR;
	function.named_parameters["port"] = LogicalType::INTEGER;
	function.named_parameters["token"] = LogicalType::VARCHAR;
	return function;
}

TableFunction GetRawServeGrpcStopFunction() {
	return TableFunction("raw_serve_grpc_stop", {}, RawServeGrpcStopFunction, RawServeGrpcStopBind, RawServeGrpcInit);
}

} // namespace duckdb
