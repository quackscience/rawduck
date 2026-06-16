#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

class OptimizerExtension;
class StorageExtension;
struct StorageExtensionInfo;
class TransactionManager;
struct RawParseOptions;
struct RawParsedPayload;

TableFunction GetRawRecordsFunction();
TableFunction GetRawIngestFunction();
TableFunction GetRawIngestFileFunction();
TableFunction GetRawStatsFunction();
TableFunction GetRawOptimizeFunction();
TableFunction GetRawTransformsFunction();
TableFunction GetRawProjectionsFunction();
TableFunction GetRawProjectFunction();
TableFunctionSet GetRawStatsSaveFunction();
TableFunctionSet GetRawStatsLoadFunction();
ScalarFunction GetRawTypeFunction();
ScalarFunction GetRawInferFunction();
ScalarFunction GetRawTransformDefineFunction();
OptimizerExtension GetRawDuckOptimizerExtension();
shared_ptr<StorageExtension> GetRawDuckStorageExtension();

// programmatic ingest (HTTP API)
struct RawIngestStats {
	bool created = false;
	idx_t columns_added = 0;
	idx_t columns_widened = 0;
	idx_t rows = 0;
	idx_t errors = 0;
};
RawIngestStats RawIngestPayload(ClientContext &context, const string &target, const string &payload,
                                const RawParseOptions &options);

TableFunction GetRawServeFunction();
TableFunction GetRawServeStopFunction();
TableFunction GetRawServeGrpcFunction();
TableFunction GetRawServeGrpcStopFunction();
TableFunction GetRawFlushFunction();
bool RawAsyncEnabled(ClientContext &context);
void RawAsyncEnqueue(ClientContext &context, const string &target, string payload, RawParseOptions options);

// streaming ingestion handle (INSERT-syntax path)
class RawStreamIngestor {
public:
	virtual ~RawStreamIngestor() = default;
	virtual void Ingest(const string &payload) = 0;
	// thread-safe: only schema work serializes; appends fan out via the pool
	virtual void IngestParsedConcurrent(shared_ptr<RawParsedPayload> parsed) = 0;
	virtual void Finish() = 0;
	virtual idx_t Rows() const = 0;
};
unique_ptr<RawStreamIngestor> RawCreateStreamIngestor(ClientContext &context, const string &target,
                                                      RawParseOptions options);
class SchemaCatalogEntry;
class TableCatalogEntry;
class PhysicalPlanGenerator;
class LogicalInsert;
class PhysicalOperator;
unique_ptr<SchemaCatalogEntry> RawCreateIngestSchema(Catalog &catalog);
bool RawIsIngestTable(const TableCatalogEntry &table);
PhysicalOperator &RawPlanIngestInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                      optional_ptr<PhysicalOperator> plan);
PhysicalOperator &RawPlanQuackIngestInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                           optional_ptr<PhysicalOperator> plan);

// rawduck:quack:host:port remote attach (requires quack extension at runtime)
bool RawDuckIsQuackAttach(const string &path);
unique_ptr<Catalog> RawDuckAttachQuack(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                       AttachedDatabase &db, const string &name, AttachInfo &info,
                                       AttachOptions &options);
unique_ptr<TransactionManager> RawDuckQuackCreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                                                                    AttachedDatabase &db, Catalog &catalog);
bool RawDuckIsQuackCatalog(Catalog &catalog);
string RawDuckQuackUri(Catalog &catalog);
string RawDuckQuackToken(Catalog &catalog);

// constant-time token comparison (HTTP and gRPC auth)
bool RawTokenEquals(const string &provided, const string &expected);

// OTLP/HTTP protobuf bodies (raw_otlp_pb.cpp; stubs when built without protobuf)
bool RawOtlpProtobufSupported();
bool RawOtlpProtobufToJson(const string &signal, const string &body, string &json_out, string &error);
string RawOtlpProtobufResponse(const string &signal, uint64_t rejected, const string &error_message);
string RawOtlpStatusBytes(const string &message);

// shared SQL generation helpers
string RawQuoteIdentifier(const string &name);
string RawQualifiedTarget(const string &target);

// shared ingest parameter handling (transform / explode / ignore_errors)
string RawNamedStringParameter(const named_parameter_map_t &parameters, const string &name);
RawParseOptions RawBindParseOptions(ClientContext &context, const named_parameter_map_t &parameters);
RawParseOptions ResolveTransform(ClientContext &context, const string &transform, const string &explode);
void RawAddIngestParameters(TableFunction &function);

} // namespace duckdb
