#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

class OptimizerExtension;
class StorageExtension;
struct RawParseOptions;

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

// shared SQL generation helpers
string RawQuoteIdentifier(const string &name);
string RawQualifiedTarget(const string &target);

// shared ingest parameter handling (transform / explode / ignore_errors)
string RawNamedStringParameter(const named_parameter_map_t &parameters, const string &name);
RawParseOptions RawBindParseOptions(ClientContext &context, const named_parameter_map_t &parameters);
RawParseOptions ResolveTransform(ClientContext &context, const string &transform, const string &explode);
void RawAddIngestParameters(TableFunction &function);

} // namespace duckdb
