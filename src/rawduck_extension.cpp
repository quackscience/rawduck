#define DUCKDB_EXTENSION_MAIN

#include "rawduck_extension.hpp"
#include "raw_functions.hpp"

#include "duckdb.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	loader.RegisterFunction(GetRawRecordsFunction());
	loader.RegisterFunction(GetRawIngestFunction());
	loader.RegisterFunction(GetRawIngestFileFunction());
	loader.RegisterFunction(GetRawStatsFunction());
	loader.RegisterFunction(GetRawOptimizeFunction());
	loader.RegisterFunction(GetRawTypeFunction());
	loader.RegisterFunction(GetRawInferFunction());
	loader.RegisterFunction(GetRawTransformsFunction());
	loader.RegisterFunction(GetRawProjectionsFunction());
	loader.RegisterFunction(GetRawProjectFunction());
	loader.RegisterFunction(GetRawStatsSaveFunction());
	loader.RegisterFunction(GetRawStatsLoadFunction());
	loader.RegisterFunction(GetRawServeFunction());
	loader.RegisterFunction(GetRawServeStopFunction());
	loader.RegisterFunction(GetRawServeGrpcFunction());
	loader.RegisterFunction(GetRawServeGrpcStopFunction());
	loader.RegisterFunction(GetRawTransformDefineFunction());

	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	// observe pushed-down predicates to drive raw_optimize()
	OptimizerExtension::Register(config, GetRawDuckOptimizerExtension());
	config.AddExtensionOption("rawduck_use_projections",
	                          "Rewrite eligible count(*) aggregations onto fresh materialized projections "
	                          "(append-only workloads)",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false));
	// ATTACH 'rawduck:store.db' AS raw
	StorageExtension::Register(config, "rawduck", GetRawDuckStorageExtension());
}

void RawduckExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string RawduckExtension::Name() {
	return "rawduck";
}

std::string RawduckExtension::Version() const {
#ifdef EXT_VERSION_RAWDUCK
	return EXT_VERSION_RAWDUCK;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(rawduck, loader) {
	duckdb::LoadInternal(loader);
}
}
