#define DUCKDB_EXTENSION_MAIN

#include "rawduck_extension.hpp"
#include "raw_functions.hpp"
#include "raw_write_settings.hpp"

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
	loader.RegisterFunction(GetRawFlushFunction());
	loader.RegisterFunction(GetRawTransformDefineFunction());

	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	// observe pushed-down predicates to drive raw_optimize()
	OptimizerExtension::Register(config, GetRawDuckOptimizerExtension());
	config.AddExtensionOption("rawduck_insert_transform",
	                          "Transform name or explode path applied by INSERTs into ingest tables",
	                          LogicalType::VARCHAR, Value(""));
	config.AddExtensionOption("rawduck_async_insert", "Buffer ingestion calls and flush asynchronously",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false));
	config.AddExtensionOption("rawduck_async_max_data_size", "Async insert buffer flush threshold in bytes",
	                          LogicalType::BIGINT, Value::BIGINT(1024 * 1024));
	config.AddExtensionOption("rawduck_async_busy_timeout_ms", "Async insert buffer flush age threshold",
	                          LogicalType::BIGINT, Value::BIGINT(200));
	config.AddExtensionOption("rawduck_use_projections",
	                          "Rewrite eligible count(*) aggregations onto fresh materialized projections "
	                          "(append-only workloads)",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false));
	config.AddExtensionOption("rawduck_overlap_flush",
	                          "Flush completed row groups during a large multi-threaded ingest while the schema is "
	                          "stable, overlapping parse with compression/IO. Faster on large stable-schema imports "
	                          "at the cost of higher peak memory; off by default (drain-free)",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false));
	config.AddExtensionOption("rawduck_pool_min_rows",
	                          "Minimum rows in a batch before the multi-threaded append pool activates",
	                          LogicalType::BIGINT, Value::BIGINT(RawWriteSettings::DEFAULT_POOL_MIN_ROWS));
	config.AddExtensionOption("rawduck_pool_threads",
	                          "Append-pool worker count (0 = auto from batch size and hardware concurrency)",
	                          LogicalType::BIGINT, Value::BIGINT(0));
	config.AddExtensionOption("rawduck_pipeline_threads",
	                          "NDJSON file parse thread count (0 = auto, up to 8 on large imports)",
	                          LogicalType::BIGINT, Value::BIGINT(0));
	config.AddExtensionOption("rawduck_pipeline_consumers",
	                          "Parallel ingest consumers for raw_ingest_file (default 1; >1 overlaps parse with "
	                          "append on stable schemas only — wide schema churn should stay at 1)",
	                          LogicalType::BIGINT, Value::BIGINT(0));
	config.AddExtensionOption("rawduck_pipeline_depth",
	                          "Bounded queue depth between reader, parse workers, and ingest consumer(s)",
	                          LogicalType::BIGINT, Value::BIGINT(RawWriteSettings::DEFAULT_PIPELINE_DEPTH));
	config.AddExtensionOption("rawduck_overlap_flush_auto",
	                          "When true, overlap parse/flush on stable re-ingest batches that meet pool_min_rows "
	                          "(without setting rawduck_overlap_flush globally)",
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
