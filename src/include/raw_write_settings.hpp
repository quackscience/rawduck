#pragma once

#include "duckdb.hpp"

namespace duckdb {

struct RawWriteSettings {
	static constexpr idx_t DEFAULT_POOL_MIN_ROWS = 4096;
	static constexpr idx_t DEFAULT_PIPELINE_DEPTH = 4;
	static constexpr idx_t MAX_POOL_THREADS = 16;
	static constexpr idx_t MAX_PIPELINE_THREADS = 16;
	static constexpr idx_t AUTO_POOL_THREAD_CAP = 4;
	static constexpr idx_t AUTO_PIPELINE_THREAD_CAP = 8;

	idx_t pool_min_rows = DEFAULT_POOL_MIN_ROWS;
	idx_t pool_threads = 0;
	idx_t pipeline_threads = 0;
	idx_t pipeline_consumers = 0;
	idx_t pipeline_depth = DEFAULT_PIPELINE_DEPTH;
	bool overlap_flush_auto = false;

	static RawWriteSettings Get(ClientContext &context);
	idx_t PoolThreadCount(idx_t batch_rows) const;
	idx_t PipelineThreadCount() const;
	idx_t PipelineConsumerCount() const;
	bool OverlapFlushForBatch(ClientContext &context, bool shape_absorbed, idx_t batch_rows) const;
};

} // namespace duckdb
