#include "raw_functions.hpp"
#include "raw_json.hpp"

#include "duckdb/function/table_function.hpp"

namespace duckdb {

struct RawRecordsBindData : public TableFunctionData {
	shared_ptr<RawParsedPayload> parsed;
};

struct RawRecordsState : public GlobalTableFunctionState {
	RawRecordsState(const RawRecordsBindData &bind_data, vector<column_t> column_ids_p)
	    : extractor(*bind_data.parsed->root, bind_data.parsed->columns, column_ids_p),
	      column_ids(std::move(column_ids_p)) {
	}

	idx_t next_row = 0;
	RawExtractor extractor;
	// projected physical indexes into bind_data.parsed->columns (DuckDB v2
	// projection pushdown: output.data[i] is column_ids[i], not schema order)
	vector<column_t> column_ids;
};

string RawNamedStringParameter(const named_argument_map_t &parameters, const string &name) {
	auto entry = parameters.find(Identifier(name));
	if (entry == parameters.end() || entry->second.IsNull()) {
		return string();
	}
	return entry->second.GetValue<string>();
}

RawParseOptions RawBindParseOptions(ClientContext &context, const named_argument_map_t &parameters) {
	auto options = ResolveTransform(context, RawNamedStringParameter(parameters, "transform"),
	                                RawNamedStringParameter(parameters, "explode"));
	auto ignore_entry = parameters.find(Identifier("ignore_errors"));
	if (ignore_entry != parameters.end() && !ignore_entry->second.IsNull()) {
		options.ignore_errors = ignore_entry->second.GetValue<bool>();
	}
	return options;
}

void RawAddNamedKwargs(TableFunction &function, const std::function<void(TypedKwargs &)> &configure) {
	auto &signature = function.GetSignature();
	if (signature.GetTypedKwargs()) {
		signature.ExtendTypedKwargs(configure);
	} else {
		signature.WithTypedKwargs("options", configure);
	}
}

void RawAddIngestParameters(TableFunction &function) {
	RawAddNamedKwargs(function, [](TypedKwargs &options) {
		options.Add("transform", LogicalType::VARCHAR)
		    .Add("explode", LogicalType::VARCHAR)
		    .Add("ignore_errors", LogicalType::BOOLEAN);
	});
}

Value RawResultValue(QueryResult &result, idx_t col, idx_t row) {
	return result.Collection().GetValue(col, row);
}

static unique_ptr<FunctionData> RawRecordsBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<RawRecordsBindData>();
	if (input.inputs[0].IsNull()) {
		throw InvalidInputException("RawDuck: payload may not be NULL");
	}
	auto payload_str = input.inputs[0].GetValue<string>();
	result->parsed = RawParsedPayload::Process(payload_str, RawBindParseOptions(context, input.named_parameters));
	if (result->parsed->columns.empty()) {
		// empty payload: keep a valid (empty) result shape
		return_types.push_back(RawOverflowType());
		names.push_back("value");
		result->parsed->payload.rows.clear();
		return std::move(result);
	}
	for (auto &column : result->parsed->columns) {
		return_types.push_back(column.type);
		names.push_back(Identifier(column.name));
	}
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> RawRecordsInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<RawRecordsState>(input.bind_data->Cast<RawRecordsBindData>(), input.column_ids);
}

static void RawRecordsFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<RawRecordsBindData>();
	auto &state = data.global_state->Cast<RawRecordsState>();
	auto &rows = bind_data.parsed->payload.rows;
	auto &columns = bind_data.parsed->columns;
	auto count = MinValue<idx_t>(rows.size() - state.next_row, STANDARD_VECTOR_SIZE);
	state.extractor.Reset(count);
	for (idx_t i = 0; i < count; i++) {
		state.extractor.AssignRow(rows[state.next_row + i], i);
	}
	for (idx_t out_col = 0; out_col < state.column_ids.size(); out_col++) {
		auto col = state.column_ids[out_col];
		if (col >= columns.size()) {
			continue;
		}
		FillVector(state.extractor.ColumnValues(col), columns[col].type, output.data[out_col], 0);
	}
	state.next_row += count;
	output.SetChildCardinality(count);
}

TableFunction GetRawRecordsFunction() {
	TableFunction function("raw_records", {LogicalType::VARCHAR}, RawRecordsFunction, RawRecordsBind, RawRecordsInit);
	RawAddIngestParameters(function);
	function.projection_pushdown = true;
	return function;
}

} // namespace duckdb
