#include "raw_functions.hpp"
#include "raw_json.hpp"

#include "duckdb/function/table_function.hpp"

namespace duckdb {

struct RawRecordsBindData : public TableFunctionData {
	shared_ptr<RawParsedPayload> parsed;
};

struct RawRecordsState : public GlobalTableFunctionState {
	explicit RawRecordsState(const RawRecordsBindData &bind_data)
	    : extractor(*bind_data.parsed->root, bind_data.parsed->columns, bind_data.parsed->payload.UsesMutRows()) {
	}

	idx_t next_row = 0;
	RawExtractor extractor;
};

string RawNamedStringParameter(const named_parameter_map_t &parameters, const string &name) {
	auto entry = parameters.find(name);
	if (entry == parameters.end() || entry->second.IsNull()) {
		return string();
	}
	return entry->second.GetValue<string>();
}

RawParseOptions RawBindParseOptions(ClientContext &context, const named_parameter_map_t &parameters) {
	auto options = ResolveTransform(context, RawNamedStringParameter(parameters, "transform"),
	                                RawNamedStringParameter(parameters, "explode"));
	auto ignore_entry = parameters.find("ignore_errors");
	if (ignore_entry != parameters.end() && !ignore_entry->second.IsNull()) {
		options.ignore_errors = ignore_entry->second.GetValue<bool>();
	}
	return options;
}

void RawAddIngestParameters(TableFunction &function) {
	function.named_parameters["transform"] = LogicalType::VARCHAR;
	function.named_parameters["explode"] = LogicalType::VARCHAR;
	function.named_parameters["ignore_errors"] = LogicalType::BOOLEAN;
}

static unique_ptr<FunctionData> RawRecordsBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<RawRecordsBindData>();
	if (input.inputs[0].IsNull()) {
		throw InvalidInputException("RawDuck: payload may not be NULL");
	}
	auto payload_str = input.inputs[0].GetValue<string>();
	result->parsed = RawParsedPayload::Process(payload_str, RawBindParseOptions(context, input.named_parameters));
	if (result->parsed->columns.empty()) {
		// empty payload: keep a valid (empty) result shape
		return_types.push_back(LogicalType::JSON());
		names.push_back("value");
		result->parsed->payload.rows.clear();
		return std::move(result);
	}
	for (auto &column : result->parsed->columns) {
		return_types.push_back(column.type);
		names.push_back(column.name);
	}
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> RawRecordsInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<RawRecordsState>(input.bind_data->Cast<RawRecordsBindData>());
}

static void RawRecordsFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<RawRecordsBindData>();
	auto &state = data.global_state->Cast<RawRecordsState>();
	auto &payload = bind_data.parsed->payload;
	auto &columns = bind_data.parsed->columns;
	auto row_count = payload.RowCount();
	auto count = MinValue<idx_t>(row_count - state.next_row, STANDARD_VECTOR_SIZE);
	state.extractor.Reset(count);
	if (payload.UsesMutRows()) {
		auto &rows = payload.mut_rows;
		for (idx_t i = 0; i < count; i++) {
			state.extractor.AssignMutRow(rows[state.next_row + i], i);
		}
		for (idx_t col = 0; col < columns.size(); col++) {
			FillVectorMut(state.extractor.ColumnMutValues(col), columns[col].type, output.data[col], 0);
		}
	} else {
		auto &rows = payload.rows;
		for (idx_t i = 0; i < count; i++) {
			state.extractor.AssignRow(rows[state.next_row + i], i);
		}
		for (idx_t col = 0; col < columns.size(); col++) {
			FillVector(state.extractor.ColumnValues(col), columns[col].type, output.data[col], 0);
		}
	}
	state.next_row += count;
	output.SetCardinality(count);
}

TableFunction GetRawRecordsFunction() {
	TableFunction function("raw_records", {LogicalType::VARCHAR}, RawRecordsFunction, RawRecordsBind, RawRecordsInit);
	RawAddIngestParameters(function);
	return function;
}

} // namespace duckdb
