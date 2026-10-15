#include "raw_functions.hpp"

#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/grammar_extension.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

namespace duckdb {

// INGEST 'file.ndjson' INTO tbl  →  SELECT * FROM raw_ingest_file('tbl', 'file.ndjson')
static arena_ptr<TransformResultValue> FinalizeIngestStatement(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list = parse_result.Cast<ListParseResult>();
	auto file = transformer.Transform<string>(list.GetChild(1));
	auto table = transformer.Transform<Identifier>(list.GetChild(3));
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(ConstantExpression::String(table.GetIdentifierName()));
	children.push_back(ConstantExpression::String(std::move(file)));
	auto function = make_uniq<FunctionExpression>(Identifier("raw_ingest_file"), std::move(children));
	auto table_ref = make_uniq<TableFunctionRef>();
	table_ref->function = std::move(function);
	auto select_node = make_uniq<SelectNode>();
	select_node->select_list.push_back(make_uniq<StarExpression>());
	select_node->from_table = std::move(table_ref);
	auto select_statement = make_uniq<SelectStatement>();
	select_statement->node = std::move(select_node);
	return transformer.MakeResult<unique_ptr<SQLStatement>>(std::move(select_statement));
}

static arena_ptr<TransformProcess> StartIngestStatementTransform(PEGTransformer &transformer,
                                                                 ParseResult &parse_result) {
	return transformer.MakeProcess<FinalizeTransformProcess>(transformer, parse_result, FinalizeIngestStatement);
}

class RawDuckGrammarExtension final : public GrammarExtension {
public:
	RawDuckGrammarExtension() : GrammarExtension("rawduck", "INGEST 'file' INTO tbl sugar for raw_ingest_file") {
	}

	vector<GrammarChange> GetChanges() const override {
		vector<GrammarChange> changes;
		changes.push_back(GrammarChange::AddRule("IngestStatement <- 'INGEST' StringLiteral 'INTO' Identifier",
		                                         StartIngestStatementTransform));
		changes.push_back(GrammarChange::AddChoice("Statement", "IngestStatement"));
		return changes;
	}
};

shared_ptr<GrammarExtension> GetRawDuckGrammarExtension() {
	return make_shared_ptr<RawDuckGrammarExtension>();
}

} // namespace duckdb
