#include "raw_functions.hpp"
#include "raw_json.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_merge_into.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/transaction/transaction_manager.hpp"

namespace duckdb {

static string RawQuackSQLString(const string &value) {
	return KeywordHelper::WriteQuoted(value, '\'');
}

static void RawEnsureQuackLoaded(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	if (StorageExtension::Find(config, "quack")) {
		return;
	}
	if (!Catalog::TryAutoLoad(context, "quack")) {
		ExtensionHelper::LoadExternalExtension(context, "quack");
	}
	if (!StorageExtension::Find(config, "quack")) {
		throw InvalidInputException(
		    "RawDuck remote attach requires the quack extension — run INSTALL quack; LOAD quack;");
	}
}

static optional_ptr<StorageExtension> RawGetQuackStorageExtension(ClientContext &context) {
	RawEnsureQuackLoaded(context);
	auto ext = StorageExtension::Find(DBConfig::GetConfig(context), "quack");
	if (!ext || !ext->attach || !ext->create_transaction_manager) {
		throw InvalidInputException("quack storage extension is not available");
	}
	return ext;
}

static void RawQuackExecute(ClientContext &context, const string &quack_uri, const string &token,
                            const string &remote_sql) {
	Connection conn(*context.db);
	auto sql = "SELECT * FROM quack_query(" + RawQuackSQLString(quack_uri) + ", " + RawQuackSQLString(remote_sql);
	if (!token.empty()) {
		sql += ", token := " + RawQuackSQLString(token);
	}
	sql += ")";
	auto result = conn.Query(sql);
	if (result->HasError()) {
		result->ThrowError();
	}
}

bool RawDuckIsQuackAttach(const string &path) {
	return StringUtil::StartsWith(path, "quack:");
}

//===--------------------------------------------------------------------===//
// RawDuckQuackCatalog: quack transport with RawDuck catalog identity + ingest lane
//===--------------------------------------------------------------------===//

class RawDuckQuackCatalog : public Catalog {
public:
	RawDuckQuackCatalog(AttachedDatabase &db_p, unique_ptr<Catalog> inner_p, string db_path_p, string token_p)
	    : Catalog(db_p), inner(std::move(inner_p)), db_path(std::move(db_path_p)), token(std::move(token_p)) {
	}

	Catalog &GetInner() {
		return *inner;
	}

	const string &GetQuackUri() const {
		return db_path;
	}

	const string &GetToken() const {
		return token;
	}

	string GetCatalogType() override {
		return "rawduck";
	}

	void Initialize(bool load_builtin) override {
		inner->Initialize(load_builtin);
	}

	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override {
		return inner->CreateSchema(transaction, info);
	}

	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const EntryLookupInfo &schema_lookup,
	                                              OnEntryNotFound if_not_found) override {
		if (schema_lookup.GetEntryName() == "ingest") {
			lock_guard<mutex> guard(ingest_lock);
			if (!ingest_schema) {
				ingest_schema = RawCreateIngestSchema(*this);
			}
			return ingest_schema.get();
		}
		return inner->LookupSchema(transaction, schema_lookup, if_not_found);
	}

	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override {
		inner->ScanSchemas(context, callback);
	}

	PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner, LogicalCreateTable &op,
	                                    PhysicalOperator &plan) override {
		return inner->PlanCreateTableAs(context, planner, op, plan);
	}

	PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                             optional_ptr<PhysicalOperator> plan) override {
		if (RawIsIngestTable(op.table)) {
			return RawPlanQuackIngestInsert(context, planner, op, plan);
		}
		return inner->PlanInsert(context, planner, op, plan);
	}

	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                             PhysicalOperator &plan) override {
		return inner->PlanDelete(context, planner, op, plan);
	}

	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                             PhysicalOperator &plan) override {
		return inner->PlanUpdate(context, planner, op, plan);
	}

	PhysicalOperator &PlanMergeInto(ClientContext &context, PhysicalPlanGenerator &planner, LogicalMergeInto &op,
	                                PhysicalOperator &plan) override {
		return inner->PlanMergeInto(context, planner, op, plan);
	}

	unique_ptr<LogicalOperator> BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table,
	                                            unique_ptr<LogicalOperator> plan) override {
		return inner->BindCreateIndex(binder, stmt, table, std::move(plan));
	}

	unique_ptr<LogicalOperator> BindAlterAddIndex(Binder &binder, TableCatalogEntry &table_entry,
	                                              unique_ptr<LogicalOperator> plan,
	                                              unique_ptr<CreateIndexInfo> create_info,
	                                              unique_ptr<AlterTableInfo> alter_info) override {
		return inner->BindAlterAddIndex(binder, table_entry, std::move(plan), std::move(create_info),
		                                std::move(alter_info));
	}

	DatabaseSize GetDatabaseSize(ClientContext &context) override {
		return DatabaseSize();
	}

	bool InMemory() override {
		return false;
	}

	string GetDBPath() override {
		return StringUtil::StartsWith(db_path, "quack:") ? db_path : "quack:" + db_path;
	}

	void DropSchema(ClientContext &context, DropInfo &info) override {
		if (info.name == "ingest") {
			throw NotImplementedException("RawDuck: the ingest schema is virtual");
		}
		inner->DropEntry(context, info);
	}

private:
	unique_ptr<Catalog> inner;
	string db_path;
	string token;
	mutex ingest_lock;
	unique_ptr<SchemaCatalogEntry> ingest_schema;
};

//===--------------------------------------------------------------------===//
// PhysicalRawQuackIngest: stream payloads to remote raw_ingest
//===--------------------------------------------------------------------===//

class RawQuackIngestGlobalState : public GlobalSinkState {
public:
	RawParseOptions options;
	idx_t rows = 0;
};

class RawQuackIngestLocalState : public LocalSinkState {
public:
	vector<string> documents;
};

class PhysicalRawQuackIngest : public PhysicalOperator {
public:
	static constexpr idx_t BATCH_ROWS = 30000;

	PhysicalRawQuackIngest(PhysicalPlan &plan, string quack_uri_p, string token_p, string table_name_p,
	                       idx_t estimated_cardinality)
	    : PhysicalOperator(plan, PhysicalOperatorType::EXTENSION, {LogicalType::BIGINT}, estimated_cardinality),
	      quack_uri(std::move(quack_uri_p)), token(std::move(token_p)), table_name(std::move(table_name_p)) {
	}

	string GetName() const override {
		return "RAW_QUACK_INGEST";
	}
	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return false;
	}
	bool IsSource() const override {
		return true;
	}

	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override {
		auto state = make_uniq<RawQuackIngestGlobalState>();
		Value setting;
		if (context.TryGetCurrentSetting("rawduck_insert_transform", setting) && !setting.IsNull() &&
		    !setting.GetValue<string>().empty()) {
			auto value = setting.GetValue<string>();
			try {
				state->options = ResolveTransform(context, value, "");
			} catch (...) {
				state->options = ResolveTransform(context, "", value);
			}
		}
		return std::move(state);
	}

	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override {
		return make_uniq<RawQuackIngestLocalState>();
	}

	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override {
		auto &gstate = input.global_state.Cast<RawQuackIngestGlobalState>();
		auto &state = input.local_state.Cast<RawQuackIngestLocalState>();
		UnifiedVectorFormat payloads;
		chunk.data[0].ToUnifiedFormat(chunk.size(), payloads);
		auto strings = UnifiedVectorFormat::GetData<string_t>(payloads);
		for (idx_t i = 0; i < chunk.size(); i++) {
			auto idx = payloads.sel->get_index(i);
			if (!payloads.validity.RowIsValid(idx)) {
				continue;
			}
			state.documents.push_back(strings[idx].GetString());
			if (state.documents.size() >= BATCH_ROWS) {
				FlushBatch(context.client, gstate, state);
			}
		}
		return SinkResultType::NEED_MORE_INPUT;
	}

	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override {
		auto &gstate = input.global_state.Cast<RawQuackIngestGlobalState>();
		auto &state = input.local_state.Cast<RawQuackIngestLocalState>();
		FlushBatch(context.client, gstate, state);
		return SinkCombineResultType::FINISHED;
	}

	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override {
		return SinkFinalizeType::READY;
	}

	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override {
		auto &state = sink_state->Cast<RawQuackIngestGlobalState>();
		chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(state.rows)));
		chunk.SetCardinality(1);
		return SourceResultType::FINISHED;
	}

private:
	static string BuildPayloadArray(const vector<string> &documents) {
		string payload = "[";
		for (idx_t i = 0; i < documents.size(); i++) {
			if (i > 0) {
				payload += ",";
			}
			payload += documents[i];
		}
		payload += "]";
		return payload;
	}

	void FlushBatch(ClientContext &context, RawQuackIngestGlobalState &gstate, RawQuackIngestLocalState &state) const {
		if (state.documents.empty()) {
			return;
		}
		auto payload = BuildPayloadArray(state.documents);
		auto remote_sql = "CALL raw_ingest(" + RawQuackSQLString(table_name) + ", " + RawQuackSQLString(payload) + ")";
		RawQuackExecute(context, quack_uri, token, remote_sql);
		gstate.rows += state.documents.size();
		state.documents.clear();
	}

	string quack_uri;
	string token;
	string table_name;
};

PhysicalOperator &RawPlanQuackIngestInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                           optional_ptr<PhysicalOperator> plan) {
	if (!plan) {
		throw NotImplementedException("RawDuck: INSERT INTO ingest tables requires a data source");
	}
	auto &catalog = op.table.ParentCatalog();
	if (!RawDuckIsQuackCatalog(catalog)) {
		throw InternalException("RawDuck: quack ingest planning requires a rawduck:quack catalog");
	}
	auto quack_uri = RawDuckQuackUri(catalog);
	auto token = RawDuckQuackToken(catalog);
	auto table_name = op.table.name;
	auto &ingest = planner.Make<PhysicalRawQuackIngest>(std::move(quack_uri), std::move(token), std::move(table_name),
	                                                    op.estimated_cardinality);
	ingest.children.push_back(*plan);
	return ingest;
}

unique_ptr<Catalog> RawDuckAttachQuack(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                       AttachedDatabase &db, const string &name, AttachInfo &info,
                                       AttachOptions &options) {
	auto quack_ext = RawGetQuackStorageExtension(context);
	string path = info.path;
	if (!StringUtil::StartsWith(path, "quack:")) {
		path = "quack:" + path;
	}
	string token;
	auto token_entry = options.options.find("token");
	if (token_entry != options.options.end()) {
		token = token_entry->second.GetValue<string>();
	}
	auto inner = quack_ext->attach(storage_info, context, db, name, info, options);
	if (!inner) {
		throw InternalException("quack attach returned no catalog");
	}
	return make_uniq<RawDuckQuackCatalog>(db, std::move(inner), std::move(path), std::move(token));
}

unique_ptr<TransactionManager> RawDuckQuackCreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                                                                    AttachedDatabase &db, Catalog &catalog) {
	auto &rawduck_quack = catalog.Cast<RawDuckQuackCatalog>();
	auto quack_ext = StorageExtension::Find(db.GetDatabase().config, "quack");
	if (!quack_ext || !quack_ext->create_transaction_manager) {
		throw InternalException("quack extension missing after rawduck:quack attach");
	}
	return quack_ext->create_transaction_manager(storage_info, db, rawduck_quack.GetInner());
}

bool RawDuckIsQuackCatalog(Catalog &catalog) {
	return dynamic_cast<RawDuckQuackCatalog *>(&catalog) != nullptr;
}

string RawDuckQuackUri(Catalog &catalog) {
	return catalog.Cast<RawDuckQuackCatalog>().GetQuackUri();
}

string RawDuckQuackToken(Catalog &catalog) {
	return catalog.Cast<RawDuckQuackCatalog>().GetToken();
}

} // namespace duckdb
