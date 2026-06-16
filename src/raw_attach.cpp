#include "raw_functions.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "duckdb/transaction/transaction_manager.hpp"

namespace duckdb {

class RawDuckCatalog : public DuckCatalog {
public:
	explicit RawDuckCatalog(AttachedDatabase &db) : DuckCatalog(db) {
	}

	string GetCatalogType() override {
		return "rawduck";
	}

	// the virtual `ingest` schema: INSERT-syntax schema-less ingestion
	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const EntryLookupInfo &schema_lookup,
	                                              OnEntryNotFound if_not_found) override {
		if (schema_lookup.GetEntryName() == "ingest") {
			lock_guard<mutex> guard(ingest_lock);
			if (!ingest_schema) {
				ingest_schema = RawCreateIngestSchema(*this);
			}
			return ingest_schema.get();
		}
		return DuckCatalog::LookupSchema(transaction, schema_lookup, if_not_found);
	}

	PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                             optional_ptr<PhysicalOperator> plan) override {
		if (RawIsIngestTable(op.table)) {
			return RawPlanIngestInsert(context, planner, op, plan);
		}
		return DuckCatalog::PlanInsert(context, planner, op, plan);
	}

private:
	mutex ingest_lock;
	unique_ptr<SchemaCatalogEntry> ingest_schema;
};

static unique_ptr<Catalog> RawDuckAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                         AttachedDatabase &db, const string &name, AttachInfo &info,
                                         AttachOptions &options) {
	if (RawDuckIsQuackAttach(info.path)) {
		return RawDuckAttachQuack(storage_info, context, db, name, info, options);
	}
	return make_uniq_base<Catalog, RawDuckCatalog>(db);
}

static unique_ptr<TransactionManager> RawDuckCreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                                                                      AttachedDatabase &db, Catalog &catalog) {
	if (RawDuckIsQuackCatalog(catalog)) {
		return RawDuckQuackCreateTransactionManager(storage_info, db, catalog);
	}
	return make_uniq_base<TransactionManager, DuckTransactionManager>(db);
}

shared_ptr<StorageExtension> GetRawDuckStorageExtension() {
	auto extension = make_shared_ptr<StorageExtension>();
	extension->attach = RawDuckAttach;
	extension->create_transaction_manager = RawDuckCreateTransactionManager;
	return extension;
}

} // namespace duckdb
