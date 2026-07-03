#include "raw_functions.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/qualified_name.hpp"

#ifndef _WIN32
#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>
#endif

namespace duckdb {

string RawResolveIngestTarget(ClientContext &context, const string &table) {
	auto qn = QualifiedName::Parse(table);
	if (!qn.catalog.empty() || !qn.schema.empty()) {
		return table;
	}
	Value prefix;
	if (!context.TryGetCurrentSetting("rawduck_ingest_prefix", prefix) || prefix.IsNull()) {
		return table;
	}
	auto prefix_str = prefix.GetValue<string>();
	if (prefix_str.empty()) {
		return table;
	}
	if (StringUtil::EndsWith(prefix_str, ".")) {
		return prefix_str + table;
	}
	return prefix_str + "." + table;
}

void RawSetIngestPrefix(DatabaseInstance &db, const string &prefix) {
	auto &config = DBConfig::GetConfig(db);
	config.SetOption("rawduck_ingest_prefix", Value(prefix));
}

static string RawCatalogSchemaLockPath(ClientContext &context, const QualifiedName &qname) {
	if (qname.catalog.empty()) {
		return string();
	}
	auto attached = DatabaseManager::Get(context).GetDatabase(context, qname.catalog);
	if (!attached) {
		return string();
	}
	auto path = attached->GetCatalog().GetDBPath();
	if (path.empty()) {
		return string();
	}
	return path + ".rawduck_schema.lock";
}

RawFallbackSchemaLock::RawFallbackSchemaLock(ClientContext &context, const QualifiedName &qname) {
	auto path = RawCatalogSchemaLockPath(context, qname);
	if (path.empty()) {
		return;
	}
#ifndef _WIN32
	fd = open(path.c_str(), O_CREAT | O_RDWR, 0644);
	if (fd < 0) {
		throw IOException("RawDuck: cannot open schema lock %s", path);
	}
	if (flock(fd, LOCK_EX) != 0) {
		close(fd);
		fd = -1;
		throw IOException("RawDuck: cannot acquire schema lock on %s", path);
	}
#else
	(void)path;
#endif
}

RawFallbackSchemaLock::~RawFallbackSchemaLock() {
#ifndef _WIN32
	if (fd >= 0) {
		flock(fd, LOCK_UN);
		close(fd);
	}
#endif
}

} // namespace duckdb
