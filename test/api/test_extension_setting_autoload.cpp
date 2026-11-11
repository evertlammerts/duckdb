#include "catch.hpp"
#include "duckdb.h"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/db_instance_cache.hpp"
#include "duckdb/main/extension/linked_extension_registry.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "test_helpers.hpp"

#include <iostream>
#include <map>
#include <set>

using namespace duckdb;

TEST_CASE("Test autoload of extension settings", "[api]") {
	DBConfig config;
	config.SetOptionByName("timezone", "America/Los_Angeles");

	config.SetOptionByName("allow_unsigned_extensions", true);
	config.SetOptionByName("autoload_known_extensions", true);
	auto env_var = std::getenv("LOCAL_EXTENSION_REPO");
	if (!env_var) {
		return;
	}
	config.SetOptionByName("autoinstall_extension_repo", string(env_var));
	REQUIRE(config.options.unrecognized_options.count("timezone"));

	// Create a connection
	duckdb::unique_ptr<DuckDB> db;
	REQUIRE_NOTHROW(db = make_uniq<DuckDB>(nullptr, &config));
	Connection con(*db);

	auto res = con.Query("select current_setting('timezone')");
	REQUIRE(CHECK_COLUMN(res, 0, {Value("America/Los_Angeles")}));
}

static bool IsLinked(const string &extension_name) {
	for (auto &linked : LinkedExtensionRegistry::Get()) {
		if (linked.load && StringUtil::CIEquals(linked.name, extension_name)) {
			return true;
		}
	}
	return false;
}

// a repository that does not exist and a fresh extension directory, so no case can install or reach the network
static string IsolateFromRepository(DBConfig &config) {
	auto extension_directory = TestCreatePath("startup_setting_extensions");
	TestDeleteDirectory(extension_directory);
	config.SetOptionByName("autoinstall_extension_repository", TestCreatePath("no_such_repository"));
	config.SetOptionByName("extension_directories", Value::LIST({Value(extension_directory)}));
	config.SetOptionByName("autoload_known_extensions", true);
	config.SetOptionByName("autoinstall_known_extensions", true);
	return extension_directory;
}

static void RequireLoaded(Connection &con, const string &extension_name) {
	auto res = con.Query("SELECT loaded FROM duckdb_extensions() WHERE extension_name = '" + extension_name + "'");
	REQUIRE(CHECK_COLUMN(res, 0, {true}));
}

static void RequireNothingInstalled(const string &extension_directory) {
	auto fs = FileSystem::CreateLocal();
	REQUIRE(!fs->DirectoryExists(extension_directory));
}

TEST_CASE("A startup setting of a linked extension does not need the extension repository", "[api]") {
	if (!IsLinked("icu")) {
		return;
	}
	DBConfig config;
	auto extension_directory = IsolateFromRepository(config);
	config.SetOptionByName("TimeZone", "America/Los_Angeles");

	duckdb::unique_ptr<DuckDB> db;
	REQUIRE_NOTHROW(db = make_uniq<DuckDB>(nullptr, &config));
	Connection con(*db);
	auto res = con.Query("SELECT current_setting('TimeZone'), TIMESTAMPTZ '2024-01-01 00:00:00+00'::VARCHAR");
	REQUIRE(CHECK_COLUMN(res, 0, {"America/Los_Angeles"}));
	REQUIRE(CHECK_COLUMN(res, 1, {"2023-12-31 16:00:00-08"}));
	RequireLoaded(con, "icu");
	RequireNothingInstalled(extension_directory);
}

TEST_CASE("Two startup settings of a linked extension both take effect without the repository", "[api]") {
	if (!IsLinked("icu")) {
		return;
	}
	DBConfig config;
	auto extension_directory = IsolateFromRepository(config);
	config.SetOptionByName("TimeZone", "America/Los_Angeles");
	config.SetOptionByName("Calendar", "japanese");

	duckdb::unique_ptr<DuckDB> db;
	REQUIRE_NOTHROW(db = make_uniq<DuckDB>(nullptr, &config));
	Connection con(*db);
	auto res = con.Query("SELECT current_setting('TimeZone'), current_setting('Calendar')");
	REQUIRE(CHECK_COLUMN(res, 0, {"America/Los_Angeles"}));
	REQUIRE(CHECK_COLUMN(res, 1, {"japanese"}));
	RequireNothingInstalled(extension_directory);
}

TEST_CASE("A startup setting of a linked extension with autoinstall off", "[api]") {
	if (!IsLinked("icu")) {
		return;
	}
	DBConfig config;
	IsolateFromRepository(config);
	config.SetOptionByName("autoinstall_known_extensions", false);
	config.SetOptionByName("TimeZone", "America/Los_Angeles");

	duckdb::unique_ptr<DuckDB> db;
	REQUIRE_NOTHROW(db = make_uniq<DuckDB>(nullptr, &config));
	Connection con(*db);
	auto res = con.Query("SELECT current_setting('TimeZone')");
	REQUIRE(CHECK_COLUMN(res, 0, {"America/Los_Angeles"}));
	RequireLoaded(con, "icu");
}

TEST_CASE("A startup setting of an extension that is neither linked nor installed still fails to autoload", "[api]") {
	if (IsLinked("httpfs")) {
		return;
	}
	for (bool autoinstall : {true, false}) {
		CAPTURE(autoinstall);
		DBConfig config;
		auto extension_directory = IsolateFromRepository(config);
		config.SetOptionByName("autoinstall_known_extensions", autoinstall);
		config.SetOptionByName("s3_region", "eu-west-1");

		duckdb::unique_ptr<DuckDB> db;
		REQUIRE_THROWS_WITH(db = make_uniq<DuckDB>(nullptr, &config),
		                    Catch::Contains("the httpfs extension needs to be loaded. But it could not be autoloaded"));
		auto fs = FileSystem::CreateLocal();
		REQUIRE(fs->DirectoryExists(extension_directory) == autoinstall);
	}
}

TEST_CASE("A runtime autoload through the client context attempts an install only with autoinstall on", "[api]") {
	if (IsLinked("encodings")) {
		return;
	}
	for (bool autoinstall : {true, false}) {
		CAPTURE(autoinstall);
		DBConfig config;
		auto extension_directory = IsolateFromRepository(config);
		config.SetOptionByName("autoinstall_known_extensions", autoinstall);

		DuckDB db(nullptr, &config);
		if (!ExtensionHelper::CanAutoloadExtension(*db.instance, "encodings")) {
			return;
		}
		Connection con(db);
		auto csv_path = TestCreatePath("startup_setting_shift_jis.csv");
		REQUIRE_NO_FAIL(con.Query("COPY (SELECT 'a' AS c) TO '" + csv_path + "'"));
		auto fs = FileSystem::CreateLocal();
		REQUIRE(!fs->DirectoryExists(extension_directory));
		// an unknown encoding asks for the encodings extension through the client context
		auto scan = con.Query("SELECT * FROM read_csv('" + csv_path + "', encoding = 'shift_jis')");
		REQUIRE(scan->HasError());
		REQUIRE(StringUtil::Contains(scan->GetError(), "does not support the encoding: \"shift_jis\""));
		REQUIRE(fs->DirectoryExists(extension_directory) == autoinstall);
	}
}

TEST_CASE("A runtime setting of a linked extension that is not loaded yet does not need the repository", "[api]") {
	if (!IsLinked("icu")) {
		return;
	}
	DBConfig config;
	auto extension_directory = IsolateFromRepository(config);
	config.options.load_extensions = false;

	DuckDB db(nullptr, &config);
	if (!ExtensionHelper::CanAutoloadExtension(*db.instance, "icu")) {
		return;
	}
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("SET TimeZone = 'America/Los_Angeles'"));
	auto res = con.Query("SELECT current_setting('TimeZone')");
	REQUIRE(CHECK_COLUMN(res, 0, {"America/Los_Angeles"}));
	RequireLoaded(con, "icu");
	RequireNothingInstalled(extension_directory);
}

TEST_CASE("A runtime autoload through the client context of a linked extension does not need the repository", "[api]") {
	if (!IsLinked("parquet")) {
		return;
	}
	DBConfig config;
	auto extension_directory = IsolateFromRepository(config);
	config.options.load_extensions = false;

	DuckDB db(nullptr, &config);
	if (!ExtensionHelper::CanAutoloadExtension(*db.instance, "parquet")) {
		return;
	}
	Connection con(db);
	// a zst replacement scan asks for parquet through the client context before it looks at the file
	auto scan = con.Query("SELECT * FROM 'no_such_file.csv.zst'");
	REQUIRE(scan->HasError());
	REQUIRE(StringUtil::Contains(scan->GetError(), "No files found that match the pattern"));
	RequireLoaded(con, "parquet");
	RequireNothingInstalled(extension_directory);
}

TEST_CASE("An autoload by the file path of a linked extension still goes through the install step", "[api]") {
	if (!IsLinked("icu")) {
		return;
	}
	DBConfig config;
	auto extension_directory = IsolateFromRepository(config);
	config.options.load_extensions = false;

	DuckDB db(nullptr, &config);
	REQUIRE(!ExtensionHelper::TryAutoLoadExtension(*db.instance, TestCreatePath("icu.duckdb_extension")));
	auto fs = FileSystem::CreateLocal();
	REQUIRE(fs->DirectoryExists(extension_directory));
	Connection con(db);
	auto res = con.Query("SELECT loaded FROM duckdb_extensions() WHERE extension_name = 'icu'");
	REQUIRE(CHECK_COLUMN(res, 0, {false}));
}

TEST_CASE("A load of a linked extension takes the linked copy only from the core repository", "[api]") {
	if (!IsLinked("icu")) {
		return;
	}
	DBConfig config;
	auto extension_directory = IsolateFromRepository(config);
	config.options.load_extensions = false;

	DuckDB db(nullptr, &config);
	Connection con(db);
	auto community = con.Query("LOAD icu FROM community");
	// a build without loadable extensions fails this load with another text, so only the failure is checked
	REQUIRE(community->HasError());
	auto res = con.Query("SELECT loaded FROM duckdb_extensions() WHERE extension_name = 'icu'");
	REQUIRE(CHECK_COLUMN(res, 0, {false}));

	REQUIRE_NO_FAIL(con.Query("LOAD icu FROM core"));
	RequireLoaded(con, "icu");
	RequireNothingInstalled(extension_directory);
}

TEST_CASE("A startup setting of a linked extension through the C API does not need the repository", "[api][capi]") {
	if (!IsLinked("icu")) {
		return;
	}
	auto extension_directory = TestCreatePath("startup_setting_extensions");
	TestDeleteDirectory(extension_directory);
	auto repository = TestCreatePath("no_such_repository");

	duckdb_config config;
	REQUIRE(duckdb_create_config(&config) == DuckDBSuccess);
	REQUIRE(duckdb_set_config(config, "autoinstall_extension_repository", repository.c_str()) == DuckDBSuccess);
	REQUIRE(duckdb_set_config(config, "extension_directories", ("[" + extension_directory + "]").c_str()) ==
	        DuckDBSuccess);
	REQUIRE(duckdb_set_config(config, "autoinstall_known_extensions", "true") == DuckDBSuccess);
	REQUIRE(duckdb_set_config(config, "autoload_known_extensions", "true") == DuckDBSuccess);
	REQUIRE(duckdb_set_config(config, "TimeZone", "America/Los_Angeles") == DuckDBSuccess);

	duckdb_database database;
	char *error = nullptr;
	auto state = duckdb_open_ext(nullptr, &database, config, &error);
	duckdb_destroy_config(&config);
	string error_message = error ? error : "";
	duckdb_free(error);
	REQUIRE(error_message == "");
	REQUIRE(state == DuckDBSuccess);

	duckdb_connection connection;
	REQUIRE(duckdb_connect(database, &connection) == DuckDBSuccess);
	duckdb_result result;
	REQUIRE(duckdb_query(connection, "SELECT current_setting('TimeZone')", &result) == DuckDBSuccess);
	auto zone = duckdb_value_varchar(&result, 0, 0);
	string zone_value = zone;
	duckdb_free(zone);
	duckdb_destroy_result(&result);
	duckdb_disconnect(&connection);
	duckdb_close(&database);
	REQUIRE(zone_value == "America/Los_Angeles");
	RequireNothingInstalled(extension_directory);
}

static void SetProbeLimits(DBConfig &config) {
	config.SetOptionByName("memory_limit", "4GB");
	config.SetOptionByName("threads", 4);
}

static string OpenError(const char *path, DBConfig &config) {
	try {
		DuckDB db(path, &config);
	} catch (std::exception &ex) {
		return ErrorData(ex).RawMessage();
	}
	return string();
}

// the configuration of an open that never autoloads, as embedders that ship their own extensions use it
static string IsolateWithoutAutoload(DBConfig &config) {
	SetProbeLimits(config);
	auto extension_directory = IsolateFromRepository(config);
	config.SetOptionByName("autoload_known_extensions", false);
	config.SetOptionByName("autoinstall_known_extensions", false);
	return extension_directory;
}

TEST_CASE("A startup setting of a linked extension takes effect with autoload off", "[api]") {
	if (!IsLinked("icu")) {
		return;
	}
	DBConfig config;
	auto extension_directory = IsolateWithoutAutoload(config);
	config.SetOptionByName("TimeZone", "America/Los_Angeles");
	config.SetOptionByName("Calendar", "japanese");

	duckdb::unique_ptr<DuckDB> db;
	REQUIRE_NOTHROW(db = make_uniq<DuckDB>(nullptr, &config));
	Connection con(*db);
	auto res = con.Query("SELECT current_setting('TimeZone'), current_setting('Calendar'), "
	                     "TIMESTAMPTZ '2024-01-01 00:00:00+00'::VARCHAR");
	REQUIRE(CHECK_COLUMN(res, 0, {"America/Los_Angeles"}));
	REQUIRE(CHECK_COLUMN(res, 1, {"japanese"}));
	REQUIRE(CHECK_COLUMN(res, 2, {"2023-12-31 16:00:00-08"}));
	RequireLoaded(con, "icu");
	RequireNothingInstalled(extension_directory);
}

TEST_CASE("A startup value of a linked extension setting is validated like a SET", "[api]") {
	if (!IsLinked("icu")) {
		return;
	}
	for (auto autoload : {false, true}) {
		DBConfig config;
		IsolateWithoutAutoload(config);
		config.SetOptionByName("autoload_known_extensions", autoload);
		// the set callback canonicalizes the zone name, as SET TimeZone does
		config.SetOptionByName("TimeZone", "america/los_angeles");
		duckdb::unique_ptr<DuckDB> db;
		REQUIRE_NOTHROW(db = make_uniq<DuckDB>(nullptr, &config));
		Connection con(*db);
		auto res = con.Query("SELECT current_setting('TimeZone'), TIMESTAMPTZ '2024-01-01 00:00:00+00'::VARCHAR");
		REQUIRE(CHECK_COLUMN(res, 0, {"America/Los_Angeles"}));
		REQUIRE(CHECK_COLUMN(res, 1, {"2023-12-31 16:00:00-08"}));
	}
	for (auto autoload : {false, true}) {
		DBConfig config;
		IsolateWithoutAutoload(config);
		config.SetOptionByName("autoload_known_extensions", autoload);
		config.SetOptionByName("TimeZone", "Not/AZone");
		REQUIRE(StringUtil::Contains(OpenError(nullptr, config), "Unknown TimeZone 'Not/AZone'"));
	}
}

TEST_CASE("A startup value of a linked extension setting that fails the cast names the setting", "[api]") {
	if (!IsLinked("parquet")) {
		return;
	}
	for (auto autoload : {false, true}) {
		DBConfig config;
		IsolateWithoutAutoload(config);
		config.SetOptionByName("autoload_known_extensions", autoload);
		config.SetOptionByName("binary_as_string", "notabool");
		auto error = OpenError(nullptr, config);
		REQUIRE(StringUtil::Contains(error, "binary_as_string"));
		REQUIRE(StringUtil::Contains(error, "Could not convert string 'notabool' to BOOL"));
	}
	DBConfig config;
	IsolateWithoutAutoload(config);
	config.SetOptionByName("binary_as_string", "true");
	DuckDB db(nullptr, &config);
	Connection con(db);
	auto res = con.Query("SELECT typeof(current_setting('binary_as_string')), current_setting('binary_as_string')");
	REQUIRE(CHECK_COLUMN(res, 0, {"BOOLEAN"}));
	REQUIRE(CHECK_COLUMN(res, 1, {true}));
}

TEST_CASE("Startup settings that no linked extension provides fail the open as before", "[api]") {
	auto path = TestCreatePath("startup_setting_unrecognized.db");
	for (auto autoload : {false, true}) {
		TestDeleteFile(path);
		DBConfig config;
		IsolateWithoutAutoload(config);
		config.SetOptionByName("autoload_known_extensions", autoload);
		config.SetOptionByName("no_such_startup_setting", 42);
		REQUIRE(OpenError(path.c_str(), config) ==
		        "The following options were not recognized: no_such_startup_setting");
		// the open fails before the database file is created
		REQUIRE(!FileSystem::CreateLocal()->FileExists(path));
	}
	if (IsLinked("httpfs")) {
		return;
	}
	DBConfig config;
	IsolateWithoutAutoload(config);
	config.SetOptionByName("s3_region", "eu-west-1");
	REQUIRE(OpenError(nullptr, config) == "The following options were not recognized: s3_region");
}

TEST_CASE("A startup value of a linked extension setting that fails its SET leaves the new database file", "[api]") {
	if (!IsLinked("icu")) {
		return;
	}
	auto path = TestCreatePath("startup_setting_failed_set.db");
	for (auto autoload : {false, true}) {
		CAPTURE(autoload);
		TestDeleteFile(path);
		DBConfig config;
		IsolateWithoutAutoload(config);
		config.SetOptionByName("autoload_known_extensions", autoload);
		config.SetOptionByName("TimeZone", "Not/AZone");
		auto error = OpenError(path.c_str(), config);
		REQUIRE(
		    StringUtil::Contains(error, "Could not set option \"TimeZone\" at startup: Unknown TimeZone 'Not/AZone'"));
		// the main database is attached before a linked extension's startup SET runs, so the new file stays
		REQUIRE(FileSystem::CreateLocal()->FileExists(path));
	}
}

TEST_CASE("A startup setting of a linked extension with load_extensions off", "[api]") {
	if (!IsLinked("icu")) {
		return;
	}
	{
		// nothing loads icu in this open, so the setting stays unknown
		DBConfig config;
		IsolateWithoutAutoload(config);
		config.options.load_extensions = false;
		config.SetOptionByName("TimeZone", "America/Los_Angeles");
		REQUIRE(OpenError(nullptr, config) == "The following options were not recognized: TimeZone");
	}
	DBConfig config;
	SetProbeLimits(config);
	auto extension_directory = IsolateFromRepository(config);
	config.options.load_extensions = false;
	config.SetOptionByName("TimeZone", "america/los_angeles");
	DuckDB db(nullptr, &config);
	Connection con(db);
	auto res = con.Query("SELECT value FROM duckdb_settings() WHERE name = 'TimeZone'");
	REQUIRE(CHECK_COLUMN(res, 0, {"America/Los_Angeles"}));
	RequireLoaded(con, "icu");
	RequireNothingInstalled(extension_directory);
}

TEST_CASE("A startup setting of a linked extension through the instance cache with autoload off", "[api]") {
	if (!IsLinked("icu")) {
		return;
	}
	// a client such as Python builds its config from a dictionary of values and opens through the instance cache
	identifier_map_t<Value> options;
	options["memory_limit"] = Value("4GB");
	options["threads"] = Value::BIGINT(4);
	options["autoload_known_extensions"] = Value::BOOLEAN(false);
	options["autoinstall_known_extensions"] = Value::BOOLEAN(false);
	options["TimeZone"] = Value("America/Los_Angeles");
	DBConfig config(options, false);
	DBInstanceCache cache;
	auto db = cache.GetOrCreateInstance(TestCreatePath("startup_setting_cache.db"), config, false);
	Connection con(*db);
	auto res = con.Query("SELECT current_setting('TimeZone')");
	REQUIRE(CHECK_COLUMN(res, 0, {"America/Los_Angeles"}));
}

TEST_CASE("A startup setting of a linked extension through the C API with autoload off", "[api][capi]") {
	if (!IsLinked("icu")) {
		return;
	}
	duckdb_config config;
	REQUIRE(duckdb_create_config(&config) == DuckDBSuccess);
	REQUIRE(duckdb_set_config(config, "memory_limit", "4GB") == DuckDBSuccess);
	REQUIRE(duckdb_set_config(config, "threads", "4") == DuckDBSuccess);
	REQUIRE(duckdb_set_config(config, "autoload_known_extensions", "false") == DuckDBSuccess);
	REQUIRE(duckdb_set_config(config, "TimeZone", "america/los_angeles") == DuckDBSuccess);

	duckdb_database database;
	char *error = nullptr;
	auto state = duckdb_open_ext(nullptr, &database, config, &error);
	duckdb_destroy_config(&config);
	string error_message = error ? error : "";
	duckdb_free(error);
	REQUIRE(error_message == "");
	REQUIRE(state == DuckDBSuccess);

	duckdb_connection connection;
	REQUIRE(duckdb_connect(database, &connection) == DuckDBSuccess);
	duckdb_result result;
	REQUIRE(duckdb_query(connection, "SELECT current_setting('TimeZone')", &result) == DuckDBSuccess);
	auto zone = duckdb_value_varchar(&result, 0, 0);
	string zone_value = zone;
	duckdb_free(zone);
	duckdb_destroy_result(&result);
	duckdb_disconnect(&connection);
	duckdb_close(&database);
	REQUIRE(zone_value == "America/Los_Angeles");
}

TEST_CASE("A startup setting of a linked extension on an instance without a database", "[api]") {
	if (!IsLinked("icu")) {
		return;
	}
	DBConfig config;
	IsolateWithoutAutoload(config);
	config.SetOptionByName("TimeZone", "america/los_angeles");
	auto db = DuckDB::CreateEmpty(&config);
	REQUIRE(db->instance->ExtensionIsLoaded("icu"));
	Value zone;
	auto lookup = db->instance->TryGetCurrentSetting("TimeZone", zone);
	REQUIRE(bool(lookup));
	REQUIRE(zone == Value("America/Los_Angeles"));
}

// linked into this one database only, through its config, and running SQL against the main database while it loads
static LinkedExtension SqlOnLoadExtension(vector<string> queries, vector<string> &seen_during_load) {
	LinkedExtension extension;
	extension.name = "sql_on_load_test";
	extension.load = [queries, &seen_during_load](DuckDB &db) {
		Connection con(db);
		for (auto &query : queries) {
			auto res = con.Query(query);
			seen_during_load.push_back(res->HasError() ? res->GetError() : res->Collection().GetValue(0, 0).ToString());
		}
	};
	return extension;
}

static const vector<string> SQL_ON_LOAD = {"SELECT current_database()", "CREATE TABLE loaded_marker AS SELECT 42 AS x"};

TEST_CASE("A linked extension that runs SQL while it loads, beside a startup setting of a linked extension", "[api]") {
	if (!IsLinked("icu")) {
		return;
	}
	auto path = TestCreatePath("startup_setting_sql_on_load.db");
	for (auto file : {false, true}) {
		TestDeleteFile(path);
		vector<string> seen_during_load;
		DBConfig config;
		IsolateWithoutAutoload(config);
		// after the extensions of the binary, as a further extension linked into it would be
		config.linked_extensions = LinkedExtensionRegistry::Get();
		auto queries = SQL_ON_LOAD;
		queries.push_back("SELECT current_setting('TimeZone')");
		config.linked_extensions.push_back(SqlOnLoadExtension(queries, seen_during_load));
		config.SetOptionByName("TimeZone", "america/los_angeles");
		duckdb::unique_ptr<DuckDB> db;
		REQUIRE_NOTHROW(db = make_uniq<DuckDB>(file ? path.c_str() : nullptr, &config));
		// the load after icu sees the zone as SET TimeZone stores it
		REQUIRE(seen_during_load ==
		        vector<string> {file ? "startup_setting_sql_on_load" : "memory", "1", "America/Los_Angeles"});
		Connection con(*db);
		auto res = con.Query("SELECT x, current_setting('TimeZone') FROM loaded_marker");
		REQUIRE(CHECK_COLUMN(res, 0, {42}));
		REQUIRE(CHECK_COLUMN(res, 1, {"America/Los_Angeles"}));
	}
}

TEST_CASE("A linked extension that owns a startup setting and runs SQL while it loads", "[api]") {
	if (!IsLinked("icu")) {
		return;
	}
	for (auto autoload : {false, true}) {
		vector<string> seen_during_load;
		DBConfig config;
		IsolateWithoutAutoload(config);
		config.SetOptionByName("autoload_known_extensions", autoload);
		config.linked_extensions = LinkedExtensionRegistry::Get();
		for (auto &linked : config.linked_extensions) {
			if (StringUtil::CIEquals(linked.name, "icu")) {
				auto icu_load = linked.load;
				auto sql_on_load = SqlOnLoadExtension(SQL_ON_LOAD, seen_during_load).load;
				linked.load = [icu_load, sql_on_load](DuckDB &db) {
					icu_load(db);
					sql_on_load(db);
				};
			}
		}
		config.SetOptionByName("TimeZone", "america/los_angeles");
		duckdb::unique_ptr<DuckDB> db;
		REQUIRE_NOTHROW(db = make_uniq<DuckDB>(nullptr, &config));
		REQUIRE(seen_during_load == vector<string> {"memory", "1"});
		Connection con(*db);
		auto res = con.Query("SELECT x, current_setting('TimeZone') FROM loaded_marker");
		REQUIRE(CHECK_COLUMN(res, 0, {42}));
		REQUIRE(CHECK_COLUMN(res, 1, {"America/Los_Angeles"}));
	}
}
