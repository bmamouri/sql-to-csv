use std::path::PathBuf;

use libsqlconv::types::Config;

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("golden")
        .join(name)
}

fn run_with_config(fixture: &str, out_name: &str) -> libsqlconv::types::Summary {
    let input = fixture_path(fixture);
    let out_dir = std::env::temp_dir().join("sql-to-csv-test").join(out_name);
    let _ = std::fs::remove_dir_all(&out_dir);

    let config = Config {
        input_path: input,
        out_dir: out_dir.clone(),
        ..Config::default()
    };

    libsqlconv::run(&config).unwrap()
}

#[test]
fn multi_table_row_counts() {
    let summary = run_with_config("multi_table.sql", "multi_table");

    assert_eq!(summary.tables_processed, 3, "Expected 3 tables");

    let users = summary.per_table.get("users").expect("users table missing");
    assert_eq!(users.row_count, 5, "users should have 5 rows");

    let posts = summary.per_table.get("posts").expect("posts table missing");
    assert_eq!(posts.row_count, 3, "posts should have 3 rows");

    let settings = summary
        .per_table
        .get("settings")
        .expect("settings table missing");
    assert_eq!(settings.row_count, 3, "settings should have 3 rows");

    assert_eq!(summary.total_rows, 11);
}

#[test]
fn multi_table_schema_generated() {
    let out_dir = std::env::temp_dir()
        .join("sql-to-csv-test")
        .join("schema_check");
    let _ = std::fs::remove_dir_all(&out_dir);

    let config = Config {
        input_path: fixture_path("multi_table.sql"),
        out_dir: out_dir.clone(),
        ..Config::default()
    };

    libsqlconv::run(&config).unwrap();

    let schema = std::fs::read_to_string(out_dir.join("schema.sql")).unwrap();

    // DDL conversions
    assert!(schema.contains("CREATE TABLE \"users\""), "users DDL missing");
    assert!(schema.contains("CREATE TABLE \"posts\""), "posts DDL missing");
    assert!(schema.contains("CREATE TABLE \"settings\""), "settings DDL missing");

    // Type conversions
    assert!(schema.contains("INTEGER GENERATED ALWAYS AS IDENTITY"), "AUTO_INCREMENT not converted");
    assert!(schema.contains("BOOLEAN"), "tinyint(1) not converted to BOOLEAN");
    assert!(schema.contains("TIMESTAMPTZ"), "timestamp not converted");
    assert!(schema.contains("JSONB"), "json not converted");
    assert!(schema.contains("TEXT"), "text types present");

    // Indexes
    assert!(schema.contains("CREATE UNIQUE INDEX \"uniq_email\""), "unique index missing");
    assert!(schema.contains("CREATE INDEX \"idx_name\""), "name index missing");

    // MySQL artifacts removed
    assert!(!schema.contains("ENGINE="), "ENGINE should be stripped");
    assert!(!schema.contains("CHARSET="), "CHARSET should be stripped");
    assert!(!schema.contains("AUTO_INCREMENT"), "AUTO_INCREMENT keyword should be gone");
}

#[test]
fn multi_table_csv_content() {
    let out_dir = std::env::temp_dir()
        .join("sql-to-csv-test")
        .join("csv_content");
    let _ = std::fs::remove_dir_all(&out_dir);

    let config = Config {
        input_path: fixture_path("multi_table.sql"),
        out_dir: out_dir.clone(),
        ..Config::default()
    };

    libsqlconv::run(&config).unwrap();

    // Read users CSV with proper CSV parsing (handles multiline fields)
    let csv_path = out_dir.join("data/users.csv");
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_path(&csv_path)
        .unwrap();
    let records: Vec<csv::StringRecord> = rdr.records().map(|r| r.unwrap()).collect();
    assert_eq!(records.len(), 5, "users.csv should have 5 records");

    // First row: Alice
    assert_eq!(&records[0][1], "Alice");
    assert_eq!(&records[0][2], "alice@example.com");

    // Check NULL handling — Charlie has NULLs
    assert_eq!(&records[2][3], "\\N", "Charlie's bio should be NULL marker");

    // Read settings CSV — check string escaping
    let settings_csv = std::fs::read_to_string(out_dir.join("data/settings.csv")).unwrap();
    assert!(settings_csv.contains("site_name"), "settings should have site_name");
}

#[test]
fn multi_table_load_script() {
    let out_dir = std::env::temp_dir()
        .join("sql-to-csv-test")
        .join("load_script");
    let _ = std::fs::remove_dir_all(&out_dir);

    let config = Config {
        input_path: fixture_path("multi_table.sql"),
        out_dir: out_dir.clone(),
        ..Config::default()
    };

    libsqlconv::run(&config).unwrap();

    let load = std::fs::read_to_string(out_dir.join("load.sql")).unwrap();
    assert!(load.contains("\\i schema.sql"), "load.sql should source schema");
    assert!(load.contains("\\COPY \"users\""), "load.sql should have users COPY");
    assert!(load.contains("\\COPY \"posts\""), "load.sql should have posts COPY");
    assert!(load.contains("\\COPY \"settings\""), "load.sql should have settings COPY");
}

#[test]
fn multi_table_manifest() {
    let out_dir = std::env::temp_dir()
        .join("sql-to-csv-test")
        .join("manifest_check");
    let _ = std::fs::remove_dir_all(&out_dir);

    let config = Config {
        input_path: fixture_path("multi_table.sql"),
        out_dir: out_dir.clone(),
        ..Config::default()
    };

    libsqlconv::run(&config).unwrap();

    let manifest = std::fs::read_to_string(out_dir.join("manifest.json")).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&manifest).unwrap();
    assert_eq!(parsed["total_rows"], 11);
    assert_eq!(parsed["tables_processed"], 3);
}

#[test]
fn schema_only_mode() {
    let out_dir = std::env::temp_dir()
        .join("sql-to-csv-test")
        .join("schema_only");
    let _ = std::fs::remove_dir_all(&out_dir);

    let config = Config {
        input_path: fixture_path("multi_table.sql"),
        out_dir: out_dir.clone(),
        schema_only: true,
        ..Config::default()
    };

    let summary = libsqlconv::run(&config).unwrap();
    assert_eq!(summary.total_rows, 0);
    assert!(out_dir.join("schema.sql").exists());
}

#[test]
fn table_filter() {
    let out_dir = std::env::temp_dir()
        .join("sql-to-csv-test")
        .join("table_filter");
    let _ = std::fs::remove_dir_all(&out_dir);

    let mut tables = std::collections::HashSet::new();
    tables.insert("users".to_string());

    let config = Config {
        input_path: fixture_path("multi_table.sql"),
        out_dir: out_dir.clone(),
        tables: Some(tables),
        ..Config::default()
    };

    let summary = libsqlconv::run(&config).unwrap();
    assert_eq!(summary.per_table.len(), 1);
    assert!(summary.per_table.contains_key("users"));
    assert_eq!(summary.per_table["users"].row_count, 5);
}

#[test]
fn unicode_end_to_end() {
    // Write a SQL dump with unicode to a temp file, run full pipeline, verify CSV bytes
    let tmp = tempfile::tempdir().unwrap();
    let sql_path = tmp.path().join("unicode.sql");
    let out_dir = tmp.path().join("out");

    // Create SQL dump with Arabic/Persian, Japanese, emoji, accented Latin
    std::fs::write(
        &sql_path,
        "CREATE TABLE `sitelinks` (\n\
         `id` int(11) NOT NULL,\n\
         `page_id` int(11) NOT NULL,\n\
         `wiki` varchar(50) NOT NULL,\n\
         `title` varchar(500) NOT NULL,\n\
         PRIMARY KEY (`id`)\n\
         ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n\
         INSERT INTO `sitelinks` VALUES \
         (488853314,2712495,'fawiki','سیارک ۹۸۸۲۵'),\
         (2,100,'jawiki','日本語のページ'),\
         (3,200,'frwiki','Café résumé naïve'),\
         (4,300,'test','🎉 emoji 🚀 test');\n",
    )
    .unwrap();

    let config = Config {
        input_path: sql_path,
        out_dir: out_dir.clone(),
        ..Config::default()
    };

    let summary = libsqlconv::run(&config).unwrap();
    assert_eq!(summary.per_table["sitelinks"].row_count, 4);

    // Read raw bytes from CSV
    let csv_bytes = std::fs::read(out_dir.join("data/sitelinks.csv")).unwrap();
    let csv_text = String::from_utf8(csv_bytes.clone())
        .expect("CSV output must be valid UTF-8");

    // Verify each unicode string is present and not corrupted
    assert!(
        csv_text.contains("سیارک ۹۸۸۲۵"),
        "Arabic/Persian text corrupted. Got: {csv_text}"
    );
    assert!(
        csv_text.contains("日本語のページ"),
        "Japanese text corrupted. Got: {csv_text}"
    );
    assert!(
        csv_text.contains("Café résumé naïve"),
        "Accented Latin text corrupted. Got: {csv_text}"
    );
    assert!(
        csv_text.contains("🎉 emoji 🚀 test"),
        "Emoji text corrupted. Got: {csv_text}"
    );

    // Also verify at the byte level that Arabic text isn't double-encoded.
    // "سیارک" in UTF-8 is D8 B3 DB 8C D8 A7 D8 B1 DA A9
    // If double-encoded, first byte would be C3 98 (UTF-8 for U+00D8 = Ø)
    let double_encoded_marker = &[0xC3u8, 0x98]; // Ø in UTF-8
    assert!(
        !csv_bytes
            .windows(2)
            .any(|w| w == double_encoded_marker && csv_bytes.contains(&0xC2)),
        "Detected double-encoded UTF-8 in output"
    );
}
