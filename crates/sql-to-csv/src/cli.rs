use std::path::PathBuf;

use clap::Parser;

/// Fast, parallel SQL dump to CSV/TSV converter.
///
/// Extracts data from SQL dumps (MySQL, PostgreSQL, SQL Server, Oracle, SQLite)
/// into clean CSV/TSV files — one per table. Optionally generates PostgreSQL DDL.
/// Auto-detects the source dialect, or specify it with --dialect.
#[derive(Parser, Debug)]
#[command(name = "sql-to-csv", version, about)]
pub struct Cli {
    /// Input SQL dump file (.sql or .sql.gz)
    pub input: PathBuf,

    /// Output directory
    pub out_dir: PathBuf,

    /// Source SQL dialect (auto-detected if not specified)
    ///
    /// Supported: mysql, postgresql (pg), mssql (sqlserver), oracle, sqlite
    #[arg(long)]
    pub dialect: Option<String>,

    /// Overwrite output directory if it already exists
    #[arg(long, short = 'f')]
    pub force: bool,

    /// Only emit DDL (schema.sql), no data files
    #[arg(long)]
    pub schema_only: bool,

    /// Only emit data files, skip DDL conversion
    #[arg(long)]
    pub data_only: bool,

    /// Comma-separated allowlist of table names to extract
    #[arg(long, value_delimiter = ',')]
    pub tables: Option<Vec<String>>,

    /// Number of parallel workers (default: number of CPU cores)
    #[arg(long)]
    pub workers: Option<usize>,

    /// Rotate/shard CSV every N rows per table
    #[arg(long)]
    pub shard_rows: Option<usize>,

    /// Disable DDL conversion (skip schema.sql generation)
    #[arg(long)]
    pub no_postgres_ddl: bool,

    /// Parse and count rows only, without writing output
    #[arg(long)]
    pub dry_run: bool,

    /// NULL marker string for CSV output (default: \N)
    #[arg(long, default_value = "\\N")]
    pub null_marker: String,

    /// Output TSV (tab-separated) instead of CSV
    #[arg(long)]
    pub tsv: bool,

    /// CSV delimiter character (ignored if --tsv is set)
    #[arg(long, default_value = ",")]
    pub delimiter: String,
}

/// Return the clap Command for man page / completion generation.
#[allow(dead_code)]
pub fn command() -> clap::Command {
    <Cli as clap::CommandFactory>::command()
}
