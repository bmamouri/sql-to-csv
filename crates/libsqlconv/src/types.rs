use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use serde::Serialize;

/// SQL dialect of the input dump.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlDialect {
    /// MySQL / MariaDB (backtick identifiers, batched INSERTs, conditional comments)
    Mysql,
    /// PostgreSQL pg_dump (double-quote identifiers, COPY FROM stdin)
    Postgresql,
    /// Microsoft SQL Server (square bracket identifiers, GO batches, N'...' strings)
    Mssql,
    /// Oracle SQL*Plus (double-quote identifiers, / terminator, Oracle types)
    Oracle,
    /// SQLite .dump (double-quote identifiers, BEGIN/COMMIT transactions)
    Sqlite,
}

impl SqlDialect {
    /// Parse from a CLI string.
    pub fn from_str_loose(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "mysql" | "mariadb" => Some(Self::Mysql),
            "postgresql" | "postgres" | "pg" => Some(Self::Postgresql),
            "mssql" | "sqlserver" | "sql-server" => Some(Self::Mssql),
            "oracle" => Some(Self::Oracle),
            "sqlite" => Some(Self::Sqlite),
            _ => None,
        }
    }
}

impl std::fmt::Display for SqlDialect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Mysql => write!(f, "MySQL"),
            Self::Postgresql => write!(f, "PostgreSQL"),
            Self::Mssql => write!(f, "SQL Server"),
            Self::Oracle => write!(f, "Oracle"),
            Self::Sqlite => write!(f, "SQLite"),
        }
    }
}

/// Configuration parsed from CLI arguments.
#[derive(Debug, Clone)]
pub struct Config {
    pub input_path: PathBuf,
    pub out_dir: PathBuf,
    pub force: bool,
    pub dialect: Option<SqlDialect>,
    pub schema_only: bool,
    pub data_only: bool,
    pub tables: Option<HashSet<String>>,
    pub workers: usize,
    pub shard_rows: Option<usize>,
    pub postgres_ddl: bool,
    pub dry_run: bool,
    pub null_marker: String,
    pub tsv: bool,
    pub delimiter: u8,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            input_path: PathBuf::new(),
            out_dir: PathBuf::from("out"),
            force: false,
            dialect: None,
            schema_only: false,
            data_only: false,
            tables: None,
            workers: num_workers(),
            shard_rows: None,
            postgres_ddl: true,
            dry_run: false,
            null_marker: "\\N".to_string(),
            tsv: false,
            delimiter: b',',
        }
    }
}

impl Config {
    /// Effective delimiter: tab if --tsv, otherwise the configured delimiter.
    pub fn effective_delimiter(&self) -> u8 {
        if self.tsv { b'\t' } else { self.delimiter }
    }

    /// File extension for data files.
    pub fn data_extension(&self) -> &str {
        if self.tsv { "tsv" } else { "csv" }
    }
}

pub fn num_workers() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}

// ---------------------------------------------------------------------------
// Phase 1 index types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatementKind {
    CreateTable,
    DropTable,
    InsertInto,
    /// PostgreSQL COPY ... FROM stdin block (header + data + \. terminator)
    CopyData,
    SetVariable,
    LockTable,
    UnlockTables,
    Comment,
    /// SQL Server GO batch terminator
    GoBatch,
    Other,
}

#[derive(Debug, Clone)]
pub struct StatementEntry {
    pub kind: StatementKind,
    pub table_name: Option<String>,
    pub byte_offset: u64,
    pub byte_length: u64,
    pub approx_line: u64,
}

pub struct StatementIndex {
    pub entries: Vec<StatementEntry>,
    pub tables_seen: HashSet<String>,
}

// ---------------------------------------------------------------------------
// DDL types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct TableDef {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub primary_key: Option<Vec<String>>,
    pub indexes: Vec<IndexDef>,
}

#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub pg_type: String,
    pub nullable: bool,
    pub default: Option<String>,
    pub is_identity: bool,
}

#[derive(Debug, Clone)]
pub struct IndexDef {
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
}

// ---------------------------------------------------------------------------
// Value types for parsed INSERT data
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    Null,
    Integer(i64),
    Float(f64),
    String(String),
    HexString(Vec<u8>),
    BitLiteral(u64),
}

// ---------------------------------------------------------------------------
// Summary / manifest
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct Summary {
    pub tables_processed: usize,
    pub total_rows: u64,
    pub per_table: HashMap<String, TableSummary>,
    pub errors: u64,
    pub warnings: u64,
}

#[derive(Debug, Serialize)]
pub struct TableSummary {
    pub row_count: u64,
    pub csv_path: PathBuf,
}
