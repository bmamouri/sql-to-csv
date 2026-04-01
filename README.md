# sql-to-csv

Fast, parallel SQL dump to CSV/TSV converter.

Extracts data from large SQL dump files and converts them to clean CSV or TSV files. One file per table, streamed with bounded memory, parallelized across all your CPU cores. Optionally generates PostgreSQL-compatible DDL for direct `COPY` import.

## Why

SQL dumps are everywhere — MySQL exports, database backups, Wikimedia data releases — but they're a terrible format for actually working with data. Loading a multi-GB dump by replaying INSERT statements takes hours. Most tools that claim to help are single-threaded, eat all your RAM, or require a running database server as an intermediary.

This tool was born out of trying to import Wikimedia's 6GB MySQL database dumps. There was no fast, straightforward way to get the data into CSV so it could be loaded anywhere — PostgreSQL, a data pipeline, pandas, DuckDB, or anything else that reads CSV.

sql-to-csv takes a different approach: it streams the dump, uses all your CPU cores via parallel workers, and outputs clean CSV/TSV files. What used to take hours now takes seconds. The CSV files work with anything. If you're targeting PostgreSQL specifically, the tool also generates `schema.sql` and a `load.sql` script for one-command `COPY` import.

## Supported dialects

| Dialect | Identifiers | Data format | Auto-detected by |
|---------|-------------|-------------|------------------|
| **MySQL / MariaDB** | `` `backticks` `` | Batched INSERT VALUES | `/*!`, `ENGINE=`, backtick identifiers |
| **PostgreSQL** | `"double quotes"` | COPY FROM stdin | `SET client_encoding`, `COPY ... FROM stdin` |
| **SQL Server** | `[square brackets]` | INSERT with N'...' strings | `[dbo]`, `SET NOCOUNT`, `NVARCHAR` |
| **Oracle** | `"DOUBLE QUOTES"` | INSERT VALUES | `VARCHAR2`, `NUMBER(`, `SPOOL` |
| **SQLite** | bare or `"quoted"` | INSERT VALUES | `BEGIN TRANSACTION`, `AUTOINCREMENT` |

## Installation

### Homebrew (macOS)

```bash
brew tap bmamouri/sql-to-csv
brew install sql-to-csv
```

### Pre-built binaries

Download from [GitHub Releases](https://github.com/bmamouri/sql-to-csv/releases):

```bash
# macOS (Apple Silicon)
curl -LO https://github.com/bmamouri/sql-to-csv/releases/latest/download/sql-to-csv-aarch64-apple-darwin.tar.gz
tar xzf sql-to-csv-aarch64-apple-darwin.tar.gz
sudo mv sql-to-csv /usr/local/bin/
```

### Cargo

```bash
cargo install --git https://github.com/bmamouri/sql-to-csv sql-to-csv
```

### Build from source

```bash
git clone https://github.com/bmamouri/sql-to-csv.git
cd sql-to-csv
cargo build --release
# Binary at target/release/sql-to-csv
```

## Quick start

```bash
# Convert a MySQL dump to CSV (dialect auto-detected)
sql-to-csv dump.sql output/

# Convert a gzipped dump
sql-to-csv dump.sql.gz output/

# SQL Server dump with explicit dialect
sql-to-csv mssql-export.sql output/ --dialect mssql

# Output TSV instead of CSV
sql-to-csv dump.sql output/ --tsv

# Extract only specific tables
sql-to-csv dump.sql output/ --tables users,posts,comments

# Just the data, no DDL
sql-to-csv dump.sql output/ --data-only

# Dry run (parse and count, no output)
sql-to-csv dump.sql output/ --dry-run
```

## Output

Given an input dump, the tool produces:

```
output/
  data/
    users.csv           # One CSV per table (or .tsv with --tsv)
    posts.csv
    comments.csv
  schema.sql            # PostgreSQL-compatible CREATE TABLE statements
  load.sql              # PostgreSQL helper: \i schema.sql + \COPY commands
  manifest.json         # Table names, row counts, file paths
  errors.log            # Warnings and errors with byte offsets
```

The CSV/TSV files work with any tool that reads tabular data — PostgreSQL, DuckDB, pandas, Excel, R, data pipelines, etc.

The tool refuses to write to a non-empty output directory. Use `--force` (`-f`) to overwrite.

### Loading into PostgreSQL

```bash
# One command — applies schema and loads all tables
psql -d mydb -f output/load.sql
```

### Using with other tools

```bash
# DuckDB
duckdb -c "CREATE TABLE users AS SELECT * FROM 'output/data/users.csv'"

# pandas
python -c "import pandas as pd; df = pd.read_csv('output/data/users.csv')"
```

## CLI reference

```
sql-to-csv [OPTIONS] <INPUT> <OUT_DIR>

Arguments:
  <INPUT>                    Input SQL dump file (.sql or .sql.gz)
  <OUT_DIR>                  Output directory

Options:
      --dialect <DIALECT>    Source SQL dialect: mysql, pg, mssql, oracle, sqlite
                             (auto-detected if omitted)
  -f, --force                Overwrite output directory if it already exists
      --schema-only          Only emit DDL (schema.sql), no data files
      --data-only            Only emit data files, skip DDL conversion
      --tables <a,b,c>       Comma-separated table allowlist
      --workers <N>          Parallel worker count [default: num_cpus]
      --shard-rows <N>       Rotate data file every N rows per table
      --no-postgres-ddl      Disable DDL conversion (skip schema.sql generation)
      --dry-run              Parse and count rows only
      --tsv                  Output TSV (tab-separated) instead of CSV
      --null-marker <STR>    NULL marker string [default: \N]
      --delimiter <CHAR>     CSV delimiter, ignored if --tsv [default: ,]
  -h, --help                 Print help
  -V, --version              Print version
```

## DDL conversion (PostgreSQL)

When DDL conversion is enabled (the default), the tool converts source DDL to PostgreSQL. Key type mappings:

| MySQL | SQL Server | Oracle | SQLite | PostgreSQL |
|-------|-----------|--------|--------|------------|
| `INT AUTO_INCREMENT` | `INT IDENTITY` | `NUMBER(9)` | `INTEGER AUTOINCREMENT` | `INTEGER GENERATED ALWAYS AS IDENTITY` |
| `TINYINT(1)` | `BIT` | - | - | `BOOLEAN` |
| `VARCHAR(255)` | `NVARCHAR(255)` | `VARCHAR2(255)` | `TEXT` | `VARCHAR(255)` / `TEXT` |
| `TEXT` / `LONGTEXT` | `NVARCHAR(MAX)` | `CLOB` | `TEXT` | `TEXT` |
| `BLOB` | `VARBINARY(MAX)` | `RAW` | `BLOB` | `BYTEA` |
| `DATETIME` | `DATETIME2` | `DATE` | - | `TIMESTAMP` |
| `JSON` | `XML` | `XMLTYPE` | - | `JSONB` / `XML` |
| - | `UNIQUEIDENTIFIER` | - | - | `UUID` |
| - | `MONEY` | - | - | `NUMERIC(19,4)` |
| `ENUM(...)` | - | - | - | `TEXT` (with warning) |
| `FLOAT` / `DOUBLE` | `FLOAT` | `NUMBER` | `REAL` | `REAL` / `DOUBLE PRECISION` |

Unsupported features (spatial types, triggers, etc.) emit warnings in `errors.log`. Use `--data-only` or `--no-postgres-ddl` to skip DDL conversion entirely if you only need the CSV files.

## Architecture

1. **Phase 1 (sequential):** Lexer-based scan builds an index of statement boundaries, respecting SQL string quoting rules. No naive regex splitting.
2. **Phase 2 (parallel):** Rayon worker pool processes disjoint index ranges via `mmap`. Per-table writers behind `Arc<Mutex<...>>`. Bounded memory — no loading entire files into RAM.

## Man page and shell completions

```bash
# Generate
just generate

# Install man page (macOS)
just install-man

# Install fish completions
just install-completions-fish
```

## License

MIT
