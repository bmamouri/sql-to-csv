use std::collections::HashSet;
use std::process;

use clap::Parser;
use libsqlconv::types::{Config, SqlDialect};

mod cli;
use cli::Cli;

fn main() {
    let cli = Cli::parse();

    let delimiter = if cli.delimiter.len() == 1 {
        cli.delimiter.as_bytes()[0]
    } else if cli.delimiter == "\\t" || cli.delimiter == "tab" {
        b'\t'
    } else {
        eprintln!("Error: delimiter must be a single character");
        process::exit(1);
    };

    let dialect = cli.dialect.map(|d| {
        SqlDialect::from_str_loose(&d).unwrap_or_else(|| {
            eprintln!("Unknown dialect: '{d}'. Supported: mysql, postgresql (pg), mssql (sqlserver), oracle, sqlite");
            process::exit(1);
        })
    });

    let config = Config {
        input_path: cli.input,
        out_dir: cli.out_dir,
        force: cli.force,
        dialect,
        schema_only: cli.schema_only,
        data_only: cli.data_only,
        tables: cli.tables.map(|v| v.into_iter().collect::<HashSet<_>>()),
        workers: cli.workers.unwrap_or_else(libsqlconv::types::num_workers),
        shard_rows: cli.shard_rows,
        postgres_ddl: !cli.no_postgres_ddl,
        dry_run: cli.dry_run,
        null_marker: cli.null_marker,
        tsv: cli.tsv,
        delimiter,
    };

    match libsqlconv::run(&config) {
        Ok(_summary) => {
            process::exit(0);
        }
        Err(e) => {
            eprintln!("\x1b[1;31merror:\x1b[0m {e}");
            process::exit(1);
        }
    }
}
