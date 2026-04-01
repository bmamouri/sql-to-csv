use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use memmap2::Mmap;
use rayon::prelude::*;

use crate::csv_emit::CsvWriter;
use crate::ddl;
use crate::errors::{ConvertError, ErrorContext, ErrorLogger, Severity};
use crate::types::{Config, SqlDialect, StatementEntry, StatementIndex, StatementKind, Summary, TableSummary};
use crate::values_parser;

/// Run Phase 2: parallel processing of indexed statements.
pub fn process(
    index: &StatementIndex,
    file_path: &Path,
    config: &Config,
    dialect: SqlDialect,
    error_logger: &ErrorLogger,
) -> Result<Summary, ConvertError> {
    let file = std::fs::File::open(file_path)?;
    let mmap = unsafe { Mmap::map(&file)? };

    // Configure rayon thread pool
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(config.workers)
        .build()
        .map_err(|e| ConvertError::Parse(format!("Failed to build thread pool: {e}")))?;

    let data_dir = config.out_dir.join("data");
    std::fs::create_dir_all(&data_dir)?;

    // --- DDL Processing (sequential) ---
    if !config.data_only && config.postgres_ddl {
        process_ddl(&index.entries, &mmap, config, dialect, error_logger)?;
    }

    // --- Data Processing (parallel) ---
    if config.schema_only {
        return Ok(Summary {
            tables_processed: index.tables_seen.len(),
            total_rows: 0,
            per_table: HashMap::new(),
            errors: error_logger.error_count(),
            warnings: error_logger.warning_count(),
        });
    }

    let insert_entries: Vec<&StatementEntry> = index
        .entries
        .iter()
        .filter(|e| e.kind == StatementKind::InsertInto || e.kind == StatementKind::CopyData)
        .filter(|e| {
            if let (Some(ref filter), Some(ref name)) = (&config.tables, &e.table_name) {
                filter.contains(name)
            } else {
                config.tables.is_none()
            }
        })
        .collect();

    if config.dry_run {
        let mut per_table: HashMap<String, u64> = HashMap::new();
        for entry in &insert_entries {
            if let Some(ref name) = entry.table_name {
                *per_table.entry(name.clone()).or_default() += 1;
            }
        }
        eprintln!("Dry run: {} INSERT statements across {} tables",
            insert_entries.len(), per_table.len());
        for (table, count) in &per_table {
            eprintln!("  {table}: {count} INSERT statements");
        }
        return Ok(Summary {
            tables_processed: per_table.len(),
            total_rows: 0,
            per_table: per_table
                .into_iter()
                .map(|(name, _)| {
                    (
                        name.clone(),
                        TableSummary {
                            row_count: 0,
                            csv_path: data_dir.join(format!("{name}.{}", config.data_extension())),
                        },
                    )
                })
                .collect(),
            errors: error_logger.error_count(),
            warnings: error_logger.warning_count(),
        });
    }

    // Create per-table CSV writers
    let writers: HashMap<String, Arc<Mutex<CsvWriter>>> = index
        .tables_seen
        .iter()
        .filter(|name| {
            config
                .tables
                .as_ref()
                .map(|f| f.contains(*name))
                .unwrap_or(true)
        })
        .map(|name| {
            let path = data_dir.join(format!("{name}.{}", config.data_extension()));
            let writer = CsvWriter::new(&path, config.effective_delimiter(), &config.null_marker, config.tsv)
                .expect("Failed to create CSV writer");
            (name.clone(), Arc::new(Mutex::new(writer)))
        })
        .collect();

    let writers_ref = &writers;
    let mmap_ref = &mmap;
    let error_logger_ref = error_logger;

    // Progress bar
    let progress = indicatif::ProgressBar::new(insert_entries.len() as u64);
    progress.set_style(
        indicatif::ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} INSERT statements ({per_sec})")
            .unwrap()
            .progress_chars("##-"),
    );

    // Process INSERT statements in parallel
    pool.install(|| {
        insert_entries.par_iter().for_each(|entry| {
            let table = match &entry.table_name {
                Some(name) => name,
                None => return,
            };

            let start = entry.byte_offset as usize;
            let end = start + entry.byte_length as usize;
            if end > mmap_ref.len() {
                error_logger_ref.log(
                    &ErrorContext {
                        severity: Severity::Error,
                        byte_offset: entry.byte_offset,
                        approx_line: entry.approx_line,
                        table_name: Some(table.clone()),
                    },
                    "Statement extends beyond file bounds",
                );
                progress.inc(1);
                return;
            }

            let raw = &mmap_ref[start..end];

            let parse_result = if entry.kind == StatementKind::CopyData {
                values_parser::parse_copy_data(raw)
            } else {
                values_parser::parse_insert_values(raw)
            };

            match parse_result {
                Ok(rows) => {
                    if let Some(writer) = writers_ref.get(table) {
                        let mut w = writer.lock().unwrap();
                        for row in &rows {
                            if let Err(e) = w.write_row(row) {
                                error_logger_ref.log(
                                    &ErrorContext {
                                        severity: Severity::Error,
                                        byte_offset: entry.byte_offset,
                                        approx_line: entry.approx_line,
                                        table_name: Some(table.clone()),
                                    },
                                    &format!("CSV write error: {e}"),
                                );
                            }
                        }
                    }
                }
                Err(e) => {
                    error_logger_ref.log(
                        &ErrorContext {
                            severity: Severity::Error,
                            byte_offset: entry.byte_offset,
                            approx_line: entry.approx_line,
                            table_name: Some(table.clone()),
                        },
                        &format!("VALUES parse error: {e}"),
                    );
                }
            }

            progress.inc(1);
        });
    });

    progress.finish_with_message("Done");

    // Flush and collect summary
    let mut per_table = HashMap::new();
    for (name, writer) in &writers {
        let mut w = writer.lock().unwrap();
        w.flush()?;
        per_table.insert(
            name.clone(),
            TableSummary {
                row_count: w.row_count(),
                csv_path: data_dir.join(format!("{name}.{}", config.data_extension())),
            },
        );
    }

    let total_rows: u64 = per_table.values().map(|s| s.row_count).sum();

    Ok(Summary {
        tables_processed: per_table.len(),
        total_rows,
        per_table,
        errors: error_logger.error_count(),
        warnings: error_logger.warning_count(),
    })
}

fn process_ddl(
    entries: &[StatementEntry],
    mmap: &Mmap,
    config: &Config,
    dialect: SqlDialect,
    error_logger: &ErrorLogger,
) -> Result<(), ConvertError> {
    let schema_path = config.out_dir.join("schema.sql");
    let mut schema_file = std::fs::File::create(&schema_path)?;

    use std::io::Write;
    writeln!(schema_file, "-- Auto-generated PostgreSQL schema")?;
    writeln!(schema_file, "-- Converted by sql-to-csv")?;
    writeln!(schema_file)?;

    for entry in entries {
        let should_process = match entry.kind {
            StatementKind::CreateTable | StatementKind::DropTable => true,
            _ => false,
        };
        if !should_process {
            continue;
        }

        // Check table filter
        if let (Some(ref filter), Some(ref name)) = (&config.tables, &entry.table_name) {
            if !filter.contains(name) {
                continue;
            }
        }

        let start = entry.byte_offset as usize;
        let end = start + entry.byte_length as usize;
        if end > mmap.len() {
            continue;
        }
        let raw = &mmap[start..end];

        match entry.kind {
            StatementKind::CreateTable => {
                match ddl::convert_create_table(raw, dialect) {
                    Ok((pg_ddl, warnings)) => {
                        write!(schema_file, "{pg_ddl}\n")?;
                        for w in &warnings {
                            error_logger.log(
                                &ErrorContext {
                                    severity: Severity::Warning,
                                    byte_offset: entry.byte_offset,
                                    approx_line: entry.approx_line,
                                    table_name: entry.table_name.clone(),
                                },
                                w,
                            );
                        }
                    }
                    Err(e) => {
                        error_logger.log(
                            &ErrorContext {
                                severity: Severity::Error,
                                byte_offset: entry.byte_offset,
                                approx_line: entry.approx_line,
                                table_name: entry.table_name.clone(),
                            },
                            &format!("DDL conversion error: {e}"),
                        );
                    }
                }
            }
            StatementKind::DropTable => {
                let pg_drop = ddl::convert_drop_table(raw, dialect);
                writeln!(schema_file, "{pg_drop}")?;
            }
            _ => {}
        }
    }

    Ok(())
}
