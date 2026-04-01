pub mod csv_emit;
pub mod ddl;
pub mod errors;
pub mod index;
pub mod input;
pub mod lexer;
pub mod manifest;
pub mod parallel;
pub mod types;
pub mod values_parser;

use errors::{ConvertError, ErrorLogger};
use types::{Config, SqlDialect, Summary};

/// Main entry point: run the full conversion pipeline.
pub fn run(config: &Config) -> Result<Summary, ConvertError> {
    // Refuse to overwrite a non-empty output directory unless --force
    if config.out_dir.exists() && !config.force {
        let has_contents = std::fs::read_dir(&config.out_dir)
            .map(|mut d| d.next().is_some())
            .unwrap_or(false);
        if has_contents {
            return Err(ConvertError::Parse(format!(
                "Output directory '{}' is not empty. Use --force to overwrite.",
                config.out_dir.display()
            )));
        }
    }

    // Create output directory
    std::fs::create_dir_all(&config.out_dir)?;

    // Set up error logger
    let error_logger = ErrorLogger::new(&config.out_dir)?;

    // Phase 0: Prepare input (decompress .gz if needed)
    eprintln!("Preparing input: {}", config.input_path.display());
    let prepared = input::PreparedInput::open(&config.input_path)?;

    // Detect or use specified dialect
    let mut reader = prepared.reader()?;
    let (dialect, head) = if let Some(d) = config.dialect {
        eprintln!("Using specified dialect: {d}");
        (d, Vec::new())
    } else {
        let (detected, head) = index::detect_dialect(&mut reader)?;
        eprintln!("Auto-detected dialect: {detected}");
        (detected, head)
    };

    // Phase 1: Build statement index
    eprintln!("Phase 1: Building statement index...");
    let idx = if head.is_empty() {
        index::build_index(reader, dialect)?
    } else {
        index::build_index_with_head(&head, reader, dialect)?
    };
    eprintln!(
        "  Found {} statements across {} tables",
        idx.entries.len(),
        idx.tables_seen.len()
    );
    for table in &idx.tables_seen {
        let data_count = idx
            .entries
            .iter()
            .filter(|e| e.table_name.as_deref() == Some(table))
            .filter(|e| {
                e.kind == types::StatementKind::InsertInto
                    || e.kind == types::StatementKind::CopyData
            })
            .count();
        let ddl_count = idx
            .entries
            .iter()
            .filter(|e| e.table_name.as_deref() == Some(table))
            .filter(|e| e.kind == types::StatementKind::CreateTable)
            .count();
        let kind = if dialect == SqlDialect::Postgresql {
            "data"
        } else {
            "INSERT"
        };
        eprintln!("  {table}: {ddl_count} DDL, {data_count} {kind} statements");
    }

    // Phase 2: Parallel processing
    eprintln!("Phase 2: Processing with {} workers...", config.workers);
    let summary = parallel::process(&idx, &prepared.path, config, dialect, &error_logger)?;

    // Write manifest
    manifest::write_manifest(&summary, &config.out_dir)?;

    // Write load script
    if !config.schema_only && !config.dry_run {
        manifest::write_load_script(&summary, &config.out_dir, config)?;
    }

    error_logger.flush();

    let messages = error_logger.messages();

    eprintln!("\nConversion complete:");
    eprintln!("  Dialect: {dialect}");
    eprintln!("  Tables: {}", summary.tables_processed);
    eprintln!("  Total rows: {}", summary.total_rows);
    if summary.errors > 0 {
        eprintln!("  Errors: {}", summary.errors);
    }
    if summary.warnings > 0 {
        eprintln!("  Warnings: {}", summary.warnings);
    }
    eprintln!("  Output: {}", config.out_dir.display());

    if !messages.is_empty() {
        eprintln!();
        for msg in &messages {
            eprintln!("  {msg}");
        }
        eprintln!();
        eprintln!("  Full log: {}", config.out_dir.join("errors.log").display());
    }

    Ok(summary)
}
