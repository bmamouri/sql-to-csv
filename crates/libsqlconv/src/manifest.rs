use std::io::Write;
use std::path::Path;

use crate::errors::ConvertError;
use crate::types::{Config, Summary};

/// Write manifest.json summarizing the conversion results.
pub fn write_manifest(summary: &Summary, out_dir: &Path) -> Result<(), ConvertError> {
    let path = out_dir.join("manifest.json");
    let json = serde_json::to_string_pretty(summary)
        .map_err(|e| ConvertError::Parse(format!("JSON serialization failed: {e}")))?;
    let mut file = std::fs::File::create(path)?;
    file.write_all(json.as_bytes())?;
    Ok(())
}

/// Write a load.sql helper script for importing data into PostgreSQL.
pub fn write_load_script(
    summary: &Summary,
    out_dir: &Path,
    config: &Config,
) -> Result<(), ConvertError> {
    let path = out_dir.join("load.sql");
    let mut file = std::fs::File::create(path)?;

    writeln!(file, "-- Auto-generated PostgreSQL load script")?;
    writeln!(file, "-- Run: psql -d your_db -f load.sql")?;
    writeln!(file)?;
    writeln!(file, "\\i schema.sql")?;
    writeln!(file)?;

    let mut tables: Vec<_> = summary.per_table.iter().collect();
    tables.sort_by_key(|(name, _)| (*name).clone());

    let format = if config.tsv { "TEXT" } else { "CSV" };
    let delimiter_clause = if config.tsv {
        String::new()
    } else if config.delimiter != b',' {
        format!(", DELIMITER '{}'", config.delimiter as char)
    } else {
        String::new()
    };

    for (table, info) in &tables {
        let data_path = info.csv_path.display();
        writeln!(
            file,
            "\\COPY \"{}\" FROM '{}' WITH (FORMAT {}{}, NULL '{}');",
            table, data_path, format, delimiter_clause, config.null_marker
        )?;
    }

    writeln!(file)?;
    writeln!(file, "-- Summary:")?;
    for (table, info) in &tables {
        writeln!(file, "--   {}: {} rows", table, info.row_count)?;
    }

    Ok(())
}
