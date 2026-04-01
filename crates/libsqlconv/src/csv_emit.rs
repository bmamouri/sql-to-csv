use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

use crate::errors::ConvertError;
use crate::types::SqlValue;

/// Data file writer supporting both CSV and TSV output.
pub struct CsvWriter {
    mode: OutputMode,
    row_count: u64,
}

enum OutputMode {
    Csv {
        inner: csv::Writer<BufWriter<File>>,
        null_marker: String,
    },
    Tsv {
        inner: BufWriter<File>,
        null_marker: String,
    },
}

impl CsvWriter {
    pub fn new(
        path: &Path,
        delimiter: u8,
        null_marker: &str,
        tsv: bool,
    ) -> Result<Self, ConvertError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = File::create(path)?;
        let mode = if tsv {
            OutputMode::Tsv {
                inner: BufWriter::with_capacity(256 * 1024, file),
                null_marker: null_marker.to_string(),
            }
        } else {
            let writer = csv::WriterBuilder::new()
                .delimiter(delimiter)
                .quote_style(csv::QuoteStyle::Necessary)
                .double_quote(true)
                .has_headers(false)
                .from_writer(BufWriter::with_capacity(256 * 1024, file));
            OutputMode::Csv {
                inner: writer,
                null_marker: null_marker.to_string(),
            }
        };
        Ok(Self { mode, row_count: 0 })
    }

    pub fn write_row(&mut self, values: &[SqlValue]) -> Result<(), ConvertError> {
        match &mut self.mode {
            OutputMode::Csv { inner, null_marker } => {
                let fields: Vec<String> = values
                    .iter()
                    .map(|v| format_csv_value(v, null_marker))
                    .collect();
                inner.write_record(&fields)?;
            }
            OutputMode::Tsv { inner, null_marker } => {
                for (i, v) in values.iter().enumerate() {
                    if i > 0 {
                        inner.write_all(b"\t")?;
                    }
                    let field = format_tsv_value(v, null_marker);
                    inner.write_all(field.as_bytes())?;
                }
                inner.write_all(b"\n")?;
            }
        }
        self.row_count += 1;
        Ok(())
    }

    pub fn row_count(&self) -> u64 {
        self.row_count
    }

    pub fn flush(&mut self) -> Result<(), ConvertError> {
        match &mut self.mode {
            OutputMode::Csv { inner, .. } => inner.flush()?,
            OutputMode::Tsv { inner, .. } => inner.flush()?,
        }
        Ok(())
    }
}

fn format_csv_value(v: &SqlValue, null_marker: &str) -> String {
    match v {
        SqlValue::Null => null_marker.to_string(),
        SqlValue::Integer(n) => n.to_string(),
        SqlValue::Float(f) => format_float(*f),
        SqlValue::String(s) => s.clone(),
        SqlValue::HexString(bytes) => format!("\\x{}", hex_encode(bytes)),
        SqlValue::BitLiteral(n) => n.to_string(),
    }
}

/// Format a value for PostgreSQL COPY TEXT format.
/// Escapes tab, newline, carriage return, and backslash.
fn format_tsv_value(v: &SqlValue, null_marker: &str) -> String {
    match v {
        SqlValue::Null => null_marker.to_string(),
        SqlValue::Integer(n) => n.to_string(),
        SqlValue::Float(f) => format_float(*f),
        SqlValue::String(s) => escape_tsv(s),
        SqlValue::HexString(bytes) => format!("\\\\x{}", hex_encode(bytes)),
        SqlValue::BitLiteral(n) => n.to_string(),
    }
}

/// Escape a string for PostgreSQL COPY TEXT format.
fn escape_tsv(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            _ => out.push(c),
        }
    }
    out
}

fn format_float(f: f64) -> String {
    if f.fract() == 0.0 && f.abs() < 1e15 {
        format!("{f:.1}")
    } else {
        format!("{f}")
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    fn write_and_read_csv(values: &[SqlValue]) -> String {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.csv");
        let mut writer = CsvWriter::new(&path, b',', "\\N", false).unwrap();
        writer.write_row(values).unwrap();
        writer.flush().unwrap();
        let mut content = Vec::new();
        std::fs::File::open(&path)
            .unwrap()
            .read_to_end(&mut content)
            .unwrap();
        String::from_utf8(content).expect("CSV output is not valid UTF-8")
    }

    fn write_and_read_tsv(values: &[SqlValue]) -> String {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.tsv");
        let mut writer = CsvWriter::new(&path, b'\t', "\\N", true).unwrap();
        writer.write_row(values).unwrap();
        writer.flush().unwrap();
        let mut content = Vec::new();
        std::fs::File::open(&path)
            .unwrap()
            .read_to_end(&mut content)
            .unwrap();
        String::from_utf8(content).expect("TSV output is not valid UTF-8")
    }

    // --- CSV tests ---

    #[test]
    fn csv_preserves_ascii() {
        let result = write_and_read_csv(&[
            SqlValue::Integer(1),
            SqlValue::String("hello".to_string()),
        ]);
        assert_eq!(result.trim(), "1,hello");
    }

    #[test]
    fn csv_preserves_japanese() {
        let result = write_and_read_csv(&[SqlValue::String("日本語".to_string())]);
        assert_eq!(result.trim(), "日本語");
    }

    #[test]
    fn csv_preserves_arabic() {
        let result = write_and_read_csv(&[SqlValue::String("سیارک ۹۸۸۲۵".to_string())]);
        assert_eq!(result.trim(), "سیارک ۹۸۸۲۵");
    }

    #[test]
    fn csv_preserves_emoji() {
        let result = write_and_read_csv(&[SqlValue::String("🎉🚀".to_string())]);
        assert_eq!(result.trim(), "🎉🚀");
    }

    #[test]
    fn csv_preserves_mixed_unicode_row() {
        let result = write_and_read_csv(&[
            SqlValue::Integer(488853314),
            SqlValue::Integer(2712495),
            SqlValue::String("fawiki".to_string()),
            SqlValue::String("سیارک ۹۸۸۲۵".to_string()),
        ]);
        assert_eq!(result.trim(), "488853314,2712495,fawiki,سیارک ۹۸۸۲۵");
    }

    #[test]
    fn csv_unicode_bytes_on_disk() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.csv");
        let text = "سیارک";
        let mut writer = CsvWriter::new(&path, b',', "\\N", false).unwrap();
        writer
            .write_row(&[SqlValue::String(text.to_string())])
            .unwrap();
        writer.flush().unwrap();

        let raw_bytes = std::fs::read(&path).unwrap();
        let expected_bytes = text.as_bytes();
        assert!(
            raw_bytes.starts_with(expected_bytes),
            "Bytes on disk don't match UTF-8. Expected {:?}, got {:?}",
            expected_bytes,
            &raw_bytes[..expected_bytes.len().min(raw_bytes.len())]
        );
    }

    // --- TSV tests ---

    #[test]
    fn tsv_basic() {
        let result = write_and_read_tsv(&[
            SqlValue::Integer(1),
            SqlValue::String("hello".to_string()),
            SqlValue::Null,
        ]);
        assert_eq!(result, "1\thello\t\\N\n");
    }

    #[test]
    fn tsv_escapes_tab_in_value() {
        let result = write_and_read_tsv(&[SqlValue::String("a\tb".to_string())]);
        assert_eq!(result, "a\\tb\n");
    }

    #[test]
    fn tsv_escapes_newline_in_value() {
        let result = write_and_read_tsv(&[SqlValue::String("line1\nline2".to_string())]);
        assert_eq!(result, "line1\\nline2\n");
    }

    #[test]
    fn tsv_escapes_backslash() {
        let result = write_and_read_tsv(&[SqlValue::String("back\\slash".to_string())]);
        assert_eq!(result, "back\\\\slash\n");
    }

    #[test]
    fn tsv_preserves_unicode() {
        let result = write_and_read_tsv(&[
            SqlValue::Integer(1),
            SqlValue::String("سیارک ۹۸۸۲۵".to_string()),
        ]);
        assert_eq!(result, "1\tسیارک ۹۸۸۲۵\n");
    }

    #[test]
    fn tsv_float_and_null() {
        let result = write_and_read_tsv(&[
            SqlValue::Float(3.14),
            SqlValue::Null,
            SqlValue::Integer(-42),
        ]);
        assert_eq!(result, "3.14\t\\N\t-42\n");
    }
}
