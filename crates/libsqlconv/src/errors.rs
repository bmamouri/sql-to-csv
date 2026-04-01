use std::fmt;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// Severity level for logged errors/warnings.
#[derive(Debug, Clone, Copy)]
pub enum Severity {
    Error,
    Warning,
}

impl fmt::Display for Severity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Severity::Error => write!(f, "ERROR"),
            Severity::Warning => write!(f, "WARN"),
        }
    }
}

/// Context attached to every logged error.
pub struct ErrorContext {
    pub severity: Severity,
    pub byte_offset: u64,
    pub approx_line: u64,
    pub table_name: Option<String>,
}

/// Thread-safe error logger that writes to errors.log and collects messages for display.
pub struct ErrorLogger {
    writer: Mutex<BufWriter<File>>,
    messages: Mutex<Vec<String>>,
    error_count: AtomicU64,
    warning_count: AtomicU64,
}

impl ErrorLogger {
    pub fn new(dir: &Path) -> std::io::Result<Self> {
        let path = dir.join("errors.log");
        let file = File::create(path)?;
        Ok(Self {
            writer: Mutex::new(BufWriter::new(file)),
            messages: Mutex::new(Vec::new()),
            error_count: AtomicU64::new(0),
            warning_count: AtomicU64::new(0),
        })
    }

    pub fn log(&self, ctx: &ErrorContext, message: &str) {
        let table = ctx.table_name.as_deref().unwrap_or("?");
        let line = format!(
            "[{}] offset={} line~{} table={}: {}",
            ctx.severity, ctx.byte_offset, ctx.approx_line, table, message
        );
        {
            let mut w = self.writer.lock().unwrap();
            let _ = writeln!(w, "{line}");
        }
        {
            let mut msgs = self.messages.lock().unwrap();
            msgs.push(line);
        }
        match ctx.severity {
            Severity::Error => self.error_count.fetch_add(1, Ordering::Relaxed),
            Severity::Warning => self.warning_count.fetch_add(1, Ordering::Relaxed),
        };
    }

    pub fn error_count(&self) -> u64 {
        self.error_count.load(Ordering::Relaxed)
    }

    pub fn warning_count(&self) -> u64 {
        self.warning_count.load(Ordering::Relaxed)
    }

    /// Return all logged messages.
    pub fn messages(&self) -> Vec<String> {
        self.messages.lock().unwrap().clone()
    }

    pub fn flush(&self) {
        let mut w = self.writer.lock().unwrap();
        let _ = w.flush();
    }
}

/// Top-level error type for the library.
#[derive(Debug)]
pub enum ConvertError {
    Io(std::io::Error),
    Parse(String),
    Csv(csv::Error),
}

impl fmt::Display for ConvertError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConvertError::Io(e) => write!(f, "{e}"),
            ConvertError::Parse(msg) => write!(f, "{msg}"),
            ConvertError::Csv(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for ConvertError {}

impl From<std::io::Error> for ConvertError {
    fn from(e: std::io::Error) -> Self {
        ConvertError::Io(e)
    }
}

impl From<csv::Error> for ConvertError {
    fn from(e: csv::Error) -> Self {
        ConvertError::Csv(e)
    }
}
