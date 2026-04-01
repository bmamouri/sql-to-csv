use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use flate2::read::GzDecoder;
use tempfile::NamedTempFile;

use crate::errors::ConvertError;

/// Prepared input file, ready for indexing and mmap.
/// For .gz files, this holds a reference to the decompressed temp file.
pub struct PreparedInput {
    /// Path to the (decompressed) file that can be mmap'd.
    pub path: PathBuf,
    /// If we decompressed, keep the temp file alive so it doesn't get deleted.
    _temp: Option<NamedTempFile>,
}

impl PreparedInput {
    /// Prepare the input for processing. If gzipped, decompress to a temp file.
    pub fn open(input_path: &Path) -> Result<Self, ConvertError> {
        // Validate file extension: must be .sql or .sql.gz
        let file_name = input_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("");
        if !file_name.ends_with(".sql") && !file_name.ends_with(".sql.gz") {
            return Err(ConvertError::Parse(format!(
                "Unsupported file extension: '{}'. Only .sql and .sql.gz files are accepted",
                input_path.display()
            )));
        }

        let is_gz = input_path
            .extension()
            .map(|e| e == "gz")
            .unwrap_or(false);

        if is_gz {
            eprintln!("Decompressing gzip input to temporary file...");
            let file = File::open(input_path)?;
            let reader = BufReader::with_capacity(256 * 1024, GzDecoder::new(file));
            let mut temp = NamedTempFile::new()?;
            {
                let mut writer = BufWriter::with_capacity(256 * 1024, &mut temp);
                copy_stream(reader, &mut writer)?;
                writer.flush()?;
            }
            let path = temp.path().to_path_buf();
            eprintln!("Decompression complete: {}", path.display());
            Ok(Self {
                path,
                _temp: Some(temp),
            })
        } else {
            Ok(Self {
                path: input_path.to_path_buf(),
                _temp: None,
            })
        }
    }

    /// Open a reader for the sequential Phase 1 scan.
    pub fn reader(&self) -> Result<BufReader<File>, io::Error> {
        let file = File::open(&self.path)?;
        Ok(BufReader::with_capacity(256 * 1024, file))
    }
}

fn copy_stream<R: Read, W: Write>(mut reader: R, writer: &mut W) -> Result<u64, io::Error> {
    let mut buf = vec![0u8; 256 * 1024];
    let mut total = 0u64;
    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        writer.write_all(&buf[..n])?;
        total += n as u64;
    }
    Ok(total)
}
