use std::collections::HashSet;
use std::io::Read;

use crate::errors::ConvertError;
use crate::lexer::{LexerEvent, LexerState};
use crate::types::{SqlDialect, StatementEntry, StatementIndex, StatementKind};

const BUF_SIZE: usize = 256 * 1024; // 256 KB
const PREFIX_CAP: usize = 256;

/// Auto-detect the SQL dialect from the first few KB of the dump.
pub fn detect_dialect<R: Read>(reader: &mut R) -> Result<(SqlDialect, Vec<u8>), ConvertError> {
    let mut head = vec![0u8; 8 * 1024];
    let n = reader.read(&mut head)?;
    head.truncate(n);

    let text = String::from_utf8_lossy(&head);
    let upper = text.to_ascii_uppercase();

    let dialect = if upper.contains("/*!") || upper.contains("ENGINE=") || upper.contains("AUTO_INCREMENT") || (upper.contains("INSERT INTO `") || upper.contains("CREATE TABLE `")) {
        SqlDialect::Mysql
    } else if upper.contains("\\COPY") || upper.contains("COPY ") && upper.contains("FROM STDIN") || upper.contains("PG_DUMP") || upper.contains("POSTGRESQL") || upper.contains("SET CLIENT_ENCODING") {
        SqlDialect::Postgresql
    } else if upper.contains("[DBO]") || upper.contains("SET IDENTITY_INSERT") || upper.contains("NVARCHAR") || upper.contains("\nGO\n") || upper.contains("\nGO\r") || upper.contains("SET NOCOUNT") {
        SqlDialect::Mssql
    } else if upper.contains("SPOOL") || upper.contains("VARCHAR2") || upper.contains("NUMBER(") || upper.contains("REM ") || upper.contains("ORACLE") {
        SqlDialect::Oracle
    } else if upper.contains("BEGIN TRANSACTION") || upper.contains("SQLITE") || upper.contains("INTEGER PRIMARY KEY AUTOINCREMENT") {
        SqlDialect::Sqlite
    } else {
        // Default to MySQL as the most common dump format
        SqlDialect::Mysql
    };

    Ok((dialect, head))
}

/// Build an index of all SQL statements in the input stream.
/// This is Phase 1: a sequential scan that records byte offsets.
pub fn build_index<R: Read>(
    mut reader: R,
    dialect: SqlDialect,
) -> Result<StatementIndex, ConvertError> {
    let mut state = LexerState::Normal;
    let mut entries = Vec::new();
    let mut tables_seen = HashSet::new();

    let mut buf = vec![0u8; BUF_SIZE];
    let mut global_offset: u64 = 0;
    let mut line_number: u64 = 1;

    let mut stmt_start: u64 = 0;
    let mut stmt_start_line: u64 = 1;
    let mut prefix = Vec::with_capacity(PREFIX_CAP);
    let mut in_conditional = false;

    // For PostgreSQL COPY FROM stdin detection
    let mut in_copy_data = false;
    let mut copy_table_name: Option<String> = None;
    let mut line_buf = Vec::new();

    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }

        for i in 0..n {
            let b = buf[i];
            let cur_offset = global_offset + i as u64;

            if b == b'\n' {
                line_number += 1;
            }

            // PostgreSQL COPY data mode: read line-by-line until `\.`
            if in_copy_data {
                if b == b'\n' {
                    if line_buf == b"\\." {
                        // End of COPY data block
                        let byte_length = cur_offset - stmt_start + 1;
                        entries.push(StatementEntry {
                            kind: StatementKind::CopyData,
                            table_name: copy_table_name.take(),
                            byte_offset: stmt_start,
                            byte_length,
                            approx_line: stmt_start_line,
                        });
                        in_copy_data = false;
                        stmt_start = cur_offset + 1;
                        stmt_start_line = line_number;
                        prefix.clear();
                    }
                    line_buf.clear();
                } else {
                    line_buf.push(b);
                }
                continue;
            }

            // MySQL conditional comment detection
            if state == LexerState::BlockComment && !in_conditional && b == b'!'
                && dialect == SqlDialect::Mysql
            {
                state = LexerState::ConditionalComment;
                in_conditional = true;
                if prefix.len() < PREFIX_CAP {
                    prefix.push(b);
                }
                continue;
            }

            let (new_state, event) = state.feed(b);

            if new_state == LexerState::ConditionalComment
                || new_state == LexerState::ConditionalMaybeEnd
            {
                in_conditional = true;
            }
            if in_conditional && new_state == LexerState::Normal && state != LexerState::Normal {
                in_conditional = false;
            }

            state = new_state;

            if event == LexerEvent::Semicolon {
                let byte_length = cur_offset - stmt_start + 1;
                let kind = classify_statement(&prefix, dialect);
                let table_name = extract_table_name(&prefix, kind, dialect);

                if let Some(ref name) = table_name {
                    tables_seen.insert(name.clone());
                }

                // Check if this is a PostgreSQL COPY ... FROM stdin
                if dialect == SqlDialect::Postgresql && kind == StatementKind::CopyData {
                    in_copy_data = true;
                    copy_table_name = table_name;
                    line_buf.clear();
                    // Don't emit the statement yet — wait for `\.`
                    // Keep stmt_start pointing to the COPY header
                    prefix.clear();
                } else {
                    entries.push(StatementEntry {
                        kind,
                        table_name,
                        byte_offset: stmt_start,
                        byte_length,
                        approx_line: stmt_start_line,
                    });

                    stmt_start = cur_offset + 1;
                    stmt_start_line = line_number;
                    prefix.clear();
                }
            } else if prefix.len() < PREFIX_CAP {
                prefix.push(b);
            }
        }

        global_offset += n as u64;
    }

    // Handle SQL Server GO batch separators (post-processing)
    if dialect == SqlDialect::Mssql {
        // GO batches are handled as Other statements; no special handling needed
        // since MSSQL dumps typically use semicolons for INSERT statements.
    }

    Ok(StatementIndex {
        entries,
        tables_seen,
    })
}

/// Build an index with prepended data (from dialect detection).
pub fn build_index_with_head<R: Read>(
    head: &[u8],
    reader: R,
    dialect: SqlDialect,
) -> Result<StatementIndex, ConvertError> {
    let combined = std::io::Cursor::new(head.to_vec()).chain(reader);
    build_index(combined, dialect)
}

fn classify_statement(prefix: &[u8], dialect: SqlDialect) -> StatementKind {
    let trimmed = skip_whitespace(prefix);
    let upper: Vec<u8> = trimmed
        .iter()
        .take(30)
        .map(|b| b.to_ascii_uppercase())
        .collect();
    let s = String::from_utf8_lossy(&upper);

    if s.starts_with("CREATE TABLE") || s.starts_with("CREATE TEMPORARY TABLE") {
        StatementKind::CreateTable
    } else if s.starts_with("DROP TABLE") {
        StatementKind::DropTable
    } else if s.starts_with("INSERT INTO")
        || s.starts_with("INSERT IGNORE INTO")
        || s.starts_with("INSERT [")
    {
        StatementKind::InsertInto
    } else if s.starts_with("COPY ") && dialect == SqlDialect::Postgresql {
        // PostgreSQL COPY ... FROM stdin
        let full: String = prefix
            .iter()
            .take(200)
            .map(|&b| b.to_ascii_uppercase() as char)
            .collect();
        if full.contains("FROM STDIN") {
            StatementKind::CopyData
        } else {
            StatementKind::Other
        }
    } else if s.starts_with("SET ") || s.starts_with("SELECT PG_CATALOG") {
        StatementKind::SetVariable
    } else if s.starts_with("LOCK ") {
        StatementKind::LockTable
    } else if s.starts_with("UNLOCK ") {
        StatementKind::UnlockTables
    } else if s.starts_with("--") || s.starts_with("/*") || s.starts_with("REM ") {
        StatementKind::Comment
    } else {
        StatementKind::Other
    }
}

fn extract_table_name(
    prefix: &[u8],
    kind: StatementKind,
    dialect: SqlDialect,
) -> Option<String> {
    match kind {
        StatementKind::CreateTable => {
            extract_name_after_keywords(prefix, &[b"CREATE", b"TABLE"], dialect)
        }
        StatementKind::DropTable => {
            extract_name_after_keywords(prefix, &[b"DROP", b"TABLE"], dialect)
        }
        StatementKind::InsertInto => {
            extract_name_after_keywords(prefix, &[b"INSERT", b"INTO"], dialect)
        }
        StatementKind::CopyData => {
            // COPY table_name FROM stdin
            extract_name_after_keywords(prefix, &[b"COPY"], dialect)
        }
        _ => None,
    }
}

/// Skip keywords in order, then extract the next SQL identifier.
fn extract_name_after_keywords(
    prefix: &[u8],
    keywords: &[&[u8]],
    dialect: SqlDialect,
) -> Option<String> {
    let mut pos = 0;
    let trimmed = skip_whitespace(prefix);

    for kw in keywords {
        pos = skip_whitespace_offset(trimmed, pos);
        let upper: Vec<u8> = trimmed[pos..]
            .iter()
            .take(kw.len())
            .map(|b| b.to_ascii_uppercase())
            .collect();
        if upper.as_slice() != *kw {
            let mut found = false;
            for scan in pos..trimmed.len().saturating_sub(kw.len()) {
                if trimmed[scan].is_ascii_whitespace() {
                    let candidate: Vec<u8> = trimmed[scan..]
                        .iter()
                        .skip_while(|b| b.is_ascii_whitespace())
                        .take(kw.len())
                        .map(|b| b.to_ascii_uppercase())
                        .collect();
                    if candidate.as_slice() == *kw {
                        pos = skip_whitespace_offset(trimmed, scan);
                        break;
                    }
                }
                if scan > pos + 50 {
                    found = false;
                    break;
                }
                found = true;
            }
            if !found && pos >= trimmed.len() {
                return None;
            }
            let re_pos = skip_whitespace_offset(trimmed, pos);
            let re_upper: Vec<u8> = trimmed[re_pos..]
                .iter()
                .take(kw.len())
                .map(|b| b.to_ascii_uppercase())
                .collect();
            if re_upper.as_slice() == *kw {
                pos = re_pos + kw.len();
            } else {
                pos += kw.len();
            }
        } else {
            pos += kw.len();
        }
    }

    // Skip optional IF NOT EXISTS / IF EXISTS
    pos = skip_whitespace_offset(trimmed, pos);
    let rest: Vec<u8> = trimmed[pos..]
        .iter()
        .take(20)
        .map(|b| b.to_ascii_uppercase())
        .collect();
    let rest_str = String::from_utf8_lossy(&rest);
    if rest_str.starts_with("IF NOT EXISTS") {
        pos += 13;
        pos = skip_whitespace_offset(trimmed, pos);
    } else if rest_str.starts_with("IF EXISTS") {
        pos += 9;
        pos = skip_whitespace_offset(trimmed, pos);
    } else if rest_str.starts_with("IGNORE INTO") {
        pos += 11;
        pos = skip_whitespace_offset(trimmed, pos);
    }

    extract_identifier(trimmed, pos, dialect)
}

fn extract_identifier(data: &[u8], pos: usize, dialect: SqlDialect) -> Option<String> {
    if pos >= data.len() {
        return None;
    }

    match data[pos] {
        b'`' => {
            // Backtick-quoted (MySQL)
            let start = pos + 1;
            let end = data[start..].iter().position(|&b| b == b'`')?;
            Some(String::from_utf8_lossy(&data[start..start + end]).into_owned())
        }
        b'"' => {
            // Double-quoted (PostgreSQL/Oracle/SQLite/standard)
            let start = pos + 1;
            let end = data[start..].iter().position(|&b| b == b'"')?;
            Some(String::from_utf8_lossy(&data[start..start + end]).into_owned())
        }
        b'[' => {
            // Square-bracket quoted (SQL Server)
            let start = pos + 1;
            let end = data[start..].iter().position(|&b| b == b']')?;
            let name = String::from_utf8_lossy(&data[start..start + end]).into_owned();

            // Handle schema.table: [dbo].[table_name]
            let after = start + end + 1;
            if after < data.len() && data[after] == b'.' {
                let table_start = after + 1;
                if table_start < data.len() && data[table_start] == b'[' {
                    let inner = table_start + 1;
                    if let Some(inner_end) = data[inner..].iter().position(|&b| b == b']') {
                        return Some(
                            String::from_utf8_lossy(&data[inner..inner + inner_end]).into_owned(),
                        );
                    }
                }
            }
            Some(name)
        }
        _ if dialect == SqlDialect::Mssql || dialect == SqlDialect::Oracle => {
            // Schema-qualified bare names: schema.table
            let start = pos;
            let end = data[start..]
                .iter()
                .position(|&b| {
                    b.is_ascii_whitespace() || b == b'(' || b == b';' || b == b','
                })
                .unwrap_or(data.len() - start);
            if end == 0 {
                return None;
            }
            let full = String::from_utf8_lossy(&data[start..start + end]).into_owned();
            // Return only the table part after the last dot
            if let Some(dot_pos) = full.rfind('.') {
                Some(full[dot_pos + 1..].to_string())
            } else {
                Some(full)
            }
        }
        _ => {
            // Bare identifier
            let start = pos;
            let end = data[start..]
                .iter()
                .position(|&b| {
                    b.is_ascii_whitespace() || b == b'(' || b == b';' || b == b','
                })
                .unwrap_or(data.len() - start);
            if end == 0 {
                return None;
            }
            Some(String::from_utf8_lossy(&data[start..start + end]).into_owned())
        }
    }
}

fn skip_whitespace(data: &[u8]) -> &[u8] {
    let start = data
        .iter()
        .position(|b| !b.is_ascii_whitespace())
        .unwrap_or(data.len());
    &data[start..]
}

fn skip_whitespace_offset(data: &[u8], from: usize) -> usize {
    data[from..]
        .iter()
        .position(|b| !b.is_ascii_whitespace())
        .map(|p| from + p)
        .unwrap_or(data.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn index_of(sql: &[u8]) -> StatementIndex {
        build_index(std::io::Cursor::new(sql), SqlDialect::Mysql).unwrap()
    }

    fn index_of_dialect(sql: &[u8], dialect: SqlDialect) -> StatementIndex {
        build_index(std::io::Cursor::new(sql), dialect).unwrap()
    }

    // --- MySQL tests ---

    #[test]
    fn single_insert() {
        let idx = index_of(b"INSERT INTO `users` VALUES (1,'alice');");
        assert_eq!(idx.entries.len(), 1);
        assert_eq!(idx.entries[0].kind, StatementKind::InsertInto);
        assert_eq!(idx.entries[0].table_name.as_deref(), Some("users"));
    }

    #[test]
    fn create_and_insert() {
        let sql = b"CREATE TABLE `t` (id INT);\nINSERT INTO `t` VALUES (1);";
        let idx = index_of(sql);
        assert_eq!(idx.entries.len(), 2);
        assert_eq!(idx.entries[0].kind, StatementKind::CreateTable);
        assert_eq!(idx.entries[0].table_name.as_deref(), Some("t"));
        assert_eq!(idx.entries[1].kind, StatementKind::InsertInto);
        assert_eq!(idx.entries[1].table_name.as_deref(), Some("t"));
    }

    #[test]
    fn multiple_tables() {
        let sql = b"INSERT INTO `a` VALUES (1);\nINSERT INTO `b` VALUES (2);";
        let idx = index_of(sql);
        assert_eq!(idx.tables_seen.len(), 2);
        assert!(idx.tables_seen.contains("a"));
        assert!(idx.tables_seen.contains("b"));
    }

    #[test]
    fn drop_table_if_exists() {
        let idx = index_of(b"DROP TABLE IF EXISTS `users`;");
        assert_eq!(idx.entries[0].kind, StatementKind::DropTable);
        assert_eq!(idx.entries[0].table_name.as_deref(), Some("users"));
    }

    #[test]
    fn set_and_lock_statements() {
        let sql = b"SET NAMES utf8;\nLOCK TABLES `t` WRITE;\nUNLOCK TABLES;";
        let idx = index_of(sql);
        assert_eq!(idx.entries[0].kind, StatementKind::SetVariable);
        assert_eq!(idx.entries[1].kind, StatementKind::LockTable);
        assert_eq!(idx.entries[2].kind, StatementKind::UnlockTables);
    }

    #[test]
    fn semicolons_inside_strings_not_boundaries() {
        let sql = b"INSERT INTO `t` VALUES ('a;b;c');";
        let idx = index_of(sql);
        assert_eq!(idx.entries.len(), 1);
    }

    #[test]
    fn byte_offsets_correct() {
        let sql = b"SELECT 1;\nSELECT 2;";
        let idx = index_of(sql);
        assert_eq!(idx.entries.len(), 2);
        assert_eq!(idx.entries[0].byte_offset, 0);
        assert_eq!(idx.entries[0].byte_length, 9);
        assert_eq!(idx.entries[1].byte_offset, 9);
        assert_eq!(idx.entries[1].byte_length, 10);
    }

    // --- SQL Server tests ---

    #[test]
    fn mssql_square_bracket_insert() {
        let sql = b"INSERT INTO [dbo].[users] ([id],[name]) VALUES (1,N'alice');";
        let idx = index_of_dialect(sql, SqlDialect::Mssql);
        assert_eq!(idx.entries.len(), 1);
        assert_eq!(idx.entries[0].kind, StatementKind::InsertInto);
        assert_eq!(idx.entries[0].table_name.as_deref(), Some("users"));
    }

    #[test]
    fn mssql_create_table() {
        let sql = b"CREATE TABLE [dbo].[orders] ([id] INT NOT NULL);";
        let idx = index_of_dialect(sql, SqlDialect::Mssql);
        assert_eq!(idx.entries[0].kind, StatementKind::CreateTable);
        assert_eq!(idx.entries[0].table_name.as_deref(), Some("orders"));
    }

    // --- PostgreSQL tests ---

    #[test]
    fn pg_double_quoted_insert() {
        let sql = b"INSERT INTO \"users\" VALUES (1,'alice');";
        let idx = index_of_dialect(sql, SqlDialect::Postgresql);
        assert_eq!(idx.entries[0].kind, StatementKind::InsertInto);
        assert_eq!(idx.entries[0].table_name.as_deref(), Some("users"));
    }

    #[test]
    fn pg_copy_from_stdin() {
        let sql = b"COPY users FROM stdin;\n1\talice\n2\tbob\n\\.\n";
        let idx = index_of_dialect(sql, SqlDialect::Postgresql);
        assert_eq!(idx.entries.len(), 1);
        assert_eq!(idx.entries[0].kind, StatementKind::CopyData);
        assert_eq!(idx.entries[0].table_name.as_deref(), Some("users"));
    }

    // --- Oracle tests ---

    #[test]
    fn oracle_insert() {
        let sql = b"INSERT INTO \"USERS\" VALUES (1,'alice');";
        let idx = index_of_dialect(sql, SqlDialect::Oracle);
        assert_eq!(idx.entries[0].kind, StatementKind::InsertInto);
        assert_eq!(idx.entries[0].table_name.as_deref(), Some("USERS"));
    }

    // --- SQLite tests ---

    #[test]
    fn sqlite_insert() {
        let sql = b"INSERT INTO users VALUES (1,'alice');";
        let idx = index_of_dialect(sql, SqlDialect::Sqlite);
        assert_eq!(idx.entries[0].kind, StatementKind::InsertInto);
        assert_eq!(idx.entries[0].table_name.as_deref(), Some("users"));
    }

    // --- Auto-detection ---

    #[test]
    fn detect_mysql() {
        let mut data = &b"/*!40101 SET NAMES utf8 */;\nCREATE TABLE `t` (id INT);"[..];
        let (dialect, _) = detect_dialect(&mut data).unwrap();
        assert_eq!(dialect, SqlDialect::Mysql);
    }

    #[test]
    fn detect_mssql() {
        let mut data =
            &b"SET NOCOUNT ON;\nINSERT INTO [dbo].[users] VALUES (1);"[..];
        let (dialect, _) = detect_dialect(&mut data).unwrap();
        assert_eq!(dialect, SqlDialect::Mssql);
    }

    #[test]
    fn detect_postgresql() {
        let mut data = &b"SET client_encoding = 'UTF8';\nCOPY users FROM stdin;"[..];
        let (dialect, _) = detect_dialect(&mut data).unwrap();
        assert_eq!(dialect, SqlDialect::Postgresql);
    }

    #[test]
    fn detect_sqlite() {
        let mut data = &b"BEGIN TRANSACTION;\nCREATE TABLE users (id INTEGER PRIMARY KEY);"[..];
        let (dialect, _) = detect_dialect(&mut data).unwrap();
        assert_eq!(dialect, SqlDialect::Sqlite);
    }
}
