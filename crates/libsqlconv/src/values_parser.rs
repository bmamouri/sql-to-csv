use crate::types::SqlValue;

/// Parse the VALUES portion of an INSERT statement.
/// Input: the full INSERT statement bytes, e.g.
///   `INSERT INTO `table` VALUES (1,'hello',NULL),(2,'world',3.14);`
/// Returns: Vec of rows, where each row is Vec<SqlValue>.
pub fn parse_insert_values(stmt: &[u8]) -> Result<Vec<Vec<SqlValue>>, String> {
    // Find "VALUES" keyword (case-insensitive)
    let values_pos = find_values_keyword(stmt).ok_or("No VALUES keyword found")?;
    let data = &stmt[values_pos..];

    let mut rows = Vec::new();
    let mut pos = 0;

    loop {
        // Skip whitespace/commas between tuples
        pos = skip_ws(data, pos);
        if pos >= data.len() {
            break;
        }

        if data[pos] == b';' {
            break;
        }

        if data[pos] == b',' {
            pos += 1;
            continue;
        }

        if data[pos] == b'(' {
            let (row, end) = parse_tuple(data, pos)?;
            rows.push(row);
            pos = end;
        } else {
            break;
        }
    }

    Ok(rows)
}

fn find_values_keyword(stmt: &[u8]) -> Option<usize> {
    // Scan for "VALUES" outside of strings
    let target = b"VALUES";
    let mut i = 0;
    while i + 6 <= stmt.len() {
        let b = stmt[i];
        if b == b'\'' {
            // Skip string
            i += 1;
            while i < stmt.len() {
                if stmt[i] == b'\\' {
                    i += 2;
                } else if stmt[i] == b'\'' {
                    i += 1;
                    if i < stmt.len() && stmt[i] == b'\'' {
                        i += 1; // '' escape
                    } else {
                        break;
                    }
                } else {
                    i += 1;
                }
            }
            continue;
        }
        if b == b'`' {
            i += 1;
            while i < stmt.len() && stmt[i] != b'`' {
                i += 1;
            }
            if i < stmt.len() {
                i += 1;
            }
            continue;
        }
        let chunk: Vec<u8> = stmt[i..i + 6].iter().map(|b| b.to_ascii_uppercase()).collect();
        if chunk == target {
            // Make sure it's a word boundary
            let before_ok =
                i == 0 || !stmt[i - 1].is_ascii_alphanumeric();
            let after_ok =
                i + 6 >= stmt.len() || !stmt[i + 6].is_ascii_alphanumeric();
            if before_ok && after_ok {
                return Some(i + 6);
            }
        }
        i += 1;
    }
    None
}

/// Parse one tuple starting at `(`, return (values, position after closing `)`).
fn parse_tuple(data: &[u8], start: usize) -> Result<(Vec<SqlValue>, usize), String> {
    if data[start] != b'(' {
        return Err(format!("Expected '(' at offset {start}"));
    }

    let mut values = Vec::new();
    let mut pos = start + 1;

    loop {
        pos = skip_ws(data, pos);
        if pos >= data.len() {
            return Err("Unexpected end of data in tuple".to_string());
        }

        if data[pos] == b')' {
            pos += 1;
            break;
        }

        if !values.is_empty() {
            if data[pos] != b',' {
                return Err(format!(
                    "Expected ',' between values at offset {pos}, got {:?}",
                    data[pos] as char
                ));
            }
            pos += 1;
            pos = skip_ws(data, pos);
        }

        let (val, end) = parse_value(data, pos)?;
        values.push(val);
        pos = end;
    }

    Ok((values, pos))
}

/// Parse a single SQL value starting at `pos`.
fn parse_value(data: &[u8], pos: usize) -> Result<(SqlValue, usize), String> {
    if pos >= data.len() {
        return Err("Unexpected end of data".to_string());
    }

    let b = data[pos];

    // NULL
    if b == b'N' || b == b'n' {
        if pos + 4 <= data.len() {
            let word: Vec<u8> = data[pos..pos + 4]
                .iter()
                .map(|b| b.to_ascii_uppercase())
                .collect();
            if word == b"NULL" {
                // Make sure it's not N'...' (MSSQL unicode prefix)
                let after = pos + 4;
                if after >= data.len() || data[after] != b'\'' {
                    return Ok((SqlValue::Null, pos + 4));
                }
            }
        }
        // N'...' — SQL Server unicode string prefix; skip the N, parse the string
        if pos + 1 < data.len() && data[pos + 1] == b'\'' {
            return parse_string_value(data, pos + 1);
        }
    }

    // String literal
    if b == b'\'' {
        return parse_string_value(data, pos);
    }

    // Hex literal: 0x...
    if b == b'0' && pos + 1 < data.len() && (data[pos + 1] == b'x' || data[pos + 1] == b'X') {
        return parse_hex_value(data, pos);
    }

    // Bit literal: b'...'
    if (b == b'b' || b == b'B') && pos + 1 < data.len() && data[pos + 1] == b'\'' {
        return parse_bit_value(data, pos);
    }

    // Numeric literal (integer or float)
    parse_numeric_value(data, pos)
}

fn parse_string_value(data: &[u8], start: usize) -> Result<(SqlValue, usize), String> {
    let mut buf: Vec<u8> = Vec::new();
    let mut pos = start + 1; // skip opening '

    while pos < data.len() {
        let b = data[pos];
        match b {
            b'\\' => {
                pos += 1;
                if pos >= data.len() {
                    return Err("Unexpected end in string escape".to_string());
                }
                let escaped = data[pos];
                match escaped {
                    b'n' => buf.push(b'\n'),
                    b'r' => buf.push(b'\r'),
                    b't' => buf.push(b'\t'),
                    b'0' => buf.push(0),
                    b'\\' => buf.push(b'\\'),
                    b'\'' => buf.push(b'\''),
                    b'"' => buf.push(b'"'),
                    b'Z' => buf.push(0x1A), // Ctrl-Z
                    _ => {
                        // Unknown escape — just keep the byte
                        buf.push(escaped);
                    }
                }
                pos += 1;
            }
            b'\'' => {
                // Check for '' escape
                if pos + 1 < data.len() && data[pos + 1] == b'\'' {
                    buf.push(b'\'');
                    pos += 2;
                } else {
                    pos += 1; // closing quote
                    break;
                }
            }
            _ => {
                buf.push(b);
                pos += 1;
            }
        }
    }

    let result = String::from_utf8(buf)
        .unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned());
    Ok((SqlValue::String(result), pos))
}

fn parse_hex_value(data: &[u8], start: usize) -> Result<(SqlValue, usize), String> {
    let mut pos = start + 2; // skip "0x"
    let hex_start = pos;
    while pos < data.len() && data[pos].is_ascii_hexdigit() {
        pos += 1;
    }
    let hex_str = std::str::from_utf8(&data[hex_start..pos]).map_err(|e| e.to_string())?;
    let bytes = hex_decode(hex_str)?;
    Ok((SqlValue::HexString(bytes), pos))
}

fn hex_decode(s: &str) -> Result<Vec<u8>, String> {
    if s.len() % 2 != 0 {
        // Pad with leading zero
        let padded = format!("0{s}");
        return hex_decode(&padded);
    }
    let mut bytes = Vec::with_capacity(s.len() / 2);
    let chars: Vec<u8> = s.bytes().collect();
    for chunk in chars.chunks(2) {
        let hi = hex_nibble(chunk[0])?;
        let lo = hex_nibble(chunk[1])?;
        bytes.push((hi << 4) | lo);
    }
    Ok(bytes)
}

fn hex_nibble(b: u8) -> Result<u8, String> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        _ => Err(format!("Invalid hex char: {}", b as char)),
    }
}

fn parse_bit_value(data: &[u8], start: usize) -> Result<(SqlValue, usize), String> {
    // b'010101'
    let mut pos = start + 2; // skip b'
    let mut val: u64 = 0;
    while pos < data.len() && data[pos] != b'\'' {
        val = val * 2 + (data[pos] - b'0') as u64;
        pos += 1;
    }
    if pos < data.len() {
        pos += 1; // skip closing '
    }
    Ok((SqlValue::BitLiteral(val), pos))
}

fn parse_numeric_value(data: &[u8], start: usize) -> Result<(SqlValue, usize), String> {
    let mut pos = start;
    let mut is_float = false;
    let mut is_negative = false;

    if pos < data.len() && data[pos] == b'-' {
        is_negative = true;
        pos += 1;
    } else if pos < data.len() && data[pos] == b'+' {
        pos += 1;
    }

    let digit_start = pos;
    while pos < data.len() && data[pos].is_ascii_digit() {
        pos += 1;
    }

    if pos < data.len() && data[pos] == b'.' {
        is_float = true;
        pos += 1;
        while pos < data.len() && data[pos].is_ascii_digit() {
            pos += 1;
        }
    }

    // Scientific notation
    if pos < data.len() && (data[pos] == b'e' || data[pos] == b'E') {
        is_float = true;
        pos += 1;
        if pos < data.len() && (data[pos] == b'+' || data[pos] == b'-') {
            pos += 1;
        }
        while pos < data.len() && data[pos].is_ascii_digit() {
            pos += 1;
        }
    }

    if pos == digit_start && !is_negative {
        return Err(format!(
            "Expected numeric value at offset {start}, got {:?}",
            data.get(start).map(|&b| b as char)
        ));
    }

    let text = std::str::from_utf8(&data[start..pos]).map_err(|e| e.to_string())?;

    if is_float {
        let f: f64 = text.parse().map_err(|e: std::num::ParseFloatError| e.to_string())?;
        Ok((SqlValue::Float(f), pos))
    } else {
        let n: i64 = text.parse().map_err(|e: std::num::ParseIntError| e.to_string())?;
        Ok((SqlValue::Integer(n), pos))
    }
}

fn skip_ws(data: &[u8], mut pos: usize) -> usize {
    while pos < data.len() && data[pos].is_ascii_whitespace() {
        pos += 1;
    }
    pos
}

/// Parse PostgreSQL COPY data block.
/// Input: the full COPY statement + data + `\.` terminator.
/// Format: `COPY table FROM stdin;\nval1\tval2\n...\n\.\n`
/// Returns: Vec of rows with tab-separated values.
pub fn parse_copy_data(stmt: &[u8]) -> Result<Vec<Vec<SqlValue>>, String> {
    // Find the first newline after the COPY header
    let header_end = stmt
        .iter()
        .position(|&b| b == b'\n')
        .ok_or("No newline after COPY header")?;
    let data = &stmt[header_end + 1..];

    let mut rows = Vec::new();

    for line in data.split(|&b| b == b'\n') {
        // Skip empty lines and the `\.` terminator
        if line.is_empty() || line == b"\\." {
            continue;
        }

        let mut row = Vec::new();
        for field in line.split(|&b| b == b'\t') {
            let value = if field == b"\\N" {
                SqlValue::Null
            } else {
                // Unescape PostgreSQL COPY escapes
                let unescaped = unescape_pg_copy(field);
                let s = String::from_utf8(unescaped)
                    .unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned());
                // Try to parse as integer or float
                if let Ok(n) = s.parse::<i64>() {
                    SqlValue::Integer(n)
                } else if let Ok(f) = s.parse::<f64>() {
                    SqlValue::Float(f)
                } else {
                    SqlValue::String(s)
                }
            };
            row.push(value);
        }
        rows.push(row);
    }

    Ok(rows)
}

/// Unescape PostgreSQL COPY text-format escapes.
fn unescape_pg_copy(field: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(field.len());
    let mut i = 0;
    while i < field.len() {
        if field[i] == b'\\' && i + 1 < field.len() {
            match field[i + 1] {
                b'n' => buf.push(b'\n'),
                b'r' => buf.push(b'\r'),
                b't' => buf.push(b'\t'),
                b'\\' => buf.push(b'\\'),
                _ => {
                    buf.push(field[i]);
                    buf.push(field[i + 1]);
                }
            }
            i += 2;
        } else {
            buf.push(field[i]);
            i += 1;
        }
    }
    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_insert() {
        let stmt = b"INSERT INTO `t` VALUES (1,'hello',NULL);";
        let rows = parse_insert_values(stmt).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], SqlValue::Integer(1));
        assert_eq!(rows[0][1], SqlValue::String("hello".to_string()));
        assert_eq!(rows[0][2], SqlValue::Null);
    }

    #[test]
    fn multiple_tuples() {
        let stmt = b"INSERT INTO `t` VALUES (1,'a'),(2,'b'),(3,'c');";
        let rows = parse_insert_values(stmt).unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[2][0], SqlValue::Integer(3));
        assert_eq!(rows[2][1], SqlValue::String("c".to_string()));
    }

    #[test]
    fn escaped_quotes() {
        let stmt = b"INSERT INTO `t` VALUES ('it''s'),('he\\'s');";
        let rows = parse_insert_values(stmt).unwrap();
        assert_eq!(rows[0][0], SqlValue::String("it's".to_string()));
        assert_eq!(rows[1][0], SqlValue::String("he's".to_string()));
    }

    #[test]
    fn backslash_escapes() {
        let stmt = b"INSERT INTO `t` VALUES ('line1\\nline2','tab\\there');";
        let rows = parse_insert_values(stmt).unwrap();
        assert_eq!(rows[0][0], SqlValue::String("line1\nline2".to_string()));
        assert_eq!(rows[0][1], SqlValue::String("tab\there".to_string()));
    }

    #[test]
    fn empty_string_vs_null() {
        let stmt = b"INSERT INTO `t` VALUES ('',NULL);";
        let rows = parse_insert_values(stmt).unwrap();
        assert_eq!(rows[0][0], SqlValue::String("".to_string()));
        assert_eq!(rows[0][1], SqlValue::Null);
    }

    #[test]
    fn float_values() {
        let stmt = b"INSERT INTO `t` VALUES (3.14,-2.5,1e10);";
        let rows = parse_insert_values(stmt).unwrap();
        assert_eq!(rows[0][0], SqlValue::Float(3.14));
        assert_eq!(rows[0][1], SqlValue::Float(-2.5));
        assert_eq!(rows[0][2], SqlValue::Float(1e10));
    }

    #[test]
    fn hex_literal() {
        let stmt = b"INSERT INTO `t` VALUES (0x48454C4C4F);";
        let rows = parse_insert_values(stmt).unwrap();
        assert_eq!(rows[0][0], SqlValue::HexString(b"HELLO".to_vec()));
    }

    #[test]
    fn bit_literal() {
        let stmt = b"INSERT INTO `t` VALUES (b'101');";
        let rows = parse_insert_values(stmt).unwrap();
        assert_eq!(rows[0][0], SqlValue::BitLiteral(5));
    }

    #[test]
    fn negative_integer() {
        let stmt = b"INSERT INTO `t` VALUES (-42);";
        let rows = parse_insert_values(stmt).unwrap();
        assert_eq!(rows[0][0], SqlValue::Integer(-42));
    }

    #[test]
    fn commas_and_parens_in_strings() {
        let stmt = b"INSERT INTO `t` VALUES ('(a,b)','c;d');";
        let rows = parse_insert_values(stmt).unwrap();
        assert_eq!(rows[0][0], SqlValue::String("(a,b)".to_string()));
        assert_eq!(rows[0][1], SqlValue::String("c;d".to_string()));
    }

    #[test]
    fn unicode_strings() {
        // Multi-byte UTF-8: Japanese, emoji, Arabic, accented Latin
        let stmt = "INSERT INTO `t` VALUES ('日本語'),('café'),('مرحبا'),('🎉🚀');".as_bytes();
        let rows = parse_insert_values(stmt).unwrap();
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[0][0], SqlValue::String("日本語".to_string()));
        assert_eq!(rows[1][0], SqlValue::String("café".to_string()));
        assert_eq!(rows[2][0], SqlValue::String("مرحبا".to_string()));
        assert_eq!(rows[3][0], SqlValue::String("🎉🚀".to_string()));
    }

    #[test]
    fn unicode_mixed_with_escapes() {
        let stmt = "INSERT INTO `t` VALUES ('héllo\\nwörld'),('it''s über');".as_bytes();
        let rows = parse_insert_values(stmt).unwrap();
        assert_eq!(rows[0][0], SqlValue::String("héllo\nwörld".to_string()));
        assert_eq!(rows[1][0], SqlValue::String("it's über".to_string()));
    }

    #[test]
    fn large_values_count() {
        let mut stmt = b"INSERT INTO `t` VALUES ".to_vec();
        for i in 0..1000 {
            if i > 0 {
                stmt.push(b',');
            }
            stmt.extend_from_slice(format!("({i},'row{i}')").as_bytes());
        }
        stmt.push(b';');
        let rows = parse_insert_values(&stmt).unwrap();
        assert_eq!(rows.len(), 1000);
    }

    // --- SQL Server N'...' strings ---

    #[test]
    fn mssql_nstring() {
        let stmt = b"INSERT INTO [t] VALUES (1,N'hello');";
        let rows = parse_insert_values(stmt).unwrap();
        assert_eq!(rows[0][0], SqlValue::Integer(1));
        assert_eq!(rows[0][1], SqlValue::String("hello".to_string()));
    }

    #[test]
    fn mssql_nstring_with_null() {
        let stmt = b"INSERT INTO [t] VALUES (N'text',NULL);";
        let rows = parse_insert_values(stmt).unwrap();
        assert_eq!(rows[0][0], SqlValue::String("text".to_string()));
        assert_eq!(rows[0][1], SqlValue::Null);
    }

    #[test]
    fn mssql_nstring_unicode() {
        let stmt = "INSERT INTO [t] VALUES (N'café résumé');".as_bytes();
        let rows = parse_insert_values(stmt).unwrap();
        assert_eq!(rows[0][0], SqlValue::String("café résumé".to_string()));
    }

    // --- PostgreSQL COPY data ---

    #[test]
    fn pg_copy_basic() {
        let data = b"COPY users FROM stdin;\n1\talice\n2\tbob\n\\.\n";
        let rows = parse_copy_data(data).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], SqlValue::Integer(1));
        assert_eq!(rows[0][1], SqlValue::String("alice".to_string()));
        assert_eq!(rows[1][0], SqlValue::Integer(2));
        assert_eq!(rows[1][1], SqlValue::String("bob".to_string()));
    }

    #[test]
    fn pg_copy_with_nulls() {
        let data = b"COPY t FROM stdin;\n1\t\\N\thello\n\\.\n";
        let rows = parse_copy_data(data).unwrap();
        assert_eq!(rows[0][0], SqlValue::Integer(1));
        assert_eq!(rows[0][1], SqlValue::Null);
        assert_eq!(rows[0][2], SqlValue::String("hello".to_string()));
    }

    #[test]
    fn pg_copy_with_escapes() {
        let data = b"COPY t FROM stdin;\nhello\\nworld\tback\\\\slash\n\\.\n";
        let rows = parse_copy_data(data).unwrap();
        assert_eq!(rows[0][0], SqlValue::String("hello\nworld".to_string()));
        assert_eq!(rows[0][1], SqlValue::String("back\\slash".to_string()));
    }
}
