use crate::types::{ColumnDef, IndexDef, SqlDialect, TableDef};

/// Parse a CREATE TABLE statement and convert to PostgreSQL DDL.
/// Handles all supported dialects.
/// Returns (pg_ddl_string, warnings).
pub fn convert_create_table(
    stmt: &[u8],
    dialect: SqlDialect,
) -> Result<(String, Vec<String>), String> {
    let text = String::from_utf8_lossy(stmt);
    let mut warnings = Vec::new();
    let table_def = parse_create_table(&text, dialect, &mut warnings)?;
    let pg_ddl = render_postgres_ddl(&table_def, &mut warnings);
    Ok((pg_ddl, warnings))
}

/// Convert a DROP TABLE statement to PostgreSQL.
pub fn convert_drop_table(stmt: &[u8], dialect: SqlDialect) -> String {
    let text = String::from_utf8_lossy(stmt);
    let converted = match dialect {
        SqlDialect::Mysql => text.replace('`', "\""),
        SqlDialect::Mssql => text.replace('[', "\"").replace(']', "\""),
        _ => text.into_owned(),
    };
    let trimmed = converted.trim().trim_end_matches(';');
    format!("{trimmed};")
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

fn parse_create_table(
    text: &str,
    dialect: SqlDialect,
    warnings: &mut Vec<String>,
) -> Result<TableDef, String> {
    // Extract table name
    let name = extract_table_name(text)?;

    // Find the column definitions block between first `(` and matching `)`
    let paren_start = text
        .find('(')
        .ok_or("No opening parenthesis in CREATE TABLE")?;
    let paren_end = find_matching_paren(text, paren_start)
        .ok_or("No matching closing parenthesis in CREATE TABLE")?;
    let body = &text[paren_start + 1..paren_end];

    let mut columns = Vec::new();
    let mut primary_key: Option<Vec<String>> = None;
    let mut indexes = Vec::new();

    // Split by top-level commas (not inside parentheses)
    let parts = split_top_level_commas(body);

    for part in &parts {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            continue;
        }

        let upper = trimmed.to_ascii_uppercase();

        if upper.starts_with("PRIMARY KEY") {
            primary_key = Some(extract_column_list(trimmed));
        } else if upper.starts_with("UNIQUE KEY") || upper.starts_with("UNIQUE INDEX") {
            let idx_name = extract_index_name(trimmed);
            let cols = extract_column_list(trimmed);
            indexes.push(IndexDef {
                name: idx_name,
                columns: cols,
                unique: true,
            });
        } else if upper.starts_with("KEY ") || upper.starts_with("INDEX ") {
            let idx_name = extract_index_name(trimmed);
            let cols = extract_column_list(trimmed);
            indexes.push(IndexDef {
                name: idx_name,
                columns: cols,
                unique: false,
            });
        } else if upper.starts_with("FULLTEXT") {
            let idx_name = extract_index_name(trimmed);
            warnings.push(format!(
                "FULLTEXT index '{}' skipped (no direct PostgreSQL equivalent)",
                idx_name
            ));
        } else if upper.starts_with("SPATIAL") {
            let idx_name = extract_index_name(trimmed);
            warnings.push(format!(
                "SPATIAL index '{}' skipped (requires PostGIS)",
                idx_name
            ));
        } else if upper.starts_with("CONSTRAINT") {
            // Pass through constraints as-is with backtick conversion
            // (may need manual fixup)
            warnings.push(format!("CONSTRAINT passed through, may need manual review: {}", truncate(trimmed, 80)));
        } else if upper.starts_with("CHECK") {
            // Pass through
        } else {
            // Column definition
            match parse_column_def(trimmed, dialect, warnings) {
                Ok(col) => columns.push(col),
                Err(e) => warnings.push(format!("Failed to parse column: {e}")),
            }
        }
    }

    Ok(TableDef {
        name,
        columns,
        primary_key,
        indexes,
    })
}

fn parse_column_def(
    def: &str,
    dialect: SqlDialect,
    warnings: &mut Vec<String>,
) -> Result<ColumnDef, String> {
    let tokens = tokenize_column_def(def);
    if tokens.is_empty() {
        return Err("Empty column definition".to_string());
    }

    let name = unquote(&tokens[0]);
    let mut pos = 1;

    if pos >= tokens.len() {
        return Err(format!("No type specified for column '{name}'"));
    }

    // Parse MySQL type
    let (pg_type, is_identity, new_pos) = convert_type(&tokens, pos, &name, dialect, warnings);
    pos = new_pos;

    // Parse column modifiers
    let mut nullable = true;
    let mut default: Option<String> = None;

    while pos < tokens.len() {
        let upper = tokens[pos].to_ascii_uppercase();
        match upper.as_str() {
            "NOT" => {
                if pos + 1 < tokens.len() && tokens[pos + 1].eq_ignore_ascii_case("NULL") {
                    nullable = false;
                    pos += 2;
                } else {
                    pos += 1;
                }
            }
            "NULL" => {
                nullable = true;
                pos += 1;
            }
            "DEFAULT" => {
                pos += 1;
                if pos < tokens.len() {
                    let (def_val, end) = parse_default_value(&tokens, pos);
                    default = Some(convert_default_value(&def_val, &pg_type));
                    pos = end;
                }
            }
            "AUTO_INCREMENT" | "AUTOINCREMENT" | "IDENTITY" => {
                // Handled via is_identity in type parsing
                pos += 1;
            }
            "UNSIGNED" => {
                warnings.push(format!(
                    "Column '{name}': UNSIGNED not supported in PostgreSQL"
                ));
                pos += 1;
            }
            "ON" => {
                // ON UPDATE CURRENT_TIMESTAMP etc — skip
                let rest: Vec<&str> = tokens[pos..].iter().map(|s| s.as_str()).collect();
                let rest_upper: String = rest.join(" ").to_ascii_uppercase();
                if rest_upper.starts_with("ON UPDATE") {
                    warnings.push(format!(
                        "Column '{name}': ON UPDATE clause stripped"
                    ));
                    pos += 3; // ON UPDATE value
                    if pos < tokens.len() {
                        pos += 1; // skip the value too
                    }
                } else {
                    pos += 1;
                }
            }
            "COMMENT" => {
                pos += 1;
                // Skip the comment string
                if pos < tokens.len() {
                    pos += 1;
                }
            }
            "CHARACTER" | "CHARSET" | "COLLATE" => {
                pos += 1;
                // Skip value
                if pos < tokens.len() && tokens[pos].eq_ignore_ascii_case("SET") {
                    pos += 1;
                }
                if pos < tokens.len() {
                    pos += 1;
                }
            }
            "GENERATED" | "VIRTUAL" | "STORED" | "AS" => {
                // Skip generated column expressions
                warnings.push(format!(
                    "Column '{name}': generated column expression may need review"
                ));
                break;
            }
            _ => {
                pos += 1;
            }
        }
    }

    Ok(ColumnDef {
        name,
        pg_type,
        nullable,
        default,
        is_identity,
    })
}

fn convert_type(
    tokens: &[String],
    pos: usize,
    col_name: &str,
    dialect: SqlDialect,
    warnings: &mut Vec<String>,
) -> (String, bool, usize) {
    if pos >= tokens.len() {
        return ("TEXT".to_string(), false, pos);
    }

    let type_name = tokens[pos].to_ascii_uppercase();
    let mut next = pos + 1;
    let mut is_identity = false;

    // Check for identity markers
    let has_auto_increment = tokens[pos..]
        .iter()
        .any(|t| t.eq_ignore_ascii_case("AUTO_INCREMENT") || t.eq_ignore_ascii_case("AUTOINCREMENT")
             || t.eq_ignore_ascii_case("IDENTITY"));

    // Parse type parameters (e.g., VARCHAR(255), DECIMAL(10,2))
    let type_params = if next < tokens.len() && tokens[next].starts_with('(') {
        let param = &tokens[next];
        next += 1;
        let mut full = param.to_string();
        while !full.ends_with(')') && next < tokens.len() {
            full.push_str(&tokens[next]);
            next += 1;
        }
        Some(full)
    } else {
        None
    };

    // Skip UNSIGNED (MySQL)
    if next < tokens.len() && tokens[next].eq_ignore_ascii_case("UNSIGNED") {
        warnings.push(format!(
            "Column '{col_name}': UNSIGNED not supported in PostgreSQL"
        ));
        next += 1;
    }

    // Skip ZEROFILL (MySQL)
    if next < tokens.len() && tokens[next].eq_ignore_ascii_case("ZEROFILL") {
        next += 1;
    }

    let pg_type = match type_name.as_str() {
        // ---- Integer types (all dialects) ----
        "TINYINT" => {
            if type_params.as_deref() == Some("(1)") {
                "BOOLEAN".to_string()
            } else {
                if has_auto_increment { is_identity = true; }
                "SMALLINT".to_string()
            }
        }
        "BOOL" | "BOOLEAN" => "BOOLEAN".to_string(),
        "SMALLINT" | "INT2" => {
            if has_auto_increment { is_identity = true; }
            "SMALLINT".to_string()
        }
        "MEDIUMINT" => {
            if has_auto_increment { is_identity = true; }
            "INTEGER".to_string()
        }
        "INT" | "INTEGER" | "INT4" => {
            if has_auto_increment { is_identity = true; }
            "INTEGER".to_string()
        }
        "BIGINT" | "INT8" => {
            if has_auto_increment { is_identity = true; }
            "BIGINT".to_string()
        }

        // ---- SQL Server specific integer types ----
        "BIT" if dialect == SqlDialect::Mssql => "BOOLEAN".to_string(),

        // ---- Oracle NUMBER ----
        "NUMBER" if dialect == SqlDialect::Oracle => {
            match type_params.as_deref() {
                Some(p) if p.contains(',') => format!("NUMERIC{p}"),
                Some(p) => {
                    // NUMBER(p) with no scale — map to integer types
                    let precision: u32 = p.trim_matches(|c: char| !c.is_ascii_digit())
                        .parse().unwrap_or(38);
                    if precision <= 4 {
                        "SMALLINT".to_string()
                    } else if precision <= 9 {
                        "INTEGER".to_string()
                    } else if precision <= 18 {
                        "BIGINT".to_string()
                    } else {
                        format!("NUMERIC{p}")
                    }
                }
                None => "NUMERIC".to_string(),
            }
        }

        // ---- Floating point ----
        "FLOAT" | "FLOAT4" => "REAL".to_string(),
        "DOUBLE" | "REAL" | "FLOAT8" => "DOUBLE PRECISION".to_string(),
        "DECIMAL" | "NUMERIC" | "DEC" | "FIXED" | "MONEY" | "SMALLMONEY" => {
            if type_name == "MONEY" || type_name == "SMALLMONEY" {
                "NUMERIC(19,4)".to_string()
            } else if let Some(ref p) = type_params {
                format!("NUMERIC{p}")
            } else {
                "NUMERIC".to_string()
            }
        }
        "NUMBER" => {
            // Non-Oracle NUMBER fallback
            if let Some(ref p) = type_params {
                format!("NUMERIC{p}")
            } else {
                "NUMERIC".to_string()
            }
        }

        // ---- String types ----
        "CHAR" | "NCHAR" => {
            if let Some(ref p) = type_params {
                format!("CHAR{p}")
            } else {
                "CHAR(1)".to_string()
            }
        }
        "VARCHAR" | "NVARCHAR" | "VARCHAR2" | "NVARCHAR2" => {
            match type_params.as_deref() {
                Some("(MAX)") | Some("(max)") => "TEXT".to_string(),
                Some(p) => format!("VARCHAR{p}"),
                None => "VARCHAR(255)".to_string(),
            }
        }
        "TINYTEXT" | "TEXT" | "MEDIUMTEXT" | "LONGTEXT" | "NTEXT" | "CLOB" | "NCLOB"
        | "LONG" => "TEXT".to_string(),

        // ---- Binary types ----
        "TINYBLOB" | "BLOB" | "MEDIUMBLOB" | "LONGBLOB" | "BYTEA" | "RAW" => "BYTEA".to_string(),
        "BINARY" | "VARBINARY" => {
            match type_params.as_deref() {
                Some("(MAX)") | Some("(max)") => "BYTEA".to_string(),
                _ => "BYTEA".to_string(),
            }
        }
        "IMAGE" => "BYTEA".to_string(),

        // ---- Date/time types ----
        "DATETIME" | "DATETIME2" | "SMALLDATETIME" => {
            if let Some(ref p) = type_params {
                format!("TIMESTAMP{p}")
            } else {
                "TIMESTAMP".to_string()
            }
        }
        "TIMESTAMP" if dialect == SqlDialect::Oracle => {
            // Oracle TIMESTAMP — keep as TIMESTAMP (no TZ by default)
            if let Some(ref p) = type_params {
                format!("TIMESTAMP{p}")
            } else {
                "TIMESTAMP".to_string()
            }
        }
        "TIMESTAMP" => {
            if let Some(ref p) = type_params {
                format!("TIMESTAMPTZ{p}")
            } else {
                "TIMESTAMPTZ".to_string()
            }
        }
        "DATETIMEOFFSET" => "TIMESTAMPTZ".to_string(),
        "DATE" => "DATE".to_string(),
        "TIME" => {
            if let Some(ref p) = type_params {
                format!("TIME{p}")
            } else {
                "TIME".to_string()
            }
        }
        "YEAR" => "SMALLINT".to_string(),

        // ---- MySQL specific ----
        "ENUM" => {
            if let Some(ref p) = type_params {
                warnings.push(format!("Column '{col_name}': ENUM{p} converted to TEXT"));
            }
            "TEXT".to_string()
        }
        "SET" => {
            warnings.push(format!(
                "Column '{col_name}': SET type has no direct PostgreSQL equivalent, using TEXT"
            ));
            "TEXT".to_string()
        }

        // ---- JSON ----
        "JSON" | "JSONB" => "JSONB".to_string(),

        // ---- BIT ----
        "BIT" => {
            if let Some(ref p) = type_params {
                format!("BIT{p}")
            } else {
                "BIT(1)".to_string()
            }
        }

        // ---- SQL Server specific ----
        "UNIQUEIDENTIFIER" => "UUID".to_string(),
        "SQL_VARIANT" => {
            warnings.push(format!("Column '{col_name}': SQL_VARIANT has no PostgreSQL equivalent, using TEXT"));
            "TEXT".to_string()
        }
        "XML" => "XML".to_string(),
        "HIERARCHYID" | "GEOGRAPHY" | "GEOMETRY" => {
            warnings.push(format!("Column '{col_name}': {type_name} may need manual conversion"));
            "TEXT".to_string()
        }

        // ---- Oracle specific ----
        "ROWID" | "UROWID" => "TEXT".to_string(),
        "BFILE" => {
            warnings.push(format!("Column '{col_name}': BFILE has no PostgreSQL equivalent"));
            "TEXT".to_string()
        }
        "XMLTYPE" => "XML".to_string(),
        "INTERVAL" => {
            // Oracle INTERVAL YEAR TO MONTH / DAY TO SECOND
            "INTERVAL".to_string()
        }

        // ---- SQLite types (very flexible) ----
        "AUTOINCREMENT" => {
            is_identity = true;
            "INTEGER".to_string()
        }

        // ---- Spatial (any dialect) ----
        "POINT" | "LINESTRING" | "POLYGON" | "MULTIPOINT" | "MULTILINESTRING"
        | "MULTIPOLYGON" | "GEOMETRYCOLLECTION" => {
            warnings.push(format!("Column '{col_name}': spatial type {type_name} requires PostGIS"));
            "TEXT".to_string()
        }

        // ---- PostgreSQL native types (pass through) ----
        "SERIAL" => { is_identity = true; "INTEGER".to_string() }
        "BIGSERIAL" => { is_identity = true; "BIGINT".to_string() }
        "SMALLSERIAL" => { is_identity = true; "SMALLINT".to_string() }
        "CIDR" | "INET" | "MACADDR" | "MACADDR8" | "UUID" | "TSQUERY" | "TSVECTOR"
        | "CITEXT" => type_name.clone(),

        _ => {
            warnings.push(format!(
                "Column '{col_name}': unknown {dialect} type '{type_name}', using TEXT"
            ));
            "TEXT".to_string()
        }
    };

    (pg_type, is_identity, next)
}

// ---------------------------------------------------------------------------
// Renderer
// ---------------------------------------------------------------------------

fn render_postgres_ddl(table: &TableDef, warnings: &mut Vec<String>) -> String {
    let mut out = String::new();
    let qname = quote_ident(&table.name);

    out.push_str(&format!("CREATE TABLE {qname} (\n"));

    for (i, col) in table.columns.iter().enumerate() {
        let col_name = quote_ident(&col.name);
        let mut line = format!("    {col_name} {}", col.pg_type);

        if col.is_identity {
            line.push_str(" GENERATED ALWAYS AS IDENTITY");
        }

        if !col.nullable {
            line.push_str(" NOT NULL");
        }

        if let Some(ref def) = col.default {
            line.push_str(&format!(" DEFAULT {def}"));
        }

        if i + 1 < table.columns.len() || table.primary_key.is_some() {
            line.push(',');
        }

        out.push_str(&line);
        out.push('\n');
    }

    if let Some(ref pk_cols) = table.primary_key {
        let cols: Vec<String> = pk_cols.iter().map(|c| quote_ident(c)).collect();
        out.push_str(&format!("    PRIMARY KEY ({})\n", cols.join(", ")));
    }

    out.push_str(");\n");

    // Emit indexes
    for idx in &table.indexes {
        let idx_name = quote_ident(&idx.name);
        let cols: Vec<String> = idx.columns.iter().map(|c| quote_ident(c)).collect();
        let unique = if idx.unique { "UNIQUE " } else { "" };
        out.push_str(&format!(
            "CREATE {unique}INDEX {idx_name} ON {qname} ({});\n",
            cols.join(", ")
        ));
    }

    // Add warnings as comments
    for w in warnings.iter() {
        out.push_str(&format!("-- WARNING: {w}\n"));
    }

    out
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn extract_table_name(text: &str) -> Result<String, String> {
    let upper = text.to_ascii_uppercase();
    let table_pos = upper
        .find("TABLE")
        .ok_or("No TABLE keyword found")?;
    let after_table = &text[table_pos + 5..];
    let trimmed = after_table.trim_start();

    // Skip IF NOT EXISTS
    let upper_trimmed = trimmed.to_ascii_uppercase();
    let name_start = if upper_trimmed.starts_with("IF NOT EXISTS") {
        trimmed[13..].trim_start()
    } else if upper_trimmed.starts_with("IF EXISTS") {
        trimmed[9..].trim_start()
    } else {
        trimmed
    };

    // Extract identifier
    if name_start.starts_with('`') {
        let end = name_start[1..]
            .find('`')
            .ok_or("Unclosed backtick in table name")?;
        Ok(name_start[1..1 + end].to_string())
    } else if name_start.starts_with('"') {
        let end = name_start[1..]
            .find('"')
            .ok_or("Unclosed quote in table name")?;
        Ok(name_start[1..1 + end].to_string())
    } else {
        let end = name_start
            .find(|c: char| c.is_ascii_whitespace() || c == '(' || c == ';')
            .unwrap_or(name_start.len());
        Ok(name_start[..end].to_string())
    }
}

fn find_matching_paren(text: &str, open_pos: usize) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut depth = 0;
    let mut in_string = false;
    let mut escape = false;

    for i in open_pos..bytes.len() {
        let b = bytes[i];
        if escape {
            escape = false;
            continue;
        }
        if in_string {
            if b == b'\\' {
                escape = true;
            } else if b == b'\'' {
                in_string = false;
            }
            continue;
        }
        match b {
            b'\'' => in_string = true,
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

fn split_top_level_commas(text: &str) -> Vec<String> {
    let bytes = text.as_bytes();
    let mut parts = Vec::new();
    let mut depth = 0;
    let mut in_string = false;
    let mut escape = false;
    let mut start = 0;

    for i in 0..bytes.len() {
        let b = bytes[i];
        if escape {
            escape = false;
            continue;
        }
        if in_string {
            if b == b'\\' {
                escape = true;
            } else if b == b'\'' {
                in_string = false;
            }
            continue;
        }
        match b {
            b'\'' => in_string = true,
            b'(' => depth += 1,
            b')' => {
                if depth > 0 {
                    depth -= 1;
                }
            }
            b',' if depth == 0 => {
                parts.push(text[start..i].to_string());
                start = i + 1;
            }
            _ => {}
        }
    }
    if start < text.len() {
        parts.push(text[start..].to_string());
    }
    parts
}

fn extract_column_list(def: &str) -> Vec<String> {
    let paren_start = match def.find('(') {
        Some(p) => p,
        None => return Vec::new(),
    };
    let paren_end = match def[paren_start..].rfind(')') {
        Some(p) => paren_start + p,
        None => return Vec::new(),
    };
    let inner = &def[paren_start + 1..paren_end];
    inner
        .split(',')
        .map(|s| {
            let trimmed = s.trim();
            // Remove length suffix like (255) from indexed columns
            let name = if let Some(p) = trimmed.find('(') {
                &trimmed[..p]
            } else {
                trimmed
            };
            unquote(name.trim())
        })
        .filter(|s| !s.is_empty())
        .collect()
}

fn extract_index_name(def: &str) -> String {
    let tokens = tokenize_column_def(def);
    // Pattern: KEY `name` (...) or UNIQUE KEY `name` (...)
    for (i, t) in tokens.iter().enumerate() {
        let upper = t.to_ascii_uppercase();
        if (upper == "KEY" || upper == "INDEX") && i + 1 < tokens.len() {
            let next = &tokens[i + 1];
            if !next.starts_with('(') {
                return unquote(next);
            }
        }
    }
    "unnamed_index".to_string()
}

fn tokenize_column_def(def: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let bytes = def.trim().as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        let b = bytes[i];
        if b.is_ascii_whitespace() {
            i += 1;
            continue;
        }

        // Quoted identifier
        if b == b'`' || b == b'"' {
            let quote = b;
            let start = i;
            i += 1;
            while i < bytes.len() && bytes[i] != quote {
                i += 1;
            }
            if i < bytes.len() {
                i += 1;
            }
            tokens.push(String::from_utf8_lossy(&bytes[start..i]).to_string());
            continue;
        }

        // String literal
        if b == b'\'' {
            let start = i;
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\\' {
                    i += 2;
                } else if bytes[i] == b'\'' {
                    i += 1;
                    break;
                } else {
                    i += 1;
                }
            }
            tokens.push(String::from_utf8_lossy(&bytes[start..i]).to_string());
            continue;
        }

        // Parenthesized group
        if b == b'(' {
            let start = i;
            let mut depth = 1;
            i += 1;
            while i < bytes.len() && depth > 0 {
                match bytes[i] {
                    b'(' => depth += 1,
                    b')' => depth -= 1,
                    b'\'' => {
                        i += 1;
                        while i < bytes.len() {
                            if bytes[i] == b'\\' {
                                i += 2;
                                continue;
                            }
                            if bytes[i] == b'\'' {
                                i += 1;
                                break;
                            }
                            i += 1;
                        }
                        continue;
                    }
                    _ => {}
                }
                i += 1;
            }
            tokens.push(String::from_utf8_lossy(&bytes[start..i]).to_string());
            continue;
        }

        // Regular word/token
        let start = i;
        while i < bytes.len()
            && !bytes[i].is_ascii_whitespace()
            && bytes[i] != b'('
            && bytes[i] != b')'
            && bytes[i] != b','
            && bytes[i] != b'\''
        {
            i += 1;
        }
        if i > start {
            tokens.push(String::from_utf8_lossy(&bytes[start..i]).to_string());
        }

        // Comma as separate token
        if i < bytes.len() && bytes[i] == b',' {
            i += 1;
        }
    }
    tokens
}

fn parse_default_value(tokens: &[String], pos: usize) -> (String, usize) {
    if pos >= tokens.len() {
        return ("NULL".to_string(), pos);
    }

    let token = &tokens[pos];

    // String literal
    if token.starts_with('\'') {
        return (token.clone(), pos + 1);
    }

    // Function call like CURRENT_TIMESTAMP
    let upper = token.to_ascii_uppercase();
    if upper == "CURRENT_TIMESTAMP" || upper == "NOW" {
        let mut end = pos + 1;
        if end < tokens.len() && tokens[end].starts_with('(') {
            end += 1;
        }
        return ("CURRENT_TIMESTAMP".to_string(), end);
    }

    (token.clone(), pos + 1)
}

fn convert_default_value(val: &str, pg_type: &str) -> String {
    let upper = val.to_ascii_uppercase();
    match upper.as_str() {
        "CURRENT_TIMESTAMP" => "CURRENT_TIMESTAMP".to_string(),
        "NULL" => "NULL".to_string(),
        _ => {
            if pg_type == "BOOLEAN" {
                if val == "1" || val == "'1'" || upper == "B'1'" || upper == "TRUE" {
                    return "TRUE".to_string();
                }
                if val == "0" || val == "'0'" || upper == "B'0'" || upper == "FALSE" {
                    return "FALSE".to_string();
                }
            }
            val.to_string()
        }
    }
}

fn unquote(s: &str) -> String {
    let trimmed = s.trim();
    if (trimmed.starts_with('`') && trimmed.ends_with('`'))
        || (trimmed.starts_with('"') && trimmed.ends_with('"'))
        || (trimmed.starts_with('[') && trimmed.ends_with(']'))
    {
        trimmed[1..trimmed.len() - 1].to_string()
    } else {
        trimmed.to_string()
    }
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}...", &s[..max])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- MySQL tests ---

    #[test]
    fn mysql_basic_create_table() {
        let sql = br#"CREATE TABLE `users` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) DEFAULT NULL,
  `email` varchar(255) NOT NULL DEFAULT '',
  PRIMARY KEY (`id`),
  KEY `idx_name` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"#;

        let (ddl, _) = convert_create_table(sql, SqlDialect::Mysql).unwrap();
        assert!(ddl.contains("\"id\" INTEGER GENERATED ALWAYS AS IDENTITY NOT NULL"));
        assert!(ddl.contains("\"name\" VARCHAR(255) DEFAULT NULL"));
        assert!(ddl.contains("\"email\" VARCHAR(255) NOT NULL DEFAULT ''"));
        assert!(ddl.contains("PRIMARY KEY (\"id\")"));
        assert!(ddl.contains("CREATE INDEX \"idx_name\" ON \"users\" (\"name\")"));
        assert!(!ddl.contains("ENGINE"));
        assert!(!ddl.contains("CHARSET"));
    }

    #[test]
    fn mysql_type_mappings() {
        let sql = br#"CREATE TABLE `types` (
  `a` tinyint(1) DEFAULT 0,
  `b` tinyint DEFAULT 0,
  `c` bigint NOT NULL,
  `d` float DEFAULT NULL,
  `e` double DEFAULT NULL,
  `f` datetime DEFAULT NULL,
  `g` timestamp DEFAULT CURRENT_TIMESTAMP,
  `h` text,
  `i` mediumblob,
  `j` json DEFAULT NULL,
  `k` year DEFAULT NULL,
  `l` enum('a','b','c') DEFAULT 'a',
  `m` decimal(10,2) DEFAULT 0.00
) ENGINE=InnoDB;"#;

        let (ddl, _) = convert_create_table(sql, SqlDialect::Mysql).unwrap();
        assert!(ddl.contains("\"a\" BOOLEAN"));
        assert!(ddl.contains("\"b\" SMALLINT"));
        assert!(ddl.contains("\"c\" BIGINT NOT NULL"));
        assert!(ddl.contains("\"d\" REAL"));
        assert!(ddl.contains("\"e\" DOUBLE PRECISION"));
        assert!(ddl.contains("\"f\" TIMESTAMP"));
        assert!(ddl.contains("\"g\" TIMESTAMPTZ"));
        assert!(ddl.contains("\"h\" TEXT"));
        assert!(ddl.contains("\"i\" BYTEA"));
        assert!(ddl.contains("\"j\" JSONB"));
        assert!(ddl.contains("\"k\" SMALLINT"));
        assert!(ddl.contains("\"l\" TEXT"));
        assert!(ddl.contains("\"m\" NUMERIC(10,2)"));
    }

    #[test]
    fn mysql_boolean_defaults() {
        let sql = br#"CREATE TABLE `flags` (
  `active` tinyint(1) DEFAULT 1,
  `deleted` tinyint(1) DEFAULT 0
) ENGINE=InnoDB;"#;

        let (ddl, _) = convert_create_table(sql, SqlDialect::Mysql).unwrap();
        assert!(ddl.contains("DEFAULT TRUE"));
        assert!(ddl.contains("DEFAULT FALSE"));
    }

    #[test]
    fn mysql_drop_table() {
        let result = convert_drop_table(b"DROP TABLE IF EXISTS `users`;", SqlDialect::Mysql);
        assert_eq!(result, "DROP TABLE IF EXISTS \"users\";");
    }

    #[test]
    fn mysql_unique_index() {
        let sql = br#"CREATE TABLE `t` (
  `id` int NOT NULL,
  `email` varchar(255) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uniq_email` (`email`)
) ENGINE=InnoDB;"#;

        let (ddl, _) = convert_create_table(sql, SqlDialect::Mysql).unwrap();
        assert!(ddl.contains("CREATE UNIQUE INDEX \"uniq_email\""));
    }

    // --- SQL Server tests ---

    #[test]
    fn mssql_types() {
        let sql = br#"CREATE TABLE [dbo].[users] (
  [id] INT IDENTITY NOT NULL,
  [name] NVARCHAR(255) NOT NULL,
  [email] VARCHAR(MAX) NULL,
  [guid] UNIQUEIDENTIFIER NOT NULL,
  [amount] MONEY NOT NULL,
  [active] BIT NOT NULL,
  [data] XML NULL,
  PRIMARY KEY ([id])
);"#;

        let (ddl, _) = convert_create_table(sql, SqlDialect::Mssql).unwrap();
        assert!(ddl.contains("\"id\" INTEGER GENERATED ALWAYS AS IDENTITY NOT NULL"));
        assert!(ddl.contains("\"name\" VARCHAR(255) NOT NULL"));
        assert!(ddl.contains("\"email\" TEXT"));
        assert!(ddl.contains("\"guid\" UUID NOT NULL"));
        assert!(ddl.contains("\"amount\" NUMERIC(19,4) NOT NULL"));
        assert!(ddl.contains("\"active\" BOOLEAN NOT NULL"));
        assert!(ddl.contains("\"data\" XML"));
    }

    #[test]
    fn mssql_drop_table() {
        let result = convert_drop_table(b"DROP TABLE [dbo].[users];", SqlDialect::Mssql);
        assert_eq!(result, "DROP TABLE \"dbo\".\"users\";");
    }

    // --- Oracle tests ---

    #[test]
    fn oracle_types() {
        let sql = br#"CREATE TABLE "USERS" (
  "ID" NUMBER(10) NOT NULL,
  "NAME" VARCHAR2(255) NOT NULL,
  "BIO" CLOB,
  "AMOUNT" NUMBER(10,2) NOT NULL,
  "CREATED" DATE NOT NULL,
  PRIMARY KEY ("ID")
);"#;

        let (ddl, _) = convert_create_table(sql, SqlDialect::Oracle).unwrap();
        assert!(ddl.contains("\"ID\" BIGINT NOT NULL"));
        assert!(ddl.contains("\"NAME\" VARCHAR(255) NOT NULL"));
        assert!(ddl.contains("\"BIO\" TEXT"));
        assert!(ddl.contains("\"AMOUNT\" NUMERIC(10,2) NOT NULL"));
        assert!(ddl.contains("\"CREATED\" DATE NOT NULL"));
    }

    // --- SQLite tests ---

    #[test]
    fn sqlite_types() {
        let sql = br#"CREATE TABLE users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  email TEXT,
  score REAL DEFAULT 0.0,
  data BLOB
);"#;

        let (ddl, _) = convert_create_table(sql, SqlDialect::Sqlite).unwrap();
        assert!(ddl.contains("\"id\" INTEGER"));
        assert!(ddl.contains("\"name\" TEXT NOT NULL"));
        assert!(ddl.contains("\"score\" DOUBLE PRECISION"));
        assert!(ddl.contains("\"data\" BYTEA"));
    }
}
