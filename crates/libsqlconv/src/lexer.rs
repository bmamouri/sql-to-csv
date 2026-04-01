/// SQL lexer state machine that tracks whether we're inside strings, comments,
/// or normal SQL. Used by the index builder to find statement boundaries.
/// Supports all dialects: MySQL backticks, MSSQL square brackets, standard double quotes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LexerState {
    Normal,
    /// Inside a single-quoted string: `'...'`
    SingleQuoted,
    /// Just saw `\` inside a single-quoted string (MySQL/Oracle backslash escape)
    SingleQuotedBackslash,
    /// Just saw `'` inside a single-quoted string — might be `''` escape or end
    SingleQuotedMaybeEnd,
    /// Inside a backtick-quoted identifier: `` `...` `` (MySQL)
    Backtick,
    /// Inside a double-quoted identifier/string: `"..."` (PostgreSQL/Oracle/SQLite/standard)
    DoubleQuoted,
    /// Inside a square-bracket identifier: `[...]` (SQL Server)
    SquareBracket,
    /// Saw one `-`; might start `--` comment
    MaybeDash,
    /// Inside `-- ...` line comment
    LineComment,
    /// Saw `/`; might start `/* */` block comment
    MaybeBlockStart,
    /// Inside `/* ... */` block comment
    BlockComment,
    /// Saw `*` inside block comment; might end with `/`
    MaybeBlockEnd,
    /// Inside `/*!nnnnn ... */` conditional comment — treated as normal SQL (MySQL)
    ConditionalComment,
    /// Saw `*` inside conditional comment; might end with `/`
    ConditionalMaybeEnd,
}

/// Events emitted by the lexer for each byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LexerEvent {
    /// Nothing interesting — just advance.
    None,
    /// A `;` was found at the Normal level (statement boundary).
    Semicolon,
}

impl LexerState {
    /// Feed one byte and return the new state + event.
    #[inline(always)]
    pub fn feed(self, b: u8) -> (LexerState, LexerEvent) {
        use LexerState::*;
        match (self, b) {
            // ---- Normal ----
            (Normal, b'\'') => (SingleQuoted, LexerEvent::None),
            (Normal, b'`') => (Backtick, LexerEvent::None),
            (Normal, b'"') => (DoubleQuoted, LexerEvent::None),
            (Normal, b'[') => (SquareBracket, LexerEvent::None),
            (Normal, b'-') => (MaybeDash, LexerEvent::None),
            (Normal, b'/') => (MaybeBlockStart, LexerEvent::None),
            (Normal, b';') => (Normal, LexerEvent::Semicolon),
            (Normal, _) => (Normal, LexerEvent::None),

            // ---- Single-quoted string ----
            (SingleQuoted, b'\\') => (SingleQuotedBackslash, LexerEvent::None),
            (SingleQuoted, b'\'') => (SingleQuotedMaybeEnd, LexerEvent::None),
            (SingleQuoted, _) => (SingleQuoted, LexerEvent::None),

            (SingleQuotedBackslash, _) => (SingleQuoted, LexerEvent::None),

            // After seeing `'` in string: if next is `'`, it's an escape; otherwise string ended
            (SingleQuotedMaybeEnd, b'\'') => (SingleQuoted, LexerEvent::None),
            // Re-feed byte to Normal
            (SingleQuotedMaybeEnd, b';') => (Normal, LexerEvent::Semicolon),
            (SingleQuotedMaybeEnd, b'`') => (Backtick, LexerEvent::None),
            (SingleQuotedMaybeEnd, b'"') => (DoubleQuoted, LexerEvent::None),
            (SingleQuotedMaybeEnd, b'[') => (SquareBracket, LexerEvent::None),
            (SingleQuotedMaybeEnd, b'-') => (MaybeDash, LexerEvent::None),
            (SingleQuotedMaybeEnd, b'/') => (MaybeBlockStart, LexerEvent::None),
            (SingleQuotedMaybeEnd, _) => (Normal, LexerEvent::None),

            // ---- Backtick identifier (MySQL) ----
            (Backtick, b'`') => (Normal, LexerEvent::None),
            (Backtick, _) => (Backtick, LexerEvent::None),

            // ---- Double-quoted identifier (PostgreSQL/Oracle/SQLite/standard) ----
            (DoubleQuoted, b'"') => (Normal, LexerEvent::None),
            (DoubleQuoted, _) => (DoubleQuoted, LexerEvent::None),

            // ---- Square-bracket identifier (SQL Server) ----
            (SquareBracket, b']') => (Normal, LexerEvent::None),
            (SquareBracket, _) => (SquareBracket, LexerEvent::None),

            // ---- Dash comment ----
            (MaybeDash, b'-') => (LineComment, LexerEvent::None),
            (MaybeDash, b';') => (Normal, LexerEvent::Semicolon),
            (MaybeDash, b'\'') => (SingleQuoted, LexerEvent::None),
            (MaybeDash, _) => (Normal, LexerEvent::None),

            (LineComment, b'\n') => (Normal, LexerEvent::None),
            (LineComment, _) => (LineComment, LexerEvent::None),

            // ---- Block comment ----
            (MaybeBlockStart, b'*') => (BlockComment, LexerEvent::None),
            (MaybeBlockStart, b';') => (Normal, LexerEvent::Semicolon),
            (MaybeBlockStart, b'\'') => (SingleQuoted, LexerEvent::None),
            (MaybeBlockStart, _) => (Normal, LexerEvent::None),

            (BlockComment, b'*') => (MaybeBlockEnd, LexerEvent::None),
            (BlockComment, _) => (BlockComment, LexerEvent::None),

            (MaybeBlockEnd, b'/') => (Normal, LexerEvent::None),
            (MaybeBlockEnd, b'*') => (MaybeBlockEnd, LexerEvent::None),
            (MaybeBlockEnd, _) => (BlockComment, LexerEvent::None),

            // ---- Conditional comment (MySQL: acts like Normal but ends with `*/`) ----
            (ConditionalComment, b'*') => (ConditionalMaybeEnd, LexerEvent::None),
            (ConditionalComment, b'\'') => (SingleQuoted, LexerEvent::None),
            (ConditionalComment, b'`') => (Backtick, LexerEvent::None),
            (ConditionalComment, b'"') => (DoubleQuoted, LexerEvent::None),
            (ConditionalComment, _) => (ConditionalComment, LexerEvent::None),

            (ConditionalMaybeEnd, b'/') => (Normal, LexerEvent::None),
            (ConditionalMaybeEnd, b'*') => (ConditionalMaybeEnd, LexerEvent::None),
            (ConditionalMaybeEnd, b'\'') => (SingleQuoted, LexerEvent::None),
            (ConditionalMaybeEnd, _) => (ConditionalComment, LexerEvent::None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scan(input: &[u8]) -> (LexerState, Vec<usize>) {
        let mut state = LexerState::Normal;
        let mut semicolons = Vec::new();
        for (i, &b) in input.iter().enumerate() {
            let (new_state, event) = state.feed(b);
            state = new_state;
            if event == LexerEvent::Semicolon {
                semicolons.push(i);
            }
        }
        (state, semicolons)
    }

    #[test]
    fn simple_statements() {
        let (_, scs) = scan(b"SELECT 1; SELECT 2;");
        assert_eq!(scs, vec![8, 18]);
    }

    #[test]
    fn semicolon_in_single_quoted_string() {
        let input = b"INSERT INTO t VALUES ('a;b');";
        let (_, scs) = scan(input);
        assert_eq!(scs, vec![input.len() - 1]);
    }

    #[test]
    fn double_single_quote_escape() {
        let input = b"INSERT INTO t VALUES ('it''s');";
        let (_, scs) = scan(input);
        assert_eq!(scs, vec![input.len() - 1]);
    }

    #[test]
    fn backslash_escape_in_string() {
        let input = b"INSERT INTO t VALUES ('a\\'b');";
        let (_, scs) = scan(input);
        assert_eq!(scs, vec![input.len() - 1]);
    }

    #[test]
    fn backslash_at_end_of_string() {
        let input = b"SELECT 'hello\\\\';";
        let (_, scs) = scan(input);
        assert_eq!(scs, vec![input.len() - 1]);
    }

    #[test]
    fn backtick_identifier() {
        let input = b"INSERT INTO `ta;ble` VALUES (1);";
        let (_, scs) = scan(input);
        assert_eq!(scs, vec![input.len() - 1]);
    }

    #[test]
    fn double_quoted_identifier() {
        let input = b"INSERT INTO \"ta;ble\" VALUES (1);";
        let (_, scs) = scan(input);
        assert_eq!(scs, vec![input.len() - 1]);
    }

    #[test]
    fn square_bracket_identifier() {
        let input = b"INSERT INTO [ta;ble] VALUES (1);";
        let (_, scs) = scan(input);
        assert_eq!(scs, vec![input.len() - 1]);
    }

    #[test]
    fn mssql_nstring() {
        // N'...' — the N is just a prefix, the string parsing starts at '
        let input = b"INSERT INTO [t] VALUES (N'hello;world');";
        let (_, scs) = scan(input);
        assert_eq!(scs, vec![input.len() - 1]);
    }

    #[test]
    fn line_comment() {
        let input = b"-- this is ; a comment\nSELECT 1;";
        let (_, scs) = scan(input);
        assert_eq!(scs, vec![input.len() - 1]);
    }

    #[test]
    fn block_comment() {
        let input = b"/* ; */ SELECT 1;";
        let (_, scs) = scan(input);
        assert_eq!(scs, vec![input.len() - 1]);
    }

    #[test]
    fn nested_star_in_block_comment() {
        let input = b"/* * ; ** */ SELECT 1;";
        let (_, scs) = scan(input);
        assert_eq!(scs, vec![input.len() - 1]);
    }

    #[test]
    fn conditional_comment_no_semicolons() {
        let input = b"/*!50001 SET NAMES utf8; */;";
        let (_, scs) = scan(input);
        assert_eq!(scs, vec![input.len() - 1]);
    }

    #[test]
    fn string_with_parentheses_and_commas() {
        let input = b"INSERT INTO t VALUES ('(a,b)','c');";
        let (_, scs) = scan(input);
        assert_eq!(scs, vec![input.len() - 1]);
    }

    #[test]
    fn empty_string() {
        let input = b"INSERT INTO t VALUES ('');";
        let (_, scs) = scan(input);
        assert_eq!(scs, vec![input.len() - 1]);
    }

    #[test]
    fn multiple_inserts() {
        let input = b"INSERT INTO t VALUES (1); INSERT INTO t VALUES (2);";
        let (_, scs) = scan(input);
        assert_eq!(scs, vec![24, 50]);
    }
}
