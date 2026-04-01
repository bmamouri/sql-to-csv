# Changelog

## [0.1.0] - 2026-04-02

Initial release.

### Features
- Multi-dialect support: MySQL, PostgreSQL, SQL Server, Oracle, SQLite
- Auto-detection of source SQL dialect
- MySQL-to-PostgreSQL DDL conversion with 30+ type mappings
- CSV and TSV output (PostgreSQL COPY-compatible)
- Two-phase architecture: sequential index pass + parallel rayon workers
- Memory-bounded streaming for multi-GB dumps
- `.sql.gz` gzipped input support
- Table allowlist filtering (`--tables`)
- Schema-only and data-only modes
- Dry-run mode for row counting
- `schema.sql`, `load.sql`, `manifest.json`, `errors.log` output
- Man page and shell completions (bash, zsh, fish) via xtask
- Progress bar during processing
