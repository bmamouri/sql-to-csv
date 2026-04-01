# sql-to-csv

> Fast, parallel SQL dump to CSV/TSV converter. Supports MySQL, PostgreSQL, SQL Server, Oracle, and SQLite dumps.

- Convert a SQL dump (dialect auto-detected):

`sql-to-csv {{path/to/dump.sql}} {{path/to/output}}`

- Convert a SQL Server dump with explicit dialect:

`sql-to-csv {{path/to/dump.sql}} {{path/to/output}} --dialect mssql`

- Output TSV instead of CSV (PostgreSQL TEXT format):

`sql-to-csv {{path/to/dump.sql}} {{path/to/output}} --tsv`

- Overwrite an existing output directory:

`sql-to-csv {{path/to/dump.sql}} {{path/to/output}} --force`

- Extract specific tables only:

`sql-to-csv {{path/to/dump.sql}} {{path/to/output}} --tables {{users,posts,comments}}`

- Only extract schema (no data files):

`sql-to-csv {{path/to/dump.sql}} {{path/to/output}} --schema-only`

- Convert a gzipped dump with 8 parallel workers:

`sql-to-csv {{path/to/dump.sql.gz}} {{path/to/output}} --workers {{8}}`

- Dry run (parse and count rows without writing output):

`sql-to-csv {{path/to/dump.sql}} {{path/to/output}} --dry-run`
