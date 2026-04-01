# sql-to-csv justfile
# Run `just --list` to see all available recipes

set shell := ["bash", "-cu"]

# Default recipe: build in release mode
default: build

# Build release binary
build:
    cargo build --release

# Build debug binary
build-debug:
    cargo build

# Run all tests
test:
    cargo test

# Run tests with output
test-verbose:
    cargo test -- --nocapture

# Run clippy lints
lint:
    cargo clippy --all-targets -- -D warnings

# Format code
fmt:
    cargo fmt --all

# Check formatting without modifying
fmt-check:
    cargo fmt --all -- --check

# Install to ~/.cargo/bin
install:
    cargo install --path crates/sql-to-csv

# Uninstall
uninstall:
    cargo uninstall sql-to-csv

# Generate man page and shell completions
generate: build
    mkdir -p target/assets/man target/assets/completions
    cargo run --package sql-to-csv-xtask -- generate-man target/assets/man
    cargo run --package sql-to-csv-xtask -- generate-completions target/assets/completions
    @echo "Man page:    target/assets/man/sql-to-csv.1"
    @echo "Completions: target/assets/completions/"

# Install man page (macOS)
install-man: generate
    cp target/assets/man/sql-to-csv.1 /usr/local/share/man/man1/
    @echo "Installed man page. Run: man sql-to-csv"

# Install fish completions
install-completions-fish: generate
    mkdir -p ~/.config/fish/completions
    cp target/assets/completions/sql-to-csv.fish ~/.config/fish/completions/
    @echo "Installed fish completions"

# Clean build artifacts
clean:
    cargo clean

# Run on a SQL dump file (pass file as argument)
run file *args:
    cargo run --release -- {{file}} {{args}}

# Build release binary and show size
release: build
    @ls -lh target/release/sql-to-csv
    @echo ""
    @file target/release/sql-to-csv

# Strip release binary for smaller size
release-stripped: build
    strip target/release/sql-to-csv
    @ls -lh target/release/sql-to-csv
