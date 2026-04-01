#!/bin/bash
set -euo pipefail

GITHUB_USER="bmamouri"
TAP_REPO="homebrew-sql-to-csv"
FORMULA_SRC="$(cd "$(dirname "$0")/.." && pwd)/dist/homebrew/sql-to-csv.rb"

if [ ! -f "$FORMULA_SRC" ]; then
  echo "error: Formula not found at $FORMULA_SRC"
  exit 1
fi

echo "Creating GitHub repo ${GITHUB_USER}/${TAP_REPO}..."
gh repo create "${GITHUB_USER}/${TAP_REPO}" \
  --public \
  --description "Homebrew tap for sql-to-csv" \
  2>/dev/null || echo "Repo already exists, continuing."

TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

echo "Cloning ${GITHUB_USER}/${TAP_REPO}..."
gh repo clone "${GITHUB_USER}/${TAP_REPO}" "$TMPDIR/repo"
cd "$TMPDIR/repo"

mkdir -p Formula
cp "$FORMULA_SRC" Formula/sql-to-csv.rb

git add -A
if git diff --cached --quiet; then
  echo "Formula already up to date."
else
  git commit -m "Update sql-to-csv formula"
  git push origin main
  echo "Pushed updated formula."
fi

echo ""
echo "Done. Users can now run:"
echo "  brew tap ${GITHUB_USER}/sql-to-csv"
echo "  brew install sql-to-csv"
