class SqlToCsv < Formula
  desc "Fast, parallel SQL dump to PostgreSQL CSV/TSV converter"
  homepage "https://github.com/bmamouri/sql-to-csv"
  license "MIT"
  version "0.1.0"

  on_macos do
    if Hardware::CPU.arm?
      url "https://github.com/bmamouri/sql-to-csv/releases/download/v#{version}/sql-to-csv-aarch64-apple-darwin.tar.gz"
      sha256 "99dd911ba240734fabfbf7f4160374fad632da1416194fbec3ba6246b5b8117e"
    else
      url "https://github.com/bmamouri/sql-to-csv/releases/download/v#{version}/sql-to-csv-x86_64-apple-darwin.tar.gz"
      sha256 "e84ec7c65973db7e7324bb8fc423a32a9cc3276105ffb2eb68e20a61de1b25bd"
    end
  end

  on_linux do
    if Hardware::CPU.arm?
      url "https://github.com/bmamouri/sql-to-csv/releases/download/v#{version}/sql-to-csv-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "7a9c4fe35614a60b5eb3b79a7fe36fe8b59c70242043b9c66444ac62f48a469c"
    else
      url "https://github.com/bmamouri/sql-to-csv/releases/download/v#{version}/sql-to-csv-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "77cf918a239e7f1704f4c69752869b35b36a65e9d4054ed7cfe5aa7bb119bf9c"
    end
  end

  def install
    bin.install "sql-to-csv"
  end

  test do
    assert_match "sql-to-csv", shell_output("#{bin}/sql-to-csv --version")
  end
end
