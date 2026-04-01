use std::fs;
use std::path::PathBuf;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: xtask <command> <output-dir>");
        eprintln!("Commands:");
        eprintln!("  generate-man <dir>          Generate man page");
        eprintln!("  generate-completions <dir>  Generate shell completions");
        std::process::exit(1);
    }

    let command = &args[1];
    let out_dir = PathBuf::from(&args[2]);
    fs::create_dir_all(&out_dir).expect("Failed to create output directory");

    match command.as_str() {
        "generate-man" => generate_man(&out_dir),
        "generate-completions" => generate_completions(&out_dir),
        _ => {
            eprintln!("Unknown command: {command}");
            std::process::exit(1);
        }
    }
}

fn generate_man(out_dir: &PathBuf) {
    let cmd = sql_to_csv::cli::command();
    let man = clap_mangen::Man::new(cmd);
    let mut buf = Vec::new();
    man.render(&mut buf).expect("Failed to render man page");
    let path = out_dir.join("sql-to-csv.1");
    fs::write(&path, buf).expect("Failed to write man page");
    eprintln!("Generated: {}", path.display());
}

fn generate_completions(out_dir: &PathBuf) {
    use clap_complete::{generate_to, Shell};

    let mut cmd = sql_to_csv::cli::command();

    for shell in [Shell::Bash, Shell::Zsh, Shell::Fish] {
        let path = generate_to(shell, &mut cmd, "sql-to-csv", out_dir)
            .expect("Failed to generate completions");
        eprintln!("Generated: {}", path.display());
    }
}
