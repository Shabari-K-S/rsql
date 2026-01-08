mod btree;
mod completer;
mod executor;
mod pager;
mod parser;
mod table;
mod tokenizer;

use colored::Colorize;
use completer::SqlCompleter;
use executor::{ExecuteResult, Executor};
use parser::Parser;
use rustyline::error::ReadlineError;
use rustyline::{Config, Editor};
use tokenizer::Tokenizer;

fn main() {
    let mut executor = Executor::new();

    // Configure rustyline
    let config = Config::builder()
        .history_ignore_space(true)
        .completion_type(rustyline::CompletionType::List)
        .build();

    let mut rl = Editor::with_config(config).expect("Failed to create editor");
    rl.set_helper(Some(SqlCompleter::new()));

    // Load history
    let history_path: std::path::PathBuf = dirs::home_dir()
        .map(|p| p.join(".rsql_history"))
        .unwrap_or_else(|| ".rsql_history".into());
    let _ = rl.load_history(&history_path);

    // Print banner
    println!(
        "{}",
        "╔═══════════════════════════════════════════════════════════╗".cyan()
    );
    println!(
        "{}",
        "║               RSQL - SQLite Clone in Rust                 ║".cyan()
    );
    println!(
        "{}",
        "╠═══════════════════════════════════════════════════════════╣".cyan()
    );
    println!(
        "{}",
        "║  Type SQL commands or .help for available commands        ║".cyan()
    );
    println!(
        "{}",
        "║  Use ↑↓ for history, Tab for completion                   ║".cyan()
    );
    println!(
        "{}",
        "╚═══════════════════════════════════════════════════════════╝".cyan()
    );
    println!();

    loop {
        let prompt = "rsql> ".green().bold().to_string();

        match rl.readline(&prompt) {
            Ok(line) => {
                let input = line.trim();
                if input.is_empty() {
                    continue;
                }

                // Add to history
                let _ = rl.add_history_entry(input);

                // Handle meta commands
                if input.starts_with('.') {
                    handle_meta_command(input, &mut executor, &mut rl, &history_path);
                    continue;
                }

                // Parse and execute SQL
                let mut tokenizer = Tokenizer::new(input);
                let tokens = tokenizer.tokenize();

                let mut parser = Parser::new(tokens);
                match parser.parse() {
                    Ok(stmt) => {
                        // Update completer with new table names
                        match &stmt {
                            parser::Statement::CreateTable(create) => {
                                if let Some(helper) = rl.helper_mut() {
                                    helper.add_table(create.table_name.clone());
                                }
                            }
                            parser::Statement::DropTable(name) => {
                                if let Some(helper) = rl.helper_mut() {
                                    helper.remove_table(name);
                                }
                            }
                            _ => {}
                        }

                        match executor.execute(stmt) {
                            Ok(result) => print_result(result),
                            Err(e) => println!("{} {}", "Error:".red().bold(), e.red()),
                        }
                    }
                    Err(e) => println!("{} {}", "Parse error:".red().bold(), e.red()),
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("{}", "^C".yellow());
                continue;
            }
            Err(ReadlineError::Eof) => {
                println!("{}", "Goodbye!".green());
                break;
            }
            Err(err) => {
                println!("{} {:?}", "Error:".red(), err);
                break;
            }
        }
    }

    // Save history
    let _ = rl.save_history(&history_path);

    // Flush all tables
    for table in executor.tables.values_mut() {
        table.pager.flush_all();
    }
}

fn handle_meta_command(
    input: &str,
    executor: &mut Executor,
    rl: &mut Editor<SqlCompleter, rustyline::history::DefaultHistory>,
    history_path: &std::path::Path,
) {
    match input {
        ".exit" | ".quit" => {
            // Save history before exiting
            let _ = rl.save_history(history_path);
            for table in executor.tables.values_mut() {
                table.pager.flush_all();
            }
            println!("{}", "Goodbye!".green());
            std::process::exit(0);
        }
        ".tables" => {
            if executor.tables.is_empty() {
                println!("{}", "(no tables)".dimmed());
            } else {
                for name in executor.tables.keys() {
                    println!("  {}", name.yellow());
                }
            }
        }
        ".schema" => {
            for (name, table) in &executor.tables {
                print!("{} {} (", "CREATE TABLE".blue(), name.yellow());
                let cols: Vec<String> = table
                    .columns
                    .iter()
                    .map(|c| {
                        let type_str = match &c.data_type {
                            table::DataType::Integer => "INTEGER".to_string(),
                            table::DataType::Text(size) => format!("TEXT({})", size),
                        };
                        format!("{} {}", c.name.cyan(), type_str)
                    })
                    .collect();
                println!("{});", cols.join(", "));
            }
        }
        ".help" => {
            println!("{}", "Meta Commands:".yellow().bold());
            println!("  {}  - Show this help", ".help".cyan());
            println!("  {}  - List all tables", ".tables".cyan());
            println!("  {}  - Show table schemas", ".schema".cyan());
            println!("  {}  - Exit the shell", ".exit".cyan());
            println!();
            println!("{}", "SQL Commands:".yellow().bold());
            println!(
                "  {}",
                "CREATE TABLE name (col1 INTEGER, col2 TEXT)".dimmed()
            );
            println!(
                "  {}",
                "INSERT INTO name VALUES (1, 'value', 'value2')".dimmed()
            );
            println!("  {}", "SELECT * FROM name".dimmed());
            println!("  {}", "SELECT * FROM name WHERE col = value".dimmed());
            println!(
                "  {}",
                "UPDATE name SET col = 'value' WHERE id = 1".dimmed()
            );
            println!("  {}", "DROP TABLE name".dimmed());
            println!();
            println!("{}", "Tips:".yellow().bold());
            println!("  • Use {} for command history", "↑↓".cyan());
            println!("  • Use {} for keyword completion", "Tab".cyan());
            println!("  • Use {} to cancel input", "Ctrl+C".cyan());
            println!("  • Use {} to exit", "Ctrl+D".cyan());
        }
        _ => println!("{} {}", "Unknown command:".red(), input),
    }
}

fn print_result(result: ExecuteResult) {
    match result {
        ExecuteResult::TableCreated(name) => {
            println!("{} Table '{}' created.", "✓".green().bold(), name.yellow());
        }
        ExecuteResult::TableDropped(name) => {
            println!("{} Table '{}' dropped.", "✓".green().bold(), name.yellow());
        }
        ExecuteResult::RowsInserted(count) => {
            println!(
                "{} {} row(s) inserted.",
                "✓".green().bold(),
                count.to_string().cyan()
            );
        }
        ExecuteResult::RowsDeleted(count) => {
            println!(
                "{} {} row(s) deleted.",
                "✓".green().bold(),
                count.to_string().cyan()
            );
        }
        ExecuteResult::RowsUpdated(count) => {
            println!(
                "{} {} row(s) updated.",
                "✓".green().bold(),
                count.to_string().cyan()
            );
        }
        ExecuteResult::Rows { headers, rows } => {
            if rows.is_empty() {
                println!("{}", "(empty result)".dimmed());
            } else {
                // Calculate column widths
                let mut widths: Vec<usize> = headers.iter().map(|h| h.len()).collect();
                for row in &rows {
                    for (i, val) in row.iter().enumerate() {
                        if i < widths.len() && val.len() > widths[i] {
                            widths[i] = val.len();
                        }
                    }
                }

                // Print header separator
                let separator: String = widths
                    .iter()
                    .map(|w| "─".repeat(w + 2))
                    .collect::<Vec<_>>()
                    .join("┼");
                println!("┌{}┐", separator.replace('┼', "┬"));

                // Print headers
                let header_row: Vec<String> = headers
                    .iter()
                    .enumerate()
                    .map(|(i, h)| format!("{:^width$}", h.yellow().bold(), width = widths[i]))
                    .collect();
                println!("│ {} │", header_row.join(" │ "));

                println!("├{}┤", separator);

                // Print data rows
                for row in rows {
                    let formatted: Vec<String> = row
                        .iter()
                        .enumerate()
                        .map(|(i, val)| {
                            let width = widths.get(i).copied().unwrap_or(val.len());
                            format!("{:width$}", val.cyan(), width = width)
                        })
                        .collect();
                    println!("│ {} │", formatted.join(" │ "));
                }

                println!("└{}┘", separator.replace('┼', "┴"));
            }
        }
    }
}
