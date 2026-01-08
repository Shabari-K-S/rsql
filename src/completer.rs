//! SQL Keyword Completer for Tab Completion

use rustyline::completion::{Completer, Pair};
use rustyline::error::ReadlineError;
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::Validator;
use rustyline::Context;
use rustyline::Helper;

pub struct SqlCompleter {
    pub table_names: Vec<String>,
}

impl SqlCompleter {
    pub fn new() -> Self {
        SqlCompleter {
            table_names: Vec::new(),
        }
    }

    pub fn add_table(&mut self, name: String) {
        if !self.table_names.contains(&name) {
            self.table_names.push(name);
        }
    }

    pub fn remove_table(&mut self, name: &str) {
        self.table_names.retain(|n| n != name);
    }
}

const SQL_KEYWORDS: &[&str] = &[
    "SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "TABLE", "FROM", "WHERE", "INTO",
    "VALUES", "SET", "AND", "OR", "NOT", "INTEGER", "TEXT", "VARCHAR", "PRIMARY", "KEY", "NULL",
    "DEFAULT", "ORDER", "BY", "ASC", "DESC", "LIMIT", "OFFSET", "JOIN", "ON", "LEFT", "RIGHT",
    "INNER", "OUTER", "GROUP", "HAVING", "COUNT", "SUM", "AVG", "MIN", "MAX", "DISTINCT", "AS",
    "LIKE", "IN", "BETWEEN", "EXISTS", "CASE", "WHEN", "THEN", "ELSE", "END",
];

impl Completer for SqlCompleter {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &Context<'_>,
    ) -> Result<(usize, Vec<Pair>), ReadlineError> {
        let line_to_cursor = &line[..pos];

        // Find the start of the current word
        let word_start = line_to_cursor
            .rfind(|c: char| c.is_whitespace() || c == '(' || c == ',')
            .map(|i| i + 1)
            .unwrap_or(0);

        let word = &line_to_cursor[word_start..];
        let word_upper = word.to_uppercase();

        let mut matches = Vec::new();

        // Match SQL keywords
        for keyword in SQL_KEYWORDS {
            if keyword.starts_with(&word_upper) && !word.is_empty() {
                matches.push(Pair {
                    display: keyword.to_string(),
                    replacement: keyword.to_string(),
                });
            }
        }

        // Match table names (case-insensitive)
        for table in &self.table_names {
            if table.to_uppercase().starts_with(&word_upper) && !word.is_empty() {
                matches.push(Pair {
                    display: table.clone(),
                    replacement: table.clone(),
                });
            }
        }

        Ok((word_start, matches))
    }
}

impl Hinter for SqlCompleter {
    type Hint = String;
}

impl Highlighter for SqlCompleter {}
impl Validator for SqlCompleter {}
impl Helper for SqlCompleter {}
