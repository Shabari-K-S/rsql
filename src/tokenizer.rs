//! SQL Tokenizer - Lexical analysis for SQL statements

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // Keywords
    Select,
    Insert,
    Into,
    Values,
    From,
    Where,
    And,
    Or,
    Create,
    Table,
    Delete,
    Update,
    Set,
    Drop,

    // Data types
    Integer,
    Text,

    // Literals
    Identifier(String),
    StringLiteral(String),
    Number(i64),

    // Operators
    Equals,        // =
    NotEquals,     // != or <>
    LessThan,      // <
    GreaterThan,   // >
    LessEquals,    // <=
    GreaterEquals, // >=

    // Punctuation
    Comma,
    Semicolon,
    LeftParen,
    RightParen,
    Asterisk,

    // End of input
    Eof,
}

pub struct Tokenizer {
    input: Vec<char>,
    pos: usize,
}

impl Tokenizer {
    pub fn new(input: &str) -> Self {
        Tokenizer {
            input: input.chars().collect(),
            pos: 0,
        }
    }

    fn peek(&self) -> Option<char> {
        self.input.get(self.pos).copied()
    }

    fn advance(&mut self) -> Option<char> {
        let ch = self.peek();
        self.pos += 1;
        ch
    }

    fn skip_whitespace(&mut self) {
        while let Some(ch) = self.peek() {
            if ch.is_whitespace() {
                self.advance();
            } else {
                break;
            }
        }
    }

    fn read_identifier(&mut self) -> String {
        let mut ident = String::new();
        while let Some(ch) = self.peek() {
            if ch.is_alphanumeric() || ch == '_' {
                ident.push(ch);
                self.advance();
            } else {
                break;
            }
        }
        ident
    }

    fn read_number(&mut self) -> i64 {
        let mut num_str = String::new();
        while let Some(ch) = self.peek() {
            if ch.is_ascii_digit() {
                num_str.push(ch);
                self.advance();
            } else {
                break;
            }
        }
        num_str.parse().unwrap_or(0)
    }

    fn read_string(&mut self, quote: char) -> String {
        let mut s = String::new();
        self.advance(); // consume opening quote
        while let Some(ch) = self.peek() {
            if ch == quote {
                self.advance(); // consume closing quote
                break;
            }
            s.push(ch);
            self.advance();
        }
        s
    }

    pub fn next_token(&mut self) -> Token {
        self.skip_whitespace();

        match self.peek() {
            None => Token::Eof,
            Some(ch) => match ch {
                ',' => {
                    self.advance();
                    Token::Comma
                }
                ';' => {
                    self.advance();
                    Token::Semicolon
                }
                '(' => {
                    self.advance();
                    Token::LeftParen
                }
                ')' => {
                    self.advance();
                    Token::RightParen
                }
                '*' => {
                    self.advance();
                    Token::Asterisk
                }
                '=' => {
                    self.advance();
                    Token::Equals
                }
                '<' => {
                    self.advance();
                    match self.peek() {
                        Some('=') => {
                            self.advance();
                            Token::LessEquals
                        }
                        Some('>') => {
                            self.advance();
                            Token::NotEquals
                        }
                        _ => Token::LessThan,
                    }
                }
                '>' => {
                    self.advance();
                    if self.peek() == Some('=') {
                        self.advance();
                        Token::GreaterEquals
                    } else {
                        Token::GreaterThan
                    }
                }
                '!' => {
                    self.advance();
                    if self.peek() == Some('=') {
                        self.advance();
                        Token::NotEquals
                    } else {
                        Token::Identifier("!".to_string())
                    }
                }
                '\'' | '"' => Token::StringLiteral(self.read_string(ch)),
                c if c.is_ascii_digit() => Token::Number(self.read_number()),
                c if c.is_alphabetic() || c == '_' => {
                    let ident = self.read_identifier();
                    match ident.to_uppercase().as_str() {
                        "SELECT" => Token::Select,
                        "INSERT" => Token::Insert,
                        "INTO" => Token::Into,
                        "VALUES" => Token::Values,
                        "FROM" => Token::From,
                        "WHERE" => Token::Where,
                        "AND" => Token::And,
                        "OR" => Token::Or,
                        "CREATE" => Token::Create,
                        "TABLE" => Token::Table,
                        "DELETE" => Token::Delete,
                        "UPDATE" => Token::Update,
                        "SET" => Token::Set,
                        "DROP" => Token::Drop,
                        "INTEGER" | "INT" => Token::Integer,
                        "TEXT" | "VARCHAR" => Token::Text,
                        _ => Token::Identifier(ident),
                    }
                }
                _ => {
                    self.advance();
                    self.next_token()
                }
            },
        }
    }

    pub fn tokenize(&mut self) -> Vec<Token> {
        let mut tokens = Vec::new();
        loop {
            let token = self.next_token();
            if token == Token::Eof {
                tokens.push(token);
                break;
            }
            tokens.push(token);
        }
        tokens
    }
}
