//! SQL Parser - Parses tokens into an Abstract Syntax Tree

use crate::tokenizer::Token;

#[derive(Debug, Clone)]
pub enum Statement {
    CreateTable(CreateTableStmt),
    Insert(InsertStmt),
    Select(SelectStmt),
    Delete(DeleteStmt),
    Update(UpdateStmt),
    DropTable(String),
}

#[derive(Debug, Clone)]
pub struct CreateTableStmt {
    pub table_name: String,
    pub columns: Vec<ColumnDef>,
}

#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: SqlType,
}

#[derive(Debug, Clone)]
pub enum SqlType {
    Integer,
    Text(Option<u32>), // Optional size for VARCHAR(n)
}

#[derive(Debug, Clone)]
pub struct InsertStmt {
    pub table_name: String,
    pub columns: Option<Vec<String>>,
    pub values: Vec<Value>,
}

#[derive(Debug, Clone)]
pub enum Value {
    Integer(i64),
    Text(String),
    Identifier(String),
}

#[derive(Debug, Clone)]
pub struct SelectStmt {
    pub columns: Vec<String>, // Empty = *, otherwise column names
    pub table_name: String,
    pub joins: Vec<JoinClause>,
    pub where_clause: Option<WhereClause>,
}

#[derive(Debug, Clone)]
pub struct JoinClause {
    pub join_type: JoinType,
    pub table_name: String,
    pub left_column: String,
    pub right_column: String,
}

#[derive(Debug, Clone)]
pub enum JoinType {
    Inner,
}

#[derive(Debug, Clone)]
pub struct DeleteStmt {
    pub table_name: String,
    pub where_clause: Option<WhereClause>,
}

#[derive(Debug, Clone)]
pub struct UpdateStmt {
    pub table_name: String,
    pub assignments: Vec<(String, Value)>,
    pub where_clause: Option<WhereClause>,
}

#[derive(Debug, Clone)]
pub struct WhereClause {
    pub conditions: Vec<Condition>,
    pub operators: Vec<LogicalOp>,
}

#[derive(Debug, Clone)]
pub enum LogicalOp {
    And,
    Or,
}

#[derive(Debug, Clone)]
pub struct Condition {
    pub column: String,
    pub operator: CompareOp,
    pub value: Value,
}

#[derive(Debug, Clone)]
pub enum CompareOp {
    Equals,
    NotEquals,
    LessThan,
    GreaterThan,
    LessEquals,
    GreaterEquals,
}

pub struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    pub fn new(tokens: Vec<Token>) -> Self {
        Parser { tokens, pos: 0 }
    }

    fn peek(&self) -> &Token {
        self.tokens.get(self.pos).unwrap_or(&Token::Eof)
    }

    fn advance(&mut self) -> Token {
        let token = self.tokens.get(self.pos).cloned().unwrap_or(Token::Eof);
        self.pos += 1;
        token
    }

    fn expect(&mut self, expected: Token) -> Result<(), String> {
        let token = self.advance();
        if std::mem::discriminant(&token) == std::mem::discriminant(&expected) {
            Ok(())
        } else {
            Err(format!("Expected {:?}, got {:?}", expected, token))
        }
    }

    fn expect_identifier(&mut self) -> Result<String, String> {
        match self.advance() {
            Token::Identifier(name) => Ok(name),
            other => Err(format!("Expected identifier, got {:?}", other)),
        }
    }

    pub fn parse(&mut self) -> Result<Statement, String> {
        match self.peek() {
            Token::Create => self.parse_create(),
            Token::Insert => self.parse_insert(),
            Token::Select => self.parse_select(),
            Token::Delete => self.parse_delete(),
            Token::Update => self.parse_update(),
            Token::Drop => self.parse_drop(),
            other => Err(format!("Unexpected token: {:?}", other)),
        }
    }

    fn parse_create(&mut self) -> Result<Statement, String> {
        self.advance(); // consume CREATE
        self.expect(Token::Table)?;

        let table_name = self.expect_identifier()?;
        self.expect(Token::LeftParen)?;

        let mut columns = Vec::new();
        loop {
            let col_name = self.expect_identifier()?;
            let data_type = match self.advance() {
                Token::Integer => SqlType::Integer,
                Token::Text => {
                    // Check for optional size: TEXT(32)
                    if *self.peek() == Token::LeftParen {
                        self.advance();
                        if let Token::Number(n) = self.advance() {
                            self.expect(Token::RightParen)?;
                            SqlType::Text(Some(n as u32))
                        } else {
                            SqlType::Text(None)
                        }
                    } else {
                        SqlType::Text(None)
                    }
                }
                other => return Err(format!("Expected data type, got {:?}", other)),
            };

            columns.push(ColumnDef {
                name: col_name,
                data_type,
            });

            match self.peek() {
                Token::Comma => {
                    self.advance();
                }
                Token::RightParen => {
                    self.advance();
                    break;
                }
                other => return Err(format!("Expected ',' or ')', got {:?}", other)),
            }
        }

        Ok(Statement::CreateTable(CreateTableStmt {
            table_name,
            columns,
        }))
    }

    fn parse_insert(&mut self) -> Result<Statement, String> {
        self.advance(); // consume INSERT
        self.expect(Token::Into)?;

        let table_name = self.expect_identifier()?;

        // Optional column list
        let columns = if *self.peek() == Token::LeftParen {
            self.advance();
            let mut cols = Vec::new();
            loop {
                cols.push(self.expect_identifier()?);
                match self.peek() {
                    Token::Comma => {
                        self.advance();
                    }
                    Token::RightParen => {
                        self.advance();
                        break;
                    }
                    other => return Err(format!("Expected ',' or ')', got {:?}", other)),
                }
            }
            Some(cols)
        } else {
            None
        };

        self.expect(Token::Values)?;
        self.expect(Token::LeftParen)?;

        let mut values = Vec::new();
        loop {
            let value = match self.advance() {
                Token::Number(n) => Value::Integer(n),
                Token::StringLiteral(s) => Value::Text(s),
                Token::Identifier(s) => Value::Identifier(s),
                other => return Err(format!("Expected value, got {:?}", other)),
            };
            values.push(value);

            match self.peek() {
                Token::Comma => {
                    self.advance();
                }
                Token::RightParen => {
                    self.advance();
                    break;
                }
                other => return Err(format!("Expected ',' or ')', got {:?}", other)),
            }
        }

        Ok(Statement::Insert(InsertStmt {
            table_name,
            columns,
            values,
        }))
    }

    fn parse_select(&mut self) -> Result<Statement, String> {
        self.advance(); // consume SELECT

        let columns = if *self.peek() == Token::Asterisk {
            self.advance();
            Vec::new() // * means all columns
        } else {
            let mut cols = Vec::new();
            loop {
                cols.push(self.expect_identifier()?);
                if *self.peek() == Token::Comma {
                    self.advance();
                } else {
                    break;
                }
            }
            cols
        };

        self.expect(Token::From)?;
        let table_name = self.expect_identifier()?;

        // Parse JOINs
        let mut joins = Vec::new();
        while *self.peek() == Token::Inner || *self.peek() == Token::Join {
            // Handle optional INNER keyword
            if *self.peek() == Token::Inner {
                self.advance();
            }
            self.expect(Token::Join)?;

            let join_table = self.expect_identifier()?;
            self.expect(Token::On)?;

            let left_column = self.expect_identifier()?;
            self.expect(Token::Equals)?;
            let right_column = self.expect_identifier()?;

            joins.push(JoinClause {
                join_type: JoinType::Inner,
                table_name: join_table,
                left_column,
                right_column,
            });
        }

        let where_clause = if *self.peek() == Token::Where {
            Some(self.parse_where()?)
        } else {
            None
        };

        Ok(Statement::Select(SelectStmt {
            columns,
            table_name,
            joins,
            where_clause,
        }))
    }

    fn parse_delete(&mut self) -> Result<Statement, String> {
        self.advance(); // consume DELETE
        self.expect(Token::From)?;

        let table_name = self.expect_identifier()?;

        let where_clause = if *self.peek() == Token::Where {
            Some(self.parse_where()?)
        } else {
            None
        };

        Ok(Statement::Delete(DeleteStmt {
            table_name,
            where_clause,
        }))
    }

    fn parse_update(&mut self) -> Result<Statement, String> {
        self.advance(); // consume UPDATE
        let table_name = self.expect_identifier()?;
        self.expect(Token::Set)?;

        let mut assignments = Vec::new();
        loop {
            let col_name = self.expect_identifier()?;
            self.expect(Token::Equals)?;
            let value = match self.advance() {
                Token::Number(n) => Value::Integer(n),
                Token::StringLiteral(s) => Value::Text(s),
                Token::Identifier(s) => Value::Identifier(s),
                other => return Err(format!("Expected value, got {:?}", other)),
            };
            assignments.push((col_name, value));

            if *self.peek() == Token::Comma {
                self.advance();
            } else {
                break;
            }
        }

        let where_clause = if *self.peek() == Token::Where {
            Some(self.parse_where()?)
        } else {
            None
        };

        Ok(Statement::Update(UpdateStmt {
            table_name,
            assignments,
            where_clause,
        }))
    }

    fn parse_drop(&mut self) -> Result<Statement, String> {
        self.advance(); // consume DROP
        self.expect(Token::Table)?;
        let table_name = self.expect_identifier()?;
        Ok(Statement::DropTable(table_name))
    }

    fn parse_where(&mut self) -> Result<WhereClause, String> {
        self.advance(); // consume WHERE

        let mut conditions = Vec::new();
        let mut operators = Vec::new();

        loop {
            let column = self.expect_identifier()?;

            let operator = match self.advance() {
                Token::Equals => CompareOp::Equals,
                Token::NotEquals => CompareOp::NotEquals,
                Token::LessThan => CompareOp::LessThan,
                Token::GreaterThan => CompareOp::GreaterThan,
                Token::LessEquals => CompareOp::LessEquals,
                Token::GreaterEquals => CompareOp::GreaterEquals,
                other => return Err(format!("Expected comparison operator, got {:?}", other)),
            };

            let value = match self.advance() {
                Token::Number(n) => Value::Integer(n),
                Token::StringLiteral(s) => Value::Text(s),
                Token::Identifier(s) => Value::Identifier(s),
                other => return Err(format!("Expected value, got {:?}", other)),
            };

            conditions.push(Condition {
                column,
                operator,
                value,
            });

            match self.peek() {
                Token::And => {
                    self.advance();
                    operators.push(LogicalOp::And);
                }
                Token::Or => {
                    self.advance();
                    operators.push(LogicalOp::Or);
                }
                _ => break,
            }
        }

        Ok(WhereClause {
            conditions,
            operators,
        })
    }
}
