//! SQL Query Executor - Executes parsed SQL statements

use crate::btree::*;
use crate::parser::*;
use crate::table::{DataType, Table};
use std::ptr;

pub struct Executor {
    pub tables: std::collections::HashMap<String, Table>,
}

impl Executor {
    pub fn new() -> Self {
        Executor {
            tables: std::collections::HashMap::new(),
        }
    }

    pub fn execute(&mut self, stmt: Statement) -> Result<ExecuteResult, String> {
        match stmt {
            Statement::CreateTable(create) => self.execute_create(create),
            Statement::Insert(insert) => self.execute_insert(insert),
            Statement::Select(select) => self.execute_select(select),
            Statement::Delete(delete) => self.execute_delete(delete),
            Statement::Update(update) => self.execute_update(update),
            Statement::DropTable(name) => self.execute_drop(name),
        }
    }

    fn execute_create(&mut self, stmt: CreateTableStmt) -> Result<ExecuteResult, String> {
        if self.tables.contains_key(&stmt.table_name) {
            return Err(format!("Table '{}' already exists", stmt.table_name));
        }

        let raw_cols: Vec<(&str, DataType)> = stmt
            .columns
            .iter()
            .map(|col| {
                let dt = match &col.data_type {
                    SqlType::Integer => DataType::Integer,
                    SqlType::Text(size) => DataType::Text(size.unwrap_or(255)),
                };
                (col.name.as_str(), dt)
            })
            .collect();

        let filename = format!("{}.db", stmt.table_name);
        let _ = std::fs::remove_file(&filename);

        let table = Table::new(&filename, raw_cols);
        let table_name = stmt.table_name.clone();
        self.tables.insert(stmt.table_name, table);

        Ok(ExecuteResult::TableCreated(table_name))
    }

    fn execute_insert(&mut self, stmt: InsertStmt) -> Result<ExecuteResult, String> {
        let table = self
            .tables
            .get_mut(&stmt.table_name)
            .ok_or_else(|| format!("Table '{}' not found", stmt.table_name))?;

        if stmt.values.is_empty() {
            return Err("No values provided".to_string());
        }

        let id = match &stmt.values[0] {
            Value::Integer(n) => *n as u32,
            Value::Text(s) => s.parse::<u32>().map_err(|_| "Invalid ID")?,
            Value::Identifier(s) => s.parse::<u32>().map_err(|_| "Invalid ID")?,
        };

        let mut row_data = vec![0u8; table.row_size];

        for (i, col) in table.columns.iter().enumerate() {
            if col.name == "id" || i == 0 {
                continue;
            }

            let value_idx = i;
            if let Some(value) = stmt.values.get(value_idx) {
                let bytes = match value {
                    Value::Integer(n) => n.to_string().into_bytes(),
                    Value::Text(s) => s.as_bytes().to_vec(),
                    Value::Identifier(s) => s.as_bytes().to_vec(),
                };
                let copy_len = bytes.len().min(col.size);
                row_data[col.offset..col.offset + copy_len].copy_from_slice(&bytes[..copy_len]);
            }
        }

        table.insert(id, &row_data)?;
        Ok(ExecuteResult::RowsInserted(1))
    }

    fn execute_select(&mut self, stmt: SelectStmt) -> Result<ExecuteResult, String> {
        // Check if we have joins
        if !stmt.joins.is_empty() {
            return self.execute_select_with_join(stmt);
        }

        // Original single-table SELECT
        let table = self
            .tables
            .get_mut(&stmt.table_name)
            .ok_or_else(|| format!("Table '{}' not found", stmt.table_name))?;

        let all_rows = table.select_all();

        let col_info: Vec<(String, usize, usize)> = table
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.size, c.offset))
            .collect();

        let pk_col_name = col_info
            .first()
            .map(|(name, _, _)| name.clone())
            .unwrap_or_default();

        let select_cols: Vec<String> = if stmt.columns.is_empty() {
            col_info.iter().map(|(name, _, _)| name.clone()).collect()
        } else {
            stmt.columns.clone()
        };

        let headers: Vec<String> = select_cols.clone();
        let mut results: Vec<Vec<String>> = Vec::new();

        for (id, row_data) in all_rows {
            if let Some(ref where_clause) = stmt.where_clause {
                if !evaluate_where(where_clause, id, &row_data, &col_info, &pk_col_name) {
                    continue;
                }
            }

            let mut row: Vec<String> = Vec::new();
            for col_name in &select_cols {
                if col_name == &pk_col_name {
                    row.push(id.to_string());
                } else if let Some((_, size, offset)) =
                    col_info.iter().find(|(name, _, _)| name == col_name)
                {
                    let data = &row_data[*offset..*offset + *size];
                    let s = String::from_utf8_lossy(data)
                        .trim_matches(char::from(0))
                        .to_string();
                    row.push(s);
                } else {
                    row.push(String::new());
                }
            }
            results.push(row);
        }

        Ok(ExecuteResult::Rows {
            headers,
            rows: results,
        })
    }

    fn execute_select_with_join(&mut self, stmt: SelectStmt) -> Result<ExecuteResult, String> {
        // Get the first join clause (supporting single join for now)
        let join = stmt.joins.first().ok_or("No join clause found")?;
        let left_table_name = stmt.table_name.clone();
        let right_table_name = join.table_name.clone();
        let left_col = join.left_column.clone();
        let right_col = join.right_column.clone();

        // Get left table data
        let left_table = self
            .tables
            .get_mut(&left_table_name)
            .ok_or_else(|| format!("Table '{}' not found", left_table_name))?;

        let left_rows = left_table.select_all();
        let left_col_info: Vec<(String, usize, usize)> = left_table
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.size, c.offset))
            .collect();
        let left_pk = left_col_info
            .first()
            .map(|(name, _, _)| name.clone())
            .unwrap_or_default();

        // Get right table data
        let right_table = self
            .tables
            .get_mut(&right_table_name)
            .ok_or_else(|| format!("Table '{}' not found", right_table_name))?;

        let right_rows = right_table.select_all();
        let right_col_info: Vec<(String, usize, usize)> = right_table
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.size, c.offset))
            .collect();
        let right_pk = right_col_info
            .first()
            .map(|(name, _, _)| name.clone())
            .unwrap_or_default();

        // Build combined column info with table prefixes for headers
        let mut all_headers: Vec<String> = Vec::new();
        let mut all_col_info: Vec<(String, String, usize, usize, bool)> = Vec::new(); // (col_name, table, size, offset, is_left)

        for (name, size, offset) in &left_col_info {
            all_headers.push(format!("{}.{}", left_table_name, name));
            all_col_info.push((name.clone(), left_table_name.clone(), *size, *offset, true));
        }
        for (name, size, offset) in &right_col_info {
            all_headers.push(format!("{}.{}", right_table_name, name));
            all_col_info.push((
                name.clone(),
                right_table_name.clone(),
                *size,
                *offset,
                false,
            ));
        }

        // Determine which columns to select
        let select_cols: Vec<String> = if stmt.columns.is_empty() {
            all_headers.clone()
        } else {
            stmt.columns.clone()
        };

        let mut results: Vec<Vec<String>> = Vec::new();

        // Nested-loop join
        for (left_id, left_data) in &left_rows {
            // Get left join column value
            let left_val =
                get_column_value(&left_col, *left_id, left_data, &left_col_info, &left_pk);

            for (right_id, right_data) in &right_rows {
                // Get right join column value
                let right_val = get_column_value(
                    &right_col,
                    *right_id,
                    right_data,
                    &right_col_info,
                    &right_pk,
                );

                // Check join condition
                if left_val == right_val {
                    let mut row: Vec<String> = Vec::new();

                    for col_name in &select_cols {
                        // Try to find column in the combined info
                        if let Some((_, table, size, offset, is_left)) =
                            all_col_info.iter().find(|(name, tbl, _, _, _)| {
                                col_name == &format!("{}.{}", tbl, name) || col_name == name
                            })
                        {
                            if *is_left {
                                if col_name.contains(&left_pk) || col_name == &left_pk {
                                    row.push(left_id.to_string());
                                } else {
                                    let data = &left_data[*offset..*offset + *size];
                                    let s = String::from_utf8_lossy(data)
                                        .trim_matches(char::from(0))
                                        .to_string();
                                    row.push(s);
                                }
                            } else {
                                if col_name.contains(&right_pk) || col_name == &right_pk {
                                    row.push(right_id.to_string());
                                } else {
                                    let data = &right_data[*offset..*offset + *size];
                                    let s = String::from_utf8_lossy(data)
                                        .trim_matches(char::from(0))
                                        .to_string();
                                    row.push(s);
                                }
                            }
                        } else {
                            row.push(String::new());
                        }
                    }

                    results.push(row);
                }
            }
        }

        Ok(ExecuteResult::Rows {
            headers: select_cols,
            rows: results,
        })
    }

    fn execute_delete(&mut self, stmt: DeleteStmt) -> Result<ExecuteResult, String> {
        let table = self
            .tables
            .get_mut(&stmt.table_name)
            .ok_or_else(|| format!("Table '{}' not found", stmt.table_name))?;

        let all_rows = table.select_all();
        let col_info: Vec<(String, usize, usize)> = table
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.size, c.offset))
            .collect();

        let pk_col_name = col_info
            .first()
            .map(|(name, _, _)| name.as_str())
            .unwrap_or("id");

        let mut ids_to_delete = Vec::new();

        for (id, row_data) in all_rows {
            let should_delete = match &stmt.where_clause {
                Some(where_clause) => {
                    evaluate_where(where_clause, id, &row_data, &col_info, pk_col_name)
                }
                None => true,
            };

            if should_delete {
                ids_to_delete.push(id);
            }
        }

        let count = ids_to_delete.len();

        // Delete each matching row
        for id in ids_to_delete {
            table.delete(id)?;
        }

        Ok(ExecuteResult::RowsDeleted(count))
    }

    fn execute_update(&mut self, stmt: UpdateStmt) -> Result<ExecuteResult, String> {
        let table = self
            .tables
            .get_mut(&stmt.table_name)
            .ok_or_else(|| format!("Table '{}' not found", stmt.table_name))?;

        let all_rows = table.select_all();
        let col_info: Vec<(String, usize, usize)> = table
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.size, c.offset))
            .collect();
        let cell_size = table.cell_size;
        let pk_col_name = col_info
            .first()
            .map(|(name, _, _)| name.as_str())
            .unwrap_or("id");

        // First, collect IDs to update
        let mut ids_to_update = Vec::new();
        for (id, row_data) in &all_rows {
            let should_update = match &stmt.where_clause {
                Some(where_clause) => {
                    evaluate_where(where_clause, *id, row_data, &col_info, pk_col_name)
                }
                None => true,
            };
            if should_update {
                ids_to_update.push(*id);
            }
        }

        let mut count = 0;

        // Now perform updates
        for id in ids_to_update {
            let leaf_page_num = table.find_leaf(id);
            let (slot, exists) = table.leaf_node_find(leaf_page_num, id);

            if exists {
                let page = table.pager.get_page(leaf_page_num as usize);
                let cell_ptr = leaf_node_cell(page, slot, cell_size);

                for (col_name, value) in &stmt.assignments {
                    if let Some((_, size, offset)) =
                        col_info.iter().find(|(name, _, _)| name == col_name)
                    {
                        let bytes = match value {
                            Value::Integer(n) => n.to_string().into_bytes(),
                            Value::Text(s) => s.as_bytes().to_vec(),
                            Value::Identifier(s) => s.as_bytes().to_vec(),
                        };

                        unsafe {
                            let row_ptr = cell_ptr.add(4);
                            let dest = row_ptr.add(*offset);
                            ptr::write_bytes(dest, 0, *size);
                            ptr::copy_nonoverlapping(bytes.as_ptr(), dest, bytes.len().min(*size));
                        }
                    }
                }

                table.pager.flush(leaf_page_num as usize);
                count += 1;
            }
        }

        Ok(ExecuteResult::RowsUpdated(count))
    }

    fn execute_drop(&mut self, table_name: String) -> Result<ExecuteResult, String> {
        if self.tables.remove(&table_name).is_some() {
            let filename = format!("{}.db", table_name);
            let _ = std::fs::remove_file(&filename);
            Ok(ExecuteResult::TableDropped(table_name))
        } else {
            Err(format!("Table '{}' not found", table_name))
        }
    }
}

// Standalone function to avoid borrow checker issues
fn evaluate_where(
    where_clause: &WhereClause,
    id: u32,
    row_data: &[u8],
    col_info: &[(String, usize, usize)],
    pk_col_name: &str,
) -> bool {
    if where_clause.conditions.is_empty() {
        return true;
    }

    let mut results: Vec<bool> = Vec::new();

    for condition in &where_clause.conditions {
        let col_value = if condition.column == pk_col_name {
            // Primary key is stored as B-Tree key
            id.to_string()
        } else if let Some((_, size, offset)) = col_info
            .iter()
            .find(|(name, _, _)| name == &condition.column)
        {
            let data = &row_data[*offset..*offset + *size];
            String::from_utf8_lossy(data)
                .trim_matches(char::from(0))
                .to_string()
        } else {
            continue;
        };

        let cond_value = match &condition.value {
            Value::Integer(n) => n.to_string(),
            Value::Text(s) => s.clone(),
            Value::Identifier(s) => s.clone(),
        };

        let result = match condition.operator {
            CompareOp::Equals => col_value == cond_value,
            CompareOp::NotEquals => col_value != cond_value,
            CompareOp::LessThan => col_value
                .parse::<i64>()
                .ok()
                .and_then(|a| cond_value.parse::<i64>().ok().map(|b| a < b))
                .unwrap_or(col_value < cond_value),
            CompareOp::GreaterThan => col_value
                .parse::<i64>()
                .ok()
                .and_then(|a| cond_value.parse::<i64>().ok().map(|b| a > b))
                .unwrap_or(col_value > cond_value),
            CompareOp::LessEquals => col_value
                .parse::<i64>()
                .ok()
                .and_then(|a| cond_value.parse::<i64>().ok().map(|b| a <= b))
                .unwrap_or(col_value <= cond_value),
            CompareOp::GreaterEquals => col_value
                .parse::<i64>()
                .ok()
                .and_then(|a| cond_value.parse::<i64>().ok().map(|b| a >= b))
                .unwrap_or(col_value >= cond_value),
        };

        results.push(result);
    }

    if results.is_empty() {
        return true;
    }

    let mut final_result = results[0];
    for (i, op) in where_clause.operators.iter().enumerate() {
        if i + 1 < results.len() {
            match op {
                LogicalOp::And => final_result = final_result && results[i + 1],
                LogicalOp::Or => final_result = final_result || results[i + 1],
            }
        }
    }

    final_result
}

// Helper function to get column value for JOIN condition
fn get_column_value(
    col_name: &str,
    id: u32,
    row_data: &[u8],
    col_info: &[(String, usize, usize)],
    pk_col_name: &str,
) -> String {
    if col_name == pk_col_name {
        id.to_string()
    } else if let Some((_, size, offset)) = col_info.iter().find(|(name, _, _)| name == col_name) {
        let data = &row_data[*offset..*offset + *size];
        String::from_utf8_lossy(data)
            .trim_matches(char::from(0))
            .to_string()
    } else {
        String::new()
    }
}

#[derive(Debug)]
pub enum ExecuteResult {
    TableCreated(String),
    TableDropped(String),
    RowsInserted(usize),
    RowsDeleted(usize),
    RowsUpdated(usize),
    Rows {
        headers: Vec<String>,
        rows: Vec<Vec<String>>,
    },
}
