//! SQL Query Executor - Executes parsed SQL statements

use crate::btree::*;
use crate::index::Index;
use crate::parser::*;
use crate::table::{DataType, Table};
use std::collections::HashMap;
use std::path::PathBuf;
use std::ptr;

pub struct Executor {
    pub tables: HashMap<String, Table>,
    pub in_transaction: bool,
    pub current_db: Option<String>,
    pub db_base_path: PathBuf,
}

impl Executor {
    pub fn new() -> Self {
        let db_base_path = dirs::home_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join(".rsql")
            .join("databases");

        // Create base directory if it doesn't exist
        let _ = std::fs::create_dir_all(&db_base_path);

        Executor {
            tables: HashMap::new(),
            in_transaction: false,
            current_db: None,
            db_base_path,
        }
    }

    /// Get the current database path, if connected
    pub fn get_db_path(&self) -> Option<PathBuf> {
        self.current_db
            .as_ref()
            .map(|db| self.db_base_path.join(db))
    }

    /// Require a database connection for operations
    fn require_connection(&self) -> Result<PathBuf, String> {
        self.get_db_path()
            .ok_or_else(|| "No database connected. Use: CONNECT db_name".to_string())
    }

    pub fn execute(&mut self, stmt: Statement) -> Result<ExecuteResult, String> {
        match stmt {
            Statement::CreateDatabase(name) => self.execute_create_database(name),
            Statement::Connect(name) => self.execute_connect(name),
            Statement::CreateTable(create) => self.execute_create(create),
            Statement::CreateIndex(create_idx) => self.execute_create_index(create_idx),
            Statement::Insert(insert) => self.execute_insert(insert),
            Statement::Select(select) => self.execute_select(select),
            Statement::Delete(delete) => self.execute_delete(delete),
            Statement::Update(update) => self.execute_update(update),
            Statement::DropTable(name) => self.execute_drop(name),
            Statement::DropIndex(name) => self.execute_drop_index(name),
            Statement::Begin => self.execute_begin(),
            Statement::Commit => self.execute_commit(),
            Statement::Rollback => self.execute_rollback(),
        }
    }

    fn execute_create_database(&mut self, name: String) -> Result<ExecuteResult, String> {
        let db_path = self.db_base_path.join(&name);

        if db_path.exists() {
            return Err(format!("Database '{}' already exists", name));
        }

        std::fs::create_dir_all(&db_path)
            .map_err(|e| format!("Failed to create database: {}", e))?;

        // Create empty metadata file
        let metadata_path = db_path.join("metadata.json");
        std::fs::write(&metadata_path, "{\"tables\":{}}")
            .map_err(|e| format!("Failed to create metadata: {}", e))?;

        Ok(ExecuteResult::DatabaseCreated(name))
    }

    fn execute_connect(&mut self, name: String) -> Result<ExecuteResult, String> {
        let db_path = self.db_base_path.join(&name);

        if !db_path.exists() {
            return Err(format!(
                "Database '{}' does not exist. Use: CREATE DATABASE {}",
                name, name
            ));
        }

        // Clear existing tables
        self.tables.clear();
        self.current_db = Some(name.clone());

        // Load metadata and restore tables
        self.load_metadata()?;

        Ok(ExecuteResult::DatabaseConnected(name))
    }

    fn save_metadata(&self) -> Result<(), String> {
        let db_path = match self.get_db_path() {
            Some(p) => p,
            None => return Ok(()), // No database connected, nothing to save
        };

        let metadata_path = db_path.join("metadata.json");

        // Build metadata JSON
        let mut tables_json = String::from("{\"tables\":{");
        let mut first = true;

        for (name, table) in &self.tables {
            if !first {
                tables_json.push(',');
            }
            first = false;

            tables_json.push_str(&format!("\"{}\":{{\"columns\":[", name));

            let mut col_first = true;
            for col in &table.columns {
                if !col_first {
                    tables_json.push(',');
                }
                col_first = false;

                let type_str = match &col.data_type {
                    DataType::Integer => "\"INTEGER\"".to_string(),
                    DataType::Text(size) => format!("\"TEXT({})\"", size),
                };
                tables_json.push_str(&format!(
                    "{{\"name\":\"{}\",\"type\":{}}}",
                    col.name, type_str
                ));
            }

            tables_json.push_str("],\"indexes\":[");

            let mut idx_first = true;
            for (idx_name, idx) in &table.indexes {
                if !idx_first {
                    tables_json.push(',');
                }
                idx_first = false;
                tables_json.push_str(&format!(
                    "{{\"name\":\"{}\",\"column\":\"{}\",\"unique\":{}}}",
                    idx_name, idx.column_name, idx.unique
                ));
            }

            tables_json.push_str("]}");
        }

        tables_json.push_str("}}");

        std::fs::write(&metadata_path, &tables_json)
            .map_err(|e| format!("Failed to save metadata: {}", e))?;

        Ok(())
    }

    fn load_metadata(&mut self) -> Result<(), String> {
        let db_path = match self.get_db_path() {
            Some(p) => p,
            None => return Err("No database connected".to_string()),
        };

        let metadata_path = db_path.join("metadata.json");

        if !metadata_path.exists() {
            return Ok(()); // No metadata yet
        }

        let content = std::fs::read_to_string(&metadata_path)
            .map_err(|e| format!("Failed to read metadata: {}", e))?;

        // Simple JSON parsing (avoiding external dependencies)
        // Format: {"tables":{"tablename":{"columns":[{"name":"col","type":"INTEGER"}],"indexes":[]}}}

        // Extract table entries
        if let Some(tables_start) = content.find("\"tables\":{") {
            let tables_content = &content[tables_start + 10..];

            // Parse each table
            let mut pos = 0;
            while let Some(name_start) = tables_content[pos..].find('"') {
                let actual_start = pos + name_start + 1;
                if let Some(name_end) = tables_content[actual_start..].find('"') {
                    let table_name = &tables_content[actual_start..actual_start + name_end];

                    if table_name == "}" || table_name.is_empty() {
                        break;
                    }

                    // Find columns array
                    if let Some(cols_start) = tables_content[actual_start..].find("\"columns\":[") {
                        let cols_section = &tables_content[actual_start + cols_start..];

                        // Parse columns
                        let mut columns: Vec<(&str, DataType)> = Vec::new();
                        let mut col_pos = 11; // After "columns":[

                        while let Some(col_start) = cols_section[col_pos..].find("{\"name\":\"") {
                            let cn_start = col_pos + col_start + 9;
                            if let Some(cn_end) = cols_section[cn_start..].find('"') {
                                let col_name = &cols_section[cn_start..cn_start + cn_end];

                                // Find type
                                let type_start = cn_start + cn_end;
                                if let Some(t_start) = cols_section[type_start..].find("\"type\":")
                                {
                                    let t_section = &cols_section[type_start + t_start + 7..];

                                    let data_type = if t_section.starts_with("\"INTEGER\"") {
                                        DataType::Integer
                                    } else if t_section.starts_with("\"TEXT(") {
                                        // Extract size
                                        if let Some(size_end) = t_section[6..].find(')') {
                                            let size: u32 =
                                                t_section[6..6 + size_end].parse().unwrap_or(255);
                                            DataType::Text(size)
                                        } else {
                                            DataType::Text(255)
                                        }
                                    } else {
                                        DataType::Text(255)
                                    };

                                    columns.push((
                                        Box::leak(col_name.to_string().into_boxed_str()),
                                        data_type,
                                    ));
                                }
                            }
                            col_pos = cn_start + 1;

                            // Check if we've reached the end of columns array
                            if cols_section[col_pos..].starts_with(']') {
                                break;
                            }
                        }

                        if !columns.is_empty() {
                            // Create table from stored data
                            let table_file = db_path.join(format!("{}.db", table_name));
                            let table = Table::new(table_file.to_str().unwrap(), columns);
                            self.tables.insert(table_name.to_string(), table);
                        }
                    }

                    // Move to next table
                    pos = actual_start + name_end + 1;
                    if let Some(next) = tables_content[pos..].find('}') {
                        pos += next + 1;
                    }
                } else {
                    break;
                }
            }
        }

        Ok(())
    }

    fn execute_begin(&mut self) -> Result<ExecuteResult, String> {
        if self.in_transaction {
            return Err("Transaction already in progress".to_string());
        }
        self.in_transaction = true;
        // Enable deferred flushing on all tables
        for table in self.tables.values_mut() {
            table.defer_flush = true;
        }
        Ok(ExecuteResult::TransactionStarted)
    }

    fn execute_commit(&mut self) -> Result<ExecuteResult, String> {
        if !self.in_transaction {
            return Err("No transaction in progress".to_string());
        }
        // Flush all pages to disk and disable deferred flushing
        for table in self.tables.values_mut() {
            table.pager.flush_all();
            table.defer_flush = false;
        }
        self.in_transaction = false;
        Ok(ExecuteResult::TransactionCommitted)
    }

    fn execute_rollback(&mut self) -> Result<ExecuteResult, String> {
        if !self.in_transaction {
            return Err("No transaction in progress".to_string());
        }
        // Discard in-memory pages by clearing and reloading from disk
        for table in self.tables.values_mut() {
            // Clear all cached pages
            for i in 0..table.pager.pages.len() {
                table.pager.pages[i] = None;
            }
            // Reset num_pages to what's actually on disk
            let file_len = table.pager.file.metadata().map(|m| m.len()).unwrap_or(0);
            table.pager.num_pages = (file_len / crate::pager::PAGE_SIZE as u64) as u32;
            table.pager.file_length = file_len;
            table.defer_flush = false;
        }
        self.in_transaction = false;
        Ok(ExecuteResult::TransactionRolledBack)
    }

    fn execute_create(&mut self, stmt: CreateTableStmt) -> Result<ExecuteResult, String> {
        let db_path = self.require_connection()?;

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

        let filename = db_path.join(format!("{}.db", stmt.table_name));
        let _ = std::fs::remove_file(&filename);

        let table = Table::new(filename.to_str().unwrap(), raw_cols);
        let table_name = stmt.table_name.clone();
        self.tables.insert(stmt.table_name, table);

        // Save metadata
        self.save_metadata()?;

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

        // Build column info for value extraction
        let col_info: Vec<(String, usize, usize)> = table
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.size, c.offset))
            .collect();

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

        // Check UNIQUE constraints on all indexes BEFORE inserting
        for index in table.indexes.values_mut() {
            if index.unique {
                // Get the value for this indexed column
                let col_value = if let Some((_, size, offset)) = col_info
                    .iter()
                    .find(|(name, _, _)| *name == index.column_name)
                {
                    let data = &row_data[*offset..*offset + *size];
                    String::from_utf8_lossy(data)
                        .trim_matches(char::from(0))
                        .to_string()
                } else {
                    continue;
                };

                // Check if value already exists in index
                let existing = index.find(&col_value);
                if !existing.is_empty() {
                    return Err(format!(
                        "UNIQUE constraint failed: column '{}' value '{}' already exists",
                        index.column_name, col_value
                    ));
                }
            }
        }

        // Insert into main table
        table.insert(id, &row_data)?;

        // Update all indexes with the new row
        for index in table.indexes.values_mut() {
            let col_value = if let Some((_, size, offset)) = col_info
                .iter()
                .find(|(name, _, _)| *name == index.column_name)
            {
                let data = &row_data[*offset..*offset + *size];
                String::from_utf8_lossy(data)
                    .trim_matches(char::from(0))
                    .to_string()
            } else {
                continue;
            };

            // Insert into index (ignore errors since we already validated uniqueness)
            let _ = index.insert(&col_value, id);
        }

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

    fn execute_create_index(&mut self, stmt: CreateIndexStmt) -> Result<ExecuteResult, String> {
        let table = self
            .tables
            .get_mut(&stmt.table_name)
            .ok_or_else(|| format!("Table '{}' not found", stmt.table_name))?;

        // Check if column exists
        let col_exists = table.columns.iter().any(|c| c.name == stmt.column_name);
        if !col_exists {
            return Err(format!(
                "Column '{}' not found in table '{}'",
                stmt.column_name, stmt.table_name
            ));
        }

        // Check if index already exists
        if table.indexes.contains_key(&stmt.index_name) {
            return Err(format!("Index '{}' already exists", stmt.index_name));
        }

        // Create the index
        let mut index = Index::new(
            &stmt.index_name,
            &stmt.table_name,
            &stmt.column_name,
            stmt.unique,
        );

        // Get column info for extracting values
        let col_info: Vec<(String, usize, usize)> = table
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.size, c.offset))
            .collect();
        let pk_col_name = col_info
            .first()
            .map(|(name, _, _)| name.clone())
            .unwrap_or_default();

        // Populate index with existing data
        let all_rows = table.select_all();
        for (row_id, row_data) in all_rows {
            let col_value = if stmt.column_name == pk_col_name {
                row_id.to_string()
            } else if let Some((_, size, offset)) = col_info
                .iter()
                .find(|(name, _, _)| *name == stmt.column_name)
            {
                let data = &row_data[*offset..*offset + *size];
                String::from_utf8_lossy(data)
                    .trim_matches(char::from(0))
                    .to_string()
            } else {
                continue;
            };

            index.insert(&col_value, row_id)?;
        }

        let index_name = stmt.index_name.clone();
        table.indexes.insert(stmt.index_name, index);

        Ok(ExecuteResult::IndexCreated(index_name))
    }

    fn execute_drop_index(&mut self, index_name: String) -> Result<ExecuteResult, String> {
        // Find and remove the index from any table
        for table in self.tables.values_mut() {
            if let Some(index) = table.indexes.remove(&index_name) {
                // Delete the index file
                let filename = format!("{}_{}.idx", index.table_name, index_name);
                let _ = std::fs::remove_file(&filename);
                return Ok(ExecuteResult::IndexDropped(index_name));
            }
        }
        Err(format!("Index '{}' not found", index_name))
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
    DatabaseCreated(String),
    DatabaseConnected(String),
    TableCreated(String),
    TableDropped(String),
    IndexCreated(String),
    IndexDropped(String),
    RowsInserted(usize),
    RowsDeleted(usize),
    RowsUpdated(usize),
    TransactionStarted,
    TransactionCommitted,
    TransactionRolledBack,
    Rows {
        headers: Vec<String>,
        rows: Vec<Vec<String>>,
    },
}
