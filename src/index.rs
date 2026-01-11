//! Secondary Index Implementation
//!
//! This module implements secondary indexes using B-Trees for fast lookups
//! on non-primary-key columns.

use crate::btree::*;
use crate::pager::Pager;
use std::collections::HashMap;
use std::ptr;

/// Size of the row ID stored in index entries
const INDEX_ROW_ID_SIZE: usize = 4;

/// Maximum key size for indexed values (truncated if longer)
const INDEX_KEY_SIZE: usize = 64;

/// Cell size in index B-Tree: key (64 bytes) + row_id (4 bytes)
const INDEX_CELL_SIZE: usize = INDEX_KEY_SIZE + INDEX_ROW_ID_SIZE;

/// Secondary index structure
pub struct Index {
    pub name: String,
    pub table_name: String,
    pub column_name: String,
    pub unique: bool,
    pub pager: Pager,
    pub root_page_num: u32,
}

impl Index {
    /// Create a new secondary index
    pub fn new(name: &str, table_name: &str, column_name: &str, unique: bool) -> Self {
        let filename = format!("{}_{}.idx", table_name, name);
        let mut pager = Pager::open(&filename).expect("Failed to open index file");

        // Initialize root page as leaf node
        if pager.num_pages == 0 {
            let root_page = pager.get_page(0);
            initialize_leaf_node(root_page);
            set_node_root(root_page, true);
            pager.num_pages = 1;
            pager.flush(0);
        }

        Index {
            name: name.to_string(),
            table_name: table_name.to_string(),
            column_name: column_name.to_string(),
            unique,
            pager,
            root_page_num: 0,
        }
    }

    /// Insert a key-value pair into the index
    /// key_value: the indexed column value
    /// row_id: the primary key of the row
    pub fn insert(&mut self, key_value: &str, row_id: u32) -> Result<(), String> {
        // Check uniqueness constraint
        if self.unique {
            let existing = self.find(key_value);
            if !existing.is_empty() {
                return Err(format!(
                    "UNIQUE constraint failed: {} already exists in index {}",
                    key_value, self.name
                ));
            }
        }

        let leaf_page_num = self.find_leaf(key_value);
        let page = self.pager.get_page(leaf_page_num as usize);
        let num_cells = leaf_node_num_cells(page);
        let max_cells = leaf_node_max_cells(INDEX_CELL_SIZE);

        if num_cells as usize >= max_cells {
            // Need to split - for simplicity, we'll just insert and handle overflow
            // A full implementation would split like the main table B-Tree
            self.split_and_insert(leaf_page_num, key_value, row_id);
        } else {
            // Find insertion position
            let slot = self.find_slot(leaf_page_num, key_value);
            self.leaf_node_insert(leaf_page_num, slot, key_value, row_id);
        }

        Ok(())
    }

    /// Delete an entry from the index
    pub fn delete(&mut self, key_value: &str, row_id: u32) -> Result<(), String> {
        let leaf_page_num = self.find_leaf(key_value);
        let page = self.pager.get_page(leaf_page_num as usize);
        let num_cells = leaf_node_num_cells(page);

        // Find and remove the entry with matching key AND row_id
        for i in 0..num_cells {
            let (stored_key, stored_row_id) = self.read_cell(leaf_page_num, i);
            if stored_key == key_value && stored_row_id == row_id {
                // Shift remaining cells left
                let page = self.pager.get_page(leaf_page_num as usize);
                for j in i..num_cells - 1 {
                    unsafe {
                        let src = leaf_node_cell(page, j + 1, INDEX_CELL_SIZE);
                        let dst = leaf_node_cell(page, j, INDEX_CELL_SIZE);
                        ptr::copy(src, dst, INDEX_CELL_SIZE);
                    }
                }
                set_leaf_node_num_cells(page, num_cells - 1);
                self.pager.flush(leaf_page_num as usize);
                return Ok(());
            }
        }

        Ok(()) // Not found, that's okay
    }

    /// Find all row IDs matching the given key value
    pub fn find(&mut self, key_value: &str) -> Vec<u32> {
        let mut results = Vec::new();
        let leaf_page_num = self.find_leaf(key_value);
        let page = self.pager.get_page(leaf_page_num as usize);
        let num_cells = leaf_node_num_cells(page);

        for i in 0..num_cells {
            let (stored_key, row_id) = self.read_cell(leaf_page_num, i);
            if stored_key == key_value {
                results.push(row_id);
            }
        }

        results
    }

    /// Find the leaf node that should contain the given key
    fn find_leaf(&mut self, key_value: &str) -> u32 {
        let mut page_num = self.root_page_num;

        loop {
            let page = self.pager.get_page(page_num as usize);
            if get_node_type(page) == NodeType::Leaf {
                return page_num;
            }

            // Internal node - find child
            let num_keys = internal_node_num_keys(page);
            let mut child_num = num_keys;

            // Read keys inline to avoid borrow issues
            for i in 0..num_keys {
                let offset = INTERNAL_NODE_HEADER_SIZE
                    + (i as usize * (INTERNAL_NODE_CHILD_SIZE + INDEX_KEY_SIZE))
                    + INTERNAL_NODE_CHILD_SIZE;
                let key_bytes = &page[offset..offset + INDEX_KEY_SIZE];
                let key_at_i = String::from_utf8_lossy(key_bytes)
                    .trim_matches(char::from(0))
                    .to_string();

                if key_value <= &key_at_i {
                    child_num = i;
                    break;
                }
            }

            page_num = internal_node_child(page, child_num);
        }
    }

    /// Find the slot where a key should be inserted
    fn find_slot(&mut self, page_num: u32, key_value: &str) -> u32 {
        let page = self.pager.get_page(page_num as usize);
        let num_cells = leaf_node_num_cells(page);

        // Collect keys to compare
        let mut keys: Vec<String> = Vec::new();
        for i in 0..num_cells {
            let offset = LEAF_NODE_HEADER_SIZE + (i as usize * INDEX_CELL_SIZE);
            let key_bytes = &page[offset..offset + INDEX_KEY_SIZE];
            let key = String::from_utf8_lossy(key_bytes)
                .trim_matches(char::from(0))
                .to_string();
            keys.push(key);
        }

        for (i, stored_key) in keys.iter().enumerate() {
            if key_value <= stored_key {
                return i as u32;
            }
        }

        num_cells
    }

    /// Read a cell from the index leaf node
    fn read_cell(&mut self, page_num: u32, cell_num: u32) -> (String, u32) {
        let page = self.pager.get_page(page_num as usize);
        let offset = LEAF_NODE_HEADER_SIZE + (cell_num as usize * INDEX_CELL_SIZE);

        // Read key (first INDEX_KEY_SIZE bytes)
        let key_bytes = &page[offset..offset + INDEX_KEY_SIZE];
        let key = String::from_utf8_lossy(key_bytes)
            .trim_matches(char::from(0))
            .to_string();

        // Read row_id (next 4 bytes)
        let row_id = unsafe {
            ptr::read_unaligned(page.as_ptr().add(offset + INDEX_KEY_SIZE) as *const u32)
        };

        (key, row_id)
    }

    /// Read internal node key
    fn read_internal_key(&mut self, page_num: u32, key_num: u32) -> String {
        let page = self.pager.get_page(page_num as usize);
        let offset = INTERNAL_NODE_HEADER_SIZE
            + (key_num as usize * (INTERNAL_NODE_CHILD_SIZE + INDEX_KEY_SIZE))
            + INTERNAL_NODE_CHILD_SIZE;

        let key_bytes = &page[offset..offset + INDEX_KEY_SIZE];
        String::from_utf8_lossy(key_bytes)
            .trim_matches(char::from(0))
            .to_string()
    }

    /// Insert into a leaf node
    fn leaf_node_insert(&mut self, page_num: u32, slot: u32, key_value: &str, row_id: u32) {
        let page = self.pager.get_page(page_num as usize);
        let num_cells = leaf_node_num_cells(page);

        // Shift cells to make room
        if slot < num_cells {
            for i in (slot..num_cells).rev() {
                unsafe {
                    let src = leaf_node_cell(page, i, INDEX_CELL_SIZE);
                    let dst = leaf_node_cell(page, i + 1, INDEX_CELL_SIZE);
                    ptr::copy(src, dst, INDEX_CELL_SIZE);
                }
            }
        }

        // Write the new cell
        let cell_ptr = leaf_node_cell(page, slot, INDEX_CELL_SIZE);
        unsafe {
            // Clear the cell first
            ptr::write_bytes(cell_ptr, 0, INDEX_CELL_SIZE);

            // Write key (truncated to INDEX_KEY_SIZE)
            let key_bytes = key_value.as_bytes();
            let copy_len = key_bytes.len().min(INDEX_KEY_SIZE);
            ptr::copy_nonoverlapping(key_bytes.as_ptr(), cell_ptr, copy_len);

            // Write row_id
            ptr::write_unaligned(cell_ptr.add(INDEX_KEY_SIZE) as *mut u32, row_id);
        }

        set_leaf_node_num_cells(page, num_cells + 1);
        self.pager.flush(page_num as usize);
    }

    /// Split a full leaf node and insert (simplified version)
    fn split_and_insert(&mut self, old_page_num: u32, key_value: &str, row_id: u32) {
        // Create new page
        let new_page_num = self.pager.num_pages;
        self.pager.num_pages += 1;

        let new_page = self.pager.get_page(new_page_num as usize);
        initialize_leaf_node(new_page);

        // Get old page data
        let old_page = self.pager.get_page(old_page_num as usize);
        let num_cells = leaf_node_num_cells(old_page);
        let split_point = num_cells / 2;

        // Move half the cells to new page
        for i in split_point..num_cells {
            let (key, rid) = self.read_cell(old_page_num, i);
            let new_page = self.pager.get_page(new_page_num as usize);
            let new_slot = i - split_point;

            let cell_ptr = leaf_node_cell(new_page, new_slot, INDEX_CELL_SIZE);
            unsafe {
                ptr::write_bytes(cell_ptr, 0, INDEX_CELL_SIZE);
                let key_bytes = key.as_bytes();
                let copy_len = key_bytes.len().min(INDEX_KEY_SIZE);
                ptr::copy_nonoverlapping(key_bytes.as_ptr(), cell_ptr, copy_len);
                ptr::write_unaligned(cell_ptr.add(INDEX_KEY_SIZE) as *mut u32, rid);
            }
        }

        // Update cell counts
        let new_page = self.pager.get_page(new_page_num as usize);
        set_leaf_node_num_cells(new_page, num_cells - split_point);

        let old_page = self.pager.get_page(old_page_num as usize);
        set_leaf_node_num_cells(old_page, split_point);

        // Link leaves
        let old_page = self.pager.get_page(old_page_num as usize);
        let old_next = leaf_node_next_leaf(old_page);
        set_leaf_node_next_leaf(old_page, new_page_num);

        let new_page = self.pager.get_page(new_page_num as usize);
        set_leaf_node_next_leaf(new_page, old_next);

        // Decide which page to insert into
        let (mid_key, _) = self.read_cell(new_page_num, 0);
        if key_value < &mid_key {
            let slot = self.find_slot(old_page_num, key_value);
            self.leaf_node_insert(old_page_num, slot, key_value, row_id);
        } else {
            let slot = self.find_slot(new_page_num, key_value);
            self.leaf_node_insert(new_page_num, slot, key_value, row_id);
        }

        self.pager.flush(old_page_num as usize);
        self.pager.flush(new_page_num as usize);

        // If this was the root, create a new root
        let old_page = self.pager.get_page(old_page_num as usize);
        if is_node_root(old_page) {
            self.create_new_root(old_page_num, &mid_key, new_page_num);
        }
    }

    /// Create a new root after splitting
    fn create_new_root(&mut self, left_child: u32, split_key: &str, right_child: u32) {
        let new_root_num = self.pager.num_pages;
        self.pager.num_pages += 1;

        let new_root = self.pager.get_page(new_root_num as usize);
        initialize_internal_node(new_root);
        set_node_root(new_root, true);
        set_internal_node_num_keys(new_root, 1);

        // Set left child
        set_internal_node_child(new_root, 0, left_child);

        // Set key (store as bytes)
        let key_offset = INTERNAL_NODE_HEADER_SIZE + INTERNAL_NODE_CHILD_SIZE;
        unsafe {
            ptr::write_bytes(new_root.as_mut_ptr().add(key_offset), 0, INDEX_KEY_SIZE);
            let key_bytes = split_key.as_bytes();
            let copy_len = key_bytes.len().min(INDEX_KEY_SIZE);
            ptr::copy_nonoverlapping(
                key_bytes.as_ptr(),
                new_root.as_mut_ptr().add(key_offset),
                copy_len,
            );
        }

        // Set right child
        set_internal_node_right_child(new_root, right_child);

        // Update old root
        let old_root = self.pager.get_page(self.root_page_num as usize);
        set_node_root(old_root, false);
        set_parent_pointer(old_root, new_root_num);

        // Update right child parent
        let right_page = self.pager.get_page(right_child as usize);
        set_parent_pointer(right_page, new_root_num);

        self.root_page_num = new_root_num;

        self.pager.flush(new_root_num as usize);
        self.pager.flush(left_child as usize);
        self.pager.flush(right_child as usize);
    }

    /// Rebuild the index from existing table data
    pub fn rebuild(&mut self, rows: &[(u32, String)]) -> Result<(), String> {
        // Clear existing index
        let filename = format!("{}_{}.idx", self.table_name, self.name);
        let _ = std::fs::remove_file(&filename);
        self.pager = Pager::open(&filename).expect("Failed to open index file");
        let root_page = self.pager.get_page(0);
        initialize_leaf_node(root_page);
        set_node_root(root_page, true);
        self.pager.num_pages = 1;
        self.root_page_num = 0;

        // Insert all rows
        for (row_id, key_value) in rows {
            self.insert(key_value, *row_id)?;
        }

        Ok(())
    }
}

/// Metadata about indexes for a table, stored separately
pub struct IndexMetadata {
    pub indexes: HashMap<String, IndexInfo>,
}

pub struct IndexInfo {
    pub column_name: String,
    pub unique: bool,
}
