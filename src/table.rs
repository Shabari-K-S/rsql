//! Table and Row handling

use crate::btree::*;
use crate::index::Index;
use crate::pager::{Pager, PAGE_SIZE};
use std::collections::HashMap;
use std::ptr;

#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    Integer,
    Text(u32),
}

pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub size: usize,
    pub offset: usize,
}

pub struct Table {
    pub pager: Pager,
    pub columns: Vec<Column>,
    pub row_size: usize,
    pub cell_size: usize,
    pub root_page_num: u32,
    pub defer_flush: bool,
    pub indexes: HashMap<String, Index>,
}

impl Table {
    pub fn new(filename: &str, raw_cols: Vec<(&str, DataType)>) -> Self {
        let mut columns = Vec::new();
        let mut current_offset = 0;

        for (c_name, c_type) in raw_cols {
            let size = match c_type {
                DataType::Integer => 4,
                DataType::Text(s) => s as usize,
            };
            columns.push(Column {
                name: c_name.to_string(),
                data_type: c_type,
                size,
                offset: current_offset,
            });
            current_offset += size;
        }

        let row_size = current_offset;
        let cell_size = 4 + row_size;

        let mut pager = Pager::open(filename).unwrap();

        if pager.num_pages == 0 {
            let page = pager.get_page(0);
            initialize_leaf_node(page);
            set_node_root(page, true);
            pager.flush(0);
        }

        Table {
            pager,
            columns,
            row_size,
            cell_size,
            root_page_num: 0,
            defer_flush: false,
            indexes: HashMap::new(),
        }
    }

    /// Find the leaf node that should contain the given key
    pub fn find_leaf(&mut self, key: u32) -> u32 {
        let mut page_num = self.root_page_num;

        loop {
            let page = self.pager.get_page(page_num as usize);
            let node_type = get_node_type(page);

            match node_type {
                NodeType::Leaf => return page_num,
                NodeType::Internal => {
                    let child_index = internal_node_find_child(page, key);
                    page_num = internal_node_child(page, child_index);
                }
            }
        }
    }

    /// Binary search within a leaf node
    pub fn leaf_node_find(&mut self, page_num: u32, key: u32) -> (u32, bool) {
        let page = self.pager.get_page(page_num as usize);
        let num_cells = leaf_node_num_cells(page);

        let mut min = 0u32;
        let mut max = num_cells;

        while min < max {
            let mid = (min + max) / 2;
            let mid_key = leaf_node_key(page, mid, self.cell_size);

            if key == mid_key {
                return (mid, true);
            }
            if key < mid_key {
                max = mid;
            } else {
                min = mid + 1;
            }
        }

        (min, false)
    }

    /// Insert a key-value pair into the B-Tree
    pub fn insert(&mut self, key: u32, row_data: &[u8]) -> Result<(), String> {
        let leaf_page_num = self.find_leaf(key);
        let (slot, exists) = self.leaf_node_find(leaf_page_num, key);

        if exists {
            return Err(format!("Duplicate key {}", key));
        }

        let num_cells = {
            let page = self.pager.get_page(leaf_page_num as usize);
            leaf_node_num_cells(page)
        };

        let max_cells = leaf_node_max_cells(self.cell_size);

        if num_cells as usize >= max_cells {
            self.split_and_insert(leaf_page_num, key, row_data);
        } else {
            self.leaf_node_insert(leaf_page_num, slot, key, row_data);
        }

        Ok(())
    }

    /// Delete a key from the B-Tree
    pub fn delete(&mut self, key: u32) -> Result<(), String> {
        let leaf_page_num = self.find_leaf(key);
        let (slot, exists) = self.leaf_node_find(leaf_page_num, key);

        if !exists {
            return Err(format!("Key {} not found", key));
        }

        let page = self.pager.get_page(leaf_page_num as usize);
        let num_cells = leaf_node_num_cells(page);

        // Shift cells left to overwrite the deleted cell
        if slot < num_cells - 1 {
            let dst = leaf_node_cell(page, slot, self.cell_size);
            let src = unsafe { dst.add(self.cell_size) };
            let bytes_to_move = (num_cells - slot - 1) as usize * self.cell_size;
            unsafe {
                ptr::copy(src, dst, bytes_to_move);
            }
        }

        // Decrement cell count
        set_leaf_node_num_cells(page, num_cells - 1);
        if !self.defer_flush {
            self.pager.flush(leaf_page_num as usize);
        }

        Ok(())
    }

    fn leaf_node_insert(&mut self, page_num: u32, slot: u32, key: u32, row_data: &[u8]) {
        let page = self.pager.get_page(page_num as usize);
        let num_cells = leaf_node_num_cells(page);

        if slot < num_cells {
            let src = leaf_node_cell(page, slot, self.cell_size);
            let dst = unsafe { src.add(self.cell_size) };
            let bytes_to_move = (num_cells - slot) as usize * self.cell_size;
            unsafe {
                ptr::copy(src, dst, bytes_to_move);
            }
        }

        let cell_ptr = leaf_node_cell(page, slot, self.cell_size);
        unsafe {
            ptr::write_unaligned(cell_ptr as *mut u32, key);
            let row_ptr = cell_ptr.add(4);
            ptr::write_bytes(row_ptr, 0, self.row_size);
            ptr::copy_nonoverlapping(
                row_data.as_ptr(),
                row_ptr,
                row_data.len().min(self.row_size),
            );
        }

        set_leaf_node_num_cells(page, num_cells + 1);
        if !self.defer_flush {
            self.pager.flush(page_num as usize);
        }
    }

    fn split_and_insert(&mut self, old_page_num: u32, key: u32, row_data: &[u8]) {
        let new_page_num = self.pager.num_pages;

        // Gather data from old page
        let old_num_cells;
        let old_max_key;
        let was_root;
        let parent;
        {
            let old_page = self.pager.get_page(old_page_num as usize);
            old_num_cells = leaf_node_num_cells(old_page);
            old_max_key = leaf_node_key(old_page, old_num_cells - 1, self.cell_size);
            was_root = is_node_root(old_page);
            parent = get_parent_pointer(old_page);
        }

        // Collect all cells including new one
        let mut all_cells: Vec<(u32, Vec<u8>)> = Vec::with_capacity(old_num_cells as usize + 1);
        {
            let old_page = self.pager.get_page(old_page_num as usize);
            for i in 0..old_num_cells {
                let cell_key = leaf_node_key(old_page, i, self.cell_size);
                let cell_ptr = leaf_node_cell(old_page, i, self.cell_size);
                let mut cell_data = vec![0u8; self.row_size];
                unsafe {
                    ptr::copy_nonoverlapping(
                        cell_ptr.add(4),
                        cell_data.as_mut_ptr(),
                        self.row_size,
                    );
                }
                all_cells.push((cell_key, cell_data));
            }
        }

        // Find slot for new key and insert
        let slot = all_cells
            .iter()
            .position(|(k, _)| *k > key)
            .unwrap_or(all_cells.len());
        all_cells.insert(slot, (key, row_data.to_vec()));

        let left_count = (all_cells.len() + 1) / 2;

        // Initialize new page
        {
            let new_page = self.pager.get_page(new_page_num as usize);
            initialize_leaf_node(new_page);
        }

        // Link leaves
        {
            let old_page = self.pager.get_page(old_page_num as usize);
            let old_next = leaf_node_next_leaf(old_page);
            set_leaf_node_next_leaf(old_page, new_page_num);

            let new_page = self.pager.get_page(new_page_num as usize);
            set_leaf_node_next_leaf(new_page, old_next);
        }

        // Write left side (old page)
        {
            let old_page = self.pager.get_page(old_page_num as usize);
            for i in 0..left_count {
                let (k, ref data) = all_cells[i];
                let cell_ptr = leaf_node_cell(old_page, i as u32, self.cell_size);
                unsafe {
                    ptr::write_unaligned(cell_ptr as *mut u32, k);
                    ptr::copy_nonoverlapping(
                        data.as_ptr(),
                        cell_ptr.add(4),
                        data.len().min(self.row_size),
                    );
                }
            }
            set_leaf_node_num_cells(old_page, left_count as u32);
        }

        // Write right side (new page)
        let right_count = all_cells.len() - left_count;
        {
            let new_page = self.pager.get_page(new_page_num as usize);
            for i in 0..right_count {
                let (k, ref data) = all_cells[left_count + i];
                let cell_ptr = leaf_node_cell(new_page, i as u32, self.cell_size);
                unsafe {
                    ptr::write_unaligned(cell_ptr as *mut u32, k);
                    ptr::copy_nonoverlapping(
                        data.as_ptr(),
                        cell_ptr.add(4),
                        data.len().min(self.row_size),
                    );
                }
            }
            set_leaf_node_num_cells(new_page, right_count as u32);
        }

        // Get split key
        let split_key = {
            let old_page = self.pager.get_page(old_page_num as usize);
            leaf_node_key(old_page, left_count as u32 - 1, self.cell_size)
        };

        if was_root {
            self.create_new_root(old_page_num, split_key, new_page_num);
        } else {
            // Update parent pointer for new page
            {
                let new_page = self.pager.get_page(new_page_num as usize);
                set_parent_pointer(new_page, parent);
            }
            self.internal_node_insert(parent, old_max_key, split_key, new_page_num);
        }

        self.pager.flush(old_page_num as usize);
        self.pager.flush(new_page_num as usize);
    }

    fn create_new_root(&mut self, left_child: u32, split_key: u32, right_child: u32) {
        if left_child == 0 {
            let new_left_page_num = self.pager.num_pages;

            // Copy page 0 to new left page
            {
                let page0 = self.pager.get_page(0);
                let page0_copy: [u8; PAGE_SIZE] = *page0;

                let new_left = self.pager.get_page(new_left_page_num as usize);
                new_left.copy_from_slice(&page0_copy);
                set_node_root(new_left, false);
                set_parent_pointer(new_left, 0);
            }

            // Update right child's parent
            {
                let right_page = self.pager.get_page(right_child as usize);
                set_parent_pointer(right_page, 0);
            }

            // Transform page 0 into internal node
            {
                let root = self.pager.get_page(0);
                initialize_internal_node(root);
                set_node_root(root, true);
                set_internal_node_num_keys(root, 1);
                set_internal_node_child(root, 0, new_left_page_num);
                set_internal_node_key(root, 0, split_key);
                set_internal_node_right_child(root, right_child);
            }

            self.pager.flush(0);
            self.pager.flush(new_left_page_num as usize);
            self.pager.flush(right_child as usize);
        }
    }

    fn internal_node_insert(
        &mut self,
        page_num: u32,
        _old_max_key: u32,
        new_key: u32,
        new_child: u32,
    ) {
        let page = self.pager.get_page(page_num as usize);
        let num_keys = internal_node_num_keys(page);
        let max_keys = internal_node_max_keys();

        if num_keys as usize >= max_keys {
            println!("Error: Internal node full. Splitting not yet implemented.");
            return;
        }

        // Find insertion position
        let mut insert_index = num_keys;
        for i in 0..num_keys {
            if internal_node_key(page, i) > new_key {
                insert_index = i;
                break;
            }
        }

        // Shift to make room
        for i in (insert_index..num_keys).rev() {
            set_internal_node_key(page, i + 1, internal_node_key(page, i));
            set_internal_node_child(page, i + 2, internal_node_child(page, i + 1));
        }

        set_internal_node_child(page, insert_index + 1, new_child);
        set_internal_node_key(page, insert_index, new_key);
        set_internal_node_num_keys(page, num_keys + 1);

        self.pager.flush(page_num as usize);
    }

    /// Get all rows from the table
    pub fn select_all(&mut self) -> Vec<(u32, Vec<u8>)> {
        let mut results = Vec::new();

        // Find leftmost leaf
        let mut page_num = self.root_page_num;
        loop {
            let page = self.pager.get_page(page_num as usize);
            if get_node_type(page) == NodeType::Leaf {
                break;
            }
            page_num = internal_node_child(page, 0);
        }

        // Traverse all leaves
        loop {
            let (num_cells, next_leaf) = {
                let page = self.pager.get_page(page_num as usize);
                (leaf_node_num_cells(page), leaf_node_next_leaf(page))
            };

            for i in 0..num_cells {
                let page = self.pager.get_page(page_num as usize);
                let key = leaf_node_key(page, i, self.cell_size);
                let cell_ptr = leaf_node_cell(page, i, self.cell_size);
                let mut row_data = vec![0u8; self.row_size];
                unsafe {
                    ptr::copy_nonoverlapping(cell_ptr.add(4), row_data.as_mut_ptr(), self.row_size);
                }
                results.push((key, row_data));
            }

            if next_leaf == 0 {
                break;
            }
            page_num = next_leaf;
        }

        results
    }
}
