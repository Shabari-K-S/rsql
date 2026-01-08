//! B-Tree Node Implementation
//!
//! This module implements both Leaf and Internal nodes for the B-Tree.

use crate::pager::PAGE_SIZE;
use std::ptr;

// --- Common Node Header ---
pub const NODE_TYPE_OFFSET: usize = 0;
pub const IS_ROOT_OFFSET: usize = 1;
pub const PARENT_POINTER_OFFSET: usize = 2;
pub const COMMON_NODE_HEADER_SIZE: usize = 6;

// --- Leaf Node Header ---
pub const LEAF_NODE_NUM_CELLS_OFFSET: usize = COMMON_NODE_HEADER_SIZE;
pub const LEAF_NODE_NEXT_LEAF_OFFSET: usize = LEAF_NODE_NUM_CELLS_OFFSET + 4;
pub const LEAF_NODE_HEADER_SIZE: usize = COMMON_NODE_HEADER_SIZE + 8;

// --- Internal Node Header ---
pub const INTERNAL_NODE_NUM_KEYS_OFFSET: usize = COMMON_NODE_HEADER_SIZE;
pub const INTERNAL_NODE_RIGHT_CHILD_OFFSET: usize = INTERNAL_NODE_NUM_KEYS_OFFSET + 4;
pub const INTERNAL_NODE_HEADER_SIZE: usize = COMMON_NODE_HEADER_SIZE + 8;

// --- Internal Node Body ---
pub const INTERNAL_NODE_CHILD_SIZE: usize = 4;
pub const INTERNAL_NODE_KEY_SIZE: usize = 4;
pub const INTERNAL_NODE_CELL_SIZE: usize = INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE;

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum NodeType {
    Internal = 0,
    Leaf = 1,
}

impl From<u8> for NodeType {
    fn from(value: u8) -> Self {
        match value {
            0 => NodeType::Internal,
            _ => NodeType::Leaf,
        }
    }
}

// ============== Common Node Operations ==============

pub fn get_node_type(page: &[u8; PAGE_SIZE]) -> NodeType {
    NodeType::from(page[NODE_TYPE_OFFSET])
}

pub fn set_node_type(page: &mut [u8; PAGE_SIZE], node_type: NodeType) {
    page[NODE_TYPE_OFFSET] = node_type as u8;
}

pub fn is_node_root(page: &[u8; PAGE_SIZE]) -> bool {
    page[IS_ROOT_OFFSET] != 0
}

pub fn set_node_root(page: &mut [u8; PAGE_SIZE], is_root: bool) {
    page[IS_ROOT_OFFSET] = if is_root { 1 } else { 0 };
}

pub fn get_parent_pointer(page: &[u8; PAGE_SIZE]) -> u32 {
    unsafe { ptr::read_unaligned(page.as_ptr().add(PARENT_POINTER_OFFSET) as *const u32) }
}

pub fn set_parent_pointer(page: &mut [u8; PAGE_SIZE], parent: u32) {
    unsafe {
        ptr::write_unaligned(
            page.as_mut_ptr().add(PARENT_POINTER_OFFSET) as *mut u32,
            parent,
        );
    }
}

// ============== Leaf Node Operations ==============

pub fn leaf_node_num_cells(page: &[u8; PAGE_SIZE]) -> u32 {
    unsafe { ptr::read_unaligned(page.as_ptr().add(LEAF_NODE_NUM_CELLS_OFFSET) as *const u32) }
}

pub fn set_leaf_node_num_cells(page: &mut [u8; PAGE_SIZE], num_cells: u32) {
    unsafe {
        ptr::write_unaligned(
            page.as_mut_ptr().add(LEAF_NODE_NUM_CELLS_OFFSET) as *mut u32,
            num_cells,
        );
    }
}

pub fn leaf_node_next_leaf(page: &[u8; PAGE_SIZE]) -> u32 {
    unsafe { ptr::read_unaligned(page.as_ptr().add(LEAF_NODE_NEXT_LEAF_OFFSET) as *const u32) }
}

pub fn set_leaf_node_next_leaf(page: &mut [u8; PAGE_SIZE], next_leaf: u32) {
    unsafe {
        ptr::write_unaligned(
            page.as_mut_ptr().add(LEAF_NODE_NEXT_LEAF_OFFSET) as *mut u32,
            next_leaf,
        );
    }
}

/// Get the key at a given cell index in a leaf node
pub fn leaf_node_key(page: &[u8; PAGE_SIZE], cell_num: u32, cell_size: usize) -> u32 {
    let offset = LEAF_NODE_HEADER_SIZE + (cell_num as usize * cell_size);
    unsafe { ptr::read_unaligned(page.as_ptr().add(offset) as *const u32) }
}

/// Get pointer to cell data at given index
pub fn leaf_node_cell(page: &mut [u8; PAGE_SIZE], cell_num: u32, cell_size: usize) -> *mut u8 {
    let offset = LEAF_NODE_HEADER_SIZE + (cell_num as usize * cell_size);
    unsafe { page.as_mut_ptr().add(offset) }
}

/// Calculate max cells that fit in a leaf node
pub fn leaf_node_max_cells(cell_size: usize) -> usize {
    (PAGE_SIZE - LEAF_NODE_HEADER_SIZE) / cell_size
}

/// Initialize a new leaf node
pub fn initialize_leaf_node(page: &mut [u8; PAGE_SIZE]) {
    set_node_type(page, NodeType::Leaf);
    set_node_root(page, false);
    set_leaf_node_num_cells(page, 0);
    set_leaf_node_next_leaf(page, 0);
}

// ============== Internal Node Operations ==============

pub fn internal_node_num_keys(page: &[u8; PAGE_SIZE]) -> u32 {
    unsafe { ptr::read_unaligned(page.as_ptr().add(INTERNAL_NODE_NUM_KEYS_OFFSET) as *const u32) }
}

pub fn set_internal_node_num_keys(page: &mut [u8; PAGE_SIZE], num_keys: u32) {
    unsafe {
        ptr::write_unaligned(
            page.as_mut_ptr().add(INTERNAL_NODE_NUM_KEYS_OFFSET) as *mut u32,
            num_keys,
        );
    }
}

pub fn internal_node_right_child(page: &[u8; PAGE_SIZE]) -> u32 {
    unsafe {
        ptr::read_unaligned(page.as_ptr().add(INTERNAL_NODE_RIGHT_CHILD_OFFSET) as *const u32)
    }
}

pub fn set_internal_node_right_child(page: &mut [u8; PAGE_SIZE], right_child: u32) {
    unsafe {
        ptr::write_unaligned(
            page.as_mut_ptr().add(INTERNAL_NODE_RIGHT_CHILD_OFFSET) as *mut u32,
            right_child,
        );
    }
}

/// Get child pointer at index
pub fn internal_node_child(page: &[u8; PAGE_SIZE], child_num: u32) -> u32 {
    let num_keys = internal_node_num_keys(page);
    if child_num == num_keys {
        return internal_node_right_child(page);
    }
    let offset = INTERNAL_NODE_HEADER_SIZE + (child_num as usize * INTERNAL_NODE_CELL_SIZE);
    unsafe { ptr::read_unaligned(page.as_ptr().add(offset) as *const u32) }
}

pub fn set_internal_node_child(page: &mut [u8; PAGE_SIZE], child_num: u32, child: u32) {
    let num_keys = internal_node_num_keys(page);
    if child_num == num_keys {
        set_internal_node_right_child(page, child);
        return;
    }
    let offset = INTERNAL_NODE_HEADER_SIZE + (child_num as usize * INTERNAL_NODE_CELL_SIZE);
    unsafe {
        ptr::write_unaligned(page.as_mut_ptr().add(offset) as *mut u32, child);
    }
}

/// Get key at index in internal node
pub fn internal_node_key(page: &[u8; PAGE_SIZE], key_num: u32) -> u32 {
    let offset = INTERNAL_NODE_HEADER_SIZE
        + (key_num as usize * INTERNAL_NODE_CELL_SIZE)
        + INTERNAL_NODE_CHILD_SIZE;
    unsafe { ptr::read_unaligned(page.as_ptr().add(offset) as *const u32) }
}

pub fn set_internal_node_key(page: &mut [u8; PAGE_SIZE], key_num: u32, key: u32) {
    let offset = INTERNAL_NODE_HEADER_SIZE
        + (key_num as usize * INTERNAL_NODE_CELL_SIZE)
        + INTERNAL_NODE_CHILD_SIZE;
    unsafe {
        ptr::write_unaligned(page.as_mut_ptr().add(offset) as *mut u32, key);
    }
}

/// Calculate max keys that fit in an internal node
pub fn internal_node_max_keys() -> usize {
    (PAGE_SIZE - INTERNAL_NODE_HEADER_SIZE) / INTERNAL_NODE_CELL_SIZE
}

/// Initialize a new internal node
pub fn initialize_internal_node(page: &mut [u8; PAGE_SIZE]) {
    set_node_type(page, NodeType::Internal);
    set_node_root(page, false);
    set_internal_node_num_keys(page, 0);
    set_internal_node_right_child(page, 0);
}

/// Find the index of the child that should contain the given key
pub fn internal_node_find_child(page: &[u8; PAGE_SIZE], key: u32) -> u32 {
    let num_keys = internal_node_num_keys(page);

    let mut min = 0u32;
    let mut max = num_keys;

    while min < max {
        let mid = (min + max) / 2;
        let key_at_mid = internal_node_key(page, mid);
        if key_at_mid >= key {
            max = mid;
        } else {
            min = mid + 1;
        }
    }

    min
}
