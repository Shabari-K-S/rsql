use std::env;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::FileExt;
use std::process;
use std::ptr;

// --- Decoration Tinkering ---
struct Color;
impl Color {
    const GREEN: &'static str = "\x1b[32m";
    const RED: &'static str = "\x1b[31m";
    const BLUE: &'static str = "\x1b[34m";
    const RESET: &'static str = "\x1b[0m";
    const BOLD: &'static str = "\x1b[1m";
}

// --- Constants ---
const COLUMN_USERNAME_SIZE: usize = 32;
const COLUMN_EMAIL_SIZE: usize = 255;
const ROW_SIZE: usize = 4 + COLUMN_USERNAME_SIZE + COLUMN_EMAIL_SIZE;
const PAGE_SIZE: usize = 4096;
const TABLE_MAX_PAGES: usize = 100;

// --- Common Node Header ---
const NODE_TYPE_SIZE: usize = 1;
const NODE_TYPE_OFFSET: usize = 0;
const IS_ROOT_SIZE: usize = 1;
const IS_ROOT_OFFSET: usize = NODE_TYPE_SIZE;
const PARENT_POINTER_SIZE: usize = 4;
const PARENT_POINTER_OFFSET: usize = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const COMMON_NODE_HEADER_SIZE: usize = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

// --- Leaf Node Header ---
const LEAF_NODE_NUM_CELLS_SIZE: usize = 4;
const LEAF_NODE_NUM_CELLS_OFFSET: usize = COMMON_NODE_HEADER_SIZE; // Offset 6
const LEAF_NODE_HEADER_SIZE: usize = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;

// --- Leaf Node Body ---
const LEAF_NODE_KEY_SIZE: usize = 4;
const LEAF_NODE_VALUE_SIZE: usize = ROW_SIZE;
const LEAF_NODE_CELL_SIZE: usize = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const LEAF_NODE_MAX_CELLS: usize = (PAGE_SIZE - LEAF_NODE_HEADER_SIZE) / LEAF_NODE_CELL_SIZE;

// --- Internal Node Header ---
const INTERNAL_NODE_NUM_KEYS_SIZE: usize = 4;
const INTERNAL_NODE_NUM_KEYS_OFFSET: usize = COMMON_NODE_HEADER_SIZE;
const INTERNAL_NODE_RIGHT_CHILD_SIZE: usize = 4;
const INTERNAL_NODE_RIGHT_CHILD_OFFSET: usize = INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE;
const INTERNAL_NODE_HEADER_SIZE: usize = COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE;

// --- Internal Node Body ---
const INTERNAL_NODE_KEY_SIZE: usize = 4;
const INTERNAL_NODE_CHILD_SIZE: usize = 4;
const INTERNAL_NODE_CELL_SIZE: usize = INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE;

// --- Split Constants ---
const LEAF_NODE_RIGHT_SPLIT_COUNT: u32 = (LEAF_NODE_MAX_CELLS as u32 + 1) / 2;
const LEAF_NODE_LEFT_SPLIT_COUNT: u32 = (LEAF_NODE_MAX_CELLS as u32 + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT;

#[derive(PartialEq, Debug)]
enum NodeType { Internal = 0, Leaf = 1 }

#[derive(Clone)]
struct Row {
    id: u32,
    username: [u8; COLUMN_USERNAME_SIZE],
    email: [u8; COLUMN_EMAIL_SIZE],
}

// --- Storage ---
struct Pager {
    file: File,
    num_pages: u32,
    pages: Vec<Option<Box<[u8; PAGE_SIZE]>>>,
}

struct Table {
    root_page_num: u32,
    pager: Pager,
}

struct Cursor<'a> {
    table: &'a mut Table,
    page_num: u32,
    cell_num: u32,
    end_of_table: bool,
}

// --- Safe Unaligned Accessors ---
fn get_node_type(node: *mut u8) -> NodeType {
    unsafe { if *node.add(NODE_TYPE_OFFSET) == 0 { NodeType::Internal } else { NodeType::Leaf } }
}

fn set_node_type(node: *mut u8, node_type: NodeType) {
    unsafe { *node.add(NODE_TYPE_OFFSET) = node_type as u8; }
}

fn is_node_root(node: *mut u8) -> bool {
    unsafe { *node.add(IS_ROOT_OFFSET) != 0 }
}

fn set_node_root(node: *mut u8, is_root: bool) {
    unsafe { *node.add(IS_ROOT_OFFSET) = if is_root { 1 } else { 0 }; }
}

// Leaf Accessors using unaligned ptrs
fn get_leaf_node_num_cells(node: *mut u8) -> u32 {
    unsafe { ptr::read_unaligned(node.add(LEAF_NODE_NUM_CELLS_OFFSET) as *const u32) }
}
fn set_leaf_node_num_cells(node: *mut u8, val: u32) {
    unsafe { ptr::write_unaligned(node.add(LEAF_NODE_NUM_CELLS_OFFSET) as *mut u32, val) }
}
fn leaf_node_cell(node: *mut u8, cell_num: u32) -> *mut u8 {
    unsafe { node.add(LEAF_NODE_HEADER_SIZE + (cell_num as usize * LEAF_NODE_CELL_SIZE)) }
}
fn get_leaf_node_key(node: *mut u8, cell_num: u32) -> u32 {
    unsafe { ptr::read_unaligned(leaf_node_cell(node, cell_num) as *const u32) }
}
fn set_leaf_node_key(node: *mut u8, cell_num: u32, val: u32) {
    unsafe { ptr::write_unaligned(leaf_node_cell(node, cell_num) as *mut u32, val) }
}
fn leaf_node_value(node: *mut u8, cell_num: u32) -> *mut u8 {
    unsafe { leaf_node_cell(node, cell_num).add(LEAF_NODE_KEY_SIZE) }
}

// Internal Accessors
fn get_internal_node_num_keys(node: *mut u8) -> u32 {
    unsafe { ptr::read_unaligned(node.add(INTERNAL_NODE_NUM_KEYS_OFFSET) as *const u32) }
}
fn set_internal_node_num_keys(node: *mut u8, val: u32) {
    unsafe { ptr::write_unaligned(node.add(INTERNAL_NODE_NUM_KEYS_OFFSET) as *mut u32, val) }
}
fn get_internal_node_right_child(node: *mut u8) -> u32 {
    unsafe { ptr::read_unaligned(node.add(INTERNAL_NODE_RIGHT_CHILD_OFFSET) as *const u32) }
}
fn set_internal_node_right_child(node: *mut u8, val: u32) {
    unsafe { ptr::write_unaligned(node.add(INTERNAL_NODE_RIGHT_CHILD_OFFSET) as *mut u32, val) }
}
fn internal_node_cell(node: *mut u8, cell_num: u32) -> *mut u8 {
    unsafe { node.add(INTERNAL_NODE_HEADER_SIZE + (cell_num as usize * INTERNAL_NODE_CELL_SIZE)) }
}
fn get_internal_node_child(node: *mut u8, child_num: u32) -> u32 {
    let num_keys = get_internal_node_num_keys(node);
    if child_num > num_keys { process::exit(1); }
    if child_num == num_keys { get_internal_node_right_child(node) }
    else { unsafe { ptr::read_unaligned(internal_node_cell(node, child_num) as *const u32) } }
}
fn set_internal_node_child(node: *mut u8, child_num: u32, val: u32) {
    unsafe { ptr::write_unaligned(internal_node_cell(node, child_num) as *mut u32, val) }
}
fn get_internal_node_key(node: *mut u8, key_num: u32) -> u32 {
    unsafe { ptr::read_unaligned(internal_node_cell(node, key_num).add(INTERNAL_NODE_CHILD_SIZE) as *const u32) }
}
fn set_internal_node_key(node: *mut u8, key_num: u32, val: u32) {
    unsafe { ptr::write_unaligned(internal_node_cell(node, key_num).add(INTERNAL_NODE_CHILD_SIZE) as *mut u32, val) }
}

// --- Tree Logic ---

fn get_node_max_key(pager: &mut Pager, node: *mut u8) -> u32 {
    match get_node_type(node) {
        NodeType::Internal => {
            let num_keys = get_internal_node_num_keys(node);
            get_internal_node_key(node, num_keys - 1)
        }
        NodeType::Leaf => {
            let num_cells = get_leaf_node_num_cells(node);
            get_leaf_node_key(node, num_cells - 1)
        }
    }
}

fn initialize_leaf_node(node: *mut u8) {
    set_node_type(node, NodeType::Leaf);
    set_node_root(node, false);
    set_leaf_node_num_cells(node, 0);
}

fn initialize_internal_node(node: *mut u8) {
    set_node_type(node, NodeType::Internal);
    set_node_root(node, false);
    set_internal_node_num_keys(node, 0);
}

//
fn create_new_root(table: &mut Table, right_child_page_num: u32) {
    let root_page_num = table.root_page_num;
    let left_child_page_num = table.pager.num_pages;
    
    // Copy root to new left child page
    let root = table.pager.get_page(root_page_num as usize).as_mut_ptr();
    let left_child = table.pager.get_page(left_child_page_num as usize).as_mut_ptr();
    unsafe { ptr::copy_nonoverlapping(root, left_child, PAGE_SIZE); }
    set_node_root(left_child, false);

    // Re-init root as internal node
    let root = table.pager.get_page(root_page_num as usize).as_mut_ptr();
    initialize_internal_node(root);
    set_node_root(root, true);
    set_internal_node_num_keys(root, 1);
    set_internal_node_child(root, 0, left_child_page_num);
    
    let left_child = table.pager.get_page(left_child_page_num as usize).as_mut_ptr();
    let left_child_max_key = get_node_max_key(&mut table.pager, left_child);
    set_internal_node_key(root, 0, left_child_max_key);
    set_internal_node_right_child(root, right_child_page_num);
}

//
fn leaf_node_split_and_insert(cursor: &mut Cursor, key: u32, value: &Row) {
    let old_page_num = cursor.page_num;
    let new_page_num = cursor.table.pager.num_pages;
    
    let old_node = cursor.table.pager.get_page(old_page_num as usize).as_mut_ptr();
    let new_node = cursor.table.pager.get_page(new_page_num as usize).as_mut_ptr();
    initialize_leaf_node(new_node);

    for i in (0..=LEAF_NODE_MAX_CELLS as i32).rev() {
        let dest_node = if i >= LEAF_NODE_LEFT_SPLIT_COUNT as i32 { new_node } else { old_node };
        let index_within_node = (i as u32) % LEAF_NODE_LEFT_SPLIT_COUNT;
        let destination = leaf_node_cell(dest_node, index_within_node);

        if i == cursor.cell_num as i32 {
            serialize_row(value, destination);
        } else if i > cursor.cell_num as i32 {
            unsafe { ptr::copy_nonoverlapping(leaf_node_cell(old_node, (i - 1) as u32), destination, LEAF_NODE_CELL_SIZE); }
        } else {
            unsafe { ptr::copy_nonoverlapping(leaf_node_cell(old_node, i as u32), destination, LEAF_NODE_CELL_SIZE); }
        }
    }

    set_leaf_node_num_cells(old_node, LEAF_NODE_LEFT_SPLIT_COUNT);
    set_leaf_node_num_cells(new_node, LEAF_NODE_RIGHT_SPLIT_COUNT);

    let old_node = cursor.table.pager.get_page(old_page_num as usize).as_mut_ptr();
    if is_node_root(old_node) {
        create_new_root(cursor.table, new_page_num);
    } else {
        println!("Need to implement updating parent after split");
        process::exit(1);
    }
}

// --- Recursive Search ---

fn internal_node_find_child(node: *mut u8, key: u32) -> u32 {
    let num_keys = get_internal_node_num_keys(node);
    let mut min_idx = 0;
    let mut max_idx = num_keys;

    while min_idx != max_idx {
        let index = (min_idx + max_idx) / 2;
        let key_to_right = get_internal_node_key(node, index);
        if key_to_right >= key { max_idx = index; }
        else { min_idx = index + 1; }
    }
    get_internal_node_child(node, min_idx)
}

fn table_find(table: &mut Table, key: u32) -> (u32, u32, bool) {
    let mut page_num = table.root_page_num;
    loop {
        let node = table.pager.get_page(page_num as usize).as_mut_ptr();
        if get_node_type(node) == NodeType::Leaf {
            return leaf_node_find(table, page_num, key);
        }
        page_num = internal_node_find_child(node, key);
    }
}

fn leaf_node_find(table: &mut Table, page_num: u32, key: u32) -> (u32, u32, bool) {
    let node = table.pager.get_page(page_num as usize).as_mut_ptr();
    let num_cells = get_leaf_node_num_cells(node);

    let mut min_idx = 0;
    let mut max_idx = num_cells;
    while min_idx != max_idx {
        let index = (min_idx + max_idx) / 2;
        let key_at_index = get_leaf_node_key(node, index);
        if key == key_at_index { return (page_num, index, false); }
        if key < key_at_index { max_idx = index; }
        else { min_idx = index + 1; }
    }
    (page_num, min_idx, min_idx == num_cells)
}

// --- Main Components ---

impl Pager {
    fn open(filename: &str) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).write(true).create(true).open(filename)?;
        let file_length = file.metadata()?.len();
        let num_pages = (file_length / PAGE_SIZE as u64) as u32;
        let mut pages = Vec::with_capacity(TABLE_MAX_PAGES);
        for _ in 0..TABLE_MAX_PAGES { pages.push(None); }
        Ok(Pager { file, num_pages, pages })
    }

    fn get_page(&mut self, page_num: usize) -> &mut [u8; PAGE_SIZE] {
        if self.pages[page_num].is_none() {
            let mut page = Box::new([0u8; PAGE_SIZE]);
            if (page_num as u32) < self.num_pages {
                self.file.read_exact_at(&mut *page, (page_num * PAGE_SIZE) as u64).unwrap();
            }
            self.pages[page_num] = Some(page);
            if page_num as u32 >= self.num_pages { self.num_pages = page_num as u32 + 1; }
        }
        self.pages[page_num].as_mut().unwrap()
    }
}

fn serialize_row(source: &Row, dest: *mut u8) {
    unsafe {
        ptr::copy_nonoverlapping(&source.id as *const u32 as *const u8, dest, 4);
        ptr::copy_nonoverlapping(source.username.as_ptr(), dest.add(4), 32);
        ptr::copy_nonoverlapping(source.email.as_ptr(), dest.add(36), 255);
    }
}

fn deserialize_row(source: *const u8) -> Row {
    let mut row = Row { id: 0, username: [0; 32], email: [0; 255] };
    unsafe {
        ptr::copy_nonoverlapping(source, &mut row.id as *mut u32 as *mut u8, 4);
        ptr::copy_nonoverlapping(source.add(4), row.username.as_mut_ptr(), 32);
        ptr::copy_nonoverlapping(source.add(36), row.email.as_mut_ptr(), 255);
    }
    row
}

fn print_tree(pager: &mut Pager, page_num: u32, level: u32) {
    let node = pager.get_page(page_num as usize).as_mut_ptr();
    let indent = "  ".repeat(level as usize);
    match get_node_type(node) {
        NodeType::Leaf => {
            let num_cells = get_leaf_node_num_cells(node);
            println!("{}- leaf (size {})", indent, num_cells);
            for i in 0..num_cells { println!("{}  - {}", indent, get_leaf_node_key(node, i)); }
        }
        NodeType::Internal => {
            let num_keys = get_internal_node_num_keys(node);
            println!("{}- internal (size {})", indent, num_keys);
            for i in 0..num_keys {
                let child = get_internal_node_child(node, i);
                print_tree(pager, child, level + 1);
                println!("{}  - key {}", indent, get_internal_node_key(node, i));
            }
            let right_child = get_internal_node_right_child(node);
            print_tree(pager, right_child, level + 1);
        }
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 { process::exit(1); }
    let mut pager = Pager::open(&args[1]).unwrap();
    if pager.num_pages == 0 {
        let root = pager.get_page(0).as_mut_ptr();
        initialize_leaf_node(root);
        set_node_root(root, true);
    }
    let mut table = Table { root_page_num: 0, pager };

    loop {
        print!("{}{}rsql > {}{}", Color::BOLD, Color::BLUE, Color::RESET, Color::RESET);
        io::stdout().flush().unwrap();
        let mut buffer = String::new();
        io::stdin().read_line(&mut buffer).unwrap();
        let input = buffer.trim();

        if input == ".exit" {
            for i in 0..table.pager.num_pages {
                if table.pager.pages[i as usize].is_some() {
                    table.pager.file.write_all_at(&**table.pager.pages[i as usize].as_ref().unwrap(), (i as usize * PAGE_SIZE) as u64).unwrap();
                }
            }
            process::exit(0);
        } else if input == ".btree" {
            println!("Tree:");
            print_tree(&mut table.pager, table.root_page_num, 0);
            continue;
        }

        if input.starts_with("insert") {
            let parts: Vec<&str> = input.split_whitespace().collect();
            if parts.len() < 4 { continue; }
            let id = parts[1].parse::<u32>().unwrap();
            let mut username = [0u8; 32];
            let mut email = [0u8; 255];
            username[..parts[2].len().min(32)].copy_from_slice(&parts[2].as_bytes()[..parts[2].len().min(32)]);
            email[..parts[3].len().min(255)].copy_from_slice(&parts[3].as_bytes()[..parts[3].len().min(255)]);
            
            let row = Row { id, username, email };
            let (page_num, cell_num, end) = table_find(&mut table, id);
            
            let node = table.pager.get_page(page_num as usize).as_mut_ptr();
            let num_cells = get_leaf_node_num_cells(node);
            
            // Duplicate Check
            if cell_num < num_cells && get_leaf_node_key(node, cell_num) == id {
                println!("{}ERROR:{} Duplicate key.", Color::RED, Color::RESET);
                continue;
            }

            let mut cursor = Cursor { table: &mut table, page_num, cell_num, end_of_table: end };
            if get_leaf_node_num_cells(node) >= LEAF_NODE_MAX_CELLS as u32 {
                leaf_node_split_and_insert(&mut cursor, id, &row);
            } else {
                let node = cursor.table.pager.get_page(page_num as usize).as_mut_ptr();
                if cell_num < num_cells {
                    for i in (cell_num + 1..=num_cells).rev() {
                        unsafe { ptr::copy_nonoverlapping(leaf_node_cell(node, i - 1), leaf_node_cell(node, i), LEAF_NODE_CELL_SIZE); }
                    }
                }
                set_leaf_node_num_cells(node, num_cells + 1);
                set_leaf_node_key(node, cell_num, id);
                serialize_row(&row, leaf_node_value(node, cell_num));
            }
            println!("Executed.");
        } else if input == "select" {
            let mut page_num = table.root_page_num;
            loop {
                let node = table.pager.get_page(page_num as usize).as_mut_ptr();
                if get_node_type(node) == NodeType::Leaf {
                    let num_cells = get_leaf_node_num_cells(node);
                    for i in 0..num_cells {
                        let r = deserialize_row(leaf_node_value(node, i));
                        println!("({}, {}, {})", r.id, 
                            String::from_utf8_lossy(&r.username).trim_matches('\0'), 
                            String::from_utf8_lossy(&r.email).trim_matches('\0'));
                    }
                    break;
                }
                page_num = get_internal_node_child(node, 0); // Temporary: only prints first leaf
            }
            println!("Executed.");
        }
    }
}