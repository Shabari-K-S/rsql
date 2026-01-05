use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::FileExt;
use std::ptr;

const PAGE_SIZE: usize = 4096;
const TABLE_MAX_PAGES: usize = 100;

// --- Node Header Layout ---
const NODE_TYPE_OFFSET: usize = 0;
const IS_ROOT_OFFSET: usize = 1;
// const PARENT_POINTER_OFFSET: usize = 2; 
const LEAF_NODE_NUM_CELLS_OFFSET: usize = 6;
const LEAF_NODE_HEADER_SIZE: usize = 10;

#[derive(Debug, Clone, PartialEq)]
enum DataType { Integer, Text(u32) }

struct Column {
    name: String,
    data_type: DataType,
    size: usize,
    offset: usize,
}

struct Pager {
    file: File,
    file_length: u64,
    num_pages: u32,
    pages: Vec<Option<Box<[u8; PAGE_SIZE]>>>,
}

impl Pager {
    fn open(filename: &str) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).write(true).create(true).open(filename)?;
        let file_length = file.metadata()?.len();
        let num_pages = (file_length / PAGE_SIZE as u64) as u32;
        let mut pages = Vec::with_capacity(TABLE_MAX_PAGES);
        for _ in 0..TABLE_MAX_PAGES { pages.push(None); }

        Ok(Pager { file, file_length, num_pages, pages })
    }

    fn get_page(&mut self, page_num: usize) -> &mut [u8; PAGE_SIZE] {
        if self.pages[page_num].is_none() {
            let mut page = Box::new([0u8; PAGE_SIZE]);
            let offset = (page_num * PAGE_SIZE) as u64;
            if offset < self.file_length {
                let _ = self.file.read_at(&mut *page, offset);
            }
            self.pages[page_num] = Some(page);
        }
        self.pages[page_num].as_mut().unwrap()
    }

    fn flush(&mut self, page_num: usize) {
        if let Some(page) = &self.pages[page_num] {
            let offset = (page_num * PAGE_SIZE) as u64;
            self.file.write_at(&**page, offset).expect("Disk write failed");
        }
    }
}

struct Table {
    pager: Pager,
    columns: Vec<Column>,
    row_size: usize,
    root_page_num: u32,
}

impl Table {
    fn new(filename: &str, raw_cols: Vec<(&str, DataType)>) -> Self {
        let mut columns = Vec::new();
        let mut current_offset = 0;
        for (c_name, c_type) in raw_cols {
            let size = match c_type { DataType::Integer => 4, DataType::Text(s) => s as usize };
            columns.push(Column { name: c_name.to_string(), data_type: c_type, size, offset: current_offset });
            current_offset += size;
        }

        let mut pager = Pager::open(filename).unwrap();
        if pager.num_pages == 0 {
            let page = pager.get_page(0);
            page[NODE_TYPE_OFFSET] = 1; // Leaf
            page[IS_ROOT_OFFSET] = 1;
            unsafe { ptr::write_unaligned(page.as_mut_ptr().add(LEAF_NODE_NUM_CELLS_OFFSET) as *mut u32, 0); }
            pager.num_pages = 1;
            pager.flush(0);
        }

        Table { pager, columns, row_size: current_offset, root_page_num: 0 }
    }

    fn get_num_cells(&mut self, page_num: u32) -> u32 {
        let page = self.pager.get_page(page_num as usize);
        // Fixed: Added cast to *const () to satisfy read_unaligned requirements
        unsafe { ptr::read_unaligned(page.as_ptr().add(LEAF_NODE_NUM_CELLS_OFFSET) as *const () as *const u32) }
    }

    fn find_leaf_node_slot(&mut self, page_num: u32, id: u32) -> (u32, bool) {
        let num_cells = self.get_num_cells(page_num);
        let cell_size = 4 + self.row_size;
        let page = self.pager.get_page(page_num as usize);
        
        let mut min = 0;
        let mut max = num_cells;
        while min < max {
            let mid = (min + max) / 2;
            let mid_id_ptr = unsafe { page.as_ptr().add(LEAF_NODE_HEADER_SIZE + (mid as usize * cell_size)) };
            let mid_id = unsafe { ptr::read_unaligned(mid_id_ptr as *const () as *const u32) };
            if id == mid_id { return (mid, true); }
            if id < mid_id { max = mid; } else { min = mid + 1; }
        }
        (min, false)
    }

    fn split_and_insert(&mut self, old_page_num: u32, _id: u32, _parts: &[&str]) {
        let new_page_num = self.pager.num_pages;
        self.pager.num_pages += 1;
        
        println!("Error: Table full. Splitting Leaf Node {} into new page {}...", old_page_num, new_page_num);
        println!("(B-Tree full split logic is the next development step).");
    }
}

fn main() {
    let mut table = Table::new("users.db", vec![
        ("id", DataType::Integer),
        ("username", DataType::Text(32)),
        ("email", DataType::Text(32)),
    ]);

    println!("RSQL B-Tree Shell. Sorted storage + No duplicates.");

    loop {
        print!("rsql > ");
        io::stdout().flush().unwrap();
        let mut input = String::new();
        io::stdin().read_line(&mut input).unwrap();
        let parts: Vec<&str> = input.trim().split_whitespace().collect();
        if parts.is_empty() { continue; }

        match parts[0] {
            "insert" => {
                if parts.len() < 4 { println!("Usage: insert <id> <user> <email>"); continue; }
                let id = parts[1].parse::<u32>().unwrap();
                let (slot, exists) = table.find_leaf_node_slot(table.root_page_num, id);
                
                if exists {
                    println!("Error: Duplicate key {}", id);
                    continue;
                }

                let num_cells = table.get_num_cells(table.root_page_num);
                let cell_size = 4 + table.row_size;

                if (LEAF_NODE_HEADER_SIZE + (num_cells as usize + 1) * cell_size) > PAGE_SIZE {
                    table.split_and_insert(table.root_page_num, id, &parts);
                    continue;
                }

                let page = table.pager.get_page(table.root_page_num as usize);
                let dest = unsafe { page.as_mut_ptr().add(LEAF_NODE_HEADER_SIZE + (slot as usize * cell_size)) };

                if slot < num_cells {
                    unsafe { ptr::copy(dest, dest.add(cell_size), (num_cells - slot) as usize * cell_size); }
                }

                unsafe {
                    ptr::write_unaligned(dest as *mut u32, id);
                    let row_dest = dest.add(4);
                    for (i, col) in table.columns.iter().enumerate() {
                        if col.name == "id" { continue; }
                        let val = parts.get(i + 1).unwrap_or(&"");
                        let b = val.as_bytes();
                        ptr::write_bytes(row_dest.add(col.offset), 0, col.size);
                        ptr::copy_nonoverlapping(b.as_ptr(), row_dest.add(col.offset), b.len().min(col.size));
                    }
                }
                
                let new_total = num_cells + 1;
                let page_update = table.pager.get_page(table.root_page_num as usize);
                unsafe { ptr::write_unaligned(page_update.as_mut_ptr().add(LEAF_NODE_NUM_CELLS_OFFSET) as *mut u32, new_total); }
                
                table.pager.flush(table.root_page_num as usize);
                println!("Inserted.");
            }
            "select" => {
                let num_cells = table.get_num_cells(0);
                let cell_size = 4 + table.row_size;
                let page = table.pager.get_page(0);
                
                for i in 0..num_cells {
                    let ptr = unsafe { page.as_ptr().add(LEAF_NODE_HEADER_SIZE + (i as usize * cell_size)) };
                    let id = unsafe { ptr::read_unaligned(ptr as *const () as *const u32) };
                    print!("| ID: {:<3} ", id);
                    
                    let row_ptr = unsafe { ptr.add(4) };
                    for col in &table.columns {
                        if col.name == "id" { continue; }
                        let buf = unsafe { std::slice::from_raw_parts(row_ptr.add(col.offset), col.size) };
                        print!("| {:<10} ", String::from_utf8_lossy(buf).trim_matches(char::from(0)));
                    }
                    println!("|");
                }
            }
            ".exit" => break,
            _ => println!("Unknown command."),
        }
    }
}