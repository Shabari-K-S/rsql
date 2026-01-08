use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;

pub const PAGE_SIZE: usize = 4096;
pub const TABLE_MAX_PAGES: usize = 100;

pub struct Pager {
    pub file: File,
    pub file_length: u64,
    pub num_pages: u32,
    pub pages: Vec<Option<Box<[u8; PAGE_SIZE]>>>,
}

impl Pager {
    pub fn open(filename: &str) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(filename)?;
        let file_length = file.metadata()?.len();
        let num_pages = (file_length / PAGE_SIZE as u64) as u32;
        let mut pages = Vec::with_capacity(TABLE_MAX_PAGES);
        for _ in 0..TABLE_MAX_PAGES {
            pages.push(None);
        }

        Ok(Pager {
            file,
            file_length,
            num_pages,
            pages,
        })
    }

    pub fn get_page(&mut self, page_num: usize) -> &mut [u8; PAGE_SIZE] {
        if self.pages[page_num].is_none() {
            let mut page = Box::new([0u8; PAGE_SIZE]);
            let offset = (page_num * PAGE_SIZE) as u64;
            if offset < self.file_length {
                let _ = self.file.read_at(&mut *page, offset);
            }
            self.pages[page_num] = Some(page);
            if page_num as u32 >= self.num_pages {
                self.num_pages = page_num as u32 + 1;
            }
        }
        self.pages[page_num].as_mut().unwrap()
    }

    pub fn flush(&mut self, page_num: usize) {
        if let Some(page) = &self.pages[page_num] {
            let offset = (page_num * PAGE_SIZE) as u64;
            self.file
                .write_at(&**page, offset)
                .expect("Disk write failed");
        }
    }

    /// Flush all dirty pages to disk
    pub fn flush_all(&mut self) {
        for i in 0..self.num_pages as usize {
            self.flush(i);
        }
    }
}
