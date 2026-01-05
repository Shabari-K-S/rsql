# RSQL: A SQLite Clone from Scratch in Rust

**RSQL** is a lightweight, disk-backed relational database management system (RDBMS) built in Rust. This project explores the "magic" of databases by implementing a persistent B-Tree storage engine from the ground up.

## 🏗️ Architecture

RSQL follows a modular architecture inspired by SQLite’s design:

| Component | Responsibility | Status |
| --- | --- | --- |
| **REPL** | The Command Line Interface for user interaction. | ✅ Active |
| **Pager** | Manages 4KB pages and handles raw byte I/O with the filesystem. | ✅ Implemented |
| **B-Tree** | Organizes data into sorted pages for  searching. | 🚧 In Progress |
| **Storage** | Uses raw memory pointers for high-performance serialization. | ✅ Implemented |

---

## 🛠️ Current Implementation Status

### ✅ Phase 1: The Pager & Persistence

* **Disk-Backed Storage:** Data is no longer volatile. It is saved to `users.db` and persists after the program closes.
* **4KB Page Logic:** Implemented a standard SQLite-style page size (4096 bytes) to optimize for disk block alignment.
* **Memory Management:** A custom `Pager` struct handles the loading and flushing of pages between RAM and Disk.

### ✅ Phase 2: Leaf Node Engine

* **Row Serialization:** Efficiently packs integers and text (up to 32 chars) into binary rows.
* **Sorted Insertion:** Uses **Binary Search** to find the correct insertion slot, maintaining a perfectly sorted index by Primary Key (ID).
* **Memory Shifting:** Utilizes `ptr::copy` to perform "on-disk surgery," shifting rows to make room for new data while keeping the page sorted.
* **Duplicate Prevention:** Detects and rejects duplicate IDs before they reach the storage layer.

### 🚧 Phase 3: B-Tree Scaling (Current Goal)

* **Node Splitting:** Implementing the logic to split a full 4KB leaf and distribute rows to a new page.
* **Internal Nodes:** Creating "signpost" nodes that store keys and pointers to child pages.
* **Root Management:** Moving the root pointer as the tree grows in height.

---

## 🧪 Technical Achievements & Benchmarks

* ** Search:** Even within a single page, RSQL uses binary search rather than linear scanning to locate records.
* **Byte-Perfect Alignment:** Uses Rust's unsafe pointer toolkit (`ptr::write_unaligned`) to ensure metadata headers and row data are packed without padding, maximizing storage density.
* **Persistence Guarantee:** Every successful `insert` command triggers a page flush, ensuring "durability" (the D in ACID).

---

## 🚀 How to Run

1. **Build and Run:**
```bash
cargo run

```


2. **Commands:**
* `insert <id> <name> <email>` - Adds a sorted record to the database.
* `select` - Displays all records currently in the root page.
* `.exit` - Safely flushes all buffers to disk and closes the file.



---

## 📈 Roadmap

* [ ] **Recursive B-Tree Search:** Allow `select` to traverse multiple pages.
* [ ] **Internal Node Implementation:** Support for trees with a height > 1.
* [ ] **Variable Length Records:** Support for strings longer than 32 bytes using overflow pages.
