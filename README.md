# RSQL: A SQLite Clone from Scratch in Rust (Still in Development)

**RSQL** is a lightweight, disk-backed relational database management system (RDBMS). This project explores database internals by implementing a SQL compiler, a Virtual Machine, and a persistent B-Tree storage engine from the ground up.

## 🏗️ Architecture

RSQL follows a modular architecture inspired by SQLite’s design:

| Component | Responsibility |
| --- | --- |
| **REPL** | The Command Line Interface for user interaction. |
| **Compiler** | Parses SQL strings into a Virtual Machine instruction set. |
| **Virtual Machine** | Executes instructions (bytecode) by interacting with the B-Tree layer. |
| **B-Tree** | Organizes data into 4KB pages for  searching and insertion. |
| **Pager** | Manages a page cache and handles raw byte I/O with the filesystem. |

---

## 🛠️ Current Implementation Status

### ✅ Phase 1: The Front-end

* **REPL:** Implemented a robust loop using `stdin` with colored status/error indicators.
* **Meta-commands:** Support for `.exit`, `.btree` (visualization), and `.constants`.
* **SQL Parser:** Sophisticated parsing with support for `insert` and `select`.

### ✅ Phase 2: The Virtual Machine & Memory

* **Row Serialization:** Rows are serialized into a compact 291-byte binary format.
* **Internal Representation:** Uses `Statement` and `StatementType` enums to guide the VM.
* **Binary Search:** Searches leaf nodes in  time to ensure sorted storage by Primary Key (ID).

### ✅ Phase 3: The Back-end (Persistence & B-Tree)

* **Pager:** Implemented a file-backed caching system that reads/writes 4096-byte pages.
* **B-Tree Leaf Nodes:** Implemented the header/body format for leaf nodes, currently holding up to 13 rows.
* **B-Tree Internal Nodes:** Implemented internal nodes with a branching factor of 511.
* **Splitting & Re-rooting:** Root nodes now split and create new internal parents when full.
* **Recursive Search:** The engine recursively traverses internal nodes to find target leaf pages.
* **Cursor Abstraction:** A location-agnostic system to navigate the table without exposing memory math.

---

## 🧪 Technical Achievements

* **Sorted Storage:** RSQL now automatically sorts records by ID using binary search, regardless of insertion order.
* **Persistence:** Data is automatically flushed to disk on `.exit` and reloaded upon startup.
* **Memory Safety:** Utilizes Rust's `ptr::read_unaligned` and `ptr::write_unaligned` to safely handle packed B-Tree metadata.
* **Duplicate Detection:** Prevents data corruption by rejecting duplicate Primary Keys.

---

## 🚀 Future Roadmap

* [ ] **Phase 4:** **Internal Node Splitting:** Currently, RSQL can only split the root; it needs recursive internal splitting to grow indefinitely.
* [ ] **Phase 5:** **Parent Pointer Updates:** Implementing the logic to update child-to-parent links after a split.
* [ ] **Phase 6:** **B-Tree Deletion:** Supporting the `DELETE` keyword and merging nodes to recover space.

