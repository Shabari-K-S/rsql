# RSQL: A SQLite Clone from Scratch in Rust

**RSQL** is a lightweight, disk-backed relational database management system (RDBMS) built in Rust. This project implements a persistent B-Tree storage engine, SQL parser, and interactive REPL from the ground up.

![Demo](https://img.shields.io/badge/Rust-1.70+-orange?style=flat-square) ![License](https://img.shields.io/badge/License-MIT-blue?style=flat-square)

## ✨ Features

- 🌳 **B-Tree Storage Engine** - Disk-backed with 4KB pages, automatic node splitting
- 📝 **SQL Parser** - Supports CREATE, INSERT, SELECT, UPDATE, DROP
- 🔍 **WHERE Clauses** - Filter with =, !=, <, >, <=, >= and AND/OR
- 🎨 **Rich REPL** - Colored output, command history, tab completion
- 💾 **Persistence** - Data survives restarts, stored in `.db` files

---

## 🏗️ Architecture

```
src/
├── main.rs          # Interactive REPL with rustyline
├── pager.rs         # 4KB page I/O management
├── btree.rs         # B-Tree node operations (leaf + internal)
├── table.rs         # Table & row handling, B-Tree traversal
├── tokenizer.rs     # SQL lexer
├── parser.rs        # SQL parser → AST
├── executor.rs      # Query execution engine
└── completer.rs     # Tab completion for SQL keywords
```

| Component | Status |
|-----------|--------|
| Pager (4KB pages) | ✅ Done |
| B-Tree (leaf + internal nodes) | ✅ Done |
| SQL Parser | ✅ Done |
| Query Executor | ✅ Done |
| WHERE clauses | ✅ Done |
| Enhanced REPL | ✅ Done |
| Transactions | 🔜 Planned |

---

## 🚀 Quick Start

```bash
# Build and run
cargo run

# Or build release
cargo build --release
./target/release/rsql
```

---

## 📖 SQL Commands

### Create a Table
```sql
CREATE TABLE users (id INTEGER, name TEXT, email TEXT)
```

### Insert Data
```sql
INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')
INSERT INTO users VALUES (2, 'Bob', 'bob@example.com')
```

### Query Data
```sql
SELECT * FROM users
SELECT name, email FROM users WHERE id > 1
SELECT * FROM users WHERE name = 'Alice' AND id < 10
```

### Update Data
```sql
UPDATE users SET email = 'new@email.com' WHERE id = 1
```

### Drop Table
```sql
DROP TABLE users
```

---

## 🔧 Meta Commands

| Command | Description |
|---------|-------------|
| `.help` | Show help |
| `.tables` | List all tables |
| `.schema` | Show table schemas |
| `.exit` | Exit (Ctrl+D also works) |

---

## ⌨️ REPL Features

- **↑↓** - Navigate command history
- **Tab** - Autocomplete SQL keywords
- **Ctrl+C** - Cancel current input
- **Ctrl+D** - Exit

History is saved to `~/.rsql_history`.

---

## 🧪 Technical Details

- **Page Size:** 4096 bytes (SQLite-compatible)
- **B-Tree:** Supports leaf node splitting and internal nodes
- **Binary Search:** O(log n) lookups within pages
- **Persistence:** Immediate flush after each write
- **Serialization:** Raw pointer operations for zero-copy I/O

---

## 📈 Roadmap

- [x] B-Tree with node splitting
- [x] SQL Parser (CREATE, INSERT, SELECT, UPDATE, DROP)
- [x] WHERE clause support
- [x] Enhanced REPL with history
- [ ] DELETE statement
- [ ] JOIN support
- [ ] Transactions (BEGIN/COMMIT/ROLLBACK)
- [ ] Secondary indexes
- [ ] Variable-length records

