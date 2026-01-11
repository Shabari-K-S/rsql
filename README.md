# RSQL: A SQLite Clone from Scratch in Rust

**RSQL** is a lightweight, disk-backed relational database management system (RDBMS) built in Rust. This project implements a persistent B-Tree storage engine, SQL parser, and interactive REPL from the ground up.

![Demo](https://img.shields.io/badge/Rust-1.70+-orange?style=flat-square) ![License](https://img.shields.io/badge/License-MIT-blue?style=flat-square)

## ✨ Features

- 🗄️ **Database Management** - CREATE DATABASE and CONNECT for organized data storage
- 🌳 **B-Tree Storage Engine** - Disk-backed with 4KB pages, automatic node splitting
- 📝 **SQL Parser** - Supports CREATE, INSERT, SELECT, UPDATE, DELETE, DROP
- 🔍 **WHERE Clauses** - Filter with =, !=, <, >, <=, >= and AND/OR
- 🔗 **JOIN Support** - INNER JOIN for combining tables
- 📇 **Secondary Indexes** - CREATE INDEX and CREATE UNIQUE INDEX for fast lookups
- 💳 **Transactions** - BEGIN, COMMIT, ROLLBACK with deferred writes
- 🎨 **Rich REPL** - Colored output, command history, tab completion
- 💾 **Persistence** - Data survives restarts, stored in `~/.rsql/databases/`

---

## 🏗️ Architecture

```
src/
├── main.rs          # Interactive REPL with rustyline
├── pager.rs         # 4KB page I/O management
├── btree.rs         # B-Tree node operations (leaf + internal)
├── table.rs         # Table & row handling, B-Tree traversal
├── index.rs         # Secondary index management (B-Tree based)
├── tokenizer.rs     # SQL lexer
├── parser.rs        # SQL parser → AST
├── executor.rs      # Query execution engine
└── completer.rs     # Tab completion for SQL keywords
```

| Component | Status |
|-----------|--------|
| Database Management | ✅ Done |
| Pager (4KB pages) | ✅ Done |
| B-Tree (leaf + internal nodes) | ✅ Done |
| SQL Parser | ✅ Done |
| Query Executor | ✅ Done |
| WHERE clauses | ✅ Done |
| JOIN support | ✅ Done |
| Transactions | ✅ Done |
| Secondary Indexes | ✅ Done |
| Enhanced REPL | ✅ Done |

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

### Database Management
```sql
-- Create a new database
CREATE DATABASE myapp

-- Connect to a database (required before any table operations)
CONNECT myapp
```

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

### Delete Data
```sql
DELETE FROM users WHERE id = 2
```

### Drop Table
```sql
DROP TABLE users
```

### Indexes
```sql
-- Create a secondary index for faster lookups
CREATE INDEX idx_email ON users(email)

-- Create a unique index (enforces uniqueness)
CREATE UNIQUE INDEX idx_name ON users(name)

-- Drop an index
DROP INDEX idx_email
```

### Transactions
```sql
BEGIN
INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com')
-- Changes are not written to disk yet
COMMIT  -- Or ROLLBACK to discard changes
```

### Joins
```sql
SELECT * FROM users JOIN orders ON users.id = orders.user_id
```

---

## 🔧 Meta Commands

| Command | Description |
|---------|-------------|
| `.help` | Show help |
| `.databases` | List all databases |
| `.tables` | List all tables |
| `.indexes` | List all indexes |
| `.schema` | Show table schemas |
| `.exit` | Exit (Ctrl+D also works) |

---

## ⌨️ REPL Features

- **Dynamic Prompt** - Shows connected database: `rsql[mydb]>`
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
- **Persistence:** Data stored in `~/.rsql/databases/<db_name>/`
- **Serialization:** Raw pointer operations for zero-copy I/O
- **Indexes:** Each secondary index uses its own B-Tree file (`.idx`)
- **Metadata:** Table schemas stored in `metadata.json` per database

### Directory Structure
```
~/.rsql/
└── databases/
    ├── myapp/
    │   ├── metadata.json     # Table schemas
    │   ├── users.db          # Table data
    │   └── users_idx_email.idx  # Index file
    └── testdb/
        └── ...
```

---

## 📈 Roadmap

- [x] B-Tree with node splitting
- [x] SQL Parser (CREATE, INSERT, SELECT, UPDATE, DROP)
- [x] WHERE clause support
- [x] Enhanced REPL with history
- [x] DELETE statement
- [x] JOIN support
- [x] Transactions (BEGIN/COMMIT/ROLLBACK)
- [x] Database management (CREATE DATABASE/CONNECT)
- [x] Secondary indexes (CREATE INDEX/DROP INDEX)
- [x] UNIQUE constraint enforcement
- [ ] Variable-length records
- [ ] Query optimizer
- [ ] Multiple column indexes

---

## 🎯 Example Session

```
$ cargo run

╔═══════════════════════════════════════════════════════════╗
║               RSQL - SQLite Clone in Rust                 ║
╠═══════════════════════════════════════════════════════════╣
║  Type SQL commands or .help for available commands        ║
║  Use ↑↓ for history, Tab for completion                   ║
╚═══════════════════════════════════════════════════════════╝

rsql> CREATE DATABASE myapp
✓ Database 'myapp' created.

rsql> CONNECT myapp
✓ Connected to database 'myapp'.

rsql[myapp]> CREATE TABLE users (id INTEGER, name TEXT, email TEXT)
✓ Table 'users' created.

rsql[myapp]> INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')
✓ 1 row(s) inserted.

rsql[myapp]> CREATE UNIQUE INDEX idx_email ON users(email)
✓ Index 'idx_email' created.

rsql[myapp]> SELECT * FROM users
┌────┬───────┬───────────────────┐
│ id │ name  │       email       │
├────┼───────┼───────────────────┤
│ 1  │ Alice │ alice@example.com │
└────┴───────┴───────────────────┘

rsql[myapp]> .exit
Goodbye!
```

Build with ❤️ by Shabari.