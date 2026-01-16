# MyDB Architecture

This document provides a technical deep dive into the internal architecture of MyDB, explaining design decisions, data structures, and algorithms.

## System Overview

MyDB follows a **layered architecture** with clear separation of concerns:

```
┌─────────────────────────────────────────┐
│         User Interface Layer            │
│  (REPL / Flask Web App)                 │
└──────────────┬──────────────────────────┘
               │
┌──────────────▼──────────────────────────┐
│         Execution Layer                  │
│  (Executor - Query Orchestration)        │
└──────────────┬──────────────────────────┘
               │
┌──────────────▼──────────────────────────┐
│         Parsing Layer                    │
│  (SQL Parser - Regex-based)              │
└──────────────┬──────────────────────────┘
               │
┌──────────────▼──────────────────────────┐
│         Storage Layer                    │
│  (Table, Database, Index)                │
└──────────────┬──────────────────────────┘
               │
┌──────────────▼──────────────────────────┐
│         Persistence Layer                │
│  (JSON File I/O)                         │
└─────────────────────────────────────────┘
```

## Component Breakdown

### 1. Storage Layer (`src/storage.py`)

**Core Classes:**

#### `Table`
Represents a single database table.

**Data Structure:**
```python
{
    "name": str,
    "columns": List[Tuple[str, str]],  # [(name, type), ...]
    "primary_key": Optional[str],
    "unique_columns": List[str],
    "rows": List[List[Any]],           # Row-oriented storage
    "indexes": Dict[str, Index],       # Column name -> Index
    "_col_map": Dict[str, int]         # Column name -> position
}
```

**Key Methods:**
- `insert_row()`: Validates constraints, updates indexes, appends row
- `select()`: Filters rows based on WHERE conditions, uses indexes when possible
- `update()`: Modifies rows matching WHERE clause
- `delete()`: Removes rows matching WHERE clause
- `inner_join()`: Performs hash join with another table

**Design Decision: Row-Oriented Storage**
- **Why**: Simplicity and ease of implementation
- **Trade-off**: Less efficient for analytical queries (column scans)
- **Alternative**: Column-oriented storage (better for aggregates)

#### `Database`
Container for multiple tables with persistence logic.

**Key Methods:**
- `create_table()`: Instantiates new Table object
- `save_to_file()`: Serializes all tables to JSON
- `load_from_file()`: Deserializes JSON and rebuilds indexes

### 2. Parsing Layer (`src/sql_parser.py`)

**Approach**: Regex-based pattern matching

**Design Decision: Why Regex?**
- ✅ **Pros**: Simple, fast to implement, no external dependencies
- ❌ **Cons**: Limited error handling, hard to extend, no AST

**Alternative Considered**: Lexer/Parser (e.g., PLY, Lark)
- Would provide better error messages and extensibility
- Overkill for this educational project

**Key Functions:**
- `parse_create_table()`: Extracts table name, columns, constraints
- `parse_select()`: Handles SELECT with JOIN and WHERE
- `parse_insert()`: Parses VALUES clause
- `_parse_key_value_pairs()`: Shared logic for WHERE/SET clauses

**Critical Fix (Phase 7):**
Updated regex from `(\w+)` to `([\w\.]+)` to support qualified column names like `users.name` in JOIN queries.

### 3. Execution Layer (`src/executor.py`)

**Role**: Orchestrates query execution by calling parser and storage methods.

**Workflow:**
```
SQL String → Parser → Parsed Dict → Executor → Storage → Result
```

**Key Method: `execute()`**
```python
def execute(sql: str) -> Union[str, List[Dict]]:
    parsed = parse_sql(sql)
    if parsed['command'] == 'SELECT':
        return self._execute_select(parsed)
    elif parsed['command'] == 'INSERT':
        return self._execute_insert(parsed)
    # ... etc
    
    # Auto-save after modifications
    if modified and self.db_file:
        self.db.save_to_file(self.db_file)
```

**Auto-Save Logic:**
After any data-modifying command (INSERT, UPDATE, DELETE), the database is automatically persisted to disk if a file is associated with the executor.

### 4. Indexing (`src/indexes.py`)

**Implementation**: Hash-based index using Python dictionaries

**Data Structure:**
```python
{
    "column_name": str,
    "is_unique": bool,
    "_index": Dict[Any, Union[int, List[int]]]  # value -> row_id(s)
}
```

**Operations:**
- `insert(value, row_id)`: O(1) - Add entry
- `lookup(value)`: O(1) - Find row IDs
- `remove(value, row_id)`: O(1) - Delete entry

**Design Decision: Hash vs B-Tree**
- **Hash**: O(1) equality lookups, no range queries
- **B-Tree**: O(log n) lookups, supports range queries
- **Choice**: Hash for simplicity (no range queries needed yet)

**Index Usage:**
- Primary keys: Always indexed
- Unique columns: Always indexed
- JOIN optimization: Uses right table's index if available

### 5. JOIN Implementation

**Algorithm**: Hash Join (optimized for equality joins)

**Steps:**
1. **Build Phase**: Create hash map of right table (or use existing index)
2. **Probe Phase**: For each row in left table, lookup matching rows in hash map
3. **Merge**: Combine matching rows with qualified column names (`table.column`)

**Pseudocode:**
```python
def inner_join(left_table, right_table, left_col, right_col):
    # Build hash map (or use index)
    hash_map = {}
    for row in right_table.rows:
        key = row[right_col_index]
        hash_map[key] = row
    
    # Probe and merge
    results = []
    for left_row in left_table.rows:
        key = left_row[left_col_index]
        if key in hash_map:
            merged = merge(left_row, hash_map[key])
            results.append(merged)
    
    return results
```

**Complexity:**
- **Time**: O(N + M) where N, M are table sizes
- **Space**: O(M) for hash map
- **Optimization**: If right table has index on join column, skip build phase

**Column Resolution:**
The executor resolves ambiguous column names in JOIN queries:
```sql
SELECT * FROM users JOIN orders ON users.id = orders.user_id
```
- Parser extracts: `on_left = "users.id"`, `on_right = "orders.user_id"`
- Executor splits by `.` to determine which column belongs to which table
- Fallback: If no `.`, check which table has the column

### 6. Persistence Layer

**Format**: JSON (human-readable, debuggable)

**File Extension**: `.josedb` (custom branding)

**Structure:**
```json
{
  "tables": {
    "users": {
      "columns": [["id", "INTEGER"], ["name", "TEXT"]],
      "primary_key": "id",
      "unique_columns": [],
      "rows": [[1, "Alice"], [2, "Bob"]]
    }
  }
}
```

**Critical: Index Rebuilding**
Indexes are **not** stored in the file (they're derived data). On load:
1. Deserialize tables and rows
2. Re-insert each row into indexes to rebuild them

**Design Decision: JSON vs Binary**
- **JSON**: Human-readable, easy debugging, portable
- **Binary** (e.g., pickle): Faster, smaller files
- **Choice**: JSON for educational transparency

## Performance Characteristics

| Operation | Complexity | Notes |
|-----------|------------|-------|
| INSERT | O(1) | Amortized (list append + index insert) |
| SELECT (no index) | O(N) | Full table scan |
| SELECT (indexed) | O(1) | Hash index lookup |
| UPDATE | O(N) | Must scan all rows |
| DELETE | O(N) | Must scan all rows |
| INNER JOIN | O(N + M) | Hash join with index optimization |
| File Save | O(N) | Serialize all rows |
| File Load | O(N) | Deserialize + rebuild indexes |

**Bottlenecks:**
- UPDATE/DELETE always scan (no index usage for WHERE)
- Full database loaded into memory
- No query optimization or execution planning

## Design Trade-offs

### 1. Regex Parser vs Proper Parser
**Choice**: Regex  
**Reason**: Simplicity for educational project  
**Cost**: Limited error messages, hard to extend

### 2. Row-Oriented vs Column-Oriented
**Choice**: Row-oriented  
**Reason**: Simpler implementation, better for OLTP  
**Cost**: Slower for analytical queries (aggregates)

### 3. Hash Index vs B-Tree
**Choice**: Hash  
**Reason**: O(1) lookups, simpler code  
**Cost**: No range queries (e.g., `WHERE age > 18`)

### 4. JSON vs Binary Persistence
**Choice**: JSON  
**Reason**: Human-readable, debuggable  
**Cost**: Larger file size, slower I/O

### 5. Auto-Save vs Manual Save
**Choice**: Auto-save  
**Reason**: Mimics real RDBMS behavior, prevents data loss  
**Cost**: Slower writes (every modification hits disk)

## Future Enhancements

### Short-Term (Easy Wins)
- Add `ORDER BY` support (Python's `sorted()`)
- Implement `LIMIT` and `OFFSET`
- Add aggregate functions (COUNT, SUM, AVG)
- Better error messages with line numbers

### Medium-Term (Moderate Effort)
- Query planner and optimizer
- LEFT/RIGHT JOIN support
- Subquery support
- Index usage for WHERE clauses in UPDATE/DELETE

### Long-Term (Major Refactor)
- Transaction support (ACID)
- Concurrent access (locks/MVCC)
- Binary file format (faster I/O)
- B-Tree indexes (range queries)
- Column-oriented storage option

## Conclusion

MyDB demonstrates core RDBMS concepts in ~1500 lines of Python. While not production-ready, it provides a solid foundation for understanding how databases work internally and serves as an excellent educational tool.

The architecture prioritizes **simplicity and readability** over performance, making it ideal for learning and teaching database internals.
