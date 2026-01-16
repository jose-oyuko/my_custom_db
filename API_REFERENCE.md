# MyDB API Reference

Complete reference for all public classes and methods in the MyDB engine.

## Table of Contents
- [Storage Module](#storage-module)
- [SQL Parser Module](#sql-parser-module)
- [Executor Module](#executor-module)
- [Index Module](#index-module)
- [REPL Module](#repl-module)

---

## Storage Module (`src/storage.py`)

### Class: `Table`

Represents a database table with rows, columns, and constraints.

#### `__init__(name, columns, primary_key=None, unique_columns=None)`

**Parameters:**
- `name` (str): Table name
- `columns` (List[Tuple[str, str]]): List of (column_name, data_type) tuples
- `primary_key` (str, optional): Name of primary key column
- `unique_columns` (List[str], optional): List of unique constraint columns

**Example:**
```python
table = Table("users", [("id", "INTEGER"), ("name", "TEXT")], primary_key="id")
```

#### `insert_row(row: List[Any]) -> None`

Insert a new row into the table.

**Parameters:**
- `row` (List[Any]): Values matching column order

**Raises:**
- `ValueError`: If primary key or unique constraint violated

**Example:**
```python
table.insert_row([1, "Alice"])
```

**SQL Equivalent:** `INSERT INTO users VALUES (1, 'Alice')`

#### `select(columns=None, where=None) -> List[Dict[str, Any]]`

Query rows from the table.

**Parameters:**
- `columns` (List[str], optional): Columns to return (None = all)
- `where` (Dict[str, Any], optional): Filter conditions

**Returns:**
- List of dictionaries (column_name -> value)

**Example:**
```python
results = table.select(columns=["name"], where={"id": 1})
# Returns: [{"name": "Alice"}]
```

**SQL Equivalent:** `SELECT name FROM users WHERE id = 1`

#### `update(set_values: Dict[str, Any], where: Dict[str, Any]) -> int`

Update rows matching WHERE clause.

**Parameters:**
- `set_values` (Dict[str, Any]): Columns to update
- `where` (Dict[str, Any]): Filter conditions

**Returns:**
- Number of rows updated

**Example:**
```python
count = table.update({"name": "Bob"}, {"id": 1})
```

**SQL Equivalent:** `UPDATE users SET name = 'Bob' WHERE id = 1`

#### `delete(where: Dict[str, Any]) -> int`

Delete rows matching WHERE clause.

**Parameters:**
- `where` (Dict[str, Any]): Filter conditions

**Returns:**
- Number of rows deleted

**Example:**
```python
count = table.delete({"id": 1})
```

**SQL Equivalent:** `DELETE FROM users WHERE id = 1`

#### `inner_join(other, left_col, right_col, select_columns=None, where=None) -> List[Dict]`

Perform INNER JOIN with another table.

**Parameters:**
- `other` (Table): Right table to join with
- `left_col` (str): Join column from this table
- `right_col` (str): Join column from other table
- `select_columns` (List[str], optional): Columns to return
- `where` (Dict[str, Any], optional): Filter conditions

**Returns:**
- List of dictionaries with qualified keys (`table.column`)

**Example:**
```python
results = users.inner_join(
    orders, 
    left_col="id", 
    right_col="user_id",
    select_columns=["users.name", "orders.amount"]
)
```

**SQL Equivalent:**
```sql
SELECT users.name, orders.amount 
FROM users 
JOIN orders ON users.id = orders.user_id
```

---

### Class: `Database`

Container for multiple tables with persistence.

#### `__init__()`

Create a new empty database.

**Example:**
```python
db = Database()
```

#### `create_table(name, columns, primary_key=None, unique_columns=None) -> Table`

Create a new table in the database.

**Parameters:**
- Same as `Table.__init__()`

**Returns:**
- The created Table object

**Raises:**
- `ValueError`: If table already exists

**Example:**
```python
table = db.create_table("users", [("id", "INTEGER"), ("name", "TEXT")], primary_key="id")
```

**SQL Equivalent:** `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`

#### `get_table(name: str) -> Table`

Retrieve a table by name.

**Parameters:**
- `name` (str): Table name

**Returns:**
- Table object

**Raises:**
- `ValueError`: If table doesn't exist

**Example:**
```python
users = db.get_table("users")
```

#### `drop_table(name: str) -> None`

Delete a table from the database.

**Parameters:**
- `name` (str): Table name

**Example:**
```python
db.drop_table("users")
```

**SQL Equivalent:** `DROP TABLE users`

#### `save_to_file(filename: str) -> None`

Persist database to a JSON file.

**Parameters:**
- `filename` (str): Path to `.josedb` file

**Example:**
```python
db.save_to_file("my_data.josedb")
```

#### `load_from_file(filename: str) -> None`

Load database from a JSON file.

**Parameters:**
- `filename` (str): Path to `.josedb` file

**Example:**
```python
db.load_from_file("my_data.josedb")
```

---

## SQL Parser Module (`src/sql_parser.py`)

### `parse_sql(sql: str) -> Dict[str, Any]`

Main entry point for parsing SQL commands.

**Parameters:**
- `sql` (str): SQL statement

**Returns:**
- Dictionary with parsed components

**Raises:**
- `ValueError`: If SQL syntax is invalid

**Example:**
```python
parsed = parse_sql("SELECT * FROM users WHERE id = 1")
# Returns: {"command": "SELECT", "table": "users", "columns": None, "where": {"id": 1}, "join": None}
```

### `parse_create_table(sql: str) -> Dict`

Parse CREATE TABLE statement.

**Returns:**
```python
{
    "command": "CREATE",
    "table": str,
    "columns": List[Tuple[str, str]],
    "primary_key": Optional[str],
    "unique_columns": List[str]
}
```

### `parse_select(sql: str) -> Dict`

Parse SELECT statement (with optional JOIN and WHERE).

**Returns:**
```python
{
    "command": "SELECT",
    "table": str,
    "columns": Optional[List[str]],
    "where": Optional[Dict[str, Any]],
    "join": Optional[Dict]  # {"table": str, "on_left": str, "on_right": str}
}
```

### `parse_insert(sql: str) -> Dict`

Parse INSERT statement.

**Returns:**
```python
{
    "command": "INSERT",
    "table": str,
    "values": List[Any]
}
```

---

## Executor Module (`src/executor.py`)

### Class: `Executor`

Orchestrates query execution.

#### `__init__(db: Database, db_file: str = None)`

**Parameters:**
- `db` (Database): Database instance
- `db_file` (str, optional): Path for auto-save

**Example:**
```python
executor = Executor(db, "my_data.josedb")
```

#### `execute(sql: str) -> Union[str, List[Dict]]`

Execute a SQL command.

**Parameters:**
- `sql` (str): SQL statement

**Returns:**
- String message (for DDL/DML) or List of dicts (for SELECT)

**Example:**
```python
result = executor.execute("SELECT * FROM users")
```

---

## Index Module (`src/indexes.py`)

### Class: `Index`

Hash-based index for fast lookups.

#### `__init__(column_name: str, is_unique: bool = False)`

**Parameters:**
- `column_name` (str): Indexed column
- `is_unique` (bool): Whether to enforce uniqueness

#### `insert(value: Any, row_id: int) -> None`

Add an entry to the index.

**Raises:**
- `ValueError`: If unique constraint violated

#### `lookup(value: Any) -> List[int]`

Find row IDs for a value.

**Returns:**
- List of row indices

#### `remove(value: Any, row_id: int) -> None`

Remove an entry from the index.

---

## REPL Module (`src/repl.py`)

### Class: `REPL`

Interactive command-line interface.

#### `__init__(filename: str = None)`

**Parameters:**
- `filename` (str, optional): Database file to load

#### `start() -> None`

Start the REPL loop.

**Example:**
```python
repl = REPL("my_data.josedb")
repl.start()
```

### Meta-Commands

| Command | Description |
|---------|-------------|
| `.tables` | List all tables |
| `.describe <table>` | Show table schema |
| `.databases` | List `.josedb` files |
| `.open <file>` | Switch database file |
| `.help` | Show help |
| `.exit` | Exit REPL |

---

## Type Inference

The parser automatically infers types from VALUES:

| Python Type | SQL Type |
|-------------|----------|
| `int` | INTEGER |
| `float` | REAL |
| `bool` | BOOLEAN |
| `str` | TEXT |

**Example:**
```sql
INSERT INTO users VALUES (1, 'Alice', 25, true)
-- Types: INTEGER, TEXT, INTEGER, BOOLEAN
```

---

## Error Handling

All functions raise `ValueError` for invalid operations:

```python
try:
    table.insert_row([1, "Alice"])
    table.insert_row([1, "Bob"])  # Duplicate primary key
except ValueError as e:
    print(f"Error: {e}")
```

---

## Complete Example

```python
from src.storage import Database
from src.executor import Executor

# Create database
db = Database()
executor = Executor(db, "example.josedb")

# Create tables
executor.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
executor.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount INTEGER)")

# Insert data
executor.execute("INSERT INTO users VALUES (1, 'Alice')")
executor.execute("INSERT INTO orders VALUES (101, 1, 500)")

# Query with JOIN
results = executor.execute("""
    SELECT users.name, orders.amount 
    FROM users 
    JOIN orders ON users.id = orders.user_id
""")

print(results)
# Output: [{"users.name": "Alice", "orders.amount": 500}]

# Data is auto-saved to example.josedb
```
