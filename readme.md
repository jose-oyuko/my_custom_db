# MyDB - A Custom RDBMS in Python

A lightweight, educational Relational Database Management System built from scratch in Python. MyDB demonstrates core database concepts including SQL parsing, query execution, indexing, JOINs, and file persistence.

![Dashboard](https://img.shields.io/badge/Status-Complete-brightgreen) ![Python](https://img.shields.io/badge/Python-3.8+-blue) ![License](https://img.shields.io/badge/License-MIT-yellow)

## ✨ Features

- **SQL Support**: CREATE, INSERT, SELECT, UPDATE, DELETE, DROP
- **INNER JOIN**: Relational queries with optimized hash-join algorithm
- **Constraints**: Primary Keys and UNIQUE constraints with automatic indexing
- **File Persistence**: Auto-save to `.josedb` files (JSON format)
- **Interactive REPL**: Command-line interface with meta-commands
- **Web Application**: Flask-based admin dashboard (Mini-Pesapal demo)
- **Hash Indexes**: O(1) lookups for primary keys and unique columns

## 🚀 Quick Start

### Prerequisites
```bash
pip install flask
```

### Running the REPL
```bash
# In-memory mode
python main.py

# Persistent mode (auto-saves to file)
python main.py my_database.josedb
```

**Example Session:**
```sql
MyDB> CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)
Table 'users' created.

MyDB> INSERT INTO users VALUES (1, 'Alice', 30)
Inserted 1 row.

MyDB> SELECT * FROM users WHERE name = 'Alice'
+----+-------+-----+
| id | name  | age |
+----+-------+-----+
| 1  | Alice | 30  |
+----+-------+-----+

MyDB> .tables
users

MyDB> .exit
```

### Running the Web Application
```bash
cd web_app
python app.py
```

Then open `http://127.0.0.1:5000` in your browser.

### Running with Docker 🐳

**Option 1: Docker Compose (Recommended)**
```bash
# Build and start the container
docker-compose up --build

# Access the app at http://localhost:5000

# Stop the container
docker-compose down
```

**Option 2: Docker CLI**
```bash
# Build the image
docker build -t mydb-app .

# Run the container
docker run -p 5000:5000 -v $(pwd)/pesapal_demo.josedb:/app/pesapal_demo.josedb mydb-app

# Access the app at http://localhost:5000
```

**Benefits:**
- ✅ No Python installation required
- ✅ Consistent environment across machines
- ✅ Data persists via volume mounting
- ✅ Easy deployment to cloud platforms

## 📁 Project Structure

```
my_custom_db/
├── src/
│   ├── storage.py       # Table & Database classes (core storage engine)
│   ├── sql_parser.py    # Regex-based SQL parser
│   ├── executor.py      # Query execution orchestrator
│   ├── indexes.py       # Hash-based index implementation
│   └── repl.py          # Interactive command-line interface
├── web_app/
│   ├── app.py           # Flask application (Mini-Pesapal)
│   └── templates/       # HTML templates (Tailwind CSS)
├── tests/
│   ├── test_storage.py
│   ├── test_parser.py
│   ├── test_executor.py
│   ├── test_join.py
│   └── test_persistence.py
├── main.py              # REPL entry point
├── README.md
├── ARCHITECTURE.md
└── API_REFERENCE.md
```

## 🎯 Use Cases

### Educational
- Learn how databases work internally
- Understand SQL parsing and execution
- Study indexing and query optimization

### Demonstration
- Portfolio project showcasing system design
- Interview preparation for database internals
- Teaching tool for database courses

### Prototyping
- Quick in-memory database for Python scripts
- Lightweight alternative to SQLite for simple apps
- Testing relational data models

## 🔧 REPL Meta-Commands

| Command | Description |
|---------|-------------|
| `.tables` | List all tables in the database |
| `.describe <table>` | Show schema for a specific table |
| `.databases` | List all `.josedb` files in current directory |
| `.open <file>` | Switch to a different database file |
| `.help` | Show all available commands |
| `.exit` | Exit the REPL |

## 🌐 Web Application (Mini-Pesapal)

The included Flask app demonstrates a **Payment Gateway Admin Dashboard** inspired by PesaPal:

**Features:**
- **Dashboard**: View transaction analytics with JOIN queries
- **Merchant Management**: Onboard businesses, update commission rates
- **POS Terminal**: Simulate payment transactions
- **Full CRUD**: Create, Read, Update, Delete operations
- **Auto-Persistence**: All changes saved to `pesapal_demo.josedb`

**Tech Stack:**
- Backend: Flask + MyDB engine
- Frontend: Tailwind CSS (CDN)
- Data: `.josedb` file (JSON format)

## 📚 Documentation

- **[ARCHITECTURE.md](ARCHITECTURE.md)**: Technical deep dive into system design
- **[API_REFERENCE.md](API_REFERENCE.md)**: Function-level documentation
- **[walkthrough.md](walkthrough.md)**: Complete feature demonstration

## ⚠️ Limitations

This is an educational project. Production use is **not recommended** due to:

- **No Transactions**: No ACID guarantees or rollback support
- **Single-Threaded**: No concurrent access handling
- **Limited SQL**: No GROUP BY, ORDER BY, subqueries, or aggregates
- **No Security**: SQL injection vulnerable (uses string formatting)
- **In-Memory**: Full database loaded into RAM
- **No Optimization**: No query planner or execution optimization

## 🧪 Running Tests

```bash
# Run all tests
python -m pytest tests/

# Run specific test file
python tests/test_join.py
```

## 🤝 Contributing

Contributions are welcome! Here are some ideas:

- Add `GROUP BY` and `ORDER BY` support
- Implement aggregate functions (COUNT, SUM, AVG)
- Add LEFT/RIGHT JOIN support
- Improve error messages
- Add query execution plans
- Implement B-tree indexes

## 📄 License

MIT License - feel free to use this for learning and teaching!

## 🙏 Acknowledgments

Built as a technical demonstration project for PesaPal's engineering assessment. The web application (Mini-Pesapal) showcases payment gateway concepts aligned with PesaPal's business domain.

---

**Author**: Jose Oyuko  
**Project**: Custom RDBMS Implementation  
**Year**: 2026