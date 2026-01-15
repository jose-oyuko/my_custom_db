import sys
from typing import List, Dict, Any

# Assuming these are importable from src
# When running main.py from root, these imports need to be handled correctly
# If this file is imported by main.py, we expect the package structure
# Usually relative imports work if module is loaded as package. 
# But let's use absolute imports assuming src is in path or we are inside the package.
# Simpler: assume main.py inserts `src` into sys.path or runs as module.

try:
    from .storage import Database
    from .executor import Executor
except ImportError:
    # Fallback for direct execution
    from storage import Database
    from executor import Executor

class REPL:
    def __init__(self):
        self.db = Database()
        self.executor = Executor(self.db)
        self.buffer = ""

    def start(self):
        print("Welcome to MyDB. Type .help for instructions.")
        print("Type .exit to quit.")
        
        # Try to enable readline usage for history if available
        try:
            import readline
        except ImportError:
            pass # Not available on all Windows pythons by default

        while True:
            try:
                prompt = "mydb> " if not self.buffer else "   -> "
                try:
                    line = input(prompt)
                except EOFError:
                    print()
                    break

                if not line and not self.buffer:
                    continue
                    
                # Meta commands handling (only on fresh line)
                if line.strip().startswith(".") and not self.buffer:
                    self.handle_meta_command(line.strip())
                    continue
                
                # SQL Accumulation
                # Add space to avoid concatenation issues like "SELECT * FROM" + "table" -> "FROMtable"
                self.buffer += line + " "
                
                # Check for semi-colon to execute
                if ";" in self.buffer:
                    # Execute
                    # Split by ; to handle multiple commands
                    commands = [c.strip() for c in self.buffer.split(';') if c.strip()]
                    
                    for sql in commands:
                        if not sql: continue
                        print(f"Executing: {sql}")
                        result = self.executor.execute(sql)
                        self.print_result(result)
                        
                    self.buffer = ""
                    
            except KeyboardInterrupt:
                print("\nType .exit to quit.")
                self.buffer = ""
            except Exception as e:
                print(f"Error: {e}")
                self.buffer = ""
    
    def handle_meta_command(self, command: str):
        parts = command.split()
        cmd = parts[0]
        
        if cmd == ".exit":
            print("Goodbye!")
            sys.exit(0)
        elif cmd == ".tables":
            print("Tables:")
            if not self.db.tables:
                print("  (no tables)")
            for name in self.db.tables:
                print(f"  - {name}")
        elif cmd == ".describe":
            if len(parts) < 2:
                print("Usage: .describe <table_name>")
                return
            table_name = parts[1]
            try:
                table = self.db.get_table(table_name)
                print(f"Table: {table.name}")
                print(f"Primary Key: {table.primary_key}")
                print("Columns:")
                for col_name, col_type in table.columns:
                    constraints = []
                    if col_name == table.primary_key:
                        constraints.append("PK")
                    if col_name in table.unique_columns:
                        constraints.append("UNIQUE")
                        
                    constr_str = f"[{', '.join(constraints)}]" if constraints else ""
                    print(f"  - {col_name} ({col_type}) {constr_str}")
            except ValueError as e:
                print(f"Error: {e}")
                
        elif cmd == ".help":
            print("Available commands:")
            print("  .tables               List all tables")
            print("  .describe <table_name> Show table schema")
            print("  .exit                 Exit the REPL")
            print("  <SQL>;                Execute SQL (must end with ;)")
        else:
            print(f"Unknown command: {command}")

    def print_result(self, result: Any):
        if isinstance(result, str):
            print(result)
        elif isinstance(result, list):
            self.print_table(result)
        else:
            print(result)
            
    def print_table(self, rows: List[Dict[str, Any]]):
        if not rows:
            print("(0 rows)")
            return
            
        # Get headers
        headers = list(rows[0].keys())
        
        # Calculate widths
        widths = {h: len(h) for h in headers}
        for row in rows:
            for h in headers:
                val = str(row.get(h, ''))
                widths[h] = max(widths[h], len(val))
        
        # Print header
        header_parts = [h.ljust(widths[h]) for h in headers]
        header_row = " | ".join(header_parts)
        separator = "-+-".join(["-" * widths[h] for h in headers])
        
        print(header_row)
        print(separator)
        
        # Print rows
        for row in rows:
            row_parts = [str(row.get(h, '')).ljust(widths[h]) for h in headers]
            print(" | ".join(row_parts))
        print(f"({len(rows)} rows)")
