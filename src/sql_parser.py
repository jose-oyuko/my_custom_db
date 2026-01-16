import re
from typing import Dict, Any, List, Optional

def parse_create_table(sql: str) -> Dict[str, Any]:
    """
    Parses regex for: CREATE TABLE table_name (col1 TYPE constr, ...)
    """
    pattern = r"CREATE\s+TABLE\s+(\w+)\s*\((.+)\)"
    match = re.search(pattern, sql, re.IGNORECASE | re.DOTALL)
    if not match:
        raise ValueError("Invalid CREATE TABLE syntax")
    
    table_name = match.group(1)
    columns_def = match.group(2)
    
    columns = []
    primary_key = None
    unique_columns = []
    
    # Split by comma, but careful with potential future complexity 
    # (for now simple splitting is enough as per requirements)
    cols = [c.strip() for c in columns_def.split(',')]
    
    for col in cols:
        parts = col.split()
        if len(parts) < 2:
             raise ValueError(f"Invalid column definition: {col}")
        
        name = parts[0]
        dtype = parts[1]
        constraints = [c.upper() for c in parts[2:]]
        
        columns.append((name, dtype))
        
        if "PRIMARY" in constraints and "KEY" in constraints:
            if primary_key:
                 raise ValueError("Multiple primary keys defined")
            primary_key = name
        
        if "UNIQUE" in constraints:
            unique_columns.append(name)
            
    return {
        "command": "CREATE_TABLE",
        "table": table_name,
        "columns": columns,
        "primary_key": primary_key,
        "unique_columns": unique_columns
    }

def _parse_key_value_pairs(clause_str: str, delimiter_regex: str) -> Dict[str, Any]:
    """
    Parses key-value pairs separated by a delimiter.
    Example: "col1 = val1, col2 = val2" or "col1=val1 AND col2=val2"
    """
    if not clause_str:
        return None
    
    conditions = {}
    parts = re.split(delimiter_regex, clause_str, flags=re.IGNORECASE)
    
    for part in parts:
        if not part.strip():
            continue
            
        m = re.match(r"([\w\.]+)\s*=\s*(.+)", part.strip())
        if not m:
             raise ValueError(f"Invalid condition: {part}")
        
        col = m.group(1)
        val_str = m.group(2).strip()
        
        if val_str.startswith("'") and val_str.endswith("'"):
            val = val_str[1:-1]
        elif val_str.lower() == 'true':
            val = True
        elif val_str.lower() == 'false':
            val = False
        elif val_str.isdigit():
            val = int(val_str)
        else:
            try:
                val = float(val_str)
            except ValueError:
                val = val_str
                
        conditions[col] = val
        
    return conditions

def _parse_where(where_str: str) -> Dict[str, Any]:
    """
    Parses WHERE clause: col1 = val1 AND col2 = val2
    """
    return _parse_key_value_pairs(where_str, r'\s+AND\s+')

def parse_insert(sql: str) -> Dict[str, Any]:
    """
    Parses regex for: INSERT INTO table_name VALUES (val1, val2, ...)
    """
    pattern = r"INSERT\s+INTO\s+(\w+)\s+VALUES\s*\((.+)\)"
    match = re.search(pattern, sql, re.IGNORECASE)
    if not match:
         raise ValueError("Invalid INSERT syntax")
         
    table_name = match.group(1)
    values_str = match.group(2)
    
    # Split by comma, handling whitespace
    raw_values = [v.strip() for v in values_str.split(',')]
    values = []
    
    for v in raw_values:
        if v.startswith("'") and v.endswith("'"):
            values.append(v[1:-1])
        elif v.lower() == 'true':
            values.append(True)
        elif v.lower() == 'false':
            values.append(False)
        elif v.isdigit():
            values.append(int(v))
        else:
            try:
                values.append(float(v))
            except ValueError:
                 values.append(v)
                 
    return {
        "command": "INSERT",
        "table": table_name,
        "values": values
    }

def parse_select(sql: str) -> Dict[str, Any]:
    """
    Parses regex for: SELECT col1, col2, ... FROM table [JOIN table2 ON condition] [WHERE condition]
    """
    # Regex groups:
    # 1. Columns
    # 2. Main Table
    # 3. Join Table (Optional)
    # 4. Join Condition (Optional)
    # 5. Where Clause (Optional)
    
    # Note: Regex complexity rises. Using non-greedy match for columns/tables.
    pattern = r"SELECT\s+(.+?)\s+FROM\s+(\w+)(?:\s+JOIN\s+(\w+)\s+ON\s+(.+?))?(?:\s+WHERE\s+(.+))?$"
    match = re.search(pattern, sql, re.IGNORECASE)
    if not match:
         raise ValueError("Invalid SELECT syntax")
         
    cols_str = match.group(1).strip()
    table_name = match.group(2)
    join_table = match.group(3)
    join_on = match.group(4)
    where_clause = match.group(5)
    
    if cols_str == '*':
        columns = None
    else:
        columns = [c.strip() for c in cols_str.split(',')]
        
    # Join Condition Parsing (expect: t1.c1 = t2.c2)
    join_info = None
    if join_table and join_on:
        # Simple parser for "colA = colB"
        # We need raw strings for columns here (no type conversion to int/bool)
        m_on = re.match(r"(.+?)\s*=\s*(.+)", join_on)
        if not m_on:
             raise ValueError(f"Invalid ON condition: {join_on}")
        left_on = m_on.group(1).strip()
        right_on = m_on.group(2).strip()
        join_info = {
            "table": join_table,
            "on_left": left_on,
            "on_right": right_on
        }
        
    return {
        "command": "SELECT",
        "table": table_name,
        "columns": columns,
        "where": _parse_where(where_clause),
        "join": join_info
    }

def parse_update(sql: str) -> Dict[str, Any]:
    """
    Parses regex for: UPDATE table SET col=val, ... [WHERE condition]
    """
    pattern = r"UPDATE\s+(\w+)\s+SET\s+(.+?)(?:\s+WHERE\s+(.+))?$"
    match = re.search(pattern, sql, re.IGNORECASE)
    if not match:
         raise ValueError("Invalid UPDATE syntax")
         
    table_name = match.group(1)
    set_clause = match.group(2)
    where_clause = match.group(3)
    
    # Use comma delimiter for SET clause
    matched_set = _parse_key_value_pairs(set_clause, r',')
    
    return {
        "command": "UPDATE",
        "table": table_name,
        "set": matched_set,
        "where": _parse_where(where_clause)
    }

def parse_delete(sql: str) -> Dict[str, Any]:
    """
    Parses regex for: DELETE FROM table [WHERE condition]
    """
    pattern = r"DELETE\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?"
    match = re.search(pattern, sql, re.IGNORECASE)
    if not match:
         raise ValueError("Invalid DELETE syntax")
         
    table_name = match.group(1)
    where_clause = match.group(2)
    
    return {
        "command": "DELETE",
        "table": table_name,
        "where": _parse_where(where_clause)
    }

def parse_drop_table(sql: str) -> Dict[str, Any]:
    """
    Parses regex for: DROP TABLE table_name
    """
    pattern = r"DROP\s+TABLE\s+(\w+)"
    match = re.search(pattern, sql, re.IGNORECASE)
    if not match:
         raise ValueError("Invalid DROP TABLE syntax")
         
    return {
        "command": "DROP_TABLE",
        "table": match.group(1)
    }

def parse_command(sql: str) -> Dict[str, Any]:
    """
    Route to specific parsers based on the command keyword.
    """
    sql = sql.strip()
    command = sql.split()[0].upper()
    
    if command == "CREATE":
        return parse_create_table(sql)
    elif command == "INSERT":
        return parse_insert(sql)
    elif command == "SELECT":
        return parse_select(sql)
    elif command == "UPDATE":
        return parse_update(sql)
    elif command == "DELETE":
        return parse_delete(sql)
    elif command == "DROP":
        return parse_drop_table(sql)
    else:
        raise ValueError(f"Unknown command: {command}")
