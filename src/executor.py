from typing import Any, List, Dict, Optional, Union, Tuple
from storage import Database, Table
from sql_parser import parse_command

class Executor:
    def __init__(self, db: Database, db_file: Optional[str] = None):
        self.db = db
        self.db_file = db_file

    def execute(self, sql: str) -> Union[str, List[Dict[str, Any]]]:
        """
        Executes a SQL command and returns the result.
        Result can be a success message string or a list of rows (for SELECT).
        """
        try:
            parsed = parse_command(sql)
            command = parsed['command']
            
            result = None
            modified = False
            
            if command == 'CREATE_TABLE':
                result = self._execute_create_table(parsed)
                modified = True
            elif command == 'INSERT':
                result = self._execute_insert(parsed)
                modified = True
            elif command == 'SELECT':
                result = self._execute_select(parsed)
            elif command == 'UPDATE':
                result = self._execute_update(parsed)
                modified = True
            elif command == 'DELETE':
                result = self._execute_delete(parsed)
                modified = True
            elif command == 'DROP_TABLE':
                result = self._execute_drop_table(parsed)
                modified = True
            else:
                raise ValueError(f"Unsupported command: {command}")
            
            # Auto-Save if modified and file is set
            if modified and self.db_file:
                self.db.save_to_file(self.db_file)
                
            return result
                
        except Exception as e:
            return f"Error: {str(e)}"

    def _execute_create_table(self, parsed: Dict[str, Any]) -> str:
        name = parsed['table']
        columns = parsed['columns']
        pk = parsed['primary_key']
        unique = parsed['unique_columns']
        
        self.db.create_table(name, columns, primary_key=pk, unique_columns=unique)
        return f"Table '{name}' created."

    def _execute_insert(self, parsed: Dict[str, Any]) -> str:
        table_name = parsed['table']
        values = parsed['values']
        
        table = self.db.get_table(table_name)
        table.insert_row(values)
        return "1 row inserted."

    def _execute_select(self, parsed: Dict[str, Any]) -> List[Dict[str, Any]]:
        table_name = parsed['table']
        columns = parsed['columns']
        where = parsed['where']
        join_info = parsed.get('join')
        
        table = self.db.get_table(table_name)
        
        if join_info:
            join_table_name = join_info['table']
            other_table = self.db.get_table(join_table_name)
            
            # Resolve fully qualified column names in ON clause
            # User might strict 't1.id = t2.uid' OR 'id = uid'
            l_on = join_info['on_left']
            r_on = join_info['on_right']
            
            # Simple resolution: if '.' in name, split. Else assume.
            # We strictly need: which col is from which table.
            
            def resolve_col(raw, t1, t2):
                if '.' in raw:
                    t, c = raw.split('.')
                    if t == t1.name: return c, t1
                    if t == t2.name: return c, t2
                    raise ValueError(f"Unknown table alias in '{raw}'")
                else:
                    # Ambiguous or implied?
                    # For inner_join method we need explicit "left_col" (on self) and "right_col" (on other).
                    # This is tricky if parser doesn't know which is which.
                    # We'll try to guess based on schema?
                    if raw in t1.column_names and raw not in t2.column_names: return raw, t1
                    if raw in t2.column_names and raw not in t1.column_names: return raw, t2
                    if raw in t1.column_names and raw in t2.column_names: 
                        # Ambiguous: Assume Left = Left Arg, Right = Right Arg?
                        # Dangerous. Let's just return raw and let caller decide?
                        # inner_join expects (left_col, right_col).
                        return raw, None 
                    raise ValueError(f"Column '{raw}' not found in tables")

            # We need to pass 'left_col' (on 'table') and 'right_col' (on 'other_table')
            # The parser gives us two expressions: l_on, r_on.
            # They could be in any order: "ON t1.id = t2.id" or "ON t2.id = t1.id"
            
            c1_name, c1_table = resolve_col(l_on, table, other_table)
            c2_name, c2_table = resolve_col(r_on, table, other_table)
            
            final_left = None
            final_right = None
            
            if c1_table == table: final_left = c1_name
            elif c1_table == other_table: final_right = c1_name
            
            if c2_table == table: final_left = c2_name
            elif c2_table == other_table: final_right = c2_name
            
            if not final_left or not final_right:
                # Fallback: simple heuristic matching
                # If we parsed "id = uid", and table has id, other has uid
                if c1_table is None and c2_table is None:
                     # Check existence
                     if c1_name in table.column_names and c2_name in other_table.column_names:
                         final_left, final_right = c1_name, c2_name
                     elif c2_name in table.column_names and c1_name in other_table.column_names:
                         final_left, final_right = c2_name, c1_name
            
            if not final_left or not final_right:
                 raise ValueError("Could not resolve JOIN columns. Please use fully qualified names (table.col).")
                 
            return table.inner_join(other_table, final_left, final_right, columns, where)
            
        return table.select(columns, where)

    def _execute_update(self, parsed: Dict[str, Any]) -> str:
        table_name = parsed['table']
        set_values = parsed['set']
        where = parsed['where']
        
        table = self.db.get_table(table_name)
        count = table.update(set_values, where)
        return f"{count} rows updated."

    def _execute_delete(self, parsed: Dict[str, Any]) -> str:
        table_name = parsed['table']
        where = parsed['where']
        
        table = self.db.get_table(table_name)
        count = table.delete(where)
        return f"{count} rows deleted."

    def _execute_drop_table(self, parsed: Dict[str, Any]) -> str:
        table_name = parsed['table']
        self.db.drop_table(table_name)
        return f"Table '{table_name}' dropped."
