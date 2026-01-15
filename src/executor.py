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
        
        table = self.db.get_table(table_name)
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
