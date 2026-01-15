from typing import Any, List, Dict, Optional, Union, Tuple
from storage import Database, Table
from sql_parser import parse_command

class Executor:
    def __init__(self, db: Database):
        self.db = db

    def execute(self, sql: str) -> Union[str, List[Dict[str, Any]]]:
        """
        Executes a SQL command and returns the result.
        Result can be a success message string or a list of rows (for SELECT).
        """
        try:
            parsed = parse_command(sql)
            command = parsed['command']
            
            if command == 'CREATE_TABLE':
                return self._execute_create_table(parsed)
            elif command == 'INSERT':
                return self._execute_insert(parsed)
            elif command == 'SELECT':
                return self._execute_select(parsed)
            elif command == 'UPDATE':
                return self._execute_update(parsed)
            elif command == 'DELETE':
                return self._execute_delete(parsed)
            elif command == 'DROP_TABLE':
                return self._execute_drop_table(parsed)
            else:
                raise ValueError(f"Unsupported command: {command}")
                
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
