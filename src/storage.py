from typing import List, Dict, Any, Optional, Set, Tuple, Union

class Table:
    def __init__(self, name: str, columns: List[Tuple[str, str]], primary_key: Optional[str] = None, unique_columns: List[str] = None):
        """
        Initialize a new table.
        columns: List of (column_name, data_type) tuples. data_type is a string "INTEGER", "TEXT", etc.
        """
        self.name = name
        self.columns = columns
        self.column_names = [col[0] for col in columns]
        self.rows: List[List[Any]] = []
        
        self.primary_key = primary_key
        self.unique_columns = unique_columns or []
        
        # Validation indices
        self.pk_values: Set[Any] = set()
        self.unique_indices: Dict[str, Set[Any]] = {col: set() for col in self.unique_columns}

        # Helper to find column index by name
        self._col_map = {name: idx for idx, name in enumerate(self.column_names)}
        
        if self.primary_key and self.primary_key not in self.column_names:
            raise ValueError(f"Primary key '{self.primary_key}' not found in columns")
        
        for col in self.unique_columns:
            if col not in self.column_names:
                raise ValueError(f"Unique column '{col}' not found in columns")

    def insert_row(self, values: List[Any]):
        """
        Insert a row into the table.
        Raises ValueError if constraints are violated or value count mismatch.
        """
        if len(values) != len(self.columns):
            raise ValueError(f"Column count mismatch. Expected {len(self.columns)}, got {len(values)}")

        # Check Primary Key Constraint
        if self.primary_key:
            pk_idx = self._col_map[self.primary_key]
            pk_val = values[pk_idx]
            if pk_val in self.pk_values:
                raise ValueError(f"Constraint Violation: Primary key {pk_val} already exists in table '{self.name}'")
        
        # Check Unique Constraints
        for col in self.unique_columns:
            u_idx = self._col_map[col]
            u_val = values[u_idx]
            if u_val in self.unique_indices[col]:
                raise ValueError(f"Constraint Violation: Unique constraint violated on column '{col}'")
        
        # Commit insert (in-memory)
        self.rows.append(values)
        
        # Update indices
        if self.primary_key:
            pk_idx = self._col_map[self.primary_key]
            self.pk_values.add(values[pk_idx])
            
        for col in self.unique_columns:
            u_idx = self._col_map[col]
            self.unique_indices[col].add(values[u_idx])

    def select(self, columns: List[str] = None, where: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """
        Select rows from the table.
        columns: List of column names to return. If None, return all.
        where: Dict of {column: value} for equality filtering.
        Returns: List of dictionaries representing rows.
        """
        target_columns = columns or self.column_names
        
        # Validate target columns
        for col in target_columns:
            if col not in self.column_names:
                raise ValueError(f"Column '{col}' not found in table '{self.name}'")

        target_indices = [self._col_map[col] for col in target_columns]
        
        results = []
        for row in self.rows:
            # Check WHERE clause
            match = True
            if where:
                for w_col, w_val in where.items():
                    if w_col not in self._col_map:
                         raise ValueError(f"Where column '{w_col}' not found")
                    if row[self._col_map[w_col]] != w_val:
                        match = False
                        break
            
            if match:
                # Construct result dict
                result_row = {}
                for i, col_name in enumerate(target_columns):
                    result_row[col_name] = row[target_indices[i]]
                results.append(result_row)
                
        return results

    def _delete_row_at_index(self, index: int):
        """Internal helper to remove row and update indices."""
        row = self.rows[index]
        
        # Update indices
        if self.primary_key:
            pk_idx = self._col_map[self.primary_key]
            self.pk_values.remove(row[pk_idx])
            
        for col in self.unique_columns:
            u_idx = self._col_map[col]
            self.unique_indices[col].remove(row[u_idx])
            
        self.rows.pop(index)

    def delete(self, where: Dict[str, Any]) -> int:
        """
        Delete rows matching the where clause.
        Returns number of deleted rows.
        """
        # We need to iterate backwards to safely modify list
        rows_to_delete = []
        
        for idx, row in enumerate(self.rows):
            match = True
            if where:
                for w_col, w_val in where.items():
                    if row[self._col_map[w_col]] != w_val:
                        match = False
                        break
            if match:
                rows_to_delete.append(idx)
        
        # Delete in reverse order
        for idx in sorted(rows_to_delete, reverse=True):
            self._delete_row_at_index(idx)
            
        return len(rows_to_delete)

    def update(self, set_values: Dict[str, Any], where: Dict[str, Any]) -> int:
        """
        Update rows matching the where clause.
        set_values: Dict of {col: new_value}
        Returns number of updated rows.
        """
        # Identification first
        rows_to_update = []
        for idx, row in enumerate(self.rows):
            match = True
            if where:
                for w_col, w_val in where.items():
                    if row[self._col_map[w_col]] != w_val:
                        match = False
                        break
            if match:
                rows_to_update.append(idx)
        
        count = 0 
        for idx in rows_to_update:
            row = self.rows[idx]
            original_row = list(row) # Copy for rollback/comparison if needed
            new_row = list(row)
            
            # Constraints Check for the NEW values
            # This is complex because we might update multiple rows.
            # For simplicity in Phase 1: check individually.
            
            # Temporarily remove old values from indices to allow self-update (e.g. set id=id) if needed, 
            # but simpler: check if new value conflicts with OTHER rows.
            
            for col, val in set_values.items():
                if col not in self._col_map:
                     raise ValueError(f"Column '{col}' not found")
                
                col_idx = self._col_map[col]
                
                # PK Check
                # Check Primary Key constraint
                # Only raise error if:
                # 1. The value is actually changing (val != row[col_idx])
                # 2. The new value already exists elsewhere (val in self.pk_values)
                if col == self.primary_key:
                    if val != row[col_idx] and val in self.pk_values:
                         raise ValueError(f"Constraint Violation: Primary key {val} already exists")
                
                # Unique Check
                if col in self.unique_columns:
                    if val != row[col_idx] and val in self.unique_indices[col]:
                         raise ValueError(f"Constraint Violation: Unique constraint violated on column '{col}'")

                new_row[col_idx] = val

            # Apply Update
            # Update indices: remove old, add new
            if self.primary_key:
                pk_idx = self._col_map[self.primary_key]
                self.pk_values.remove(row[pk_idx])
                self.pk_values.add(new_row[pk_idx])
            
            for col in self.unique_columns:
                u_idx = self._col_map[col]
                self.unique_indices[col].remove(row[u_idx])
                self.unique_indices[col].add(new_row[u_idx])
                
            self.rows[idx] = new_row
            count += 1
            
        return count


class Database:
    def __init__(self):
        self.tables: Dict[str, Table] = {}

    def create_table(self, name: str, columns: List[Tuple[str, str]], primary_key: Optional[str] = None, unique_columns: List[str] = None):
        if name in self.tables:
            raise ValueError(f"Table '{name}' already exists")
        self.tables[name] = Table(name, columns, primary_key, unique_columns)

    def get_table(self, name: str) -> Table:
        if name not in self.tables:
            raise ValueError(f"Table '{name}' not found")
        return self.tables[name]

    def drop_table(self, name: str):
        if name not in self.tables:
            raise ValueError(f"Table '{name}' not found")
        del self.tables[name]
