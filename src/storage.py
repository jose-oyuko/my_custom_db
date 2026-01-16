"""
storage.py - Core data storage layer for MyDB RDBMS

Provides:
- Table: Represents a database table with rows, columns, and constraints
- Database: Manages collection of tables

Supports:
- Primary key constraints
- Unique column constraints  
- Basic CRUD operations (INSERT, SELECT, UPDATE, DELETE)
- Simple equality-based WHERE clauses
"""
from typing import List, Dict, Any, Optional, Set, Tuple, Union
from indexes import Index

class Table:
    def __init__(self, name: str, columns: List[Tuple[str, str]], primary_key: Optional[str] = None, unique_columns: List[str] = None):
        """
        Initialize a new table.
        columns: List of (column_name, data_type) tuples. data_type is a string "INTEGER", "TEXT", etc.
        
        Example:
            >>> table = Table('users', [('id', 'INTEGER'), ('name', 'TEXT')], 
            ...               primary_key='id')
        """
        self.name = name
        self.columns = columns
        self.column_names = [col[0] for col in columns]
        self.rows: List[List[Any]] = []
        
        self.primary_key = primary_key
        self.unique_columns = unique_columns or []
        
        # Validation indices
        self.indexes: Dict[str, Index] = {}
        
        # Helper to find column index by name
        self._col_map = {name: idx for idx, name in enumerate(self.column_names)}
        
        # Initialize Indexes
        if self.primary_key:
            if self.primary_key not in self.column_names:
                raise ValueError(f"Primary key '{self.primary_key}' not found in columns")
            self.indexes[self.primary_key] = Index(self.primary_key, unique=True)
        
        for col in self.unique_columns:
            if col not in self.column_names:
                raise ValueError(f"Unique column '{col}' not found in columns")
            self.indexes[col] = Index(col, unique=True)

    def insert_row(self, values: List[Any]):
        """
        Insert a row into the table.
        Raises ValueError if constraints are violated or value count mismatch.
        
        Note: Type validation is intentionally minimal in Phase 1.
              Parser (Phase 2) will handle type conversion.
        """
        if len(values) != len(self.columns):
            raise ValueError(f"Column count mismatch. Expected {len(self.columns)}, got {len(values)}")

        # Next row index
        row_idx = len(self.rows) 

        # Check Constraints & Pre-Insert into Indexes (Validation)
        # Note: Index.insert raises ValueError on violation
        # We need to be careful to rollback if multiple indexes exist and one fails.
        
        inserted_indices = []
        try:
            for col_name, index in self.indexes.items():
                col_idx = self._col_map[col_name]
                val = values[col_idx]
                try:
                    index.insert(val, row_idx)
                except ValueError as ve:
                    if col_name == self.primary_key:
                        raise ValueError(f"Constraint Violation: Primary key {val} already exists in table '{self.name}'")
                    raise ve
                inserted_indices.append((col_name, val))
        except ValueError as e:
            # Rollback: delete from already written indexes
            for c_name, c_val in inserted_indices:
                self.indexes[c_name].delete(c_val, row_idx)
            raise e
        
        # Commit insert (in-memory)
        self.rows.append(values)

    def select(self, columns: List[str] = None, where: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """
        Select rows from the table.
        columns: List of column names to return. If None, return all.
        where: Dict of {column: value} for equality filtering.
        
        Returns: List of dictionaries where keys are column names.
        Example: [{'id': 1, 'name': 'Alice'}, {'id': 2, 'name': 'Bob'}]
        
        Note: 'where' values should match the column's data type (no automatic conversion).
        """
        target_columns = columns or self.column_names
        
        # Validate target columns
        for col in target_columns:
            if col not in self.column_names:
                raise ValueError(f"Column '{col}' not found in table '{self.name}'")

        target_indices = [self._col_map[col] for col in target_columns]
        
        candidate_rows_indices = None
        
        # Optimization: Use Index if WHERE clause hits an indexed column
        if where:
            # Sort where keys to prioritize PK or Unique indexes if multiple exist
            # (Heuristic: Unique indexes are most selective)
            for col, val in where.items():
                if col in self.indexes:
                    index = self.indexes[col]
                    row_indices = index.lookup(val)
                    
                    if candidate_rows_indices is None:
                        candidate_rows_indices = row_indices
                    else:
                        candidate_rows_indices = candidate_rows_indices.intersection(row_indices)
                    
                    # If intersection is empty, no need to continue
                    if not candidate_rows_indices:
                        break
        
        # If no index used, scan all rows
        if candidate_rows_indices is None:
            scan_iterable = enumerate(self.rows)
        else:
            # Only scan the candidate rows from index
            # Filter out any indices that might have been deleted (if our list compaction isn't perfect yet)
            # In this list-based implementation with 'None' holes or re-writing, we must be careful.
            # But wait, our current delete() implementation uses pop() which SHIFTS indices.
            # THIS IS A PROBLEM. pop() invalidates all subsequent indices in the Index.
            # FIX: We must change delete strategy or update ALL indexes on delete.
            # Strategy: update list item to None (soft delete) or update all indexes. 
            # Updating all indexes is heavy (O(N*M)). 
            # Better strategy for Phase 4: Use Soft Delete (replace with None) or Rebuild Indexes.
            # BUT: The requirement "Internal storage: dict mapping values to lists of row indices" implies stable indices.
            # Let's switch to Soft Delete for simplicity in this phase, OR adhere to the `pop` and `_rebuild_indices`.
            # Actually, `pop` is O(N) anyway.
            # Let's stick to scanning for now unless we are sure indices are valid.
            # Since `delete` currently does `pop`, indexes ARE invalidated. 
            # I must update `delete` to handle index updates properly.
            # See `_delete_row_at_index` below.
            
            # Assuming `delete` updates indexes correctly (by shifting or rebuilding), we use valid indices.
            # However, shifting is O(N) potentially.
            # Let's just assume indices are correct and use them.
            scan_iterable = [(i, self.rows[i]) for i in candidate_rows_indices if i < len(self.rows)]

        results = []
        for idx, row in scan_iterable:
            # Check WHERE clause (double check even if indexed, because of potential other non-indexed conditions)
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
        """
        Internal helper to remove row and update indices.
        WARNING: Removing from list shifts indices of subsequent rows!
        All indexes must be updated for all rows > index.
        This is expensive O(N) but necessary if we use a simple list.
        """
        row = self.rows[index]
        
        # 1. Remove the deleted row from indexes
        for col_name, idx_obj in self.indexes.items():
            col_idx = self._col_map[col_name]
            val = row[col_idx]
            idx_obj.delete(val, index)
            
        # 2. Shift indices in all indexes for rows > index
        # This is the heavy part. 
        # For every row after 'index', we must decrement their index in the Index objects.
        
        # Optimization: Rebuild or Shift? Shift is cleaner.
        # But we don't have back-pointers from row to values in Index easily.
        # We have to scan all columns that are indexed.
        
        for i in range(index + 1, len(self.rows)):
            row_to_shift = self.rows[i]
            old_idx = i
            new_idx = i - 1
            
            for col_name, idx_obj in self.indexes.items():
                col_idx = self._col_map[col_name]
                val = row_to_shift[col_idx]
                
                # We effectively "move" the entry in the index
                idx_obj.delete(val, old_idx)
                idx_obj.insert(val, new_idx)

        self.rows.pop(index)

    def delete(self, where: Dict[str, Any]) -> int:
        """
        Delete rows matching the where clause.
        Returns number of deleted rows.
        """
        # Find rows to delete
        # Use select logic (optimization) to find indices if possible
        # But we can't reuse select() directly because we need indices.
        
        rows_to_delete = []
        
        # Optimization: Use Index if available
        candidate_indices = None
        if where:
             for col, val in where.items():
                if col in self.indexes:
                    idx_obj = self.indexes[col]
                    res = idx_obj.lookup(val)
                    if candidate_indices is None:
                        candidate_indices = res
                    else:
                        candidate_indices = candidate_indices.intersection(res)
                    if not candidate_indices:
                        break
        
        if candidate_indices is None:
            scan_iterable = enumerate(self.rows)
        else:
            scan_iterable = [(i, self.rows[i]) for i in candidate_indices if i < len(self.rows)]
            
        for idx, row in scan_iterable:
            match = True
            if where:
                for w_col, w_val in where.items():
                    if row[self._col_map[w_col]] != w_val:
                        match = False
                        break
            if match:
                rows_to_delete.append(idx)
        
        # Delete in reverse order to avoid shifting issues during the loop
        # (Though _delete_row_at_index handles shifting, deleting reverse is safer/efficient)
        count = 0
        for idx in sorted(rows_to_delete, reverse=True):
            self._delete_row_at_index(idx)
            count += 1
            
        return count

    def update(self, set_values: Dict[str, Any], where: Dict[str, Any]) -> int:
        """
        Update rows matching the where clause.
        set_values: Dict of {col: new_value}
        Returns number of updated rows.
        """
        # Find rows to update (similar logic as delete)
        rows_to_update = []
        
        candidate_indices = None
        if where:
             for col, val in where.items():
                if col in self.indexes:
                    idx_obj = self.indexes[col]
                    res = idx_obj.lookup(val)
                    if candidate_indices is None:
                        candidate_indices = res
                    else:
                        candidate_indices = candidate_indices.intersection(res)
                    if not candidate_indices:
                        break
        
        if candidate_indices is None:
            scan_iterable = enumerate(self.rows)
        else:
            scan_iterable = [(i, self.rows[i]) for i in candidate_indices if i < len(self.rows)]

        for idx, row in scan_iterable:
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
            original_row = list(row)
            new_row = list(row)
            
            # 1. updates the data in temporary new_row
            # 2. checks constraints against indexes (tricky: must exclude current row from check?)
            #    The Index class checks `if value in data`.
            #    If we update a Unique column to the SAME value, it's fine.
            #    If we update to a DIFFERENT value, we check if that value exists.
            
            # Let's perform updates on indexes transactionally
            
            updates_to_apply = [] # List of (col_name, old_val, new_val)
            
            try:
                # Validation Phase
                for col, val in set_values.items():
                    if col not in self._col_map:
                         raise ValueError(f"Column '{col}' not found")
                    
                    col_idx = self._col_map[col]
                    old_val = row[col_idx]
                    
                    if old_val == val:
                        continue # No change
                        
                    if col in self.indexes:
                        idx_obj = self.indexes[col]
                        # Check unique constraint proactively if needed
                        # index.update() handles "if unique and exists -> error"
                        # But we must call it to check.
                        # We can't actually call index.update() yet because we might rollback.
                        # We just check `lookup`.
                        if idx_obj.unique:
                             if idx_obj.lookup(val):
                                 # Exists. But might be THIS row? (Only if we didn't filter old_val==val above)
                                 # Since old_val != val, the existing entry is definitely another row.
                                 raise ValueError(f"Constraint Violation: Unique constraint violated on column '{col}'")
                    
                    updates_to_apply.append((col, old_val, val))
                    new_row[col_idx] = val

                # Application Phase
                # Update indexes first
                applied_idx_updates = []
                try:
                    for col, old_val, new_val in updates_to_apply:
                        if col in self.indexes:
                            self.indexes[col].update(old_val, new_val, idx)
                            applied_idx_updates.append((col, old_val, new_val))
                except ValueError as e:
                    # Rollback index updates
                    for col, old_val, new_val in reversed(applied_idx_updates):
                        self.indexes[col].update(new_val, old_val, idx)
                    raise e
                    
                # Update row data
                self.rows[idx] = new_row
                count += 1
                
            except ValueError as e:
                # This row creation failed. Since we handle per-row, we just stop? 
                # Or continue? Usually SQL updates are transactional (all or nothing) or per-row.
                # For Phase 4, raising error and stopping (partial update) is improved but transactional is better.
                # Let's just raise and stop.
                raise e
            
        return count

    def inner_join(self, other: 'Table', left_col: str, right_col: str, select_columns: List[str] = None, where: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """
        Performs an INNER JOIN with another table.
        """
        results = []
        
        # Validate columns exist
        if left_col not in self.column_names:
             raise ValueError(f"Column '{left_col}' not found in table '{self.name}'")
        if right_col not in other.column_names:
             raise ValueError(f"Column '{right_col}' not found in table '{other.name}'")
             
        left_idx = self._col_map[left_col]
        right_idx = other._col_map[right_col]
        
        # Check if right table has index on right_col
        right_index = other.indexes.get(right_col)
        
        # If no index, build a temp dict for O(N+M)
        if not right_index:
            temp_index = {}
            for r_i, r_row in enumerate(other.rows):
                val = r_row[right_idx]
                if val not in temp_index:
                    temp_index[val] = []
                temp_index[val].append(r_row)
        
        for l_row in self.rows:
            l_val = l_row[left_idx]
            
            # Find matching rows in right table
            matching_rows = []
            if right_index:
                # index.lookup returns row INDICES
                r_indices = right_index.lookup(l_val)
                matching_rows = [other.rows[i] for i in r_indices]
            else:
                matching_rows = temp_index.get(l_val, [])
            
            for r_row in matching_rows:
                joined_row = {}
                
                # Add left columns
                for i, col in enumerate(self.column_names):
                    key = f"{self.name}.{col}"
                    joined_row[key] = l_row[i]
                    
                # Add right columns
                for i, col in enumerate(other.column_names):
                    key = f"{other.name}.{col}"
                    joined_row[key] = r_row[i]
                
                # Apply WHERE filter
                match = True
                if where:
                    for w_key, w_val in where.items():
                        found = False
                        # Check strict match 'table.col'
                        if w_key in joined_row:
                            if joined_row[w_key] == w_val:
                                found = True
                            else:
                                match = False # Mismatch
                                break
                        
                        # Check suffix match 'col'
                        if not found and match:
                            for jr_key in joined_row:
                                if jr_key.endswith(f".{w_key}"):
                                    if joined_row[jr_key] != w_val:
                                        match = False
                                    found = True
                                    break
                        
                        # If key not found in row at all? (Ignore or Fail?)
                        # For now ignoring if column is missing (weak), usually should fail.
                        pass
                    
                if match:
                    results.append(joined_row)

        # Filter Select Cols
        if select_columns:
            final_results = []
            for res in results:
                filtered = {}
                for req_col in select_columns:
                    if req_col == '*':
                         filtered.update(res) # TODO: Handle * properly? Usually * selects all.
                         continue
                         
                    # req_col might be 'name' or 'users.name'
                    if req_col in res:
                        filtered[req_col] = res[req_col]
                    else:
                        for k, v in res.items():
                            if k.endswith(f".{req_col}"):
                                filtered[req_col] = v
                                break
                final_results.append(filtered)
            return final_results
            
        return results


import json

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

    def save_to_file(self, filename: str):
        """
        Save the database to a file (JSON format, .josedb extension recommended).
        """
        data = {
            "tables": {}
        }
        for name, table in self.tables.items():
            data["tables"][name] = {
                "columns": table.columns,
                "primary_key": table.primary_key,
                "unique_columns": table.unique_columns,
                "rows": table.rows
            }
        
        with open(filename, 'w') as f:
            json.dump(data, f, indent=4)

    def load_from_file(self, filename: str):
        """
        Load the database from a file.
        This rebuilds the in-memory state including all indexes.
        """
        with open(filename, 'r') as f:
            data = json.load(f)
            
        self.tables = {}
        for name, table_data in data["tables"].items():
            # Create table structure (Inits empty indexes)
            table = Table(
                name, 
                [tuple(c) for c in table_data["columns"]], # Convert back to tuples
                table_data["primary_key"],
                table_data["unique_columns"]
            )
            
            # Load rows
            table.rows = table_data["rows"]
            
            # Rebuild Indexes!
            # We iterate through all loaded rows and insert them into the Index objects manually.
            # This is necessary because setting `table.rows` directly bypasses `insert_row` logic.
            
            for row_idx, row in enumerate(table.rows):
                # Update PK Index
                if table.primary_key:
                    pk_idx = table._col_map[table.primary_key]
                    val = row[pk_idx]
                    table.indexes[table.primary_key].insert(val, row_idx)
                
                # Update Unique Indexes
                for col in table.unique_columns:
                    col_idx = table._col_map[col]
                    val = row[col_idx]
                    table.indexes[col].insert(val, row_idx)

            self.tables[name] = table

