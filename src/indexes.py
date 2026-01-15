from typing import Any, Dict, List, Set

class Index:
    """
    Hash-based index implementation.
    Maps values to a set of row indices.
    """
    def __init__(self, name: str, unique: bool = False):
        self.name = name
        self.unique = unique
        self.data: Dict[Any, Set[int]] = {}

    def insert(self, value: Any, row_index: int):
        """
        Insert a value and its row index into the index.
        Raises ValueError if unique constraint is violated.
        """
        if self.unique:
            if value in self.data:
                 raise ValueError(f"Constraint Violation: Unique constraint violated on index '{self.name}' (value: {value})")
            self.data[value] = {row_index}
        else:
            if value not in self.data:
                self.data[value] = set()
            self.data[value].add(row_index)

    def delete(self, value: Any, row_index: int):
        """
        Remove a row index for a given value.
        """
        if value in self.data:
            if row_index in self.data[value]:
                self.data[value].remove(row_index)
                if not self.data[value]:
                    del self.data[value]

    def update(self, old_value: Any, new_value: Any, row_index: int):
        """
        Update a value in the index.
        """
        if old_value == new_value:
            return

        # Check constraint BEFORE deleting old value to ensure atomicity/safety
        if self.unique and new_value in self.data:
             raise ValueError(f"Constraint Violation: Unique constraint violated on index '{self.name}' (value: {new_value})")

        self.delete(old_value, row_index)
        self.insert(new_value, row_index)

    def lookup(self, value: Any) -> Set[int]:
        """
        Returns a set of row indices for the given value.
        """
        return self.data.get(value, set())
