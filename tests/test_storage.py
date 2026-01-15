import unittest
import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from storage import Table, Database

class TestTable(unittest.TestCase):
    def setUp(self):
        self.columns = [("id", "INTEGER"), ("name", "TEXT"), ("email", "TEXT")]
        self.table = Table("users", self.columns, primary_key="id", unique_columns=["email"])

    def test_insert_valid(self):
        self.table.insert_row([1, "Alice", "alice@example.com"])
        self.assertEqual(len(self.table.rows), 1)
        self.assertEqual(self.table.rows[0], [1, "Alice", "alice@example.com"])

    def test_insert_pk_violation(self):
        self.table.insert_row([1, "Alice", "alice@example.com"])
        with self.assertRaises(ValueError) as cm:
            self.table.insert_row([1, "Bob", "bob@example.com"])
        self.assertIn("Primary key 1 already exists", str(cm.exception))

    def test_insert_unique_violation(self):
        self.table.insert_row([1, "Alice", "alice@example.com"])
        with self.assertRaises(ValueError) as cm:
            self.table.insert_row([2, "Bob", "alice@example.com"])
        self.assertIn("Unique constraint violated", str(cm.exception))

    def test_select_all(self):
        self.table.insert_row([1, "Alice", "a@a.com"])
        self.table.insert_row([2, "Bob", "b@b.com"])
        results = self.table.select()
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]["name"], "Alice")

    def test_select_where(self):
        self.table.insert_row([1, "Alice", "a@a.com"])
        self.table.insert_row([2, "Bob", "b@b.com"])
        results = self.table.select(where={"name": "Bob"})
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["id"], 2)

    def test_update(self):
        self.table.insert_row([1, "Alice", "a@a.com"])
        count = self.table.update({"name": "Alice Cooper"}, where={"id": 1})
        self.assertEqual(count, 1)
        results = self.table.select(where={"id": 1})
        self.assertEqual(results[0]["name"], "Alice Cooper")

    def test_delete(self):
        self.table.insert_row([1, "Alice", "a@a.com"])
        self.table.insert_row([2, "Bob", "b@b.com"])
        count = self.table.delete(where={"id": 1})
        self.assertEqual(count, 1)
        results = self.table.select()
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["name"], "Bob")

class TestDatabase(unittest.TestCase):
    def setUp(self):
        self.db = Database()

    def test_create_table(self):
        self.db.create_table("users", [("id", "INTEGER")], primary_key="id")
        self.assertIsNotNone(self.db.get_table("users"))

    def test_drop_table(self):
        self.db.create_table("users", [("id", "INTEGER")])
        self.db.drop_table("users")
        with self.assertRaises(ValueError):
            self.db.get_table("users")

if __name__ == '__main__':
    unittest.main()
