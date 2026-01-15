import unittest
import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from executor import Executor
from storage import Database

class TestExecutor(unittest.TestCase):
    def setUp(self):
        self.db = Database()
        self.executor = Executor(self.db)

    def test_create_table(self):
        result = self.executor.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        self.assertEqual(result, "Table 'users' created.")
        self.assertIsNotNone(self.db.get_table("users"))

    def test_insert(self):
        self.executor.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        result = self.executor.execute("INSERT INTO users VALUES (1, 'Alice')")
        self.assertEqual(result, "1 row inserted.")
        
        table = self.db.get_table("users")
        self.assertEqual(len(table.rows), 1)

    def test_select(self):
        self.executor.execute("CREATE TABLE users (id INTEGER, name TEXT)")
        self.executor.execute("INSERT INTO users VALUES (1, 'Alice')")
        self.executor.execute("INSERT INTO users VALUES (2, 'Bob')")
        
        result = self.executor.execute("SELECT name FROM users WHERE id = 2")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['name'], 'Bob')

    def test_update(self):
        self.executor.execute("CREATE TABLE users (id INTEGER, name TEXT)")
        self.executor.execute("INSERT INTO users VALUES (1, 'Alice')")
        
        result = self.executor.execute("UPDATE users SET name = 'Cooper' WHERE id = 1")
        self.assertEqual(result, "1 rows updated.")
        
        rows = self.executor.execute("SELECT name FROM users WHERE id = 1")
        self.assertEqual(rows[0]['name'], 'Cooper')

    def test_delete(self):
        self.executor.execute("CREATE TABLE users (id INTEGER, name TEXT)")
        self.executor.execute("INSERT INTO users VALUES (1, 'Alice')")
        
        result = self.executor.execute("DELETE FROM users WHERE id = 1")
        self.assertEqual(result, "1 rows deleted.")
        
        rows = self.executor.execute("SELECT * FROM users")
        self.assertEqual(len(rows), 0)

    def test_error_handling(self):
        result = self.executor.execute("SELECT * FROM non_existent_table")
        self.assertTrue(result.startswith("Error:"))

if __name__ == '__main__':
    unittest.main()
