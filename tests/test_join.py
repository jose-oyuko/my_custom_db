import unittest
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from storage import Database
from executor import Executor

class TestJoin(unittest.TestCase):
    def setUp(self):
        self.db = Database()
        self.executor = Executor(self.db)
        
        # Setup schema: Users and Orders
        self.executor.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        self.executor.execute("CREATE TABLE orders (oid INTEGER PRIMARY KEY, uid INTEGER, item TEXT)")
        
        # Insert Data
        self.executor.execute("INSERT INTO users VALUES (1, 'Alice')")
        self.executor.execute("INSERT INTO users VALUES (2, 'Bob')")
        self.executor.execute("INSERT INTO users VALUES (3, 'Charlie')")
        
        self.executor.execute("INSERT INTO orders VALUES (101, 1, 'Laptop')")
        self.executor.execute("INSERT INTO orders VALUES (102, 1, 'Mouse')")
        self.executor.execute("INSERT INTO orders VALUES (103, 2, 'Keyboard')")
        # Charlie has no orders

    def test_inner_join(self):
        # Query: SELECT users.name, orders.item FROM users JOIN orders ON users.id = orders.uid
        sql = "SELECT users.name, orders.item FROM users JOIN orders ON users.id = orders.uid"
        results = self.executor.execute(sql)
        
        if isinstance(results, str):
            self.fail(f"Execution failed with error: {results}")

        self.assertEqual(len(results), 3) # Alice*2, Bob*1
        
        # Check content
        # Keys are fully qualified: 'users.name', 'orders.item'
        items = sorted([r['orders.item'] for r in results])
        self.assertEqual(items, ['Keyboard', 'Laptop', 'Mouse'])
        
        # Check names
        names = sorted([r['users.name'] for r in results])
        self.assertEqual(names, ['Alice', 'Alice', 'Bob'])

    def test_join_with_where(self):
        # Query: ... WHERE users.name = 'Alice'
        sql = "SELECT users.name, orders.item FROM users JOIN orders ON users.id = orders.uid WHERE users.name = 'Alice'"
        results = self.executor.execute(sql)
        
        if isinstance(results, str):
             self.fail(f"Execution failed with error: {results}")
             
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]['users.name'], 'Alice')

    def test_invalid_join_col(self):
        sql = "SELECT * FROM users JOIN orders ON users.id = orders.invalid_col"
        result = self.executor.execute(sql)
        self.assertTrue(isinstance(result, str) and result.startswith("Error"))

if __name__ == '__main__':
    unittest.main()
