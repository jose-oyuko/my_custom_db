import unittest
import sys
import os
import json

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from storage import Database
from executor import Executor

class TestPersistence(unittest.TestCase):
    def setUp(self):
        self.db_filename = "test_db.josedb"
        self.db = Database()
        self.executor = Executor(self.db)

    def tearDown(self):
        if os.path.exists(self.db_filename):
            os.remove(self.db_filename)

    def test_save_and_load(self):
        # 1. Create Data
        self.executor.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        self.executor.execute("INSERT INTO users VALUES (1, 'Alice')")
        self.executor.execute("INSERT INTO users VALUES (2, 'Bob')")
        
        # 2. Save
        self.db.save_to_file(self.db_filename)
        self.assertTrue(os.path.exists(self.db_filename))
        
        # 3. Load into NEW database instance
        new_db = Database()
        new_db.load_from_file(self.db_filename)
        
        # 4. Verify Data
        table = new_db.get_table("users")
        self.assertEqual(len(table.rows), 2)
        
        # 5. Verify Indexes are rebuilt (by trying an indexed lookup)
        # Access index directly to verify
        pk_index = table.indexes["id"]
        rows = pk_index.lookup(1)
        self.assertEqual(rows, {0}) # Row 0 should be ID 1

    def test_load_persisted_updates(self):
        # 1. Setup
        self.executor.execute("CREATE TABLE config (k TEXT PRIMARY KEY, v TEXT)")
        self.executor.execute("INSERT INTO config VALUES ('theme', 'dark')")
        self.db.save_to_file(self.db_filename)
        
        # 2. Update via new DB
        db2 = Database()
        db2.load_from_file(self.db_filename)
        ex2 = Executor(db2)
        ex2.execute("UPDATE config SET v = 'light' WHERE k = 'theme'")
        
        # Check update logic works on loaded DB (implies indexes are working)
        res = ex2.execute("SELECT v FROM config WHERE k = 'theme'")
        self.assertEqual(res[0]['v'], 'light')

if __name__ == '__main__':
    unittest.main()
