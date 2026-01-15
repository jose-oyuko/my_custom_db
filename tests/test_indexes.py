import unittest
import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from indexes import Index

class TestIndex(unittest.TestCase):
    def setUp(self):
        self.idx = Index("id", unique=True)
        self.non_unique = Index("name", unique=False)

    def test_insert_unique(self):
        self.idx.insert(1, 0)
        self.assertEqual(self.idx.lookup(1), {0})
        
        with self.assertRaises(ValueError):
            self.idx.insert(1, 1)

    def test_insert_non_unique(self):
        self.non_unique.insert("Alice", 0)
        self.non_unique.insert("Alice", 1)
        self.assertEqual(self.non_unique.lookup("Alice"), {0, 1})

    def test_update_unique(self):
        self.idx.insert(1, 0)
        self.idx.update(1, 2, 0)
        self.assertEqual(self.idx.lookup(2), {0})
        self.assertEqual(self.idx.lookup(1), set())

    def test_delete(self):
        self.idx.insert(1, 0)
        self.idx.delete(1, 0)
        self.assertEqual(self.idx.lookup(1), set())

if __name__ == '__main__':
    unittest.main()
