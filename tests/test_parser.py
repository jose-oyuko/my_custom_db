import unittest
import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from sql_parser import parse_create_table, parse_insert, parse_select, parse_update, parse_delete, parse_command

class TestParser(unittest.TestCase):
    
    def test_create_table(self):
        sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT UNIQUE)"
        res = parse_create_table(sql)
        self.assertEqual(res['table'], 'users')
        self.assertEqual(res['primary_key'], 'id')
        self.assertIn('email', res['unique_columns'])
        self.assertEqual(len(res['columns']), 3)

    def test_insert(self):
        sql = "INSERT INTO users VALUES (1, 'Alice', 3.5)"
        res = parse_insert(sql)
        self.assertEqual(res['table'], 'users')
        self.assertEqual(res['values'], [1, 'Alice', 3.5])

    def test_select_all(self):
        sql = "SELECT * FROM users"
        res = parse_select(sql)
        self.assertEqual(res['columns'], None)
        self.assertEqual(res['table'], 'users')
        self.assertIsNone(res['where'])

    def test_select_where(self):
        sql = "SELECT name, email FROM users WHERE id = 1 AND active = true"
        res = parse_select(sql)
        self.assertEqual(res['columns'], ['name', 'email'])
        self.assertEqual(res['where']['id'], 1)
        self.assertEqual(res['where']['active'], True)

    def test_update(self):
        sql = "UPDATE users SET name = 'Bob', age = 30 WHERE id = 1"
        res = parse_update(sql)
        self.assertEqual(res['table'], 'users')
        self.assertEqual(res['set']['name'], 'Bob')
        self.assertEqual(res['set']['age'], 30)
        self.assertEqual(res['where']['id'], 1)

    def test_delete(self):
        sql = "DELETE FROM users WHERE id = 5"
        res = parse_delete(sql)
        self.assertEqual(res['table'], 'users')
        self.assertEqual(res['where']['id'], 5)

    def test_command_routing(self):
        res = parse_command("SELECT * FROM users")
        self.assertEqual(res['command'], 'SELECT')

if __name__ == '__main__':
    unittest.main()
