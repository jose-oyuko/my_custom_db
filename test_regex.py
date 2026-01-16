import re

def _parse_key_value_pairs(clause_str, delimiter_regex):
    if not clause_str: return None
    print(f"Parsing KV: '{clause_str}'")
    parts = re.split(delimiter_regex, clause_str, flags=re.IGNORECASE)
    res = {}
    for part in parts:
        part = part.strip()
        if not part: continue
        # Original regex caused issue?
        m = re.match(r"(\w+)\s*=\s*(.+)", part)
        if m:
            print(f"Match: {m.groups()}")
            res[m.group(1)] = m.group(2)
        else:
            print(f"No match for '{part}' with r'(\w+)\s*=\s*(.+)'")
            # Try better regex
            m2 = re.match(r"([\w\.]+)\s*=\s*(.+)", part)
            if m2:
                print(f"Match m2: {m2.groups()}")
    return res

sql = "SELECT users.name, orders.item FROM users JOIN orders ON users.id = orders.uid WHERE users.name = 'Alice'"
pattern = r"SELECT\s+(.+?)\s+FROM\s+(\w+)(?:\s+JOIN\s+(\w+)\s+ON\s+(.+?))?(?:\s+WHERE\s+(.+))?$"

match = re.search(pattern, sql, re.IGNORECASE)
if match:
    print("Main Regex Matched")
    print(f"Groups: {match.groups()}")
    cols = match.group(1)
    table = match.group(2)
    join_tbl = match.group(3)
    join_on = match.group(4)
    where = match.group(5)
    
    print(f"Where clause: '{where}'")
    _parse_key_value_pairs(where, r'\s+AND\s+')
else:
    print("Main Regex Failed")
