import sys
import os

# Ensure src is in python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from repl import REPL

if __name__ == "__main__":
    filename = sys.argv[1] if len(sys.argv) > 1 else None
    repl = REPL(filename)
    repl.start()
