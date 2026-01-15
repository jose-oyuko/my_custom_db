import sys
import os

# Ensure src is in python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from repl import REPL

if __name__ == "__main__":
    repl = REPL()
    repl.start()
