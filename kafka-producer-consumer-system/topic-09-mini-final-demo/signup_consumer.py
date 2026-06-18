"""
Topic 09 — thin wrapper around the refactored portfolio consumer.
Run from this folder or use: python consumer.py (from project root)
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from consumer import main

if __name__ == "__main__":
    main()
