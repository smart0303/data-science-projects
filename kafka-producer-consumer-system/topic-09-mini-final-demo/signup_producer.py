"""
Topic 09 — thin wrapper around the refactored portfolio producer.
Run from this folder or use: python producer.py (from project root)
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from producer import main

if __name__ == "__main__":
    main()
