#!/usr/bin/env python3
"""Round 47 fixture promoted to the Round 48 production acceptance suite."""
import runpy
from pathlib import Path

if __name__ == "__main__":
    runpy.run_path(str(Path(__file__).with_name("test-adopted-source-final-index-sql.py")), run_name="__main__")
