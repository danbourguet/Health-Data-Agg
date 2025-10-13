"""Convenience wrapper to run ingestion CLI.

Execute `python whoop.py --help` for options.
"""
import sys
from whoop_ingest import main

if __name__ == '__main__':
	main(sys.argv[1:])
