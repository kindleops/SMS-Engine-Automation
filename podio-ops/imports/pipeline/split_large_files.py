#!/usr/bin/env python3
"""Split very large CSV files into deterministic chunks.

Usage:
  python split_large_files.py --input path/to/input.csv --output-dir path/to/chunks --rows-per-chunk 200000
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Iterable


def chunk_writer(header: list[str], rows: Iterable[list[str]], output_file: Path) -> int:
    """Write one chunk and return number of rows written."""
    output_file.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with output_file.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        for row in rows:
            writer.writerow(row)
            count += 1
    return count


def split_csv(input_file: Path, output_dir: Path, rows_per_chunk: int) -> list[Path]:
    """Split CSV into chunked files while preserving header in each chunk."""
    if rows_per_chunk <= 0:
        raise ValueError("rows_per_chunk must be greater than 0")

    output_dir.mkdir(parents=True, exist_ok=True)

    generated_files: list[Path] = []
    with input_file.open("r", newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        header = next(reader, None)
        if not header:
            raise ValueError(f"Input file '{input_file}' is empty or missing header")

        chunk_rows: list[list[str]] = []
        chunk_index = 1
        for row in reader:
            chunk_rows.append(row)
            if len(chunk_rows) >= rows_per_chunk:
                output_file = output_dir / f"{input_file.stem}.part{chunk_index:05d}.csv"
                chunk_writer(header, chunk_rows, output_file)
                generated_files.append(output_file)
                chunk_rows = []
                chunk_index += 1

        if chunk_rows:
            output_file = output_dir / f"{input_file.stem}.part{chunk_index:05d}.csv"
            chunk_writer(header, chunk_rows, output_file)
            generated_files.append(output_file)

    return generated_files


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Split a large CSV file into smaller chunks.")
    parser.add_argument("--input", required=True, type=Path, help="Path to source CSV")
    parser.add_argument("--output-dir", required=True, type=Path, help="Directory for chunked CSV files")
    parser.add_argument("--rows-per-chunk", type=int, default=200_000, help="Rows per chunk (default: 200000)")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    files = split_csv(args.input, args.output_dir, args.rows_per_chunk)
    print(f"Generated {len(files)} chunk file(s)")
    for file in files:
        print(file)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
