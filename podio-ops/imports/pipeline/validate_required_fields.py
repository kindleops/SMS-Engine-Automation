#!/usr/bin/env python3
"""Validate required CSV fields before import.

Usage:
  python validate_required_fields.py \
    --input path/to/chunk.csv \
    --required property-id owner-id zip_code
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path


@dataclass
class ValidationError:
    row_number: int
    field_name: str
    message: str


def validate_required_fields(input_file: Path, required_fields: list[str]) -> list[ValidationError]:
    errors: list[ValidationError] = []

    with input_file.open("r", newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        if reader.fieldnames is None:
            raise ValueError(f"CSV file '{input_file}' is missing headers")

        missing_headers = [field for field in required_fields if field not in reader.fieldnames]
        if missing_headers:
            raise ValueError(f"Missing required header(s): {', '.join(missing_headers)}")

        for row_idx, row in enumerate(reader, start=2):
            for field in required_fields:
                value = row.get(field)
                if value is None or not str(value).strip():
                    errors.append(
                        ValidationError(
                            row_number=row_idx,
                            field_name=field,
                            message="Required field is empty",
                        )
                    )

    return errors


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate required fields in a CSV file")
    parser.add_argument("--input", required=True, type=Path, help="Input CSV file")
    parser.add_argument("--required", nargs="+", required=True, help="List of required header names")
    parser.add_argument(
        "--max-errors",
        type=int,
        default=100,
        help="Maximum number of errors to print (default: 100)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    errors = validate_required_fields(args.input, args.required)

    if not errors:
        print("Validation passed: all required fields are present.")
        return 0

    print(f"Validation failed: {len(errors)} missing required field value(s) found.")
    for err in errors[: args.max_errors]:
        print(f"row={err.row_number} field={err.field_name}: {err.message}")

    if len(errors) > args.max_errors:
        print(f"... truncated {len(errors) - args.max_errors} additional error(s)")

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
