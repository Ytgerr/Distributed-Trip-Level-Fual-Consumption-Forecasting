#!/usr/bin/env python3
"""Audit raw downloaded project data files."""

from pathlib import Path
import csv
import os
import pandas as pd


DATA_DIR = Path("data")
OUTPUT_DIR = Path("output")
OUTPUT_FILE = OUTPUT_DIR / "data_audit.csv"


def file_size_mb(path: Path) -> float:
    """Return file size in MB."""
    return round(path.stat().st_size / 1024 / 1024, 3)


def count_csv_rows_and_columns(path: Path):
    """Count rows and columns in CSV without loading full file into memory."""
    try:
        with path.open("r", encoding="utf-8", errors="ignore", newline="") as file:
            reader = csv.reader(file)
            header = next(reader, [])
            row_count = sum(1 for _ in reader)
            return row_count, len(header)
    except Exception as exc:
        return None, None


def count_excel_sheets(path: Path):
    """Count sheets in Excel file."""
    try:
        excel_file = pd.ExcelFile(path)
        return len(excel_file.sheet_names)
    except Exception:
        return None


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    rows = []

    if not DATA_DIR.exists():
        raise FileNotFoundError("data/ directory does not exist. Run data collection first.")

    for path in sorted(DATA_DIR.rglob("*")):
        if not path.is_file():
            continue

        suffix = path.suffix.lower()
        size_mb = file_size_mb(path)

        row_count = None
        column_count = None
        sheet_count = None

        if suffix == ".csv":
            row_count, column_count = count_csv_rows_and_columns(path)

        if suffix in {".xls", ".xlsx"}:
            sheet_count = count_excel_sheets(path)

        rows.append(
            {
                "file_path": str(path),
                "file_name": path.name,
                "extension": suffix,
                "size_mb": size_mb,
                "rows": row_count,
                "columns": column_count,
                "excel_sheets": sheet_count,
            }
        )

    total_size_mb = round(sum(row["size_mb"] for row in rows), 3)
    total_csv_rows = sum(row["rows"] or 0 for row in rows)

    rows.append(
        {
            "file_path": "__TOTAL__",
            "file_name": "__TOTAL__",
            "extension": "",
            "size_mb": total_size_mb,
            "rows": total_csv_rows,
            "columns": "",
            "excel_sheets": "",
        }
    )

    with OUTPUT_FILE.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "file_path",
                "file_name",
                "extension",
                "size_mb",
                "rows",
                "columns",
                "excel_sheets",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"Saved audit to {OUTPUT_FILE}")
    print(f"Total files: {len(rows) - 1}")
    print(f"Total size MB: {total_size_mb}")
    print(f"Total CSV rows: {total_csv_rows}")


if __name__ == "__main__":
    main()
