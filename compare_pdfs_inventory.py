#!/usr/bin/env python3

import argparse
import csv
import json
import os
from pathlib import Path
import unicodedata
from typing import Dict, Set, Tuple


def load_json_data(
    json_path: str,
) -> Tuple[Set[str], Dict[str, str], Dict[str, str]]:
    """Load data from Paperpile JSON export."""
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    expected_files = set()
    filename_to_pmid = {}
    filename_to_json_path = {}

    for entry in data:
        if entry.get("attachments") and len(entry["attachments"]) > 0:
            filename = entry["attachments"][0].get("filename")
            if filename and filename.startswith("All Papers/"):
                rel_path = filename[len("All Papers/") :]
                expected_files.add(rel_path)
                if entry.get("pmid"):
                    filename_to_pmid[rel_path] = entry["pmid"]
                filename_to_json_path[rel_path] = filename

    return expected_files, filename_to_pmid, filename_to_json_path


def scan_pdf_directory(pdf_dir: str) -> Dict[str, str]:
    """Recursively scan directory for PDF files."""
    pdf_files = {}
    base_dir = Path(pdf_dir)
    for root, _, files in os.walk(pdf_dir):
        for file in files:
            if file.lower().endswith(".pdf"):
                full_path = Path(root) / file
                rel_path = str(full_path.relative_to(base_dir))
                pdf_files[rel_path] = str(full_path.absolute())
    return pdf_files


def write_csv_report(
    output_path: str,
    actual_files: Dict[str, str],
    filename_to_pmid: Dict[str, str],
    filename_to_json_path: Dict[str, str],
):
    """Write detailed CSV report."""
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Actual File Path", "PMID", "JSON Filename"])

        for rel_path, abs_path in sorted(actual_files.items()):
            assert Path(abs_path).exists()
            pmid = filename_to_pmid.get(rel_path, "")
            json_path = filename_to_json_path.get(rel_path, "")
            writer.writerow([abs_path, pmid, json_path])


def main():
    parser = argparse.ArgumentParser(
        description="Compare PDF files with Paperpile JSON data"
    )
    parser.add_argument(
        "--output",
        "-o",
        default="comparison.csv",
        help="Output CSV file path (default: comparison.csv)",
    )
    args = parser.parse_args()

    # Define paths
    base_dir = Path("/Users/lawrimorejg/repos/utils/paperpile_pdfs")
    json_path = base_dir / "Paperpile - References - Jan 30.json"
    pdf_dir = base_dir / "All Papers"

    # Load JSON data
    print("Loading JSON data...")
    expected_files, filename_to_pmid, filename_to_json_path = load_json_data(
        str(json_path)
    )
    print(f"Found {len(expected_files)} entries in JSON with attachments")

    # Scan PDF directory
    print("\nScanning PDF directory...")
    actual_files = scan_pdf_directory(str(pdf_dir))
    print(f"Found {len(actual_files)} PDF files in directory")

    # Compare sets
    actual_set = set(actual_files.keys())
    # Remove accents from expected_files
    expected_files = {
        unicodedata.normalize("NFD", file) for file in expected_files
    }
    actual_set = {unicodedata.normalize("NFD", file) for file in actual_set}
    missing_pdfs = expected_files - actual_set
    extra_pdfs = actual_set - expected_files

    # Report results
    print("\nResults:")
    print(f"PDFs missing from directory: {len(missing_pdfs)}")
    print(f"Extra PDFs in directory: {len(extra_pdfs)}")

    # Write CSV report
    print(f"\nWriting detailed report to {args.output}...")
    write_csv_report(
        args.output, actual_files, filename_to_pmid, filename_to_json_path
    )
    print("Done!")


if __name__ == "__main__":
    main()
