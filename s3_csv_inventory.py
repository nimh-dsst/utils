#!/usr/bin/env python3

import argparse
import csv
import logging
from pathlib import Path
from typing import Dict, Set

import boto3
import pandas as pd

# Configure logger to write to a log file
logging.basicConfig(
    filename="inventory_compare.log",
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def parse_csv_pmids(csv_path: str, has_header: bool) -> Set[int]:
    """
    Parse PMIDs from a CSV file and log any duplicates.

    Parameters
    ----------
    csv_path : str
        Path to the CSV file containing PMIDs
    has_header : bool
        Indicates if the CSV file has a header row

    Returns
    -------
    Set[int]
        Set of unique PMIDs from the CSV file

    Notes
    -----
    This function logs any duplicate PMIDs found in the CSV file
    """
    pmids: set = set()
    seen_pmids: dict[int, int] = (
        {}
    )  # Dictionary to store PMID and its first occurrence row number

    with open(csv_path, "r") as f:
        reader: csv.DictReader = csv.DictReader(f)
        start_row = 2 if has_header else 1
        for i, row in enumerate(reader, start=start_row):
            try:
                pmid = int(row["PMID"])
                if pmid in pmids:
                    logger.warning(
                        f"Duplicate PMID {pmid} found in row {i}. "
                        + f"First occurrence in row {seen_pmids[pmid]}"
                    )
                else:
                    pmids.add(pmid)
                    seen_pmids[pmid] = i
            except ValueError:
                logger.error(f"Invalid PMID format in row {i}: {row['PMID']}")

    return pmids


def get_s3_pmids(s3_uri: str) -> Set[int]:
    """
    Retrieve all PMIDs from PDF filenames in an S3 bucket/prefix.

    Parameters
    ----------
    s3_uri : str
        S3 URI in the format 's3://bucket-name/optional/prefix'

    Returns
    -------
    Set[int]
        Set of unique PMIDs from S3 PDF filenames
    """
    # Parse bucket and prefix from URI
    bucket_name = s3_uri.split("//")[1].split("/")[0]
    prefix = (
        "/".join(s3_uri.split("//")[1].split("/")[1:])
        if "/" in s3_uri.split("//")[1]
        else ""
    )

    s3_client = boto3.client("s3")
    pmids = set()

    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if "Contents" in page:
            for obj in page["Contents"]:
                filename = Path(
                    obj["Key"]
                ).stem  # Get filename without extension
                try:
                    pmid = int(filename)
                    pmids.add(pmid)
                except ValueError:
                    logger.warning(
                        f"Could not parse PMID from filename: {filename}"
                    )

    return pmids


def analyze_pmids(csv_pmids: Set[int], s3_pmids: Set[int]) -> Dict:
    """
    Compare PMIDs from CSV and S3 to generate analysis.

    Parameters
    ----------
    csv_pmids : Set[int]
        Set of PMIDs from CSV file
    s3_pmids : Set[int]
        Set of PMIDs from S3 bucket

    Returns
    -------
    Dict
        Dictionary containing analysis results
    """
    common_pmids = csv_pmids.intersection(s3_pmids)
    missing_pmids = csv_pmids - s3_pmids
    extra_pmids = s3_pmids - csv_pmids

    return {
        "csv_unique": len(csv_pmids),
        "s3_unique": len(s3_pmids),
        "common": len(common_pmids),
        "missing": len(missing_pmids),
        "extra": len(extra_pmids),
        "missing_pmids": sorted(list(missing_pmids)),
        "extra_pmids": sorted(list(extra_pmids)),
    }


def create_excel_report(
    analysis: dict, csv_path: str, s3_uri: str, output_path: str | None = None
) -> None:
    """
    Create Excel report with multiple sheets containing analysis results.

    Parameters
    ----------
    analysis : Dict
        Dictionary containing analysis results
    csv_path : str
        Path to the CSV file containing PMIDs
    s3_uri : str
        S3 URI (e.g., s3://bucket-name/optional/prefix)
    output_path : str | None, optional
        Path where Excel file should be saved, by default concaentation of
        csv_stem and modified s3_uri
    """
    if output_path is None:
        csv_stem: str = Path(csv_path).stem
        s3_uri_str: str = s3_uri.split("s3://")[1].replace("/", "_")
        output_path = f"pmid_compare_{csv_stem}_{s3_uri_str}.xlsx"
    with pd.ExcelWriter(output_path) as writer:
        # Summary sheet
        summary_data = {
            "Metric": [
                "CSV Filepath",
                "S3 URI",
                "Unique PMIDs in CSV",
                "Unique PDFs in S3",
                "PMIDs in both CSV and S3",
                "PMIDs in CSV but not in S3",
                "PMIDs in S3 but not in CSV",
            ],
            "Count": [
                csv_path,
                s3_uri,
                analysis["csv_unique"],
                analysis["s3_unique"],
                analysis["common"],
                analysis["missing"],
                analysis["extra"],
            ],
        }
        print(summary_data)
        pd.DataFrame(summary_data).to_excel(
            writer, sheet_name="Summary", index=False
        )

        # Missing PMIDs sheet
        pd.DataFrame({"Missing PMIDs": analysis["missing_pmids"]}).to_excel(
            writer, sheet_name="Missing", index=False
        )

        # Extra PMIDs sheet
        pd.DataFrame({"Extra PMIDs": analysis["extra_pmids"]}).to_excel(
            writer, sheet_name="Extras", index=False
        )


def main():
    """
    Main function to run the PMID analysis script.
    """
    parser = argparse.ArgumentParser(
        description="Analyze PMIDs from CSV and S3 PDFs"
    )
    parser.add_argument("csv_path", help="Path to CSV file containing PMIDs")
    parser.add_argument(
        "s3_uri", help="S3 URI (e.g., s3://bucket-name/optional/prefix)"
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output path for Excel report (default: pmid_analysis.xlsx)",
    )
    parser.add_argument(
        "--has-header",
        action="store_true",
        help="Indicates if the CSV file has a header row",
    )

    args = parser.parse_args()

    logger.info("Parsing CSV PMIDs...")
    csv_pmids = parse_csv_pmids(args.csv_path, args.has_header)

    logger.info("Retrieving S3 PMIDs...")
    s3_pmids = get_s3_pmids(args.s3_uri)

    logger.info("Analyzing PMIDs...")
    analysis = analyze_pmids(csv_pmids, s3_pmids)

    logger.info("Creating Excel report...")
    create_excel_report(analysis, args.csv_path, args.s3_uri, args.output)

    logger.info(f"Analysis complete. Report saved to {args.output}")


if __name__ == "__main__":
    main()
