#!/usr/bin/env python3

import argparse
import csv
import logging
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import List, Set

import boto3
from botocore.exceptions import ClientError

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("pdf_upload.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


def parse_csv_inventory(csv_path: str) -> tuple[List[Path], Set[int]]:
    """
    Parse CSV inventory file and create a list of unique PDF paths.

    Parameters
    ----------
    csv_path : str
        Path to the CSV inventory file.

    Returns
    -------
    tuple[List[Path], Set[int]]
        A tuple containing:
        - List of Path objects for unique PDFs
        - Set of integers representing processed accession numbers

    Notes
    -----
    Skips duplicate accession numbers and logs them.
    """
    upload_list = []
    processed_accessions = set()

    try:
        with open(csv_path, "r") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                pdf_path = Path(row["name"])
                try:
                    accession_num = int(pdf_path.stem)
                    if accession_num in processed_accessions:
                        logger.warning(
                            "Duplicate accession number found: "
                            + f"{accession_num} - {pdf_path}"
                        )
                        continue

                    processed_accessions.add(accession_num)
                    upload_list.append(pdf_path)
                except ValueError:
                    logger.error(
                        f"Invalid accession number format: {pdf_path.stem}"
                    )
                    continue

    except Exception as e:
        logger.error(f"Error processing CSV file: {e}")
        raise

    return upload_list, processed_accessions


def get_s3_inventory(s3_uri: str) -> Set[str]:
    """
    Get inventory of existing PDF files in S3 bucket.

    Parameters
    ----------
    s3_uri : str
        S3 URI in format s3://bucket-name/prefix

    Returns
    -------
    Set[str]
        Set of PDF filenames (without path) that exist in S3

    Notes
    -----
    Only includes files with .pdf extension (case insensitive)
    """
    try:
        bucket_name = s3_uri.split("/")[2]
        prefix = "/".join(s3_uri.split("/")[3:])

        s3_client = boto3.client("s3")
        existing_files = set()

        paginator = s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            if "Contents" in page:
                for obj in page["Contents"]:
                    if obj["Key"].lower().endswith(".pdf"):
                        existing_files.add(Path(obj["Key"]).name)

        return existing_files

    except Exception as e:
        logger.error(f"Error getting S3 inventory: {e}")
        raise


def filter_upload_list(
    upload_list: List[Path], s3_inventory: Set[str]
) -> List[Path]:
    """
    Remove files that already exist in S3 from upload list.

    Parameters
    ----------
    upload_list : List[Path]
        List of Path objects to potentially upload
    s3_inventory : Set[str]
        Set of filenames that already exist in S3

    Returns
    -------
    List[Path]
        Filtered list of Path objects to upload
    """
    filtered_list = [
        path for path in upload_list if path.name not in s3_inventory
    ]

    skipped = len(upload_list) - len(filtered_list)
    if skipped > 0:
        logger.info(f"Skipping {skipped} files that already exist in S3")

    return filtered_list


def upload_file(file_path: Path, s3_uri: str) -> bool:
    """
    Upload a single file to S3.

    Parameters
    ----------
    file_path : Path
        Path to file to upload
    s3_uri : str
        S3 URI destination

    Returns
    -------
    bool
        True if upload successful, False otherwise
    """
    try:
        bucket_name = s3_uri.split("/")[2]
        prefix = "/".join(s3_uri.split("/")[3:])
        s3_key = f"{prefix}/{file_path.name}"

        s3_client = boto3.client("s3")
        s3_client.upload_file(str(file_path), bucket_name, s3_key)
        logger.info(f"Successfully uploaded: {file_path.name}")
        return True

    except ClientError as e:
        logger.error(f"Error uploading {file_path.name}: {e}")
        return False


def parallel_upload(file_list: List[Path], s3_uri: str, num_threads: int):
    """
    Upload files to S3 in parallel using threading.

    Parameters
    ----------
    file_list : List[Path]
        List of files to upload
    s3_uri : str
        S3 URI destination
    num_threads : int
        Number of upload threads to use

    Notes
    -----
    Uses ThreadPoolExecutor for parallel uploads
    """
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        results = list(
            executor.map(lambda x: upload_file(x, s3_uri), file_list)
        )

    success_count = sum(1 for r in results if r)
    logger.info(
        f"Upload complete. {success_count}/{len(file_list)} "
        + "files uploaded successfully"
    )


def main():
    """
    Main function to process command line arguments and orchestrate the upload
    process.
    """
    parser = argparse.ArgumentParser(
        description="Process PDF inventory and upload to S3"
    )
    parser.add_argument(
        "--inventory", required=True, help="Path to CSV inventory file"
    )
    parser.add_argument(
        "--s3-uri",
        required=True,
        help="S3 URI destination (s3://bucket-name/prefix)",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=6,
        help="Number of upload threads (default: 6)",
    )

    args = parser.parse_args()

    try:
        # Parse inventory and get unique files
        logger.info("Parsing CSV inventory...")
        upload_list, processed_accessions = parse_csv_inventory(args.inventory)
        logger.info(f"Found {len(upload_list)} unique PDFs to process")

        # Get S3 inventory
        logger.info("Getting S3 inventory...")
        s3_inventory = get_s3_inventory(args.s3_uri)
        logger.info(f"Found {len(s3_inventory)} existing PDFs in S3")

        # Filter out existing files
        filtered_list = filter_upload_list(upload_list, s3_inventory)
        logger.info(f"{len(filtered_list)} files to upload")

        # Perform parallel upload
        if filtered_list:
            logger.info(
                f"Starting parallel upload with {args.threads} threads..."
            )
            parallel_upload(filtered_list, args.s3_uri, args.threads)
        else:
            logger.info("No files to upload")

    except Exception as e:
        logger.error(f"Error in main process: {e}")
        raise


if __name__ == "__main__":
    main()
