import pandas as pd
import boto3
from typing import NamedTuple, Set
import logging
from pathlib import Path
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures


class PMIDStatus(NamedTuple):
    pmid: int
    found: bool
    s3_key: str | None
    s3_uri: str | None


def setup_logging() -> None:
    """Configure logging"""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )


def read_pmids_from_csv(csv_path: str | Path) -> list[int]:
    """Read PMIDs from the CSV file"""
    try:
        df = pd.read_csv(str(csv_path))  # Convert Path to str explicitly
        if "PMID" not in df.columns:
            raise ValueError("CSV file does not contain a 'PMID' column")

        pmids = df["PMID"].dropna().astype(int).unique().tolist()
        logging.info(f"Found {len(pmids)} unique PMIDs in CSV")
        return pmids
    except Exception as e:
        logging.error(f"Error reading CSV file: {str(e)}")
        raise


def get_existing_pdfs(s3_client: boto3.client, bucket_name: str) -> set[str]:
    """
    Get set of existing PDF filenames in the bucket

    Args:
        s3_client: Initialized boto3 S3 client
        bucket_name: Name of the S3 bucket

    Returns:
        Set of PDF filenames that exist in the bucket
    """
    existing_pdfs = set()
    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        for page in tqdm(
            paginator.paginate(Bucket=bucket_name, Prefix="pdfs/"),
            desc="Caching S3 contents",
        ):
            if "Contents" in page:
                for obj in page["Contents"]:
                    filename = Path(obj["Key"]).name
                    if filename.endswith(".pdf"):
                        existing_pdfs.add(filename)
        return existing_pdfs
    except Exception as e:
        logging.error(f"Error listing S3 bucket contents: {str(e)}")
        raise


def check_single_pmid(
    pmid: int, existing_pdfs: Set[str], bucket_name: str
) -> PMIDStatus:
    """
    Check if a single PMID has a corresponding PDF in S3

    Args:
        pmid: PMID to check
        existing_pdfs: Set of existing PDF filenames
        bucket_name: Name of the bucket for constructing S3 URI

    Returns:
        PMIDStatus object indicating if the PDF was found
    """
    filename = f"{pmid}.pdf"
    if filename in existing_pdfs:
        s3_key = f"pdfs/{filename}"
        s3_uri = f"s3://{bucket_name}/{s3_key}"
        return PMIDStatus(pmid=pmid, found=True, s3_key=s3_key, s3_uri=s3_uri)
    return PMIDStatus(pmid=pmid, found=False, s3_key=None, s3_uri=None)


def check_pmids_in_s3(pmids: list[int], bucket_name: str) -> list[PMIDStatus]:
    """
    Check which PMIDs have corresponding PDFs in S3

    Args:
        pmids: List of PMIDs to check
        bucket_name: Name of the S3 bucket

    Returns:
        List of PMIDStatus objects indicating which PMIDs were found
    """
    s3_client = boto3.client("s3")
    results = []

    # Get existing PDFs once
    existing_pdfs = get_existing_pdfs(s3_client, bucket_name)

    with ThreadPoolExecutor(max_workers=16) as executor:
        # Create futures for all PMIDs
        futures = [
            executor.submit(
                check_single_pmid, pmid, existing_pdfs, bucket_name
            )
            for pmid in pmids
        ]

        # Process results as they complete
        for future in tqdm(
            concurrent.futures.as_completed(futures),
            desc="Checking PMIDs in S3",
            total=len(pmids),
        ):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                logging.error(f"Error processing future: {str(e)}")

    return results


def save_results(
    found_pmids: list[PMIDStatus], missing_pmids: list[PMIDStatus]
) -> None:
    """
    Save results to output files

    Args:
        found_pmids: List of PMIDStatus objects for found PDFs
        missing_pmids: List of PMIDStatus objects for missing PDFs
    """
    # Save results to files
    for filename, results, include_uri in [
        ("found_pmids.txt", found_pmids, True),
        ("missing_pmids.txt", missing_pmids, False),
    ]:
        try:
            with open(filename, "w") as f:
                for result in results:
                    if include_uri:
                        f.write(f"{result.pmid},{result.s3_uri}\n")
                    else:
                        f.write(f"{result.pmid}\n")
            logging.info(f"Results saved to {filename}")
        except Exception as e:
            logging.error(f"Error saving to {filename}: {str(e)}")


def main():
    setup_logging()

    # Configuration
    CSV_PATH = "total_pmid_articles.csv"
    BUCKET_NAME = "osm-pdf-uploads"

    try:
        # Read PMIDs
        pmids = read_pmids_from_csv(CSV_PATH)

        # Check S3
        results = check_pmids_in_s3(pmids, BUCKET_NAME)

        # Analyze results
        found_pmids = [r for r in results if r.found]
        missing_pmids = [r for r in results if not r.found]

        # Log results
        logging.info(f"Found {len(found_pmids)} PDFs in S3")
        logging.info(f"Missing {len(missing_pmids)} PDFs")

        # Save results
        save_results(found_pmids, missing_pmids)

    except Exception as e:
        logging.error(f"Script failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()
