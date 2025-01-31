import pandas as pd
import boto3
import os
import logging
import argparse


def setup_logging():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    return logging.getLogger(__name__)


def rename_and_upload_pdfs(
    csv_path, s3_uri="s3://osm-pdf-uploads/pdfs", dry_run=False
):
    """
    Read CSV file, rename PDFs using PMID, and upload to S3.

    Args:
        csv_path (str): Path to the CSV file
        s3_uri (str): S3 URI where files should be uploaded
        dry_run (bool): If True, only show what would be done without uploading
    """
    logger = setup_logging()

    if dry_run:
        logger.info("DRY RUN - No files will be uploaded")

    # Parse S3 URI
    bucket_name = s3_uri.split("/")[2]
    prefix = "/".join(s3_uri.split("/")[3:])

    # Initialize S3 client if not dry run
    if not dry_run:
        s3_client = boto3.client("s3")

    # Read CSV file
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        logger.error(f"Failed to read CSV file: {e}")
        return

    # Process each row
    for _, row in df.iterrows():
        try:
            source_path = row["Actual File Path"]
            pmid = str(int(row["PMID"]))

            # Skip if PMID is empty
            if not pmid or pd.isna(pmid):
                logger.warning(f"Skipping file {source_path} - No PMID found")
                continue

            # Create new filename
            new_filename = f"{pmid}.pdf"
            s3_key = f"{prefix}/{new_filename}".rstrip("/")

            # Check if source file exists
            if not os.path.exists(source_path):
                logger.warning(f"Source file not found: {source_path}")
                continue

            # Upload to S3 or show what would be done
            if dry_run:
                logger.info(
                    "Would upload {} to "
                    "s3://{}/{}".format(source_path, bucket_name, s3_key)
                )
            else:
                try:
                    logger.info(
                        "Uploading {} to "
                        "s3://{}/{}".format(source_path, bucket_name, s3_key)
                    )
                    s3_client.upload_file(source_path, bucket_name, s3_key)
                    logger.info(f"Successfully uploaded {new_filename}")
                except Exception as e:
                    logger.error(f"Failed to upload {source_path}: {e}")

        except Exception as e:
            logger.error(f"Error processing row: {e}")
            continue


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Rename PDFs using PMID and upload to S3"
    )
    parser.add_argument("csv_path", help="Path to the CSV file")
    parser.add_argument(
        "--s3-uri",
        default="s3://osm-pdf-uploads/pdfs",
        help="S3 URI where files should be uploaded",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without actually uploading files",
    )

    args = parser.parse_args()
    rename_and_upload_pdfs(args.csv_path, args.s3_uri, args.dry_run)
