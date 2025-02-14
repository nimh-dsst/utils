import concurrent.futures
import logging
import multiprocessing
from pathlib import Path
from urllib.parse import urlparse

import boto3


def parse_s3_uri(s3_uri: str) -> tuple[str, str]:
    """
    Parse an S3 URI into bucket name and key

    Args:
        s3_uri: URI in format s3://bucket-name/path/to/file

    Returns:
        Tuple of (bucket_name, key)
    """
    parsed = urlparse(s3_uri)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    return bucket, key


def read_pmid_locations(filepath: str) -> list[dict[str, str]]:
    """
    Read the PMID locations file and return list of file info dicts

    Args:
        filepath: Path to the file containing PMID,S3_URI pairs

    Returns:
        List of dicts with s3_key and local_name for each file
    """
    file_manifest = []
    with open(filepath) as f:
        for line in f:
            if not line.strip():
                continue
            try:
                pmid, s3_uri = line.strip().split(",")
                bucket, key = parse_s3_uri(s3_uri)
                # Create subdirectory path based on first 3 digits of PMID
                subdir = pmid[:3] if len(pmid) >= 3 else "other"
                file_manifest.append(
                    {
                        "s3_key": key,
                        "local_name": f"{subdir}/{pmid}.pdf",
                        "pmid": pmid,
                    }
                )
            except ValueError:
                logging.warning(f"Skipping malformed line: {line.strip()}")
    return file_manifest


def download_file(
    file_info: dict[str, str], bucket_name: str, local_dir: Path
) -> bool:
    """
    Download a single file from S3

    Args:
        file_info: Dict containing 's3_key' and 'local_name' for the file
        bucket_name: Name of the S3 bucket
        local_dir: Local directory to save files
    """
    s3_client = boto3.client("s3")
    try:
        local_path = local_dir / file_info["local_name"]
        local_path.parent.mkdir(parents=True, exist_ok=True)

        s3_client.download_file(
            Bucket=bucket_name,
            Key=file_info["s3_key"],
            Filename=str(local_path),
        )
        logging.info(
            f"Successfully downloaded PMID {file_info['pmid']}"
            + f" to {file_info['local_name']}"
        )
        return True
    except Exception:
        try:
            # add in a / to the prefix
            parts = file_info["s3_key"].split("/")
            amended_key = "//".join(parts)
            s3_client.download_file(
                Bucket=bucket_name,
                Key=amended_key,
                Filename=str(local_path),
            )
        except Exception as e:
            logging.error(
                f"Failed to download PMID {file_info['pmid']}"
                + f" ({file_info['s3_key']}): {str(e)}"
            )
        return False


def parallel_download(
    file_manifest: list[dict[str, str]],
    bucket_name: str,
    local_dir: str,
    max_workers: int = 16,
) -> None:
    """
    Download multiple files in parallel from S3

    Args:
        file_manifest: List of dicts containing file information
        bucket_name: Name of the S3 bucket
        local_dir: Local directory to save files
        max_workers: Number of concurrent downloads
    """
    local_path = Path(local_dir)
    local_path.mkdir(parents=True, exist_ok=True)

    success_count = 0
    failure_count = 0

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=max_workers
    ) as executor:
        # Submit all download tasks
        future_to_file = {
            executor.submit(
                download_file, file_info, bucket_name, local_path
            ): file_info
            for file_info in file_manifest
        }

        # Process completed downloads
        for future in concurrent.futures.as_completed(future_to_file):
            file_info = future_to_file[future]
            try:
                if future.result():
                    success_count += 1
                else:
                    failure_count += 1
            except Exception as e:
                logging.error(
                    f"Exception downloading {file_info['s3_key']}: {str(e)}"
                )
                failure_count += 1

    logging.info(
        "Download complete. Successes:"
        + f" {success_count}, Failures: {failure_count}"
    )


# Add this function to get optimal worker count
def get_optimal_worker_count() -> int:
    """
    Calculate optimal number of workers based on CPU count.
    Returns a value between 4 and 32.
    """
    cpu_count = multiprocessing.cpu_count()
    # Use 2x CPU count for I/O bound tasks, but cap between 4 and 32
    worker_count = min(max(cpu_count * 2, 4), 32)
    return worker_count


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    # Read the PMID locations file
    file_manifest = read_pmid_locations("found_pmids.txt")

    # Get bucket name from first entry
    _, first_uri = next(open("found_pmids.txt")).strip().split(",")
    bucket_name, _ = parse_s3_uri(first_uri)

    logging.info(f"Found {len(file_manifest)} files to download")

    max_workers = get_optimal_worker_count()
    logging.info(f"Using {max_workers} workers based on system CPU count")

    parallel_download(
        file_manifest=file_manifest[:100],
        bucket_name=bucket_name,
        local_dir="downloaded_pdfs",
        max_workers=max_workers,
    )
