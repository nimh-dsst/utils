# AWS PMID and PDF Finding and Downloading Tools

This repository contains two Python scripts for finding and downloading PDF files from AWS S3:

1. `find_pmids_in_s3.py`: Checks which PMIDs have corresponding PDFs in an S3 bucket
2. `aws_download.py`: Downloads the found PDFs from S3 to local storage

## Prerequisites

- Python 3.11+
- AWS credentials configured (either through AWS CLI or environment variables)
- Required Python packages:

  ```python
  boto3
  pandas
  tqdm
  ```

## find_pmids_in_s3.py

This script checks an S3 bucket for existing PDFs corresponding to a list of PMIDs.

### Usage: find_pmids_in_s3.py

1. Prepare a CSV file named `total_pmid_articles.csv` containing a column named "PMID"
2. Run the script:

   ```bash
   python find_pmids_in_s3.py
   ```

### Output

The script generates two files:

- `found_pmids.txt`: List of PMIDs with their S3 URIs for found PDFs
- `missing_pmids.txt`: List of PMIDs that don't have corresponding PDFs in S3

### Configuration

The script uses these default settings:

- Input file: `total_pmid_articles.csv`
- S3 bucket: `osm-pdf-uploads`
- PDF location in bucket: `pdfs/` prefix. Also checks for `pdfs//` prefix due to Josh's mistake.

## aws_download.py

This script downloads PDFs from S3 to local storage using the output from `find_pmids_in_s3.py`.

### Usage: aws_download.py

1. Ensure you have the `found_pmids.txt` file from the previous script
2. Run:

   ```bash
   python aws_download.py
   ```

### Features

- Parallel downloading using multiple threads
- Automatic directory structure creation (PDFs are organized by first 3 digits of PMID)
- Robust error handling and logging
- Automatic optimization of worker count based on CPU cores

### Output: aws_download.py

Downloads are saved to the `downloaded_pdfs/` directory with this structure:

### Future Work

- Convert scripts to CLIs using argparse
- Add SLURM job scripts to run the scripts on a cluster
