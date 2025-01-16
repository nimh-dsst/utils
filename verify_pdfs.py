import csv
from multiprocessing import Manager, Pool, Process
from multiprocessing.synchronize import Event as EventType
from pathlib import Path
from queue import Empty, Queue

import pypdf
from pypdf._doc_common import DocumentInformation
from pypdf.errors import EmptyFileError, PdfStreamError
from tqdm import tqdm
import pandas as pd


def is_valid_pdf(file_path: Path) -> bool:
    """Check if a file is a valid PDF.

    Parameters
    ----------
    file_path : Path
        Path to the file to check

    Returns
    -------
    bool
        True if file is a valid PDF, False otherwise
    """
    try:
        with open(file_path, "rb") as f:
            header = f.read(5)
            return header.startswith(b"%PDF-")
    except Exception:
        return False


def validate_pdfs(directory: Path, num_processes: int = 4) -> dict:
    """Validate all PDFs in a directory using multiprocessing.

    Parameters
    ----------
    directory : Path
        Directory to search for PDFs
    num_processes : int, optional
        Number of processes to use, by default 4

    Returns
    -------
    dict
        Dictionary with results containing:
        - total_files: Total number of PDF files found
        - valid_files: Number of valid PDFs
        - invalid_files: Number of invalid PDFs
        - invalid_paths: List of paths to invalid PDFs
    """
    from multiprocessing import Pool

    # Find all PDF files
    pdf_files = list(directory.rglob("*.pdf"))

    if not pdf_files:
        return {
            "total_files": 0,
            "valid_files": 0,
            "invalid_files": 0,
            "invalid_paths": [],
        }

    # Process files in parallel
    with Pool(processes=num_processes) as pool:
        results = pool.map(is_valid_pdf, pdf_files)

    # Combine results
    valid_results = list(zip(pdf_files, results))
    invalid_pdfs = [
        str(pdf) for pdf, is_valid in valid_results if not is_valid
    ]

    return {
        "total_files": len(pdf_files),
        "valid_files": sum(results),
        "invalid_files": len(invalid_pdfs),
        "invalid_paths": invalid_pdfs,
    }


def segregate_pdfs(validation_results: dict) -> None:
    """Move invalid PDFs to an 'invalid' subdirectory.

    Parameters
    ----------
    validation_results : dict
        Dictionary containing validation results with:
        - invalid_paths: List of paths to invalid PDFs
    """
    if not validation_results["invalid_paths"]:
        return

    # Get directory from first invalid path
    first_path = Path(validation_results["invalid_paths"][0])
    base_dir = first_path.parent

    # Create invalid subdirectory
    invalid_dir = base_dir / "invalid"
    invalid_dir.mkdir(exist_ok=True)

    # Move invalid files
    for invalid_path in validation_results["invalid_paths"]:
        src_path = Path(invalid_path)
        dst_path = invalid_dir / src_path.name
        src_path.rename(dst_path)


def extract_hhs_info(pdf: Path) -> dict[str, str | None]:
    """
    Extract HHS (Health and Human Services) related information from a
    PDF file.

    Parameters
    ----------
    pdf : Path
        Path to the PDF file from which to extract information.

    Returns
    -------
    dict[str, str | None]
        A dictionary containing HHS-related PDF metadata with the
        following keys:
        - 'name': Full path of the PDF file
        - 'producer': PDF producer metadata
        - 'creator': PDF creator metadata
        - 'header': PDF header information
        - 'has_hhs_text': Whether 'HHS Public Access' text is found
        - 'error': Any error encountered during extraction, or None

    Notes
    -----
    Attempts to extract PDF metadata and check for 'HHS Public Access' text.
    Handles various potential errors during PDF reading and text extraction.
    """
    try:
        with open(pdf, "rb") as file:
            pdf_reader = pypdf.PdfReader(file)
            metadata = pdf_reader.metadata
            try:
                page = pdf_reader.pages[0]
                text = page.extract_text()
                has_hhs_text: bool = "HHS Public Access" in text
            except Exception as e:
                hhs_info: dict[str, str | None] = {
                    "name": str(pdf.absolute()),
                    "producer": None,
                    "creator": None,
                    "header": None,
                    "has_hhs_text": None,
                    "error": str(e),
                }
                return hhs_info

            if isinstance(metadata, DocumentInformation):
                hhs_info = {
                    "name": str(pdf.absolute()),
                    "producer": metadata.producer,
                    "creator": metadata.creator,
                    "header": pdf_reader.pdf_header,
                    "has_hhs_text": str(has_hhs_text),
                    "error": None,
                }
            else:
                hhs_info = {
                    "name": str(pdf.absolute()),
                    "producer": None,
                    "creator": None,
                    "header": None,
                    "has_hhs_text": str(has_hhs_text),
                    "error": "No metadata",
                }
            return hhs_info
    except (PdfStreamError, OSError, EmptyFileError) as e:
        hhs_info = {
            "name": str(pdf.absolute()),
            "producer": None,
            "creator": None,
            "header": None,
            "has_hhs_text": None,
            "error": str(e),
        }
        return hhs_info


def writer_process(
    queue: Queue, done_event: EventType, output_file: Path
) -> None:
    """Process that handles writing results to CSV."""
    fieldnames = [
        "name",
        "producer",
        "creator",
        "header",
        "has_hhs_text",
        "error",
    ]

    with open(output_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        while not (done_event.is_set() and queue.empty()):
            try:
                result = queue.get(timeout=1)
                writer.writerow(result)
            except Empty:
                continue
            except Exception as e:
                print(f"Error writing to CSV: {e}")


def process_pdf(args: tuple[Path, Queue]) -> bool:
    """Process a single PDF and put results in queue."""
    pdf_path, queue = args
    try:
        result = extract_hhs_info(pdf_path)
        queue.put(result)
        return True
    except Exception as e:
        queue.put(
            {
                "name": str(pdf_path),
                "producer": None,
                "creator": None,
                "header": None,
                "has_hhs_text": None,
                "error": f"Processing error: {str(e)}",
            }
        )
        return False


def process_pdfs_parallel(
    directory: Path, output_csv: Path, num_processes: int = 12
) -> None:
    """Process PDFs in parallel and write results to CSV."""
    # Find all PDF files
    pdf_files = list(directory.glob("*.pdf"))
    total_files = len(pdf_files)

    if total_files == 0:
        print("No PDF files found")
        return

    # Set up multiprocessing manager and queue
    with Manager() as manager:
        result_queue = manager.Queue()
        done_event = manager.Event()

        writer_proc = Process(
            target=writer_process, args=(result_queue, done_event, output_csv)
        )
        writer_proc.start()

        try:
            with Pool(processes=num_processes) as pool:
                args = [(pdf, result_queue) for pdf in pdf_files]
                list(
                    tqdm(
                        pool.imap_unordered(process_pdf, args),
                        total=total_files,
                        desc="Processing PDFs",
                    )
                )
        finally:
            done_event.set()
            writer_proc.join()

        print(f"Results written to {output_csv}")


def segregate_hhs(csv_path: Path) -> None:
    """Segregate PDFs based on HHS status from CSV results.

    Parameters
    ----------
    csv_path : Path
        Path to CSV file containing HHS extraction results

    Notes
    -----
    Creates 'hhs' and 'unknown' subdirectories in the same directory as the PDFs.
    Moves files with HHS status True to 'hhs' dir and files with NA status to 'unknown' dir.
    """
    # Read CSV
    df = pd.read_csv(csv_path)

    # Get base directory from first file path
    first_path = Path(df["name"].iloc[0])
    base_dir = first_path.parent

    # Create subdirectories
    hhs_dir = base_dir / "hhs"
    unknown_dir = base_dir / "unknown"
    hhs_dir.mkdir(exist_ok=True)
    unknown_dir.mkdir(exist_ok=True)

    # Move unknown files
    unknown_files = df.loc[df["has_hhs_text"].isna(), "name"]
    for file_path in unknown_files:
        src = Path(file_path)
        dst = unknown_dir / src.name
        src.rename(dst)
    df.dropna(subset=["has_hhs_text"], inplace=True)

    # Move HHS files
    hhs_files = df.loc[df["has_hhs_text"] == True, "name"]
    for file_path in hhs_files:
        src = Path(file_path)
        dst = hhs_dir / src.name
        src.rename(dst)
