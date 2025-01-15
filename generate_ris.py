import argparse
import os
from pathlib import Path
from typing import Literal

import pandas as pd
from dotenv import load_dotenv
from eutils import QueryService  # type: ignore
from metapub.pubmedarticle import PubMedArticle  # type: ignore

# Load environment variables from .env file
load_dotenv()


def parse_excel_file(filepath: Path) -> set[int]:
    """
    Parse PMIDs from an Excel file.

    Parameters
    ----------
    filepath : Path
        Path to the Excel file containing PMIDs

    Returns
    -------
    set[int]
        Set of PMIDs from the Excel file
    """
    df: pd.DataFrame = pd.read_excel(filepath, sheet_name="Missing")
    pmids: set[int] = set(df["Missing PMIDs"].astype(int))
    return pmids


def get_dois(pmids: set[int]) -> pd.DataFrame:
    """
    Retrieve DOIs for given PMIDs using the NCBI API.

    Parameters
    ----------
    pmids : set[int]
        Set of PMIDs

    Returns
    -------
    pd.DataFrame
        DataFrame containing PMIDs and their corresponding DOIs
    """
    df_dicts: list[dict[str, str | int]] = []
    qs = QueryService(
        email=os.getenv("NCBI_EMAIL"), api_key=os.getenv("NCBI_API_KEY")
    )
    for pmid in pmids:
        result = qs.efetch({"db": "pubmed", "id": pmid})
        pma: PubMedArticle = PubMedArticle(result)
        df_dict: dict[str, str | int] = {"PMID": pmid, "DOI": pma.doi}
        df_dicts.append(df_dict)
    return pd.DataFrame(df_dicts)


def generate_ris_file(
    out_filepath: Path,
    to_ris: pd.DataFrame,
    write_type: Literal["a", "w"] = "w",
) -> None:
    """
    Generate an RIS file from a DataFrame of DOIs.

    Parameters
    ----------
    out_filepath : Path
        Path to the output RIS file
    to_ris : pd.DataFrame
        DataFrame containing DOIs to be written to the RIS file
    write_type : Literal["a", "w"], optional
        Write type, either 'a' for append or 'w' for write (default is 'w')
    """
    with open(out_filepath, write_type) as file:
        for i, (_, row) in enumerate(to_ris.iterrows()):
            file.write("TY  - JOUR\n")
            file.write(f"DO  - {row['DOI']}\n")
            file.write("ER  -\n")
            if i < (len(to_ris) - 1):
                file.write("\n")


def main():
    """
    Main function to run the script as a CLI.
    """
    parser = argparse.ArgumentParser(
        description="Generate RIS file from Excel file containing PMIDs"
    )
    parser.add_argument(
        "excel_file", type=Path, help="Path to the Excel file containing PMIDs"
    )
    parser.add_argument(
        "ris_file", type=Path, help="Path to the output RIS file"
    )

    args = parser.parse_args()

    pmids = parse_excel_file(args.excel_file)
    dois = get_dois(pmids)
    generate_ris_file(args.ris_file, dois)


if __name__ == "__main__":
    main()
