import pandas as pd
from pathlib import Path

OG_TOTAL_INVENTORY: Path = Path(r"total_pmid_articles.csv")
OG_2019_2023_INVENTORY: Path = Path(r"All_ICs19_23_noDups_DM.csv")
ARTICLES_2024: Path = Path(r"pmids_articles_2024.csv")
TOTAL_INVENTORY: Path = Path(
    r"pmid_compare_total_pmid_articles_osm-pdf-uploads_pdfs.xlsx"
)
ALL_2019_2023_INVENTORY: Path = Path(
    r"pmid_compare_All_ICs19_23_noDups_DM_osm-pdf-uploads_pdfs.xlsx"
)
assert (
    OG_TOTAL_INVENTORY.exists()
), f"{OG_TOTAL_INVENTORY} not found, please check the path"
assert (
    OG_2019_2023_INVENTORY.exists()
), f"{OG_2019_2023_INVENTORY} not found, please check the path"
assert (
    ARTICLES_2024.exists()
), f"{ARTICLES_2024} not found, please check the path"
assert (
    TOTAL_INVENTORY.exists()
), f"{TOTAL_INVENTORY} not found, please check the path"
assert (
    ALL_2019_2023_INVENTORY.exists()
), f"{ALL_2019_2023_INVENTORY} not found, please check the path"


total_missing_df: pd.DataFrame = pd.read_excel(
    TOTAL_INVENTORY, sheet_name="Missing"
)
total_missing_pmids: set[int] = set(
    total_missing_df["Missing PMIDs"].astype(int)
)

all_2019_2023_df: pd.DataFrame = pd.read_excel(
    ALL_2019_2023_INVENTORY, sheet_name="Missing"
)
all_2019_2023_pmids: set[int] = set(
    all_2019_2023_df["Missing PMIDs"].astype(int)
)

articles_2024_df: pd.DataFrame = pd.read_csv(ARTICLES_2024)
articles_2024_pmids: set[int] = set(articles_2024_df["PMID"])
all_2019_2023_not_in_2024: set[int] = all_2019_2023_pmids - articles_2024_pmids
all_2019_2023_from_2024: set[int] = all_2019_2023_pmids.intersection(
    articles_2024_pmids
)
total_missing_not_in_2024: set[int] = total_missing_pmids - articles_2024_pmids
total_missing_from_2024: set[int] = total_missing_pmids.intersection(
    articles_2024_pmids
)
print(f"Total unique PMIDs in 2024: {len(articles_2024_pmids)}")
print(
    f"Missing from 2019-2024 IRP inventory, not in 2024: {len(total_missing_not_in_2024)}"
)
print(
    f"Missing from 2019-2024 IRP inventory, in 2024: {len(total_missing_from_2024)}"
)
print(
    f"Missing from 2019-2023 IRP inventory, not in 2024: {len(all_2019_2023_not_in_2024)}"
)
print(
    f"Missing from 2019-2023 IRP inventory, in 2024: {len(all_2019_2023_from_2024)}"
)

og_total: pd.DataFrame = pd.read_csv(OG_TOTAL_INVENTORY)
og_2019_2023: pd.DataFrame = pd.read_csv(OG_2019_2023_INVENTORY)
is_subset: bool = set(og_2019_2023["PMID"].astype(int)).issubset(
    set(og_total["PMID"].astype(int))
)
print(
    f"Is the 2019-2023 inventory a subset of the total inventory? {is_subset}"
)
