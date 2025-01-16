import asyncio
import aiohttp
import pandas as pd
import logging
import time
from pathlib import Path
import aiofiles
from typing import List, Dict
import os
import ssl

# Set up logging
epoch = int(time.time())
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(f"download_{epoch}.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


async def download_pdf(
    session: aiohttp.ClientSession, pmid: int, url: str, backup_url: str
) -> Dict:
    """Attempt to download PDF from primary and backup URLs"""
    filename = f"{pmid}.pdf"
    filepath = Path("pdfs") / filename

    # Create pdfs directory if it doesn't exist
    os.makedirs("pdfs", exist_ok=True)

    try:
        # Try primary URL first
        if url and pd.notna(url):
            try:
                async with session.get(url) as response:
                    if response.status == 200:
                        async with aiofiles.open(filepath, "wb") as f:
                            await f.write(await response.read())
                        logger.info(
                            f"Successfully downloaded {pmid} from primary URL"
                        )
                        return {
                            "PMID": pmid,
                            "Status": "success",
                            "Filepath": str(filepath),
                        }
            except Exception as e:
                logger.warning(f"Primary URL failed for {pmid}: {str(e)}")

        # Try backup URL if primary failed
        if backup_url and pd.notna(backup_url):
            try:
                async with session.get(backup_url) as response:
                    if response.status == 200:
                        async with aiofiles.open(filepath, "wb") as f:
                            await f.write(await response.read())
                        logger.info(
                            f"Successfully downloaded {pmid} from backup URL"
                        )
                        return {
                            "PMID": pmid,
                            "Status": "success",
                            "Filepath": str(filepath),
                        }
            except Exception as e:
                logger.warning(f"Backup URL failed for {pmid}: {str(e)}")

        # Both URLs failed
        logger.error(f"Download failed for {pmid}")
        return {"PMID": pmid, "Status": "failed", "Filepath": ""}

    except Exception as e:
        logger.error(f"Unexpected error for {pmid}: {str(e)}")
        return {"PMID": pmid, "Status": "failed", "Filepath": ""}


async def process_chunk(
    session: aiohttp.ClientSession,
    chunk_data: pd.DataFrame,
    chunk_num: int,
    total_chunks: int,
) -> List[Dict]:
    """Process a chunk of PMIDs"""
    logger.info(f"Processing chunk {chunk_num}/{total_chunks}")
    tasks = []
    for _, row in chunk_data.iterrows():
        task = download_pdf(
            session, row["PMID"], row["URL"], row["Backup URL"]
        )
        tasks.append(task)

    results = await asyncio.gather(*tasks, return_exceptions=True)
    return [r for r in results if not isinstance(r, Exception)]


async def main():
    # Read the CSV file
    df = pd.read_csv("missing_pmids_urls_with_cert_10s_timeout.csv")

    # Calculate chunks
    chunk_size = 10
    chunks = [df[i : i + chunk_size] for i in range(0, len(df), chunk_size)]
    total_chunks = len(chunks)

    results = []
    timeout = aiohttp.ClientTimeout(total=30)  # 30 second timeout
    ca_file: Path = Path(
        r".venv/lib/python3.11/site-packages/certifi/cacert.pem"
    )
    assert ca_file.exists(), "CA file not found"
    ssl_context = ssl.create_default_context(cafile=str(ca_file.absolute()))
    connector = aiohttp.TCPConnector(ssl=ssl_context)
    async with aiohttp.ClientSession(
        connector=connector, timeout=timeout
    ) as session:
        for i, chunk in enumerate(chunks, 1):
            chunk_results = await process_chunk(
                session, chunk, i, total_chunks
            )
            results.extend(chunk_results)

            if i < total_chunks:  # Don't sleep after the last chunk
                logger.info("Sleeping between chunks...")
                await asyncio.sleep(1)

    # Create results DataFrame and save to CSV
    results_df = pd.DataFrame(results)
    output_file = f"download_results_{epoch}.csv"
    results_df.to_csv(output_file, index=False)
    logger.info(f"Results saved to {output_file}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.error(f"Process failed: {e}")
    finally:
        logger.info("Process completed")
