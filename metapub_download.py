import asyncio
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from functools import partial
import logging
import time
import concurrent.futures

import pandas as pd

from dotenv import load_dotenv

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

load_dotenv()
from metapub import FindIt  # type: ignore

# Add rate limiting constants
REQUESTS_PER_SECOND = (
    30  # E-utilities allows 3 requests per second without API key
)
DELAY = 1.0 / REQUESTS_PER_SECOND
_last_request_time = 0


def find_with_timeout(pmid: int, timeout: int = 10) -> FindIt:
    # Add rate limiting
    global _last_request_time
    current_time = time.time()
    time_since_last_request = current_time - _last_request_time
    if time_since_last_request < DELAY:
        logger.debug(
            f"Rate limiting: sleeping for {DELAY - time_since_last_request:.2f} seconds"
        )
        time.sleep(DELAY - time_since_last_request)
    _last_request_time = time.time()

    logger.debug(f"Starting FindIt for PMID {pmid}")
    with ThreadPoolExecutor(max_workers=1) as executor:  # Limit to 1 worker
        future = executor.submit(FindIt, pmid)
        try:
            result = future.result(timeout=timeout)
            logger.debug(f"Successfully completed FindIt for PMID {pmid}")
            return result
        except (TimeoutError, concurrent.futures.TimeoutError) as e:
            logger.error(f"Timeout in find_with_timeout for PMID {pmid}")
            # Cancel the future if possible
            future.cancel()
            # Instead of raising, return None to indicate timeout
            return None
        except Exception as e:
            logger.error(
                f"Unexpected error in find_with_timeout for PMID {pmid}: {str(e)}"
            )
            future.cancel()
            return None


async def get_urls(pmid: int) -> dict[str, int | str | None]:
    try:
        logger.info(f"Starting get_urls for PMID: {pmid}")
        article = await asyncio.wait_for(
            asyncio.get_event_loop().run_in_executor(
                None, partial(find_with_timeout, pmid)
            ),
            timeout=2,  # Add another timeout layer
        )

        if article is None:
            return {
                "PMID": pmid,
                "URL": None,
                "Backup URL": None,
                "Reason": "Timeout or error in FindIt",
                "Title": None,
                "DOI": None,
                "Authors": None,
                "Journal": None,
            }

        logger.debug(f"Successfully got article for PMID {pmid}")

        url = article.url
        logger.debug(f"Found URL for PMID {pmid}: {url}")

        # Add more detailed logging for each step
        try:
            backup_url = article.backup_url
            logger.debug(f"Got backup URL for PMID {pmid}: {backup_url}")
        except AttributeError:
            logger.debug(f"No backup URL available for PMID {pmid}")
            backup_url = ""

        if article and article.pma:
            logger.debug(f"Processing PMA data for PMID {pmid}")
            doi: str = article.pma.doi or ""
            title: str = article.pma.title or ""
            authors_str: str = article.pma.authors_str or ""
            journal: str = article.pma.journal or ""
            reason: str = article.reason or ""
        else:
            doi = ""
            title = ""
            authors_str = ""
            journal = ""
            if article.reason:
                reason = article.reason
            else:
                reason = "article.pma.reason was None"
        return {
            "PMID": pmid,
            "URL": url,
            "Backup URL": backup_url,
            "Reason": reason,
            "Title": title,
            "DOI": doi,
            "Authors": authors_str,
            "Journal": journal,
        }
    except TimeoutError as e:
        logger.error(f"Timeout error for PMID {pmid}: {e}")
        return {
            "PMID": pmid,
            "URL": None,
            "Backup URL": None,
            "Reason": f"Timeout: {str(e)}",
            "Title": None,
            "DOI": None,
            "Authors": None,
            "Journal": None,
        }
    except Exception as e:
        logger.error(f"Unexpected error for PMID {pmid}: {e}")
        return {
            "PMID": pmid,
            "URL": None,
            "Backup URL": None,
            "Reason": f"Error: {str(e)}",
            "Title": None,
            "DOI": None,
            "Authors": None,
            "Journal": None,
        }


async def gather_urls(pmids: set[int]) -> pd.DataFrame:
    chunk_size = 10  # Reduce chunk size if needed
    all_results = []
    total_pmids = len(pmids)

    logger.info(
        f"Starting to process {total_pmids} PMIDs in chunks of {chunk_size}"
    )

    for i in range(0, total_pmids, chunk_size):
        chunk = list(pmids)[i : i + chunk_size]
        chunk_num = i // chunk_size + 1
        total_chunks = -(-total_pmids // chunk_size)

        logger.info(
            f"Starting chunk {chunk_num}/{total_chunks} with PMIDs: {chunk}"
        )

        tasks = [get_urls(pmid) for pmid in chunk]
        try:
            logger.debug(f"Awaiting gather for chunk {chunk_num}")
            # Add timeout for the entire chunk
            chunk_results = await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True),
                timeout=30,  # Timeout for entire chunk
            )
            logger.debug(f"Gather completed for chunk {chunk_num}")

            # Filter out exceptions and log them
            processed_results = []
            for idx, result in enumerate(chunk_results):
                if isinstance(result, Exception):
                    pmid = chunk[idx]  # Get the PMID that caused the error
                    logger.error(
                        f"Failed to process PMID {pmid} in chunk {chunk_num}: {result}"
                    )
                else:
                    processed_results.append(result)

            logger.info(
                f"Successfully processed {len(processed_results)}/{len(chunk)} PMIDs in chunk {chunk_num}"
            )
            all_results.extend(processed_results)
        except asyncio.TimeoutError:
            logger.error(
                f"Entire chunk {chunk_num} timed out, moving to next chunk"
            )
            continue
        except Exception as e:
            logger.error(f"Fatal error processing chunk {chunk_num}: {e}")
            continue

        logger.info(
            f"Completed chunk {chunk_num}/{total_chunks}, sleeping for 1 second"
        )
        await asyncio.sleep(1)

    logger.info(f"Finished processing all {total_pmids} PMIDs")
    return pd.DataFrame(all_results)


async def main() -> None:
    try:
        logger.info("Starting main process")
        df: pd.DataFrame = pd.read_excel(
            "pmid_compare_total_pmid_articles_osm-pdf-uploads_pdfs.xlsx",
            sheet_name="Missing",
        )
        pmids: set[int] = set(df["Missing PMIDs"].astype(int))
        logger.info(f"Loaded {len(pmids)} PMIDs from Excel file")

        urls_df = await gather_urls(pmids)
        logger.info("Saving results to CSV file")
        urls_df.to_csv("missing_pmids_urls.csv", index=False)
        logger.info("Process completed successfully")
    except Exception as e:
        logger.error(f"Fatal error in main: {e}")
        raise
    finally:
        # Clean up any remaining tasks
        tasks = [
            t for t in asyncio.all_tasks() if t is not asyncio.current_task()
        ]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

        # Ensure the event loop is closed
        loop = asyncio.get_event_loop()
        loop.stop()
        loop.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.error(f"Process failed: {e}")
    finally:
        # Force exit if still hanging
        import sys

        sys.exit(0)
