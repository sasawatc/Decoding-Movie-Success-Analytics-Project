import asyncio
import httpx
import logging
import os
from pathlib import Path
from tqdm import tqdm


DOWNLOAD_DIR = "data-source-ingester/downloads"

DOWNLOAD_URLS = [
        ("https://datasets.imdbws.com/name.basics.tsv.gz", "name_basics.tsv.gz"),
        ("https://datasets.imdbws.com/title.akas.tsv.gz", "title_akas.tsv.gz"),
        ("https://datasets.imdbws.com/title.basics.tsv.gz", "title_basics.tsv.gz"),
        ("https://datasets.imdbws.com/title.crew.tsv.gz", "title_crew.tsv.gz"),
        ("https://datasets.imdbws.com/title.crew.tsv.gz", "title_episode.tsv.gz"),
        (
            "https://datasets.imdbws.com/title.principals.tsv.gz",
            "title_principals.tsv.gz",
        ),
        ("https://datasets.imdbws.com/title.ratings.tsv.gz", "title_ratings.tsv.gz"),
    ]


async def download(url: str, filename: str, output_dir: str):

    full_path = os.path.join(output_dir, filename)

    with open(full_path, "wb") as f:
        async with httpx.AsyncClient() as client:
            async with client.stream("GET", url) as get_request:
                get_request.raise_for_status()
                total = int(get_request.headers.get("content-length", 0))

                # tqdm progress bar
                tqdm_params = {
                    "desc": url,
                    "total": total,
                    "miniters": 1,
                    "unit": "B",
                    "unit_scale": True,
                    "unit_divisor": 1024,
                }

                with tqdm(**tqdm_params) as pb:
                    downloaded = get_request.num_bytes_downloaded
                    async for chunk in get_request.aiter_bytes():
                        pb.update(get_request.num_bytes_downloaded - downloaded)
                        f.write(chunk)
                        downloaded = get_request.num_bytes_downloaded


async def downloader(urls: list):
    loop = asyncio.get_running_loop()
    tasks = [loop.create_task(download(url, name, DOWNLOAD_DIR)) for url, name in urls]
    await asyncio.gather(*tasks, return_exceptions=True)

logger = logging.getLogger(__name__)

confirm_prompt = input("You are about to download 2 GiB of data from IMDB. Are you sure? (Y/n): ")

if confirm_prompt.lower() in ("y", ""):
    logger.info(f"Downloading {len(DOWNLOAD_URLS)} files...")
    Path(DOWNLOAD_DIR).mkdir(parents=True, exist_ok=True)
    asyncio.run(downloader(DOWNLOAD_URLS))
else:
    logger.info(f"Okay, will not download anything.")
