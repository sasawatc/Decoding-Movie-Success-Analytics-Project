import asyncio
import httpx
import os
from tqdm import tqdm


DOWNLOAD_DIR = "data-source-ingester/downloads"


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


async def main():
    """
    Downloading three large files simultaneously.
    Each file has its own progress bar.
    """
    loop = asyncio.get_running_loop()
    urls = [
        ("https://datasets.imdbws.com/name.basics.tsv.gz", "name.basics.tsv.gz"),
        ("https://datasets.imdbws.com/title.akas.tsv.gz", "title.akas.tsv.gz"),
        ("https://datasets.imdbws.com/title.basics.tsv.gz", "title.basics.tsv.gz"),
        ("https://datasets.imdbws.com/title.crew.tsv.gz", "title.crew.tsv.gz"),
        ("https://datasets.imdbws.com/title.crew.tsv.gz", "title.episode.tsv.gz"),
        (
            "https://datasets.imdbws.com/title.principals.tsv.gz",
            "title.principals.tsv.gz",
        ),
        ("https://datasets.imdbws.com/title.ratings.tsv.gz", "title.ratings.tsv.gz"),
    ]
    tasks = [loop.create_task(download(url, name, DOWNLOAD_DIR)) for url, name in urls]
    await asyncio.gather(*tasks, return_exceptions=True)


asyncio.run(main())
