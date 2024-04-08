import humanfriendly
import pathlib
import logging
import time
import sys
from google.cloud import bigquery


logging.basicConfig(level=logging.INFO)

PROJECT_ID = "school-and-small-projects-1412"
DATASET_NAME = "wju_imdb"

DOWNLOAD_DIR = "data-source-ingester/downloads"
SCHEMA_DIR = "data-source-ingester/table_schemas"

client = bigquery.Client()

tables = [
    "name_basics",
    "title_akas",
    "title_basics",
    "title_crew",
    "title_episode",
    # "title_principles",
    "title_ratings",
]
# tables = ["test"]


def create_table_job_config(schema_json_path: str):

    schema = client.schema_from_json(schema_json_path)

    return bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=False,  # don't allow infering schema
        field_delimiter="\t",  # tab-separated
        allow_jagged_rows=False,  # don't allow missing columns
        ignore_unknown_values=False,  # don't allow missing columns
        null_marker="\\N",
        quote_character='',  # allow double quotes in data as a character
        schema=schema,
        write_disposition="WRITE_TRUNCATE",  # overwrites, if table already exists
    )


def load_to_bq(
    table: str,
    job_config: bigquery.LoadJobConfig,
    file_path: str,
    dataset: str,
    project: str,
):
    start = time.time()

    table_id = f"{PROJECT_ID}.{DATASET_NAME}.{table}"

    logging.info(f"Loading {file_path} to {table_id}")

    with open(file_path, "rb") as source_file:
        job = client.load_table_from_file(
            source_file,
            destination=f"{table_id}",
            job_config=job_config,
            project=PROJECT_ID,
        )

    job.result()
    logging.info(
        f"Finished loading {table_id} in {humanfriendly.format_timespan(time.time() - start)}"
    )


if __name__ == "__main__":
    confirm_prompt = input(
        f"You are about to overwrite the following tables: {', '.join(tables)}. Are you sure? (Y/n): "
    )

    if confirm_prompt.lower() in ("y", ""):
        logging.info(f"Loading {len(tables)} files...")
    else:
        logging.info(f"Okay, will not load anything.")
        sys.exit()

    for table in tables:
        source_path = pathlib.Path(DOWNLOAD_DIR) / f"{table}.tsv.gz"
        schema_path = pathlib.Path(SCHEMA_DIR) / f"{table}.json"

        load_to_bq(
            table,
            job_config=create_table_job_config(schema_path),
            file_path=source_path,
            dataset=DATASET_NAME,
            project=PROJECT_ID,
        )


# title_akas
# partition: region
# cluster: titleId

# title_basics
# partition: titleType
# cluster: startYear, tconst

# title_crew
# nothing

# title_episode
# partition: parentTconst
# cluster: seasonNumber, tconst

# title_principals
# DO NOT LOAD!!!
# partition: tconst
# cluster: nconst

# title_ratings
# nothing

# name.basics
# partition: birthYear
# cluster: nconst
