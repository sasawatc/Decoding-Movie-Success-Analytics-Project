import humanfriendly
import pathlib
import logging
import time
import sys
from google.cloud import bigquery


logging.basicConfig(level=logging.INFO)

PROJECT_ID = "school-and-small-projects-1412"
SCHEMA_DIR = "data-source-ingester/table_schemas"

client = bigquery.Client()

tables_imdb_noncomm = [
    "name_basics",
    "title_akas",
    "title_basics",
    "title_crew",
    "title_episode",
    # "title_principles",
    "title_ratings",
]
# tables = ["test"]


def create_table_job_config_imdb_noncomm(schema_json_path: str):

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

def create_table_job_config_kaggle_imdb_movie_and_crew_data(schema_json_path: str):

    schema = client.schema_from_json(schema_json_path)

    return bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=False,  # don't allow infering schema
        field_delimiter=",",  # comma-separated
        allow_jagged_rows=False,  # don't allow missing columns
        ignore_unknown_values=False,  # don't allow missing columns
        null_marker="",
        quote_character='"',
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

    table_id = f"{project}.{dataset}.{table}"

    logging.info(f"Loading {file_path} to {table_id}")

    with open(file_path, "rb") as source_file:
        job = client.load_table_from_file(
            source_file,
            destination=f"{table_id}",
            job_config=job_config,
            project=project,
        )

    job.result()
    logging.info(
        f"Finished loading {table_id} in {humanfriendly.format_timespan(time.time() - start)}"
    )


if __name__ == "__main__":
    confirm_prompt = input(
        f"You are about to overwrite the following tables: {', '.join(tables_imdb_noncomm)}. Are you sure? (Y/n): "
    )

    if confirm_prompt.lower() in ("y", ""):
        logging.info(f"Loading {len(tables_imdb_noncomm)} files...")
    else:
        logging.info(f"Okay, will not load anything.")
        sys.exit()

    # IMDB Noncommercial Datasets
    DOWNLOAD_DIR_IMDB_NONCOMM = "data-source-ingester/downloads/imdb_noncommercial_datasets"
    for table in tables_imdb_noncomm:
        source_path = pathlib.Path(DOWNLOAD_DIR_IMDB_NONCOMM) / f"{table}.tsv.gz"
        schema_path = pathlib.Path(SCHEMA_DIR) / f"{table}.json"

        load_to_bq(
            table,
            job_config=create_table_job_config_imdb_noncomm(schema_path),
            file_path=source_path,
            dataset="wju_imdb",
            project=PROJECT_ID,
        )
    
    # kaggle — The Devastator — IMDB Movie and Crew Data
    source_path = "data-source-ingester/downloads/manual_downloads/kaggle-the_devastator-imdb_movie_and_crew_data.csv"
    schema_path = "data-source-ingester/table_schemas/kaggle-the_devastator-imdb_movie_and_crew_data.json"

    load_to_bq(
        "the_devastator_imdb_movie_and_crew",
        job_config=create_table_job_config_kaggle_imdb_movie_and_crew_data(schema_path),
        file_path=source_path,
        dataset="wju_kaggle",
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
