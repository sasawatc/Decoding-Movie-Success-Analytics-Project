{{-
    config(
        materialized='table'
    )
-}}


SELECT
    *
FROM {{ ref('stg_kaggle__imdb_movies_metadata') }}
