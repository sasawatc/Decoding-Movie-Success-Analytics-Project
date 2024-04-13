{{-
    config(
        materialized='table'
    )
-}}


SELECT
    * EXCEPT (genre_array)
FROM {{ ref('stg_kaggle__imdb_movies_metadata') }}
CROSS JOIN UNNEST(genre_array) AS genre
