{{-
    config(
        materialized='table'
    )
-}}


SELECT
    * EXCEPT (genre_array)
FROM {{ ref('int__dim_movies_usd') }}
CROSS JOIN UNNEST(genre_array) AS genre
