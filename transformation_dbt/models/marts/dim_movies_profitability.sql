{{-
    config(
        materialized='view'
    )
-}}


SELECT
    imdb_title_id,
    title,
    year,
    genre,
    duration,
    language,
    director,
    production_company_consolidated,
    budget_usd,
    gross_income_usd
FROM {{ ref('int__dim_movies_exploded_genre_profitability') }}
