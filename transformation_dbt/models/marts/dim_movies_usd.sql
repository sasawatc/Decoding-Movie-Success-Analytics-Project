{{-
    config(
        materialized='view'
    )
-}}


SELECT
    imdb_title_id,
    title,
    year,
    genres,
    genre_1,
    genre_2,
    genre_3,
    duration,
    language,
    director,
    production_company_consolidated,
    budget_usd,
    gross_income_usd,
    profit_usd
FROM {{ ref('int__dim_movies_usd') }}
