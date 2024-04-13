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
    gross_income_usd,
    profit_usd,
    avg_vote,
    vote_count,
    budget_size,
    is_profit
FROM {{ ref('int__dim_movies_exploded_genre') }}
