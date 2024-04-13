{{-
    config(
        materialized='view'
    )
-}}


WITH
    base AS (
        SELECT
            imdb_title_id,
            title,
            original_title,
            year,
            date_published,
            genre AS genres,
            SPLIT(REPLACE(genre, ' ', ''), ',') AS genre_array,
            duration,
            country,
            language,
            director,
            writer,
            production_company,
            CASE
                WHEN LOWER(production_company) LIKE 'dreamworks%' THEN 'DreamWorks Animation'
                WHEN LOWER(production_company) LIKE 'marvel%' THEN 'Marvel Studios'
                WHEN LOWER(production_company) LIKE 'paramount%' THEN 'Paramount Pictures'
                WHEN LOWER(production_company) LIKE 'twentieth century%' OR LOWER(production_company) LIKE 'walt disney%' THEN 'Walt Disney Studios'
                WHEN LOWER(production_company) LIKE 'warner bros%' THEN 'Warner Bros. Pictures'
                WHEN LOWER(production_company) LIKE 'legendary%' THEN 'Legendary Entertainment'
                ELSE production_company
            END AS production_company_consolidated,
            actors,
            description,
            avg_vote,
            votes AS vote_count,
            SPLIT(budget, ' ')[OFFSET(0)] AS budget_currency,
            CAST(SPLIT(budget, ' ')[OFFSET(1)] AS INT64) AS budget,
            CAST(SPLIT(usa_gross_income, ' ')[OFFSET(1)] AS INT64) AS usa_gross_income_usd,
            CAST(SPLIT(worldwide_gross_income, ' ')[OFFSET(1)] AS INT64) AS worldwide_gross_income_usd,
            metascore,
            reviews_from_users,
            reviews_from_critics
        FROM {{ source('kaggle','the_devastator_imdb_movie_and_crew') }}
    ),

    final AS (
        SELECT
            *,
            COALESCE(worldwide_gross_income_usd, usa_gross_income_usd) AS gross_income_usd
        FROM base
    )

SELECT * FROM final
