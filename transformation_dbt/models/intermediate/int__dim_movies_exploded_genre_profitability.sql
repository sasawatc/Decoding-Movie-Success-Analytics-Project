{{-
    config(
        materialized='table'
    )
-}}


WITH
    usa_budget_size AS (
        SELECT
            * EXCEPT (budget),
            budget AS budget_usd,
            CASE
                WHEN budget >= 100000000 THEN 'big' -- assume 10 million USD is considered big budget film
                ELSE 'small'
            END AS budget_size
        FROM {{ ref('int__dim_movies_exploded_genre') }}
        WHERE budget_currency = '$' -- since gross income are all in USD and want to avoid the nuances of foreign exchange conversion
    ),

    final AS (
        SELECT
            *,
            -- a general rule of thumb figure that the breakeven point of a big budget movie is around 2.5 times of the production budget
            CASE
                WHEN gross_income_usd >= (budget_usd * 2.5) THEN TRUE
                ELSE FALSE
            END AS is_profit
        FROM usa_budget_size
        WHERE budget_size = 'big'
     )

SELECT * FROM final
