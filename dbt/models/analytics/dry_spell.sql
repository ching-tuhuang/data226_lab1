WITH base AS (
    SELECT
        latitude,
        longitude,
        date,
        precipitation,

        CASE
            WHEN precipitation IS NOT NULL
                 AND precipitation < 1
            THEN 1
            ELSE 0
        END AS is_dry

    FROM {{ ref('rolling_rainfall') }}
),

spell_groups AS (
    SELECT
        *,
        SUM(
            CASE
                WHEN is_dry = 0 THEN 1
                ELSE 0
            END
        ) OVER (
            PARTITION BY latitude, longitude
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS spell_group

    FROM base
)

SELECT
    latitude,
    longitude,
    date,
    precipitation,
    is_dry,

    SUM(is_dry) OVER (
        PARTITION BY latitude, longitude, spell_group
        ORDER BY date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS dry_spell_length

FROM spell_groups