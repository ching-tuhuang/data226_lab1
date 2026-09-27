SELECT
    latitude,
    longitude,
    date,
    rain,

    SUM(
        CASE
            WHEN rain > 0 THEN 1
            ELSE 0
        END
    ) OVER (
        PARTITION BY latitude, longitude
        ORDER BY date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rainy_days_7day

FROM {{ ref('rolling_rainfall') }}
WHERE latitude IS NOT NULL
  AND longitude IS NOT NULL
  AND date IS NOT NULL