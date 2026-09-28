SELECT
    latitude,
    longitude,
    date,
    data_type,
    AVG(temp_mean) OVER (
        PARTITION BY latitude, longitude
        ORDER BY date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS temperature_7day_avg

FROM {{ ref('temperature_range') }}
WHERE latitude IS NOT NULL
  AND longitude IS NOT NULL
  AND date IS NOT NULL