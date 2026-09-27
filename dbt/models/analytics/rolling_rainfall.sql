SELECT
    latitude,
    longitude,
    date,
    precipitation,
    rain,

    SUM(precipitation) OVER (
        PARTITION BY latitude, longitude
        ORDER BY date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rainfall_7day_total

FROM {{ ref('weather_data_lab1') }}
WHERE latitude IS NOT NULL
  AND longitude IS NOT NULL
  AND date IS NOT NULL