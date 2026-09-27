SELECT
    latitude,
    longitude,
    date,
    humidity_mean,

    AVG(humidity_mean) OVER (
        PARTITION BY latitude, longitude
        ORDER BY date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS humidity_7day_avg

FROM {{ ref('weather_data_lab1') }}
WHERE latitude IS NOT NULL
  AND longitude IS NOT NULL
  AND date IS NOT NULL