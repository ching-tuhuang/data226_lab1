SELECT
    latitude,
    longitude,
    date,
    COUNT(*) AS record_count
FROM {{ ref('weather_data_lab1') }}
GROUP BY
    latitude,
    longitude,
    date
HAVING COUNT(*) > 1