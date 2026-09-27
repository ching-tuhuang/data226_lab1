SELECT
    latitude,
    longitude,
    date,
    temp_mean,
    temp_max,
    temp_min,
    temp_max - temp_min AS temperature_range
FROM {{ ref('weather_data_lab1') }}
WHERE latitude IS NOT NULL
  AND longitude IS NOT NULL
  AND date IS NOT NULL