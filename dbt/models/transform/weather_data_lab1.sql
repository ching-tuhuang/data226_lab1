SELECT *
FROM {{ source('raw', 'weather_data_lab1') }}
WHERE latitude IS NOT NULL
  AND longitude IS NOT NULL
  AND date IS NOT NULL