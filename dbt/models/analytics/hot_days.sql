SELECT
    latitude,
    longitude,
    date,
    temp_max,
    temp_mean,

    CASE
        WHEN temp_max >= 25 THEN TRUE
        ELSE FALSE
    END AS is_hot_day

FROM {{ ref('weather_data_lab1') }}
WHERE latitude IS NOT NULL
  AND longitude IS NOT NULL
  AND date IS NOT NULL