SELECT
    latitude,
    longitude,
    date,
    data_type,
    wind_speed_mean,
    wind_speed_max,
    wind_gusts_max,

    AVG(wind_speed_mean) OVER (
        PARTITION BY latitude, longitude
        ORDER BY date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS wind_speed_7day_avg

FROM {{ ref('weather_data_lab1') }}
WHERE latitude IS NOT NULL
  AND longitude IS NOT NULL
  AND date IS NOT NULL