WITH temperature_baseline AS (
    SELECT
        latitude,
        longitude,
        date,
        data_type,
        temp_mean,
        temp_max,
        temp_min,

        AVG(temp_mean) OVER (
            PARTITION BY latitude, longitude
        ) AS temperature_7day_avg

    FROM {{ ref('weather_data_lab1') }}
)

SELECT
    latitude,
    longitude,
    date,
    data_type,
    temp_mean,
    temperature_7day_avg,
    temp_mean - temperature_7day_avg
        AS temperature_anomaly

FROM temperature_baseline
