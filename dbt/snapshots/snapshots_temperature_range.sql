{% snapshot snapshot_temperature_range %}

{{
    config(
        target_database='LAB1',
        target_schema='analytics',
        unique_key='weather_key',
        strategy='check',
        check_cols='all'
    )
}}

SELECT
    *,
    CONCAT(
        CAST(latitude AS VARCHAR),
        '|',
        CAST(longitude AS VARCHAR),
        '|',
        CAST(date AS VARCHAR)
    ) AS weather_key
FROM {{ ref('temperature_range') }}

{% endsnapshot %}