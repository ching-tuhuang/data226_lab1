{% snapshot snapshot_hot_days %}

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

FROM {{ ref('hot_days') }}

{% endsnapshot %}