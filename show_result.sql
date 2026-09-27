USE DATABASE lab1;
USE SCHEMA RAW;
SELECT * FROM RAW.weather_data_lab1;

select * from analytics.temperature_range;
SELECT * FROM ANALYTICS.snapshot_temperature_range;

select * from analytics.temperature_moving_average;
select * from analytics.snapshot_temperature_moving_average;

select * from analytics.rolling_rainfall;
select * from analytics.snapshot_rolling_rainfall;

select * from analytics.dry_spell;
select * from analytics.snapshot_dry_spell;

select * from analytics.temperature_anomaly;
select * from analytics.snapshot_temperature_anomaly;

select * from analytics.rainy_days;
select * from analytics.snapshot_rainy_days;

select * from analytics.hot_days;
select * from analytics.snapshot_hot_days;

select * from analytics.sunshine_trend;
select * from analytics.snapshot_sunshine_trend;

select * from analytics.wind_trend;
select * from analytics.snapshot_wind_trend;