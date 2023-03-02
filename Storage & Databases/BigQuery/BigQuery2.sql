CREATE VIEW `bikeshare.busiest_stations_by_hour` AS
WITH busy_stations AS
(
SELECT 
  trip_date, 
  trip_start_hour, 
  start_station_name, 
  trip_count AS max_trips, 
  RANK()  OVER (PARTITION BY trip_date, trip_start_hour ORDER BY trip_count) AS busy_rank 
FROM  `viraj-patil-bootcamp.bikeshare.hourly_summary_trips` 
ORDER BY trip_date, trip_start_hour
)
SELECT 
  bs.trip_date, 
  bs.trip_start_hour, 
  bs.start_station_name, 
  bs.max_trips 
FROM busy_stations as bs 
WHERE bs.busy_rank = 1 
ORDER BY bs.trip_date,bs.trip_start_hour


-- SELECT trip_date, start_station_name, trip_start_hour, trip_count FROM `viraj-patil-bootcamp.bikeshare.hourly_summary_trips` ORDER BY trip_date, start_station_name,trip_start_hour;