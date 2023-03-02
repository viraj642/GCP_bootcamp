-- # Create view named 'game_attendance' from public dataset 'baseball.shedules' which contains columns team name, #average attendance on weekday and average attendence on weekend

CREATE VIEW baseball.games_attendance AS
SELECT 
  homeTeamName, 
  AVG(CASE WHEN EXTRACT(DAYOFWEEK FROM startTime AT TIME ZONE 'UTC') BETWEEN 2 AND 6 THEN attendance END) AS avg_weekday_attendance, 
  AVG(CASE WHEN EXTRACT(DAYOFWEEK FROM startTime AT TIME ZONE 'UTC') IN (1, 7) THEN attendance END) AS avg_weekend_attendance 
FROM `bigquery-public-data.baseball.schedules` 
WHERE status= 'closed' 
GROUP BY homeTeamName
