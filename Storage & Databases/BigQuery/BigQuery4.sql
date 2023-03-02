-- Write an SQL & create View: Find out the number of titles in each Country Availability grouped by Runtime

CREATE VIEW netflix.view_titles_by_country_runtime AS
SELECT
  CountryAvailability,
  Runtime,
  COUNT(Title) AS Titles_Count
FROM
  `netflix.netflix-raw-data`, UNNEST(SPLIT(Country_Availability, ',')) AS CountryAvailability 
GROUP BY
  CountryAvailability, Runtime
ORDER BY 
  CountryAvailability, Runtime;

-- Write an SQL & create View : Find out Number of Titles against each actor. Should cover all actors available in data.

CREATE VIEW netflix.view_titles_by_actor AS
WITH actors AS (
  SELECT Actor, Title
  FROM `netflix.netflix-raw-data`, UNNEST(SPLIT(Actors, ',')) AS Actor
)
SELECT Actor, COUNT(Title) AS title_count
FROM actors
GROUP BY Actor;

-- Write an SQL & create View : Find out the number of Titles for each Genre. Should cover all genres available in data.
CREATE VIEW netflix.view_titles_by_genre AS
WITH genres AS (
  SELECT Genre, Title
  FROM `netflix.netflix-raw-data`, UNNEST(SPLIT(Genre, ',')) AS Genre
)
SELECT Genre, COUNT(Title) AS title_count
FROM genres
GROUP BY Genre;

-- Write an SQL & create View : Find out the number of Titles available in each Country Availability by Genre.
CREATE VIEW netflix.view_titles_by_country_gener AS 
WITH genres AS (
  SELECT Genre, Title, Country_Availability
  FROM `netflix.netflix-raw-data`, UNNEST(SPLIT(Genre, ', ')) AS Genre, UNNEST(SPLIT(Country_Availability, ',')) AS Country_Availability
)
SELECT Country_Availability, Genre, COUNT(Title) AS title_count
FROM genres
GROUP BY Country_Availability,Genre
ORDER BY Country_Availability,Genre;

-- Write an SQL & create View : Find out top 3 Box Office grossers for each year: Release Year, Title, Box Office, Actors, Genre
CREATE VIEW netflix.top_3_grossers_by_year AS
SELECT Release_Date, Title, Boxoffice, Actors, Genre
FROM (
  SELECT Release_Date, Title, Boxoffice, Actors, Genre,
         ROW_NUMBER() OVER (PARTITION BY Release_Date ORDER BY Boxoffice DESC) AS rank
  FROM `netflix.netflix-raw-data`
)
WHERE rank <= 3
ORDER BY Release_Date DESC, Boxoffice DESC