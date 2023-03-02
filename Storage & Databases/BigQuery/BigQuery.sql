-- # COUNTS NO. ROWS FOR EACH PARTITION FROM VERSION1, VERSION2 TABLE AND THEIR DIFFERENCE
WITH VER1 AS 
(
SELECT 
  DATE(Column5) AS dates, 
  COUNT(Column1) AS counts 
FROM `viraj-patil-bootcamp.partition_comp.version1` 
GROUP BY dates
),
VER2 AS(
SELECT 
  DATE(Column5) AS dates, 
  COUNT(Column1) AS counts 
FROM `viraj-patil-bootcamp.partition_comp.version2` 
GROUP BY dates
)
SELECT VER2.dates AS partitiond,VER1.counts AS NumberOfRowsinVersion1, VER2.counts AS NumberOfRowsinVersion2, (VER2.counts - VER1.counts) AS DifferenceOfRowsVer2_Ver1 
FROM VER1
FULL OUTER JOIN
VER2 ON VER1.dates = VER2.dates;




-- # USING INFORMATION_SCHEMA.PARTITIONS
WITH VER1 AS
(
SELECT 
  partition_id, 
  total_rows 
FROM `viraj-patil-bootcamp.partition_comp.INFORMATION_SCHEMA.PARTITIONS` 
WHERE table_name = "version1"
),
VER2 AS
(
SELECT 
  partition_id, 
  total_rows 
FROM `viraj-patil-bootcamp.partition_comp.INFORMATION_SCHEMA.PARTITIONS` 
WHERE table_name = "version2"
)
SELECT 
  VER2.partition_id AS partitioned, 
  VER1.total_rows AS NumberOfRowsinVersion1, 
  VER2.total_rows AS NumberOfRowsinVersion2, 
  (VER2.total_rows - VER1.total_rows) AS DifferenceOfRowsVer2_Ver1  FROM VER1
FULL OUTER JOIN
VER2 ON VER1.partition_id = VER2.partition_id;

