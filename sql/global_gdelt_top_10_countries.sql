--  Top countries by number of events
SELECT
  Action_Geo_Country_Code AS country,
  COUNT(*)            AS events_count
FROM gdeltcatalog.gdeltdlt.gdelt_silver
WHERE Action_Geo_Country_Code != "Unknown"
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10;