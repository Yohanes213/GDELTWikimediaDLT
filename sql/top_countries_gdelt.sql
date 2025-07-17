-- Top countries by number of events
SELECT
  Action_Geo_FullName AS country,
  COUNT(*) AS event_count
FROM gdeltcatalog.gdeltdlt.gdelt_silver
WHERE Action_Geo_FullName!="Unknown"
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10;