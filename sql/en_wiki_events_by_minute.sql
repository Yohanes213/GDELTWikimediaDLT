-- En Wiki events by minute
SELECT
  date_trunc('minute', to_timestamp(timestamp)) AS minute_ts,
  COUNT(*)                AS cnt
FROM gdeltcatalog.gdeltdlt.wikimedia_silver
WHERE wiki = 'enwiki'
GROUP BY 1
ORDER BY 1;