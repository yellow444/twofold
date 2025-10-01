\echo 'Smoke: ST_Intersects between flights and region boundaries'
SELECT COUNT(*) AS intersected
FROM flights_geo fg
JOIN regions r ON r.id = fg.region_id
WHERE ST_Intersects(fg.location, r.boundary);

\echo 'Smoke: Yearly aggregates sample'
SELECT dataset_version_id,
       region_id,
       year,
       flights_count,
       duration_sum_min,
       duration_avg_min
FROM aggregates_year
ORDER BY dataset_version_id, region_id, year
LIMIT 10;

\echo 'Smoke: Aggregates vs normalized flights'
SELECT a.dataset_version_id,
       a.region_id,
       a.year,
       a.flights_count,
       COUNT(fn.id) AS normalized_flights
FROM aggregates_year a
LEFT JOIN flights_norm fn
  ON fn.dataset_version_id = a.dataset_version_id
 AND fn.region_id = a.region_id
 AND fn.year = a.year
GROUP BY a.dataset_version_id, a.region_id, a.year, a.flights_count
ORDER BY a.dataset_version_id, a.region_id, a.year
LIMIT 10;
