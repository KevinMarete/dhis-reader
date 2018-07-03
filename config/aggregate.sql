
SELECT
	mi.regimen_id,
	SUM(mi.total) total
FROM tbl_maps_item mi
INNER JOIN tbl_maps m ON m.id = mi.maps_id
INNER JOIN tbl_facility f ON f.id = m.facility_id
WHERE m.code = 'F-MAPS'
AND m.period_begin = '2018-05-01' AND m.period_end = '2018-05-31'
AND f.parent_id = '6352'
GROUP BY mi.regimen_id
ORDER BY mi.regimen_id