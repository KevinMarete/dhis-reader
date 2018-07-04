/*Save cdrr*/
DROP PROCEDURE IF EXISTS proc_save_cdrr;
DELIMITER //
CREATE PROCEDURE proc_save_cdrr(
	IN ordcode VARCHAR(15)
	)
BEGIN
	/*Update dhiscode to facility_id*/
	UPDATE tbl_order o INNER JOIN tbl_facility f ON f.dhiscode = o.facility SET o.facility = f.id;

	/*Format period to period_begin date type*/
	UPDATE tbl_order o SET o.period = DATE_FORMAT(STR_TO_DATE(o.period, '%Y%m') , "%Y-%m-01");

	/*Delete data from tbl_order that exists on tbl_cdrr*/
	DELETE o.* FROM tbl_order o INNER JOIN tbl_cdrr c ON c.period_begin = o.period AND c.facility_id = o.facility WHERE o.period = c.period_begin AND o.facility = c.facility_id AND c.code = ordcode;

	/*Upsert cdrr from tbl_order*/
	REPLACE INTO tbl_cdrr(status, created, code, period_begin, period_end, facility_id) SELECT 'pending' status, NOW() created, ordcode code, o.period period_begin, LAST_DAY(o.period) period_end, o.facility facility_id FROM tbl_order o INNER JOIN tbl_facility f ON f.id = o.facility GROUP BY o.facility, o.period;

	/*Update report_id to cdrr_id*/
	UPDATE tbl_order o INNER JOIN tbl_cdrr c ON c.facility_id = o.facility AND o.period = c.period_begin AND c.code = ordcode SET o.report_id = c.id;

	/*Upsert cdrr_log based on cdrr*/
	REPLACE INTO tbl_cdrr_log(description, created, user_id, cdrr_id) SELECT 'pending' status, NOW() created, '1' user_id, o.report_id cdrr_id FROM tbl_order o INNER JOIN tbl_facility f ON f.id = o.facility GROUP BY o.facility, o.period;

	/*Update dimension to drug_id*/
	UPDATE tbl_order o INNER JOIN tbl_dhis_elements de ON de.dhis_code = o.dimension SET o.dimension = de.target_id;

END//
DELIMITER ;

/*Save cdrr_item*/
DROP PROCEDURE IF EXISTS proc_save_cdrr_item;
DELIMITER //
CREATE PROCEDURE proc_save_cdrr_item()
BEGIN
	DECLARE bDone INT;
	DECLARE k VARCHAR(255);
	DECLARE v VARCHAR(255);

	/*Upsert cdrr_item based on cdrr and drug_id*/
	DECLARE curs CURSOR FOR  SELECT CONCAT_WS(',', GROUP_CONCAT(DISTINCT o.category SEPARATOR ','), 'cdrr_id', 'drug_id'), CONCAT_WS(',', GROUP_CONCAT(o.value SEPARATOR ','), report_id, dimension) FROM tbl_order o INNER JOIN tbl_facility f ON f.id = o.facility GROUP BY o.report_id, o.dimension;
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET bDone = 1;

	OPEN curs;

	SET bDone = 0;
	REPEAT
		FETCH curs INTO k,v;

		SET @sqlv=CONCAT('REPLACE INTO tbl_cdrr_item (', k, ') VALUES (', v, ')');
		PREPARE stmt FROM @sqlv;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;

	UNTIL bDone END REPEAT;

	CLOSE curs;

	TRUNCATE tbl_order;

END//
DELIMITER ;

/*Save maps*/
DROP PROCEDURE IF EXISTS proc_save_maps;
DELIMITER //
CREATE PROCEDURE proc_save_maps(
	IN ordcode VARCHAR(15)
	)
BEGIN
	/*Update dhiscode to facility_id*/
	UPDATE tbl_order o INNER JOIN tbl_facility f ON f.dhiscode = o.facility SET o.facility = f.id;

	/*Format period to period_begin date type*/
	UPDATE tbl_order o SET o.period = DATE_FORMAT(STR_TO_DATE(o.period, '%Y%m') , "%Y-%m-01");

	/*Delete data from tbl_order that exists on tbl_maps*/
	DELETE o.* FROM tbl_order o INNER JOIN tbl_maps m ON m.period_begin = o.period AND m.facility_id = o.facility WHERE o.period = m.period_begin AND o.facility = m.facility_id AND m.code = ordcode;

	/*Upsert maps from tbl_order*/
	REPLACE INTO tbl_maps(status, created, code, period_begin, period_end, facility_id) SELECT 'pending' status, NOW() created, ordcode code, o.period period_begin, LAST_DAY(o.period) period_end, o.facility facility_id FROM tbl_order o INNER JOIN tbl_facility f ON f.id = o.facility GROUP BY o.facility, o.period;

	/*Update report_id to maps_id*/
	UPDATE tbl_order o INNER JOIN tbl_maps m ON m.facility_id = o.facility AND o.period = m.period_begin AND m.code = ordcode SET o.report_id = m.id;

	/*Upsert maps_log based on maps*/
	REPLACE INTO tbl_maps_log(description, created, user_id, maps_id) SELECT 'pending' status, NOW() created, '1' user_id, o.report_id maps_id FROM tbl_order o INNER JOIN tbl_facility f ON f.id = o.facility GROUP BY o.facility, o.period;

	/*Update dimension to regimen_id*/
	UPDATE tbl_order o INNER JOIN tbl_dhis_elements de ON de.dhis_code = o.dimension SET o.dimension = de.target_id;

END//
DELIMITER ;

/*save maps_item*/
DROP PROCEDURE IF EXISTS proc_save_maps_item;
DELIMITER //
CREATE PROCEDURE proc_save_maps_item()
BEGIN
	DECLARE bDone INT;
	DECLARE k VARCHAR(255);
	DECLARE v VARCHAR(255);

	/*Upsert maps_item based on maps and regimen_id*/
	DECLARE curs CURSOR FOR  SELECT CONCAT_WS(',', GROUP_CONCAT(DISTINCT o.category SEPARATOR ','), 'maps_id', 'regimen_id'), CONCAT_WS(',', GROUP_CONCAT(o.value SEPARATOR ','), report_id, dimension) FROM tbl_order o INNER JOIN tbl_facility f ON f.id = o.facility GROUP BY o.report_id, o.dimension;
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET bDone = 1;

	OPEN curs;

	SET bDone = 0;
	REPEAT
		FETCH curs INTO k,v;

		SET @sqlv=CONCAT('REPLACE INTO tbl_maps_item (', k, ') VALUES (', v, ')');
		PREPARE stmt FROM @sqlv;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;

	UNTIL bDone END REPEAT;

	CLOSE curs;

	TRUNCATE tbl_order;
END//
DELIMITER ;

/*update dhis data on tbl_facility*/
DROP PROCEDURE IF EXISTS proc_update_dhis;
DELIMITER //
CREATE PROCEDURE proc_update_dhis(
    IN f_code VARCHAR(20),
    IN f_name VARCHAR(150), 
    IN f_category VARCHAR(20),
    IN f_dhiscode VARCHAR(50),
    IN f_longitude VARCHAR(200),
    IN f_latitude VARCHAR(200),
    IN f_parent_mfl VARCHAR(20)
    )
BEGIN
    DECLARE parent INT DEFAULT NULL;
    SET f_name = LOWER(f_name);

    SELECT id INTO parent FROM tbl_facility WHERE mflcode = f_parent_mfl;

    IF NOT EXISTS(SELECT * FROM tbl_facility WHERE mflcode = f_code) THEN
        INSERT INTO tbl_facility(name, mflcode, category, dhiscode, longitude, latitude, parent_id) VALUES(f_name, f_code, f_category, f_dhiscode, f_longitude, f_latitude, parent);
    ELSE
        UPDATE tbl_facility SET category = f_category, dhiscode = f_dhiscode, longitude = f_longitude, latitude = f_latitude, parent_id = parent WHERE mflcode = f_code; 
    END IF;
END//
DELIMITER ;

/*update D-CDRR aggregate_data*/
DROP PROCEDURE IF EXISTS proc_update_central_cdrr;
DELIMITER //
CREATE PROCEDURE proc_update_central_cdrr()
BEGIN
    REPLACE INTO tbl_cdrr_item (aggr_consumed, aggr_on_hand, cdrr_id, drug_id)
	SELECT t.aggr_consumed, t.aggr_on_hand, c.id, t.drug_id
	FROM tbl_cdrr_item ci 
	INNER JOIN tbl_cdrr c ON c.id = ci.cdrr_id
	RIGHT JOIN (
		SELECT
			f.parent_id facility_id,
			c.period_begin, 
			c.period_end, 
			ci.drug_id,
			SUM(ci.dispensed_packs) aggr_consumed,
			SUM(ci.count) aggr_on_hand
		FROM tbl_cdrr_item ci
		INNER JOIN tbl_cdrr c ON c.id = ci.cdrr_id
		INNER JOIN tbl_facility f ON f.id = c.facility_id
		WHERE c.code = 'F-CDRR'
		AND (c.period_begin, c.period_end, f.parent_id) IN (
			SELECT c.period_begin, c.period_end, c.facility_id 
			FROM tbl_cdrr c
			WHERE c.code = 'D-CDRR'
			GROUP BY c.period_begin, c.period_end, c.facility_id
		)
		GROUP BY f.parent_id, c.period_begin, c.period_end, ci.drug_id
		ORDER BY f.parent_id, c.period_begin, c.period_end, ci.drug_id
	) t ON t.facility_id = c.facility_id AND t.period_begin = c.period_begin AND t.period_end = c.period_end AND c.code = 'D-CDRR'
	GROUP BY t.aggr_consumed, t.aggr_on_hand, c.id, t.drug_id
	ORDER BY c.id, t.drug_id;
END//
DELIMITER ;

/*update D-MAPS totals*/
DROP PROCEDURE IF EXISTS proc_update_central_maps;
DELIMITER //
CREATE PROCEDURE proc_update_central_maps()
BEGIN
    REPLACE INTO tbl_maps_item (total, maps_id, regimen_id)
	SELECT t.total, m.id, t.regimen_id
	FROM tbl_maps_item mi 
	INNER JOIN tbl_maps m ON m.id = mi.maps_id
	RIGHT JOIN (
		SELECT
			f.parent_id facility_id,
			m.period_begin, 
			m.period_end, 
			mi.regimen_id,
			SUM(mi.total) total
		FROM tbl_maps_item mi
		INNER JOIN tbl_maps m ON m.id = mi.maps_id
		INNER JOIN tbl_facility f ON f.id = m.facility_id
		WHERE m.code = 'F-MAPS'
		AND (m.period_begin, m.period_end, f.parent_id) IN (
			SELECT m.period_begin, m.period_end, m.facility_id 
			FROM tbl_maps m
			WHERE m.code = 'D-MAPS'
			GROUP BY m.period_begin, m.period_end, m.facility_id
		)
		GROUP BY f.parent_id, m.period_begin, m.period_end, mi.regimen_id
		ORDER BY f.parent_id, m.period_begin, m.period_end, mi.regimen_id
	) t ON t.facility_id = m.facility_id AND t.period_begin = m.period_begin AND t.period_end = m.period_end AND m.code = 'D-MAPS'
	GROUP BY t.total, m.id, t.regimen_id
	ORDER BY m.id, t.regimen_id;
END//
DELIMITER ;