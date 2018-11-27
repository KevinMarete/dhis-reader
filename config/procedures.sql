/*Save cdrr*/
DROP PROCEDURE IF EXISTS proc_save_cdrr;
DELIMITER //
CREATE PROCEDURE proc_save_cdrr(
	IN ordcode VARCHAR(15)
	)
BEGIN
	IF (ordcode = 'F-CDRR') THEN
		/*Update dhiscode to facility_id*/
		UPDATE tbl_order o INNER JOIN tbl_facility f ON f.dhiscode = o.facility SET o.facility = f.id;

		/*Format period to period_begin date type*/
		UPDATE tbl_order o SET o.period = STR_TO_DATE(CONCAT_WS('-', o.period,'01'),'%Y%m-%e');

		/*Delete orders [allocated | approved | reviewed]*/
		DELETE FROM tbl_order WHERE (facility, period) IN (SELECT c.facility_id, c.period_begin FROM tbl_cdrr c WHERE c.code = ordcode AND c.status IN ('allocated', 'approved', 'reviewed'));

		/*Add qty_allocated figures*/
		REPLACE INTO tbl_order(facility, period, dimension, category, value) SELECT c.facility_id, c.period_begin, ci.drug_id, 'qty_allocated', ci.qty_allocated FROM tbl_cdrr_item ci INNER JOIN tbl_cdrr c ON c.id = ci.cdrr_id WHERE c.code = ordcode AND c.status IN ('prepared', 'rejected') AND ci.qty_allocated IS NOT NULL;

		/*Add qty_allocated_mos figures*/
		REPLACE INTO tbl_order(facility, period, dimension, category, value) SELECT c.facility_id, c.period_begin, ci.drug_id, 'qty_allocated_mos', ci.qty_allocated_mos FROM tbl_cdrr_item ci INNER JOIN tbl_cdrr c ON c.id = ci.cdrr_id WHERE c.code = ordcode AND c.status IN ('prepared', 'rejected') AND ci.qty_allocated_mos IS NOT NULL;

		/*Upsert cdrr from tbl_order*/
		REPLACE INTO tbl_cdrr(status, created, updated, code, period_begin, period_end, non_arv, facility_id) SELECT 'pending' status, NOW() created, NOW() updated, ordcode code, o.period period_begin, LAST_DAY(o.period) period_end, 0 non_arv, o.facility facility_id FROM tbl_order o INNER JOIN tbl_facility f ON f.id = o.facility GROUP BY o.facility, o.period;

		/*Update report_id to cdrr_id*/
		UPDATE tbl_order o INNER JOIN tbl_cdrr c ON c.facility_id = o.facility AND o.period = c.period_begin AND c.code = ordcode SET o.report_id = c.id;

		/*Upsert cdrr_log based on cdrr*/
		REPLACE INTO tbl_cdrr_log(description, created, user_id, cdrr_id) SELECT 'pending' status, NOW() created, '1' user_id, o.report_id cdrr_id FROM tbl_order o INNER JOIN tbl_facility f ON f.id = o.facility GROUP BY o.facility, o.period;

		/*Update dimension to drug_id*/
		UPDATE tbl_order o INNER JOIN tbl_dhis_elements de ON de.dhis_code = o.dimension SET o.dimension = de.target_id;
	ELSE
		/*Drop unique index*/
		ALTER TABLE tbl_order DROP INDEX facility_period_dimension_category;

		/*Update dhiscode to facility_id(for dispensed_packs and count)*/
		UPDATE tbl_order o INNER JOIN tbl_facility f ON f.dhiscode = o.facility SET o.facility = f.parent_id WHERE o.category IN ('dispensed_packs', 'count') AND dimension IN (SELECT dhis_code FROM tbl_dhis_elements WHERE target_report = 'unknown');

		/*Update dhiscode to facility_id(for all)*/
		UPDATE tbl_order o INNER JOIN tbl_facility f ON f.dhiscode = o.facility SET o.facility = f.id;

		/*Add aggr_consumed and aggr_on_hand data*/
		INSERT INTO tbl_order(facility, period, dimension, category, value) SELECT o.facility, o.period, o.dimension, 'aggr_consumed' category, SUM(o.value) value FROM tbl_order o WHERE category = 'dispensed_packs' AND dimension IN (SELECT dhis_code FROM tbl_dhis_elements WHERE target_report = 'unknown') GROUP BY o.facility, o.period, o.dimension;
		INSERT INTO tbl_order(facility, period, dimension, category, value) SELECT o.facility, o.period, o.dimension, 'aggr_on_hand' category, SUM(o.value) value FROM tbl_order o WHERE category = 'count' AND dimension IN (SELECT dhis_code FROM tbl_dhis_elements WHERE target_report = 'unknown') GROUP BY o.facility, o.period, o.dimension;

		/*Delete none dispensed_packs and count rows*/
		DELETE FROM tbl_order WHERE dimension IN (SELECT dhis_code FROM tbl_dhis_elements WHERE target_report = 'unknown') AND category NOT IN ('aggr_consumed', 'aggr_on_hand');

		/*Add unique index back*/
		ALTER TABLE tbl_order ADD UNIQUE facility_period_dimension_category (facility, period, dimension, category);

		/*Format period to period_begin date type*/
		UPDATE tbl_order o SET o.period = STR_TO_DATE(CONCAT_WS('-', o.period,'01'),'%Y%m-%e');

		/*Delete orders [allocated | approved | reviewed]*/
		DELETE FROM tbl_order WHERE (facility, period) IN (SELECT c.facility_id, c.period_begin FROM tbl_cdrr c WHERE c.code = ordcode AND c.status IN ('allocated', 'approved', 'reviewed'));
		
		/*Add qty_allocated figures*/
		REPLACE INTO tbl_order(facility, period, dimension, category, value) SELECT c.facility_id, c.period_begin, ci.drug_id, 'qty_allocated', ci.qty_allocated FROM tbl_cdrr_item ci INNER JOIN tbl_cdrr c ON c.id = ci.cdrr_id WHERE c.code = ordcode AND c.status IN ('prepared', 'rejected') AND ci.qty_allocated IS NOT NULL;

		/*Add qty_allocated_mos figures*/
		REPLACE INTO tbl_order(facility, period, dimension, category, value) SELECT c.facility_id, c.period_begin, ci.drug_id, 'qty_allocated_mos', ci.qty_allocated_mos FROM tbl_cdrr_item ci INNER JOIN tbl_cdrr c ON c.id = ci.cdrr_id WHERE c.code = ordcode AND c.status IN ('prepared', 'rejected') AND ci.qty_allocated_mos IS NOT NULL;

		/*Upsert cdrr from tbl_order*/
		REPLACE INTO tbl_cdrr(status, created, updated, code, period_begin, period_end, non_arv, facility_id) SELECT 'pending' status, NOW() created, NOW() updated, ordcode code, o.period period_begin, LAST_DAY(o.period) period_end, 0 non_arv, o.facility facility_id FROM tbl_order o INNER JOIN tbl_facility f ON f.id = o.facility GROUP BY o.facility, o.period;

		/*Update report_id to cdrr_id*/
		UPDATE tbl_order o INNER JOIN tbl_cdrr c ON c.facility_id = o.facility AND o.period = c.period_begin AND c.code = ordcode SET o.report_id = c.id;

		/*Upsert cdrr_log based on cdrr*/
		REPLACE INTO tbl_cdrr_log(description, created, user_id, cdrr_id) SELECT 'pending' status, NOW() created, '1' user_id, o.report_id cdrr_id FROM tbl_order o INNER JOIN tbl_facility f ON f.id = o.facility GROUP BY o.facility, o.period;

		/*Update dimension to drug_id*/
		UPDATE tbl_order o INNER JOIN tbl_dhis_elements de ON de.dhis_code = o.dimension SET o.dimension = de.target_id;
    END IF;
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
	DECLARE curs CURSOR FOR  SELECT CONCAT_WS(',', GROUP_CONCAT(o.category SEPARATOR ','), 'cdrr_id', 'drug_id'), CONCAT_WS(',', GROUP_CONCAT(o.value SEPARATOR ','), report_id, dimension) FROM tbl_order o INNER JOIN tbl_facility f ON f.id = o.facility GROUP BY o.report_id, o.dimension;
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
	IF (ordcode = 'F-MAPS') THEN
		/*Update dhiscode to facility_id*/
		UPDATE tbl_order o INNER JOIN tbl_facility f ON f.dhiscode = o.facility SET o.facility = f.id;

		/*Format period to period_begin date type*/
		UPDATE tbl_order o SET o.period = STR_TO_DATE(CONCAT_WS('-', o.period,'01'),'%Y%m-%e');

		/*Delete orders [allocated | approved | reviewed]*/
		DELETE FROM tbl_order WHERE (facility, period) IN (SELECT m.facility_id, m.period_begin FROM tbl_maps m WHERE m.code = ordcode AND m.status IN ('allocated', 'approved', 'reviewed'));
		
		/*Upsert maps from tbl_order*/
		REPLACE INTO tbl_maps(status, created, updated, code, period_begin, period_end, facility_id) SELECT 'pending' status, NOW() created, NOW() updated, ordcode code, o.period period_begin, LAST_DAY(o.period) period_end, o.facility facility_id FROM tbl_order o INNER JOIN tbl_facility f ON f.id = o.facility GROUP BY o.facility, o.period;

		/*Update report_id to maps_id*/
		UPDATE tbl_order o INNER JOIN tbl_maps m ON m.facility_id = o.facility AND o.period = m.period_begin AND m.code = ordcode SET o.report_id = m.id;

		/*Upsert maps_log based on maps*/
		REPLACE INTO tbl_maps_log(description, created, user_id, maps_id) SELECT 'pending' status, NOW() created, '1' user_id, o.report_id maps_id FROM tbl_order o INNER JOIN tbl_facility f ON f.id = o.facility GROUP BY o.facility, o.period;

		/*Update dimension to regimen_id*/
		UPDATE tbl_order o INNER JOIN tbl_dhis_elements de ON de.dhis_code = o.dimension SET o.dimension = de.target_id;
    ELSE
    	/*Drop unique index*/
		ALTER TABLE tbl_order DROP INDEX facility_period_dimension_category;

		/*Update dhiscode to facility_id(for dispensed_packs and count)*/
		UPDATE tbl_order o INNER JOIN tbl_facility f ON f.dhiscode = o.facility SET o.facility = f.parent_id;

		/*Add aggr_total data*/
		INSERT INTO tbl_order(facility, period, dimension, category, value) SELECT o.facility, o.period, o.dimension, 'aggr_total' category, SUM(o.value) value FROM tbl_order o WHERE category = 'total' GROUP BY o.facility, o.period, o.dimension;

		/*Delete none dispensed_packs and count rows*/
		DELETE FROM tbl_order WHERE category IN ('total');

		/*Add unique index back*/
		ALTER TABLE tbl_order ADD UNIQUE facility_period_dimension_category (facility, period, dimension, category);

		/*Update category from aggr_total to total*/
		UPDATE tbl_order SET category = 'total' WHERE category = 'aggr_total';

		/*Format period to period_begin date type*/
		UPDATE tbl_order o SET o.period = STR_TO_DATE(CONCAT_WS('-', o.period,'01'),'%Y%m-%e');

		/*Delete orders [allocated | approved | reviewed]*/
		DELETE FROM tbl_order WHERE (facility, period) IN (SELECT m.facility_id, m.period_begin FROM tbl_maps m WHERE m.code = ordcode AND m.status IN ('allocated', 'approved', 'reviewed'));

		/*Upsert maps from tbl_order*/
		REPLACE INTO tbl_maps(status, created, updated, code, period_begin, period_end, facility_id) SELECT 'pending' status, NOW() created, NOW() updated, ordcode code, o.period period_begin, LAST_DAY(o.period) period_end, o.facility facility_id FROM tbl_order o INNER JOIN tbl_facility f ON f.id = o.facility GROUP BY o.facility, o.period;

		/*Update report_id to maps_id*/
		UPDATE tbl_order o INNER JOIN tbl_maps m ON m.facility_id = o.facility AND o.period = m.period_begin AND m.code = ordcode SET o.report_id = m.id;

		/*Upsert maps_log based on maps*/
		REPLACE INTO tbl_maps_log(description, created, user_id, maps_id) SELECT 'pending' status, NOW() created, '1' user_id, o.report_id maps_id FROM tbl_order o INNER JOIN tbl_facility f ON f.id = o.facility GROUP BY o.facility, o.period;

		/*Update dimension to regimen_id*/
		UPDATE tbl_order o INNER JOIN tbl_dhis_elements de ON de.dhis_code = o.dimension SET o.dimension = de.target_id;

    END IF;
END//
DELIMITER ;

/*Save maps_item*/
DROP PROCEDURE IF EXISTS proc_save_maps_item;
DELIMITER //
CREATE PROCEDURE proc_save_maps_item()
BEGIN
	DECLARE bDone INT;
	DECLARE k VARCHAR(255);
	DECLARE v VARCHAR(255);

	/*Upsert maps_item based on maps and regimen_id*/
	DECLARE curs CURSOR FOR  SELECT CONCAT_WS(',', GROUP_CONCAT(o.category SEPARATOR ','), 'maps_id', 'regimen_id'), CONCAT_WS(',', GROUP_CONCAT(o.value SEPARATOR ','), report_id, dimension) FROM tbl_order o INNER JOIN tbl_facility f ON f.id = o.facility GROUP BY o.report_id, o.dimension;
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

/*Save on tbl_facility from DHIS data*/
DROP PROCEDURE IF EXISTS proc_save_facility_dhis;
DELIMITER //
CREATE PROCEDURE proc_save_facility_dhis(
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
        UPDATE tbl_facility SET dhiscode = f_dhiscode, longitude = f_longitude, latitude = f_latitude, parent_id = parent WHERE mflcode = f_code; 
    END IF;
END//
DELIMITER ;