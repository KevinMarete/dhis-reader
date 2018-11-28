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

/*Save data to Dashboard Tables*/
DROP PROCEDURE IF EXISTS proc_save_dsh_tables;
DELIMITER //
CREATE PROCEDURE proc_save_dsh_tables(
    IN p_begin DATE
    )
BEGIN
    /*tbl_consumption*/
    REPLACE INTO tbl_consumption(total, allocated, period_year, period_month, facility_id, drug_id) 
    SELECT SUM(ci.dispensed_packs) dispensed, SUM(ci.qty_allocated) allocated, YEAR(c.period_begin) period_year, MONTHNAME(c.period_begin) period_month, c.facility_id, ci.drug_id 
    FROM tbl_cdrr_item ci 
    INNER JOIN tbl_cdrr c ON c.id = ci.cdrr_id 
    WHERE c.period_begin = p_begin 
    AND c.code = 'F-CDRR' 
    GROUP BY c.facility_id, ci.drug_id, c.period_begin; 

    /*tbl_patient*/
    REPLACE INTO tbl_patient (total, period_year, period_month, regimen_id, facility_id) 
    SELECT SUM(mi.total) total, YEAR(m.period_begin) period_year, MONTHNAME(m.period_begin) period_month, mi.regimen_id, m.facility_id 
    FROM tbl_maps_item mi 
    INNER JOIN tbl_maps m ON m.id = mi.maps_id 
    WHERE m.period_begin = p_begin 
    AND m.code = 'F-MAPS' 
    GROUP BY m.facility_id, mi.regimen_id, m.period_begin; 

    /*tbl_stock*/
    REPLACE INTO tbl_stock(total, period_year, period_month, facility_id, drug_id) 
    SELECT SUM(ci.count) soh, YEAR(c.period_begin) period_year, MONTHNAME(c.period_begin) period_month, c.facility_id, ci.drug_id 
    FROM tbl_cdrr_item ci 
    INNER JOIN tbl_cdrr c ON c.id = ci.cdrr_id 
    WHERE c.period_begin = p_begin 
    GROUP BY c.facility_id, ci.drug_id, c.period_begin; 

    /*tbl_kemsa*/
    REPLACE INTO tbl_kemsa(issue_total, soh_total, supplier_total, received_total, period_year, period_month, drug_id) 
    SELECT SUM(p.issues_kemsa) issues, s.supplier, SUM(p.receipts_kemsa) receipts, SUM(p.close_kemsa) close_bal, p.transaction_year, p.transaction_month, p.drug_id 
    FROM tbl_procurement p 
    INNER JOIN (
        SELECT p.drug_id, SUM(quantity) supplier 
        FROM tbl_procurement p  
        INNER JOIN tbl_procurement_item pi ON pi.procurement_id = p.id 
        WHERE STR_TO_DATE(CONCAT(CONCAT_WS('-', p.transaction_year, p.transaction_month), '-01'), '%Y-%b-%d') > p_begin 
        GROUP BY p.drug_id
    ) s ON s.drug_id = p.drug_id 
    WHERE STR_TO_DATE(CONCAT(CONCAT_WS('-', p.transaction_year, p.transaction_month), '-01'), '%Y-%b-%d') = p_begin 
    GROUP BY p.drug_id, p.transaction_year, p.transaction_month;

    /*tbl_procurement (monthly-consumption)*/
    UPDATE tbl_procurement p 
    INNER JOIN (
        SELECT SUM(ci.dispensed_packs) consumed, YEAR(c.period_begin) transaction_year, DATE_FORMAT(c.period_begin, '%b') transaction_month, ci.drug_id  
        FROM tbl_cdrr_item ci 
        INNER JOIN tbl_cdrr c ON c.id = ci.cdrr_id 
        WHERE c.period_begin = p_begin 
        AND c.code = 'F-CDRR' 
        GROUP BY ci.drug_id, c.period_begin
    ) t  ON t.drug_id = p.drug_id AND t.transaction_year = p.transaction_year AND t.transaction_month = p.transaction_month SET p.monthly_consumption = t.consumed;

    /*Change procurment_status for recieved order when date not reached*/
    UPDATE tbl_procurement_item pi 
    INNER JOIN tbl_procurement p ON p.id = pi.procurement_id 
    SET pi.procurement_status_id = '2' 
    WHERE pi.procurement_status_id = '3' 
    AND STR_TO_DATE(CONCAT(CONCAT_WS('-', p.transaction_year, p.transaction_month), '-01'), '%Y-%b-%d') > p_begin;

    /*dsh_mos*/
    REPLACE INTO dsh_mos(facility_mos, cms_mos, supplier_mos, data_year, data_month, data_date, drug)
    SELECT 
        IFNULL(ROUND(SUM(fs.total)/fn_get_national_amc(k.drug_id, STR_TO_DATE(CONCAT(CONCAT_WS('-', k.period_year, k.period_month), '-01'),'%Y-%b-%d')),1),0) AS facility_mos,
        IFNULL(ROUND(k.soh_total/fn_get_national_amc(k.drug_id, STR_TO_DATE(CONCAT(CONCAT_WS('-', k.period_year, k.period_month), '-01'),'%Y-%b-%d')),1),0) AS cms_mos,
        IFNULL(ROUND(k.supplier_total/fn_get_national_amc(k.drug_id, STR_TO_DATE(CONCAT(CONCAT_WS('-', k.period_year, k.period_month), '-01'),'%Y-%b-%d')),1),0) AS supplier_mos,
        k.period_year,
        k.period_month,
        STR_TO_DATE(CONCAT_WS('-', k.period_year, k.period_month, '01'),'%Y-%b-%d') AS data_date,
        d.name
    FROM tbl_kemsa k
    INNER JOIN tbl_stock fs ON fs.drug_id = k.drug_id AND fs.period_month = k.period_month AND fs.period_year = k.period_year
    INNER JOIN vw_drug_list d ON d.id = k.drug_id
    WHERE STR_TO_DATE(CONCAT_WS('-', k.period_year, k.period_month, '01'),'%Y-%b-%d') = p_begin
    GROUP BY d.name, k.period_month, k.period_year;

    /*dsh_consumption*/
    REPLACE INTO dsh_consumption(total, allocated, data_year, data_month, data_date, sub_county, county, facility, drug)
    SELECT 
        SUM(cf.total) AS total,
        SUM(cf.allocated) AS allocated,
        cf.period_year AS data_year,
        cf.period_month AS data_month,
        STR_TO_DATE(CONCAT_WS('-', cf.period_year, cf.period_month, '01'),'%Y-%b-%d') AS data_date,
        cs.name AS sub_county,
        c.name AS county,
        f.name AS facility,
        d.name AS drug
    FROM tbl_consumption cf 
    INNER JOIN vw_drug_list d ON cf.drug_id = d.id
    INNER JOIN tbl_facility f ON cf.facility_id = f.id
    INNER JOIN tbl_subcounty cs ON cs.id = f.subcounty_id
    INNER JOIN tbl_county c ON c.id = cs.county_id
    WHERE STR_TO_DATE(CONCAT_WS('-', cf.period_year, cf.period_month, '01'),'%Y-%b-%d') = p_begin
    GROUP by drug, facility, county, sub_county, data_month, data_year;

    /*dsh_patient*/
    REPLACE INTO dsh_patient(total, data_year, data_month, data_date, sub_county, county, facility, partner, regimen, age_category, regimen_service, regimen_line, nnrti_drug, nrti_drug, regimen_category)
    SELECT
        SUM(rp.total) AS total,
        rp.period_year AS data_year,
        rp.period_month AS data_month,
        STR_TO_DATE(CONCAT_WS('-', rp.period_year, rp.period_month, '01'),'%Y-%b-%d') AS data_date,
        cs.name AS sub_county,
        c.name AS county,
        f.name AS facility,
        p.name AS partner,
        CONCAT_WS(' | ', r.code, r.name) AS regimen,
        CASE 
            WHEN ct.name LIKE '%adult%' OR ct.name LIKE '%mother%' THEN 'adult' 
            WHEN ct.name LIKE '%paediatric%' OR ct.name  LIKE '%child%' THEN 'paed'
            ELSE NULL
        END AS age_category,
        s.name AS regimen_service,
        l.name AS regimen_line,
        nn.name AS nnrti_drug,
        n.name AS nrti_drug,
        ct.name AS regimen_category
    FROM tbl_patient rp
    INNER JOIN tbl_regimen r ON rp.regimen_id = r.id
    INNER JOIN tbl_service s ON s.id = r.service_id
    INNER JOIN tbl_line l ON l.id = r.line_id
    INNER JOIN tbl_category ct ON ct.id = r.category_id
    LEFT JOIN tbl_nrti n ON n.regimen_id = r.id
    LEFT JOIN tbl_nnrti nn ON nn.regimen_id = r.id
    INNER JOIN tbl_facility f ON rp.facility_id = f.id
    LEFT JOIN tbl_partner p ON p.id = f.partner_id
    INNER JOIN tbl_subcounty cs ON cs.id = f.subcounty_id
    INNER JOIN tbl_county c ON c.id = cs.county_id
    WHERE STR_TO_DATE(CONCAT_WS('-', rp.period_year, rp.period_month, '01'),'%Y-%b-%d') = p_begin
    GROUP by regimen_category, nrti_drug, nnrti_drug, regimen_line, regimen_service, age_category, regimen, facility, county, sub_county, data_month, data_year;

    /*dsh_stock*/
    REPLACE INTO dsh_stock(total, amc_total, data_year, data_month, data_date, sub_county, county, facility, drug)
    SELECT 
        SUM(st.total) AS total,
        fn_get_facility_amc(st.drug_id, d.amc_months, STR_TO_DATE(CONCAT(CONCAT_WS('-', st.period_year, st.period_month), '-01'),'%Y-%b-%d'), st.facility_id),
        st.period_year AS data_year,
        st.period_month AS data_month,
        STR_TO_DATE(CONCAT_WS('-', st.period_year, st.period_month, '01'),'%Y-%b-%d') AS data_date,
        cs.name AS sub_county,
        c.name AS county,
        f.name AS facility,
        d.name AS drug
    FROM tbl_stock st 
    INNER JOIN vw_drug_list d ON st.drug_id = d.id
    INNER JOIN tbl_facility f ON st.facility_id = f.id
    INNER JOIN tbl_subcounty cs ON cs.id = f.subcounty_id
    INNER JOIN tbl_county c ON c.id = cs.county_id
    WHERE STR_TO_DATE(CONCAT_WS('-', st.period_year, st.period_month, '01'),'%Y-%b-%d') = p_begin
    GROUP by drug, facility, county, sub_county, data_month, data_year;

    /*dsh_order*/
    REPLACE INTO dsh_order(data_year, data_month, data_date, sub_county, county, facility, facility_category)
    SELECT
        YEAR(c.period_begin) data_year,
        MONTHNAME(c.period_begin) data_month,
        c.period_begin data_date,
        cs.name AS sub_county,
        co.name AS county,
        f.name AS facility,
        CASE 
            WHEN c.code = 'D-CDRR' THEN 'central'
            WHEN c.code = 'F-CDRR' AND f.category = 'standalone' THEN 'standalone'
            ELSE 'satellite'
        END AS facility_category
    FROM tbl_cdrr c 
    INNER JOIN tbl_maps m ON m.facility_id = c.facility_id AND c.period_begin = m.period_begin AND SUBSTRING(c.code, 1, 1) = SUBSTRING(m.code, 1, 1)
    INNER JOIN tbl_facility f ON c.facility_id = f.id
    INNER JOIN tbl_subcounty cs ON cs.id = f.subcounty_id
    INNER JOIN tbl_county co ON co.id = cs.county_id
    WHERE c.period_begin = p_begin
    GROUP by facility_category, facility, county, sub_county, data_month, data_year;

    /*dsh_order_item*/
    REPLACE INTO dsh_order_item(opening_bal_qty, received_qty, consumed_qty, losses_qty, adjustment_qty, closing_bal_qty, data_year, data_month, data_date, sub_county, county, facility, facility_category, drug)
    SELECT
        ci.balance opening_bal_qty,
        ci.received received_qty,
        ci.dispensed_packs consumed_qty,
        ci.losses losses_qty,
        (ci.adjustments - ci.adjustments_neg) adjustment_qty,
        ci.count closing_bal_qty,
        YEAR(c.period_begin) data_year,
        MONTHNAME(c.period_begin) data_month,
        c.period_begin data_date,
        cs.name AS sub_county,
        co.name AS county,
        f.name AS facility,
        CASE 
            WHEN c.code = 'D-CDRR' THEN 'central'
            WHEN c.code = 'F-CDRR' AND f.category = 'standalone' THEN 'standalone'
            ELSE 'satellite'
        END AS facility_category,
        d.name AS drug
    FROM tbl_cdrr_item ci 
    INNER JOIN tbl_cdrr c ON c.id = ci.cdrr_id
    INNER JOIN vw_drug_list d ON ci.drug_id = d.id
    INNER JOIN tbl_maps m ON m.facility_id = c.facility_id AND c.period_begin = m.period_begin AND SUBSTRING(c.code, 1, 1) = SUBSTRING(m.code, 1, 1)
    INNER JOIN tbl_facility f ON c.facility_id = f.id
    INNER JOIN tbl_subcounty cs ON cs.id = f.subcounty_id
    INNER JOIN tbl_county co ON co.id = cs.county_id
    WHERE c.period_begin = p_begin
    GROUP by drug, facility_category, facility, county, sub_county, data_month, data_year;
END//
DELIMITER ;