/*Facility AMC*/
DROP FUNCTION IF EXISTS fn_get_facility_amc;
DELIMITER //
CREATE FUNCTION fn_get_facility_amc(pm_drug_id integer, pm_drug_mos integer, pm_period_date date, pm_facility_id integer) RETURNS INT(10)
    DETERMINISTIC
BEGIN
    DECLARE amc INT(10);

    SELECT (SUM(total)/pm_drug_mos) INTO amc 
    FROM tbl_consumption
    WHERE STR_TO_DATE(CONCAT(CONCAT_WS('-', period_year, period_month), '-01'), '%Y-%b-%d') >= DATE_SUB(pm_period_date, INTERVAL pm_drug_mos MONTH)
    AND STR_TO_DATE(CONCAT(CONCAT_WS('-', period_year, period_month), '-01'), '%Y-%b-%d') <= pm_period_date
    AND drug_id = pm_drug_id
    AND facility_id = pm_facility_id;

    RETURN (amc);
END//
DELIMITER ;