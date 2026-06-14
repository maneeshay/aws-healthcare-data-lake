-- verify row count matches CMS source data
-- Expected : 1,296,739 rows
SELECT COUNT(*) FROM healthcare_bronze_db.cms_providers_bronze;