-- 02 — Build Silver Layer (SQL)
-- Purpose: Clean, conform, and index join-ready tables

-- Set mode to prevent strict errors on invalid dates during ETL (Optional but recommended for Bronze->Silver)
SET sql_mode = 'NO_ENGINE_SUBSTITUTION';

DROP DATABASE IF EXISTS silver;
CREATE DATABASE silver DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
USE silver;

-- ==============================================================================
-- 1. crm_cust_info 
-- Changes: Trim names; Standardize Marital/Gender; Deduplicate by ID
-- ==============================================================================
CREATE TABLE IF NOT EXISTS crm_cust_info (
  cst_id INT,
  cst_key VARCHAR(50),
  cst_firstname VARCHAR(50),
  cst_lastname VARCHAR(50),
  cst_marital_status VARCHAR(50),
  cst_gndr VARCHAR(50),
  cst_create_date DATE,
  dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP,
  INDEX ix_crm_cust_info_key (cst_key),
  INDEX ix_crm_cust_info_id (cst_id)
) ENGINE=InnoDB;

TRUNCATE TABLE silver.crm_cust_info;

INSERT INTO silver.crm_cust_info (
  cst_id, cst_key, cst_firstname, cst_lastname,
  cst_marital_status, cst_gndr, cst_create_date
)
SELECT 
    cst_id,
    cst_key,
    TRIM(cst_firstname),
    TRIM(cst_lastname),
    CASE 
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'n/a'
    END,
    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'n/a'
    END,
    cst_create_date
FROM (
   SELECT *,
          ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rn
   FROM bronze.crm_cust_info
   WHERE cst_id IS NOT NULL
) t
WHERE rn = 1;

SELECT COUNT(*) AS rows_loaded_cust FROM silver.crm_cust_info;


-- ==============================================================================
-- 2. crm_prd_info 
-- Changes: Extract Cat_ID, Decode Product Line, Calculate End Date (SCD Type 2 logic)
-- ==============================================================================
CREATE TABLE IF NOT EXISTS crm_prd_info (
  prd_id INT,
  cat_id VARCHAR(50),
  prd_key VARCHAR(50),
  prd_nm VARCHAR(50),
  prd_cost INT,
  prd_line VARCHAR(50),
  prd_start_dt DATE,
  prd_end_dt DATE,
  dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP,
  INDEX ix_crm_prd_key (prd_key),
  INDEX ix_crm_cat_id (cat_id)
) ENGINE=InnoDB;

TRUNCATE TABLE silver.crm_prd_info;

INSERT INTO silver.crm_prd_info (
  prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt
)
SELECT
  prd_id,
  REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
  SUBSTRING(prd_key, 7) AS prd_key,
  prd_nm,
  COALESCE(prd_cost, 0),
  CASE UPPER(TRIM(prd_line))
    WHEN 'M' THEN 'Mountain'
    WHEN 'R' THEN 'Road'
    WHEN 'S' THEN 'Other Sales'
    WHEN 'T' THEN 'Touring'
    ELSE 'n/a'
  END,
  CAST(prd_start_dt AS DATE),
  -- Calculate End Date by looking at the Next Start Date for the same product
  CAST(
     DATE_SUB(
        LEAD(prd_start_dt) OVER (PARTITION BY SUBSTRING(prd_key, 7) ORDER BY prd_start_dt), 
        INTERVAL 1 DAY
     ) AS DATE
  )
FROM bronze.crm_prd_info;

SELECT COUNT(*) AS rows_loaded_prd FROM silver.crm_prd_info;


-- ==============================================================================
-- 3. crm_sales_details 
-- Changes: Parse Dates, Recalculate Invalid Sales, Backfill Price
-- ==============================================================================
CREATE TABLE IF NOT EXISTS crm_sales_details (
  sls_ord_num VARCHAR(50),
  sls_prd_key VARCHAR(50),
  sls_cust_id INT,
  sls_order_dt DATE,
  sls_ship_dt DATE,
  sls_due_dt DATE,
  sls_sales INT,
  sls_quantity INT,
  sls_price INT,
  dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP,
  INDEX ix_sales_ord (sls_ord_num),
  INDEX ix_sales_prd (sls_prd_key),
  INDEX ix_sales_cust (sls_cust_id)
) ENGINE=InnoDB;

TRUNCATE TABLE silver.crm_sales_details;

INSERT INTO silver.crm_sales_details (
  sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt,
  sls_sales, sls_quantity, sls_price
)
SELECT 
  sls_ord_num,
  sls_prd_key,
  sls_cust_id,
  -- Fix Date Parsing: Handle 0 or invalid lengths, then cast to CHAR before parsing
  CASE WHEN sls_order_dt = 0 OR LENGTH(CAST(sls_order_dt AS CHAR)) != 8 THEN NULL
       ELSE STR_TO_DATE(CAST(sls_order_dt AS CHAR), '%Y%m%d') END,
  CASE WHEN sls_ship_dt = 0 OR LENGTH(CAST(sls_ship_dt AS CHAR)) != 8 THEN NULL
       ELSE STR_TO_DATE(CAST(sls_ship_dt AS CHAR), '%Y%m%d') END,
  CASE WHEN sls_due_dt = 0 OR LENGTH(CAST(sls_due_dt AS CHAR)) != 8 THEN NULL
       ELSE STR_TO_DATE(CAST(sls_due_dt AS CHAR), '%Y%m%d') END,
  -- Fix Sales Calculation: Recalculate if NULL, Zero, or doesn't match Qty*Price
  CASE 
    WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
      THEN sls_quantity * ABS(sls_price)
    ELSE sls_sales
  END,
  sls_quantity,
  -- Fix Price: Backfill if NULL
  CASE 
    WHEN sls_price IS NULL OR sls_price <= 0 
      THEN NULLIF(sls_sales,0) / NULLIF(sls_quantity,0)
    ELSE sls_price
  END
FROM bronze.crm_sales_details;

SELECT COUNT(*) AS rows_loaded_sales FROM silver.crm_sales_details;


-- ==============================================================================
-- 4. erp_cust_az12 
-- Changes: Remove 'NAS' prefix; Fix Birthdates; Normalize Gender
-- ==============================================================================
CREATE TABLE IF NOT EXISTS erp_cust_az12 (
  cid VARCHAR(50),
  bdate DATE,
  gen VARCHAR(50),
  dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP,
  INDEX ix_custaz_cid (cid)
) ENGINE=InnoDB;

TRUNCATE TABLE silver.erp_cust_az12;

INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
SELECT
  -- Correction: Changed 'NAS%%' to 'NAS%'
  CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4) ELSE cid END, 
  CASE WHEN bdate > CURRENT_DATE THEN NULL ELSE bdate END,
  CASE
     WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
     WHEN UPPER(TRIM(gen)) IN ('M','MALE')   THEN 'Male'
     ELSE 'n/a'
  END
FROM bronze.erp_cust_az12;

SELECT COUNT(*) AS rows_loaded_az12 FROM silver.erp_cust_az12;


-- ==============================================================================
-- 5. erp_loc_a101 
-- Changes: Remove dashes from ID; Normalize Country names
-- ==============================================================================
CREATE TABLE IF NOT EXISTS erp_loc_a101 (
  cid VARCHAR(50),
  cntry VARCHAR(50),
  dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP,
  INDEX ix_loc_cid (cid)
) ENGINE=InnoDB;

TRUNCATE TABLE silver.erp_loc_a101;

INSERT INTO silver.erp_loc_a101 (cid, cntry)
SELECT
  REPLACE(cid, '-', ''),
  CASE
     WHEN TRIM(cntry) = 'DE' THEN 'Germany'
     WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
     WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
     ELSE TRIM(cntry)
  END
FROM bronze.erp_loc_a101;

SELECT COUNT(*) AS rows_loaded_loc FROM silver.erp_loc_a101;


-- ==============================================================================
-- 6. erp_px_cat_g1v2 
-- Changes: Direct copy
-- ==============================================================================
CREATE TABLE IF NOT EXISTS erp_px_cat_g1v2 (
  id VARCHAR(50),
  cat VARCHAR(50),
  subcat VARCHAR(50),
  maintenance VARCHAR(50),
  dwh_create_date DATETIME DEFAULT CURRENT_TIMESTAMP,
  INDEX ix_cat_id (id)
) ENGINE=InnoDB;

TRUNCATE TABLE silver.erp_px_cat_g1v2;

INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
SELECT id, cat, subcat, maintenance
FROM bronze.erp_px_cat_g1v2;

SELECT COUNT(*) AS rows_loaded_cat FROM silver.erp_px_cat_g1v2;